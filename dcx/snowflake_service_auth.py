"""Snowflake service profile resolution for API endpoints.

Profiles are loaded server-side so API callers never send Snowflake secrets
over HTTP.

Environment variables:
- `DCX_VAULT_ADDR` (or `VAULT_ADDR`)
- `DCX_VAULT_TOKEN` (or `VAULT_TOKEN`)
- `DCX_VAULT_KV_MOUNT` (default: `secret`)
- `DCX_SNOWFLAKE_SERVICE_PROFILE_PREFIX`
  (default: `dcx/snowflake/service-users`)
- `DCX_SNOWFLAKE_SERVICE_PROFILE_SOURCE`
    (`vault`/`env`/`file`/`auto`; default: `vault`)
- `DCX_SNOWFLAKE_SERVICE_PROFILE_ENV_PREFIX`
    (default: `DCX_SNOWFLAKE_PROFILE_`)
- `DCX_SNOWFLAKE_SERVICE_PROFILE_DIR`
    (default: `/var/run/secrets/dcx/snowflake`)

A profile document should contain Snowflake connector kwargs, for example:

    {
      "account": "xy12345.eu-west-1",
      "user": "svc_dcx",
      "authenticator": "snowflake",
      "password": "...",
      "role": "DCX_APPLY",
      "warehouse": "COMPUTE_WH"
    }
"""

from __future__ import annotations

import json
import os
from enum import Enum
from pathlib import Path
from typing import Any


class ServiceProfileError(Exception):
    """Service profile cannot be loaded or is invalid."""


_DEFAULT_MOUNT = "secret"
_DEFAULT_PREFIX = "dcx/snowflake/service-users"
_DEFAULT_SOURCE = "vault"
_DEFAULT_ENV_PREFIX = "DCX_SNOWFLAKE_PROFILE_"
_DEFAULT_PROFILE_DIR = "/var/run/secrets/dcx/snowflake"

# Allow only known Snowflake connector fields from Vault.
_ALLOWED_CONNECT_KWARGS = {
    "account",
    "user",
    "password",
    "role",
    "warehouse",
    "database",
    "schema",
    "authenticator",
    "private_key_file",
    "private_key_file_pwd",
    "token",
}


class ServiceProfileSource(str, Enum):
    auto = "auto"
    vault = "vault"
    env = "env"
    file = "file"


def _source_from_input(
    source: str | ServiceProfileSource | None,
) -> ServiceProfileSource:
    value = str(source).strip().lower() if source else ""
    if "." in value:
        # Be tolerant of enum-like string values from some clients,
        # e.g. "ServiceProfileSource.env".
        value = value.rsplit(".", 1)[-1]
    if not value:
        value = (
            os.environ.get("DCX_SNOWFLAKE_SERVICE_PROFILE_SOURCE", _DEFAULT_SOURCE)
            .strip()
            .lower()
        )
    try:
        return ServiceProfileSource(value)
    except ValueError:
        allowed = ", ".join(s.value for s in ServiceProfileSource)
        raise ServiceProfileError(
            f"Invalid service profile source '{value}'. Expected one of: {allowed}."
        )


def _env_var_key(profile: str, field: str) -> str:
    prefix = os.environ.get(
        "DCX_SNOWFLAKE_SERVICE_PROFILE_ENV_PREFIX", _DEFAULT_ENV_PREFIX
    )
    p = profile.strip().upper().replace("-", "_")
    f = field.strip().upper()
    return f"{prefix}{p}_{f}"


def _read_profile_from_env(profile: str) -> dict[str, Any]:
    conn_kwargs: dict[str, Any] = {}
    for field in _ALLOWED_CONNECT_KWARGS:
        value = os.environ.get(_env_var_key(profile, field))
        if value is not None and value != "":
            conn_kwargs[field] = value
    return conn_kwargs


def _read_profile_from_file(profile: str) -> dict[str, Any]:
    base_dir = os.environ.get("DCX_SNOWFLAKE_SERVICE_PROFILE_DIR", _DEFAULT_PROFILE_DIR)
    profile_name = profile.strip()
    for suffix in (".json", ".yaml", ".yml"):
        path = Path(base_dir) / f"{profile_name}{suffix}"
        if not path.exists():
            continue

        try:
            raw = path.read_text(encoding="utf-8")
        except Exception as exc:
            raise ServiceProfileError(f"Could not read profile file '{path}': {exc}")

        if suffix == ".json":
            try:
                data = json.loads(raw)
            except Exception as exc:
                raise ServiceProfileError(
                    f"Invalid JSON in profile file '{path}': {exc}"
                )
        else:
            try:
                import yaml as yamllib
            except ImportError:
                raise ServiceProfileError(
                    "PyYAML is required to load YAML profile files."
                )
            try:
                data = yamllib.safe_load(raw)
            except Exception as exc:
                raise ServiceProfileError(
                    f"Invalid YAML in profile file '{path}': {exc}"
                )

        if isinstance(data, dict):
            return data
        raise ServiceProfileError(f"Profile file '{path}' must contain an object/map.")

    return {}


def _validate_conn_kwargs(
    profile: str, conn_kwargs: dict[str, Any], *, source_label: str
) -> dict[str, Any]:
    cleaned = {
        k: conn_kwargs[k]
        for k in _ALLOWED_CONNECT_KWARGS
        if conn_kwargs.get(k) is not None
    }
    print(f"Validated {source_label} profile '{profile}': {cleaned}")
    if not cleaned.get("account"):
        raise ServiceProfileError(
            f"{source_label} profile '{profile}' is missing required field 'account'."
        )
    if not cleaned.get("user") and cleaned.get("authenticator") != "oauth":
        raise ServiceProfileError(
            f"{source_label} profile '{profile}' is missing required field 'user'."
        )

    has_secret = any(cleaned.get(k) for k in ("password", "private_key_file", "token"))
    if not has_secret:
        raise ServiceProfileError(
            f"{source_label} profile '{profile}' must define one secret auth field: "
            "password, private_key_file, or token."
        )
    return cleaned


def _vault_settings() -> tuple[str, str, str, str]:
    addr = os.environ.get("DCX_VAULT_ADDR") or os.environ.get("VAULT_ADDR")
    token = os.environ.get("DCX_VAULT_TOKEN") or os.environ.get("VAULT_TOKEN")
    mount = os.environ.get("DCX_VAULT_KV_MOUNT", _DEFAULT_MOUNT)
    prefix = os.environ.get(
        "DCX_SNOWFLAKE_SERVICE_PROFILE_PREFIX", _DEFAULT_PREFIX
    ).strip("/")

    if not addr:
        raise ServiceProfileError(
            "Vault is not configured: set DCX_VAULT_ADDR (or VAULT_ADDR)."
        )
    if not token:
        raise ServiceProfileError(
            "Vault token is missing: set DCX_VAULT_TOKEN (or VAULT_TOKEN)."
        )
    return addr, token, mount, prefix


def _read_profile_from_vault(profile: str) -> dict[str, Any]:
    try:
        import hvac
    except ImportError:
        raise ServiceProfileError(
            "Vault client dependency is missing: install `hvac` to use service profiles."
        )

    addr, token, mount, prefix = _vault_settings()
    path = f"{prefix}/{profile}".strip("/")

    client = hvac.Client(url=addr, token=token)
    if not client.is_authenticated():
        raise ServiceProfileError(
            "Vault authentication failed with the configured token."
        )

    # Try KV v2 first, then KV v1.
    try:
        payload = client.secrets.kv.v2.read_secret_version(path=path, mount_point=mount)
        data = payload.get("data", {}).get("data", {})
        if isinstance(data, dict):
            return data
    except Exception:
        pass

    try:
        payload = client.secrets.kv.v1.read_secret(path=path, mount_point=mount)
        data = payload.get("data", {})
        if isinstance(data, dict):
            return data
    except Exception as exc:
        raise ServiceProfileError(
            f"Could not load Vault profile '{profile}' from {mount}/{path}: {exc}"
        )

    raise ServiceProfileError(
        f"Vault profile '{profile}' exists but is empty or malformed."
    )


def load_snowflake_service_profile(
    profile: str,
    *,
    source: str | ServiceProfileSource | None = None,
) -> dict[str, Any]:
    """Load and validate a Snowflake service profile.

    Sources:
    - `vault` (HashiCorp Vault)
    - `env` (environment variables)
    - `file` (profile files in `DCX_SNOWFLAKE_SERVICE_PROFILE_DIR`)
    - `auto` (env -> file -> vault)

    Returns Snowflake connector kwargs.
    """
    if not profile or not profile.strip():
        raise ServiceProfileError("A non-empty service profile name is required.")
    normalized = profile.strip()
    resolved_source = _source_from_input(source)

    if resolved_source == ServiceProfileSource.env:
        raw = _read_profile_from_env(normalized)
        return _validate_conn_kwargs(normalized, raw, source_label="Env")

    if resolved_source == ServiceProfileSource.file:
        raw = _read_profile_from_file(normalized)
        return _validate_conn_kwargs(normalized, raw, source_label="File")

    if resolved_source == ServiceProfileSource.vault:
        raw = _read_profile_from_vault(normalized)
        return _validate_conn_kwargs(normalized, raw, source_label="Vault")

    # auto: prefer env or file when the profile is explicitly present there,
    # then fall back to Vault.
    env_raw = _read_profile_from_env(normalized)
    if env_raw:
        return _validate_conn_kwargs(normalized, env_raw, source_label="Env")

    file_raw = _read_profile_from_file(normalized)
    if file_raw:
        return _validate_conn_kwargs(normalized, file_raw, source_label="File")

    raw = _read_profile_from_vault(normalized)
    return _validate_conn_kwargs(normalized, raw, source_label="Vault")
