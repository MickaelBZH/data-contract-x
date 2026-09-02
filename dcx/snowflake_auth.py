"""Caller-supplied Snowflake credentials for the REST API.

The API endpoints that touch a live Snowflake (`POST /import/snowflake`,
`POST /apply/snowflake`) do not rely on ambient server credentials: each request
carries its own `auth` block, so the server acts on behalf of the caller rather
than with one shared identity. This module defines that block and translates it
into `snowflake.connector.connect()` kwargs.

Four methods:

- `oauth` — a bearer token. Also accepted via the `Authorization` header, which
  is the original (and still supported) shape of these endpoints.
- `key_pair` — user + private key (+ optional passphrase). The key travels as
  PEM text or base64-encoded DER; we normalise it to DER here because the
  connector's `private_key` accepts DER bytes / base64-DER, *not* PEM, and has
  no passphrase parameter of its own.
- `password` — user + password.
- `config` — a named profile from the server's own
  connection config (`config.toml`/`connections.toml`, located by the connector:
  `$SNOWFLAKE_HOME` or `~/.snowflake/` when it exists, else the platform config
  dir such as `~/.config/snowflake/`). This one reads credentials off the host's
  filesystem rather than from the caller, so it is **disabled by default** and
  must be turned on at server start (`dcx api --allow-local-credentials`). On a
  shared deployment it would let any caller who can reach the port borrow the
  host's credentials; on a personal localhost server it is the safest option,
  because no secret goes over the wire at all. Only a profile *name* is
  accepted — never a path, which would hand callers a file-read primitive
  against the server.

Named profiles may use exact ``${ENV_VAR}`` values. dcx resolves those values
from a per-connection copy before calling the connector; literal profiles remain
fully connector-managed.

Secrets are `SecretStr` so they do not leak through model reprs, logs, or
FastAPI validation error echoes.
"""

import base64
import copy
import os
import re
from pathlib import Path
from typing import Any, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, SecretStr
from typing_extensions import Annotated


class SnowflakeAuthError(Exception):
    """Unusable credentials in the request (bad key material, empty token, ...)."""


class LocalCredentialsDisabled(SnowflakeAuthError):
    """`connection_name` was requested but the server was not started with it enabled."""


# Opt-in at server start. Read at request time (not import time) so that
# `dcx api --allow-local-credentials` and tests can set it late.
ALLOW_LOCAL_CREDENTIALS_ENV = "DCX_API_ALLOW_LOCAL_CREDENTIALS"

_TRUTHY = {"1", "true", "yes", "on"}


def local_credentials_allowed() -> bool:
    """True when the server was started with server-side credential loading enabled."""
    return os.environ.get(ALLOW_LOCAL_CREDENTIALS_ENV, "").strip().lower() in _TRUTHY


class _AuthBase(BaseModel):
    model_config = ConfigDict(extra="forbid")


class OAuthAuth(_AuthBase):
    """Snowflake OAuth token, equivalent to `Authorization: Bearer <token>`."""

    type: Literal["oauth"]
    token: SecretStr = Field(..., description="Snowflake OAuth access token.")


class KeyPairAuth(_AuthBase):
    """Key-pair (JWT) auth: the caller's own private key, never a server-side path."""

    type: Literal["key_pair"]
    user: str = Field(..., description="Snowflake username the key belongs to.")
    private_key: SecretStr = Field(
        ...,
        description=(
            "RSA private key as PEM text (`-----BEGIN [ENCRYPTED] PRIVATE KEY-----...`) "
            "or base64-encoded PKCS#8 DER."
        ),
    )
    private_key_passphrase: Optional[SecretStr] = Field(
        None, description="Passphrase, if the private key is encrypted."
    )


class PasswordAuth(_AuthBase):
    """Username + password on Snowflake's default authenticator."""

    type: Literal["password"]
    user: str = Field(..., description="Snowflake username.")
    password: SecretStr = Field(..., description="Snowflake password.")


class ConfigAuth(_AuthBase):
    """Credentials from the *server's* own Snowflake connection config.

    The odd one out of the union: every other type carries the caller's own
    credentials, this one uses the API host's. `type: "config"` names that source,
    rather than the field it happens to take.

    Requires the server to run with `--allow-local-credentials`; otherwise the
    request is rejected with 403.
    """

    type: Literal["config"]
    connection_name: Optional[str] = Field(
        None,
        description=(
            "Profile name in the server's Snowflake config.toml. Omit to use the "
            "server's `default_connection_name`."
        ),
    )


SnowflakeAuth = Annotated[
    Union[OAuthAuth, KeyPairAuth, PasswordAuth, ConfigAuth],
    Field(discriminator="type"),
]


def default_connection_name() -> Optional[str]:
    """The connector's *default* connection profile name, if one is configured.

    Snowflake's own config (`connections.toml`, or the `[connections]` section of
    `config.toml`) can nominate a `default_connection_name`, which the
    connector uses when `connect()` is called with no arguments at all. dcx always
    passes timeouts, so that path is never reached — we resolve the name here and
    route it through the same handling as an explicit `--connection-name`.

    Returns None when there is no config file, no such connection, or it cannot be
    read: callers then report their normal "cannot determine ..." error.
    """
    try:
        from snowflake.connector.config_manager import CONFIG_MANAGER

        name = CONFIG_MANAGER["default_connection_name"]
        if name and name in CONFIG_MANAGER["connections"]:
            return name
    except Exception:
        return None
    return None


_ENV_REFERENCE = re.compile(r"^\$\{([A-Za-z_][A-Za-z0-9_]*)\}$")


def _contains_env_reference(value: Any) -> bool:
    if isinstance(value, str):
        return _ENV_REFERENCE.fullmatch(value) is not None
    if isinstance(value, dict):
        return any(_contains_env_reference(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(_contains_env_reference(item) for item in value)
    return False


def _resolve_env_references(value: Any) -> Any:
    if isinstance(value, str):
        match = _ENV_REFERENCE.fullmatch(value)
        if match is None:
            return value
        variable_name = match.group(1)
        try:
            return os.environ[variable_name]
        except KeyError:
            raise SnowflakeAuthError(
                f"Environment variable '{variable_name}' is not defined"
            ) from None
    if isinstance(value, dict):
        return {key: _resolve_env_references(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_resolve_env_references(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_resolve_env_references(item) for item in value)
    return value


def profile_connection_kwargs(
    connection_name: str,
    overrides: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Resolve environment-backed profile values into per-connection kwargs.

    Literal profiles stay connector-managed. Profiles containing exact
    ``${ENV_VAR}`` values are copied and resolved without mutating or caching the
    connector's global configuration.
    """
    explicit = {key: value for key, value in (overrides or {}).items() if value is not None}
    legacy = {"connection_name": connection_name, **explicit}
    try:
        from snowflake.connector.config_manager import CONFIG_MANAGER

        profile = copy.deepcopy(CONFIG_MANAGER["connections"][connection_name])
    except Exception:
        return legacy

    if not _contains_env_reference(profile):
        return legacy

    resolved = _resolve_env_references(profile)
    return {**resolved, **explicit}


def _load_private_key_der(material: str, passphrase: Optional[str]) -> bytes:
    """Normalise PEM text or base64-DER to unencrypted PKCS#8 DER bytes."""
    from cryptography.hazmat.primitives import serialization

    text = (material or "").strip()
    if not text:
        raise SnowflakeAuthError("private_key is empty.")

    pwd = passphrase.encode() if passphrase else None
    if "-----BEGIN" in text:
        data, loader = text.encode(), serialization.load_pem_private_key
    else:
        try:
            data = base64.b64decode(text, validate=True)
        except Exception:
            raise SnowflakeAuthError("private_key must be PEM text or base64-encoded DER.")
        loader = serialization.load_der_private_key

    try:
        key = loader(data, password=pwd)
    except TypeError as exc:
        # cryptography signals both "encrypted key, no passphrase given" and
        # "passphrase given for an unencrypted key" as TypeError; the message is
        # descriptive and quotes no key material, so it is safe to pass through.
        raise SnowflakeAuthError(f"Could not load private_key: {exc}")
    except Exception:
        # Everything else (bad passphrase, truncated key) is reported generically:
        # the underlying exception can echo key bytes.
        raise SnowflakeAuthError(
            "Could not load private_key: malformed key material or wrong passphrase."
        )

    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def connect_kwargs(auth: Any) -> dict[str, Any]:
    """Translate a request `auth` block into `snowflake.connector.connect()` kwargs.

    Returns the credential half only; the caller adds account/database/schema/
    role/warehouse and the connection timeouts.
    """
    if isinstance(auth, OAuthAuth):
        token = auth.token.get_secret_value()
        if not token:
            raise SnowflakeAuthError("An OAuth token is required.")
        return {"authenticator": "oauth", "token": token}

    if isinstance(auth, KeyPairAuth):
        passphrase = (
            auth.private_key_passphrase.get_secret_value()
            if auth.private_key_passphrase
            else None
        )
        return {
            "user": auth.user,
            "authenticator": "SNOWFLAKE_JWT",
            "private_key": _load_private_key_der(
                auth.private_key.get_secret_value(), passphrase or None
            ),
        }

    if isinstance(auth, PasswordAuth):
        password = auth.password.get_secret_value()
        if not password:
            raise SnowflakeAuthError("A password is required.")
        return {"user": auth.user, "password": password}

    if isinstance(auth, ConfigAuth):
        # Gate first: whether a default exists is none of an unauthorised caller's
        # business, and the 403 must not depend on the server's config contents.
        if not local_credentials_allowed():
            raise LocalCredentialsDisabled(
                "Server-side credentials are disabled. Start the server with "
                "`dcx api --allow-local-credentials` to allow `connection_name` auth, "
                "or send credentials in the request `auth` block."
            )
        name = auth.connection_name or default_connection_name()
        if not name:
            raise SnowflakeAuthError(
                "No `connection_name` given and the server has no "
                "`default_connection_name` configured — name a profile explicitly."
            )
        return profile_connection_kwargs(name)

    raise SnowflakeAuthError(f"Unsupported auth type: {type(auth).__name__}")


def uses_server_config(auth: Any) -> bool:
    """True when the auth block resolves account/user from the server's own config."""
    return isinstance(auth, ConfigAuth)


# Filenames the connector recognises inside its config directory. Anything else is
# ignored, so accepting an arbitrary filename would silently do nothing.
_CONFIG_FILENAMES = ("config.toml", "connections.toml")


def resolve_snowflake_home(path: str) -> str:
    """Validate a `--snowflake-config` value and return the directory for SNOWFLAKE_HOME.

    Accepts the directory holding `config.toml` / `connections.toml`, or one of
    those files directly. Validation is not pedantry: the connector treats
    `SNOWFLAKE_HOME` as authoritative *only if the directory exists* and otherwise
    falls back to the platform config dir — so a typo would silently serve a
    different config instead of failing.
    """
    p = Path(path).expanduser()

    if p.is_dir():
        if not any((p / name).exists() for name in _CONFIG_FILENAMES):
            raise SnowflakeAuthError(
                f"{p} contains neither config.toml nor connections.toml — "
                "the connector would find no profiles there."
            )
        return str(p)

    if p.is_file():
        if p.name not in _CONFIG_FILENAMES:
            raise SnowflakeAuthError(
                f"Snowflake reads only {' or '.join(_CONFIG_FILENAMES)}, not '{p.name}'. "
                f"Rename it, or pass the directory containing it."
            )
        return str(p.parent)

    raise SnowflakeAuthError(
        f"No such file or directory: {p}. It must exist — the connector silently "
        "ignores a missing SNOWFLAKE_HOME and falls back to its default location."
    )


def apply_snowflake_home(home: str) -> None:
    """Point the connector at `home`, even if it has already been imported.

    `snowflake.connector.constants` computes `CONFIG_FILE` / `CONNECTIONS_FILE` at
    **import time** from `SNOWFLAKE_HOME`, so setting the variable afterwards is a
    no-op for an already-imported connector. dcx imports it lazily, so a fresh
    `dcx api` is fine on the env var alone — but that is an accident of import
    order, not a guarantee, and a silently-ignored `--snowflake-config` on a
    credential path is exactly the kind of quiet failure worth spending code on.

    Sets the variable (child processes from `--workers`/`--reload` inherit it) and,
    when the connector is already loaded, re-resolves its paths in place.
    """
    os.environ["SNOWFLAKE_HOME"] = home

    import sys

    if "snowflake.connector.constants" not in sys.modules:
        return  # not imported yet — it will pick up the variable on first import

    from snowflake.connector import constants
    from snowflake.connector.config_manager import CONFIG_MANAGER
    from snowflake.connector.sf_dirs import _resolve_platform_dirs

    constants.DIRS = _resolve_platform_dirs()
    constants.CONFIG_FILE = constants.DIRS.user_config_path / "config.toml"
    constants.CONNECTIONS_FILE = constants.DIRS.user_config_path / "connections.toml"

    CONFIG_MANAGER.file_path = constants.CONFIG_FILE
    CONFIG_MANAGER._slices = [
        s._replace(path=constants.CONNECTIONS_FILE) if s.section == "connections" else s
        for s in CONFIG_MANAGER._slices
    ]
    CONFIG_MANAGER.conf_file_cache = None  # drop what was read from the old path
    CONFIG_MANAGER.read_config()


def connection_error_message(exc: Exception) -> str:
    """Render a `connect()` failure, adding a hint for the tilde-path trap.

    `snowflake-connector-python` opens `private_key_file` with a bare `open()` and
    does **not** expand `~` (unlike SNOWFLAKE_HOME, stage paths and its cache), so a
    profile written with a tilde fails with a bare `[Errno 2] No such file or
    directory: '~/...'` — which reads like a missing file, not a path that was
    never resolved. dcx cannot fix the value (with `config` auth the connector
    reads the profile itself), so it explains it instead.
    """
    msg = f"Snowflake connection failed: {exc}"
    text = str(exc)
    if "No such file or directory" in text and "'~" in text:
        msg += (
            " — the path starts with `~`, which the Snowflake connector does not "
            "expand. Use an absolute path for `private_key_file` in the connection "
            "profile."
        )
    return msg
