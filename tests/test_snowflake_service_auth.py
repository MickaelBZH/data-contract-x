import json

import pytest

from dcx.snowflake_service_auth import (
    ServiceProfileError,
    load_snowflake_service_profile,
)


def test_load_profile_from_env(monkeypatch):
    monkeypatch.setenv("DCX_SNOWFLAKE_PROFILE_SVC_APPLY_ACCOUNT", "ACME")
    monkeypatch.setenv("DCX_SNOWFLAKE_PROFILE_SVC_APPLY_USER", "svc")
    monkeypatch.setenv("DCX_SNOWFLAKE_PROFILE_SVC_APPLY_PASSWORD", "pw")

    profile = load_snowflake_service_profile("svc-apply", source="env")
    assert profile["account"] == "ACME"
    assert profile["user"] == "svc"
    assert profile["password"] == "pw"


def test_load_profile_from_file_json(tmp_path, monkeypatch):
    profile_dir = tmp_path / "profiles"
    profile_dir.mkdir()
    (profile_dir / "svc-apply.json").write_text(
        json.dumps({"account": "ACME", "user": "svc", "password": "pw"}),
        encoding="utf-8",
    )
    monkeypatch.setenv("DCX_SNOWFLAKE_SERVICE_PROFILE_DIR", str(profile_dir))

    profile = load_snowflake_service_profile("svc-apply", source="file")
    assert profile["account"] == "ACME"
    assert profile["user"] == "svc"
    assert profile["password"] == "pw"


def test_auto_prefers_env_over_file(tmp_path, monkeypatch):
    profile_dir = tmp_path / "profiles"
    profile_dir.mkdir()
    (profile_dir / "svc-apply.json").write_text(
        json.dumps({"account": "FILE", "user": "svcf", "password": "pwf"}),
        encoding="utf-8",
    )
    monkeypatch.setenv("DCX_SNOWFLAKE_SERVICE_PROFILE_DIR", str(profile_dir))

    monkeypatch.setenv("DCX_SNOWFLAKE_PROFILE_SVC_APPLY_ACCOUNT", "ENV")
    monkeypatch.setenv("DCX_SNOWFLAKE_PROFILE_SVC_APPLY_USER", "svce")
    monkeypatch.setenv("DCX_SNOWFLAKE_PROFILE_SVC_APPLY_PASSWORD", "pwe")

    profile = load_snowflake_service_profile("svc-apply", source="auto")
    assert profile["account"] == "ENV"
    assert profile["user"] == "svce"


def test_env_source_missing_required_fields_raises(monkeypatch):
    monkeypatch.setenv("DCX_SNOWFLAKE_PROFILE_SVC_APPLY_USER", "svc")
    monkeypatch.setenv("DCX_SNOWFLAKE_PROFILE_SVC_APPLY_PASSWORD", "pw")

    with pytest.raises(ServiceProfileError, match="missing required field 'account'"):
        load_snowflake_service_profile("svc-apply", source="env")


def test_invalid_source_raises():
    with pytest.raises(ServiceProfileError, match="Invalid service profile source"):
        load_snowflake_service_profile("svc-apply", source="unsupported")


def test_enum_like_source_string_is_accepted(monkeypatch):
    monkeypatch.setenv("DCX_SNOWFLAKE_PROFILE_SVC_APPLY_ACCOUNT", "ACME")
    monkeypatch.setenv("DCX_SNOWFLAKE_PROFILE_SVC_APPLY_USER", "svc")
    monkeypatch.setenv("DCX_SNOWFLAKE_PROFILE_SVC_APPLY_PASSWORD", "pw")

    profile = load_snowflake_service_profile(
        "svc-apply", source="ServiceProfileSource.env"
    )
    assert profile["account"] == "ACME"
