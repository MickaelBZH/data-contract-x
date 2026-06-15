import textwrap

import pytest
from open_data_contract_standard.model import OpenDataContractStandard
from typer.testing import CliRunner

from dcx.apply.snowflake import ApplyError, _resolve_connection_params
from dcx.cli import app

runner = CliRunner()


CONTRACT_YAML = textwrap.dedent(
    """\
    apiVersion: v3.1.0
    kind: DataContract
    id: orders
    name: Orders
    version: 1.0.0
    status: draft
    servers:
      - server: prod
        type: snowflake
        account: ENTERPRISE
        database: MY_DB
        schema: LOAD
        warehouse: PROD_DP_WH
    schema:
      - name: orders
        physicalType: table
        properties:
          - name: id
            logicalType: integer
            physicalType: NUMBER
            primaryKey: true
    """
)


def _contract():
    return OpenDataContractStandard.from_string(CONTRACT_YAML)


# === Connection param resolution ============================================


def test_resolve_uses_contract_server_block_by_default(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_USER", "me")
    monkeypatch.delenv("SNOWFLAKE_ACCOUNT", raising=False)
    params = _resolve_connection_params(_contract())
    assert params["account"] == "ENTERPRISE"
    assert params["database"] == "MY_DB"
    assert params["schema"] == "LOAD"
    assert params["warehouse"] == "PROD_DP_WH"
    assert params["user"] == "me"


def test_env_var_overrides_contract(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_USER", "me")
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "ANOTHER-ACCT")
    params = _resolve_connection_params(_contract())
    assert params["account"] == "ANOTHER-ACCT"


def test_cli_flag_overrides_env(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_USER", "me")
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "FROM_ENV")
    params = _resolve_connection_params(_contract(), account="FROM_CLI")
    assert params["account"] == "FROM_CLI"


def test_password_read_from_env_only(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_USER", "me")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "s3cret")
    params = _resolve_connection_params(_contract())
    assert params["password"] == "s3cret"


def test_private_key_path_from_env(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_USER", "me")
    monkeypatch.setenv("SNOWFLAKE_PRIVATE_KEY_PATH", "/keys/svc.pk8")
    monkeypatch.setenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "p4ss")
    params = _resolve_connection_params(_contract())
    assert params["private_key_file"] == "/keys/svc.pk8"
    assert params["private_key_file_pwd"] == "p4ss"


def test_missing_user_errors(monkeypatch):
    monkeypatch.delenv("SNOWFLAKE_USER", raising=False)
    with pytest.raises(ApplyError, match="Cannot determine Snowflake user"):
        _resolve_connection_params(_contract())


def test_missing_account_errors(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_USER", "me")
    monkeypatch.delenv("SNOWFLAKE_ACCOUNT", raising=False)
    # Contract without an account
    yaml = textwrap.dedent(
        """\
        apiVersion: v3.1.0
        kind: DataContract
        id: x
        name: X
        version: 1.0.0
        status: draft
        schema:
          - name: t
            physicalType: table
            properties:
              - name: a
                logicalType: integer
        """
    )
    contract = OpenDataContractStandard.from_string(yaml)
    with pytest.raises(ApplyError, match="Cannot determine Snowflake account"):
        _resolve_connection_params(contract)


# === Dry-run via CLI ========================================================


def test_dry_run_prints_sql_no_connection(tmp_path):
    contract_path = tmp_path / "datacontract.yaml"
    contract_path.write_text(CONTRACT_YAML)
    result = runner.invoke(app, [
        "apply", "snowflake", str(contract_path), "--dry-run", "--ddl-mode", "always",
    ])
    assert result.exit_code == 0, result.output
    assert "CREATE TABLE MY_DB.LOAD.orders" in result.output


def test_dry_run_with_tags_and_quality(tmp_path):
    contract_yaml = textwrap.dedent(
        """\
        apiVersion: v3.1.0
        kind: DataContract
        id: orders
        name: Orders
        version: 1.0.0
        status: draft
        servers:
          - server: prod
            type: snowflake
            account: ENTERPRISE
            database: MY_DB
            schema: LOAD
        schema:
          - name: orders
            physicalType: table
            properties:
              - name: id
                logicalType: integer
                physicalType: NUMBER
                primaryKey: true
                tags:
                  - DATA_CLASSIFICATION=PD_DATA
        """
    )
    contract_path = tmp_path / "datacontract.yaml"
    contract_path.write_text(contract_yaml)
    result = runner.invoke(app, [
        "apply", "snowflake", str(contract_path), "--dry-run",
        "--tag-namespace", "GOV.TAGS",
    ])
    assert result.exit_code == 0, result.output
    assert "ALTER TABLE MY_DB.LOAD.orders" in result.output
    assert "GOV.TAGS.DATA_CLASSIFICATION" in result.output


# === Real execution via mocked connector ====================================


class _MockCursor:
    def __init__(self, sentinel="ok"):
        self.sentinel = sentinel
        self.closed = False

    def close(self):
        self.closed = True


class _MockConn:
    def __init__(self, parsed_statements):
        self._parsed = parsed_statements
        self.closed = False

    def execute_string(self, sql):
        # Mirrors `SnowflakeConnection.execute_string`: parse `;`-separated
        # statements and return one cursor per statement.
        self._parsed["captured_sql"] = sql
        stmts = [s for s in (s.strip() for s in sql.split(";")) if s and not s.startswith("--")]
        return [_MockCursor() for _ in stmts]

    def close(self):
        self.closed = True


@pytest.fixture
def mock_snowflake_connector(monkeypatch):
    """Patch snowflake.connector.connect to capture its kwargs and return a fake conn."""
    state: dict = {"connect_kwargs": None, "captured_sql": None}

    def fake_connect(**kwargs):
        state["connect_kwargs"] = kwargs
        return _MockConn(state)

    import snowflake.connector as _connector_module
    monkeypatch.setattr(_connector_module, "connect", fake_connect)
    return state


def test_quiet_aws_credential_noise_lowers_botocore_logger():
    import logging
    from dcx.apply.snowflake import quiet_aws_credential_noise

    log = logging.getLogger("botocore.credentials")
    log.setLevel(logging.WARNING)
    quiet_aws_credential_noise()
    assert log.level == logging.ERROR


def test_connect_path_quiets_botocore_noise(tmp_path, mock_snowflake_connector, monkeypatch):
    """The Snowflake connect path silences botocore's SSO refresh noise — covering the
    API/apply paths the CLI-only command suppression used to miss."""
    import logging

    logging.getLogger("botocore.credentials").setLevel(logging.WARNING)
    monkeypatch.setenv("SNOWFLAKE_USER", "me")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "s3cret")
    contract_path = tmp_path / "datacontract.yaml"
    contract_path.write_text(CONTRACT_YAML)

    result = runner.invoke(app, ["apply", "snowflake", str(contract_path), "--ddl-mode", "always"])
    assert result.exit_code == 0, result.output
    assert logging.getLogger("botocore.credentials").level == logging.ERROR


def test_apply_executes_against_connector(tmp_path, mock_snowflake_connector, monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_USER", "me")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "s3cret")
    contract_path = tmp_path / "datacontract.yaml"
    contract_path.write_text(CONTRACT_YAML)

    result = runner.invoke(app, [
        "apply", "snowflake", str(contract_path),
        "--role", "DEPLOYER", "--ddl-mode", "always",
    ])
    assert result.exit_code == 0, result.output

    # Connector was called with the right kwargs
    kwargs = mock_snowflake_connector["connect_kwargs"]
    assert kwargs is not None
    assert kwargs["user"] == "me"
    assert kwargs["account"] == "ENTERPRISE"
    assert kwargs["password"] == "s3cret"
    assert kwargs["role"] == "DEPLOYER"
    assert kwargs["database"] == "MY_DB"

    # The SQL was sent to execute_string (--ddl-mode always → plain CREATE TABLE)
    assert "CREATE TABLE MY_DB.LOAD.orders" in mock_snowflake_connector["captured_sql"]

    # Summary line printed to stderr
    assert "Applied" in result.output
    assert "ENTERPRISE" in result.output


def test_apply_propagates_connector_error(tmp_path, monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_USER", "me")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "s3cret")
    contract_path = tmp_path / "datacontract.yaml"
    contract_path.write_text(CONTRACT_YAML)

    def fake_connect(**_kwargs):
        raise RuntimeError("DNS resolution failed for ENTERPRISE.snowflakecomputing.com")

    import snowflake.connector as _connector_module
    monkeypatch.setattr(_connector_module, "connect", fake_connect)

    result = runner.invoke(app, [
        "apply", "snowflake", str(contract_path),
    ])
    assert result.exit_code == 1
    assert "Snowflake connection failed" in result.output
    assert "DNS resolution failed" in result.output


def test_no_password_cli_flag_exists(tmp_path):
    """`--password` must not be a real flag — passing it should error."""
    contract_path = tmp_path / "datacontract.yaml"
    contract_path.write_text(CONTRACT_YAML)
    result = runner.invoke(app, [
        "apply", "snowflake", str(contract_path),
        "--password", "anything",
    ])
    assert result.exit_code != 0
    # Click reports unknown options with "No such option" / "Got unexpected"
    assert "password" in result.output.lower()


def test_apply_command_listed_in_dcx_commands():
    """'apply' must be in DCX_COMMANDS so the migration shim doesn't munge our flags."""
    from dcx.cli import DCX_COMMANDS
    assert "apply" in DCX_COMMANDS


_CONTRACT_WITH_TAG = textwrap.dedent(
    """\
    apiVersion: v3.1.0
    kind: DataContract
    id: orders
    name: Orders
    version: 1.0.0
    status: draft
    servers:
      - server: prod
        type: snowflake
        account: ENTERPRISE
        database: MY_DB
        schema: LOAD
    schema:
      - name: orders
        physicalType: table
        properties:
          - name: id
            logicalType: integer
            physicalType: NUMBER
            primaryKey: true
            tags:
              - DATA_CLASSIFICATION=PD_DATA
    """
)


# === Alter-only mode (--no-ddl) =============================================


def test_no_ddl_dry_run_omits_create_table(tmp_path):
    contract_path = tmp_path / "datacontract.yaml"
    contract_path.write_text(CONTRACT_YAML)
    result = runner.invoke(app, [
        "apply", "snowflake", str(contract_path), "--dry-run", "--ddl-mode", "never",
    ])
    assert result.exit_code == 0, result.output
    assert "CREATE TABLE" not in result.output


def test_alter_only_emits_comments_for_existing_table(tmp_path):
    """`--ddl-mode never` sets COMMENT ON for descriptions, no CREATE TABLE."""
    contract_yaml = textwrap.dedent(
        """\
        apiVersion: v3.1.0
        kind: DataContract
        id: orders
        name: Orders
        version: 1.0.0
        status: draft
        servers:
          - server: prod
            type: snowflake
            account: ENTERPRISE
            database: MY_DB
            schema: LOAD
            warehouse: PROD_DP_WH
        schema:
          - name: orders
            physicalType: table
            description: One row per order.
            properties:
              - name: id
                logicalType: integer
                physicalType: NUMBER
                primaryKey: true
                description: Surrogate key for the order.
        """
    )
    contract_path = tmp_path / "datacontract.yaml"
    contract_path.write_text(contract_yaml)
    result = runner.invoke(app, ["apply", "snowflake", str(contract_path), "--dry-run", "--ddl-mode", "never"])
    assert result.exit_code == 0, result.output
    assert "CREATE TABLE" not in result.output
    assert "COMMENT ON TABLE MY_DB.LOAD.orders IS 'One row per order.'" in result.output
    assert "COMMENT ON COLUMN MY_DB.LOAD.orders.id IS 'Surrogate key for the order.'" in result.output


def test_no_comments_flag_suppresses_comment_sql(tmp_path):
    contract_yaml = textwrap.dedent(
        """\
        apiVersion: v3.1.0
        kind: DataContract
        id: orders
        name: Orders
        version: 1.0.0
        status: draft
        servers:
          - server: prod
            type: snowflake
            account: ENTERPRISE
            database: MY_DB
            schema: LOAD
        schema:
          - name: orders
            physicalType: table
            description: One row per order.
            properties:
              - name: id
                logicalType: integer
                physicalType: NUMBER
                description: Surrogate key.
        """
    )
    contract_path = tmp_path / "datacontract.yaml"
    contract_path.write_text(contract_yaml)
    result = runner.invoke(app, [
        "apply", "snowflake", str(contract_path), "--dry-run", "--ddl-mode", "never", "--no-comments",
    ])
    assert result.exit_code == 0, result.output
    assert "COMMENT ON" not in result.output


def test_no_ddl_keeps_tags(tmp_path):
    contract_path = tmp_path / "datacontract.yaml"
    contract_path.write_text(_CONTRACT_WITH_TAG)
    result = runner.invoke(app, [
        "apply", "snowflake", str(contract_path), "--dry-run", "--ddl-mode", "never",
        "--tag-namespace", "GOV.TAGS",
    ])
    assert result.exit_code == 0, result.output
    assert "CREATE TABLE" not in result.output
    assert "SET TAG" in result.output.upper() or "GOV.TAGS" in result.output


# === Schema drift detection =================================================


def test_detect_drift_reports_missing_extra_and_type_mismatch():
    from dcx.apply.snowflake import _detect_drift

    contract = OpenDataContractStandard.from_string(textwrap.dedent(
        """\
        apiVersion: v3.1.0
        kind: DataContract
        id: orders
        name: Orders
        version: 1.0.0
        schema:
          - name: ORDERS
            properties:
              - name: id
                physicalType: NUMBER
              - name: amount
                physicalType: NUMBER
              - name: customer_id
                physicalType: NUMBER
        """
    ))

    class _Cur:
        # DESCRIBE TABLE result header: first two columns are `name`, `type`.
        description = [("name",), ("type",), ("kind",)]

        def execute(self, *a):
            pass

        def fetchall(self):
            # Live table: ID matches, AMOUNT is TEXT (mismatch), LEGACY is extra,
            # CUSTOMER_ID is absent (missing).
            return [
                ("ID", "NUMBER(38,0)", "COLUMN"),
                ("AMOUNT", "TEXT", "COLUMN"),
                ("LEGACY", "TEXT", "COLUMN"),
            ]

        def close(self):
            pass

    class _Conn:
        def cursor(self):
            return _Cur()

    warnings = _detect_drift(_Conn(), contract, "DB", "LOAD")
    joined = " ".join(warnings)
    assert "CUSTOMER_ID" in joined and "not in the Snowflake table" in joined  # missing
    assert "LEGACY" in joined and "not in the contract" in joined              # extra
    assert "AMOUNT" in joined and "differs" in joined                          # type mismatch


def test_detect_drift_skips_nonexistent_table():
    from dcx.apply.snowflake import _detect_drift

    contract = OpenDataContractStandard.from_string(textwrap.dedent(
        """\
        apiVersion: v3.1.0
        kind: DataContract
        id: orders
        name: Orders
        version: 1.0.0
        schema:
          - name: NEW_TABLE
            properties:
              - name: id
                physicalType: NUMBER
        """
    ))

    class _Cur:
        description = [("name",), ("type",)]

        def execute(self, *a):
            raise RuntimeError("Object 'DB.LOAD.NEW_TABLE' does not exist or not authorized.")

        def fetchall(self):
            return []

        def close(self):
            pass

    class _Conn:
        def cursor(self):
            return _Cur()

    assert _detect_drift(_Conn(), contract, "DB", "LOAD") == []


# === OAuth apply (API path) =================================================


def test_apply_oauth_auto_creates_if_not_exists_by_default(mock_snowflake_connector):
    from dcx.apply.snowflake import apply_snowflake_oauth
    result = apply_snowflake_oauth(_contract(), token="tok123")
    assert result["dry_run"] is False
    kwargs = mock_snowflake_connector["connect_kwargs"]
    assert kwargs["authenticator"] == "oauth"
    assert kwargs["token"] == "tok123"
    assert kwargs["account"] == "ENTERPRISE"   # from contract server block
    assert "password" not in kwargs
    # auto default → create-if-missing + govern existing
    assert "CREATE TABLE IF NOT EXISTS" in mock_snowflake_connector["captured_sql"]


def test_apply_oauth_dry_run_needs_no_token():
    from dcx.apply.snowflake import apply_snowflake_oauth
    result = apply_snowflake_oauth(_contract(), token="", dry_run=True)
    assert result["dry_run"] is True
    assert "CREATE TABLE IF NOT EXISTS" in result["sql"]   # auto default
    assert result["account"] == "ENTERPRISE"


def test_apply_oauth_execute_requires_token():
    from dcx.apply.snowflake import apply_snowflake_oauth, ApplyError
    with pytest.raises(ApplyError, match="OAuth token is required"):
        apply_snowflake_oauth(_contract(), token="", dry_run=False)


# === API endpoint ===========================================================


def _api_client():
    from fastapi.testclient import TestClient
    from dcx.api import build_dcx_api_app
    return TestClient(build_dcx_api_app())


_API_CONTRACT = {
    "apiVersion": "v3.1.0", "kind": "DataContract", "id": "orders", "name": "Orders",
    "version": "1.0.0",
    "servers": [{"server": "prod", "type": "snowflake", "account": "ACME",
                 "database": "DB", "schema": "LOAD"}],
    "schema": [{"name": "orders", "properties": [
        {"name": "id", "logicalType": "integer", "tags": ["DATA_CLASSIFICATION=PD_DATA"]},
    ]}],
}


def test_api_apply_dry_run_no_token_returns_sql():
    r = _api_client().post(
        "/apply/snowflake", json={"contract": _API_CONTRACT, "options": {"dry_run": True}},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["dry_run"] is True
    assert "CREATE TABLE IF NOT EXISTS" in body["sql"]      # auto default
    assert body["statements_executed"] == 0


def test_api_apply_tag_namespace_filter():
    contract = {
        "apiVersion": "v3.1.0", "kind": "DataContract", "id": "o", "name": "O", "version": "1.0.0",
        "servers": [{"server": "p", "type": "snowflake", "account": "ACME", "database": "DB", "schema": "LOAD"}],
        "schema": [{"name": "orders", "properties": [
            {"name": "id", "logicalType": "integer",
             "tags": ["GOV.TAGS.CLASS=A", "CORP.GLOBAL.SRC=x"]},
        ]}],
    }
    r = _api_client().post(
        "/apply/snowflake",
        json={"contract": contract, "options": {"dry_run": True, "tag_namespace_filter": ["GOV.TAGS"]}},
    )
    assert r.status_code == 200, r.text
    sql = r.json()["sql"]
    assert "GOV.TAGS.CLASS" in sql
    assert "CORP.GLOBAL" not in sql


def test_api_apply_execute_requires_token():
    r = _api_client().post("/apply/snowflake", json={"contract": _API_CONTRACT, "options": {}})
    assert r.status_code == 401
    assert "Bearer" in r.json()["detail"]


def test_api_apply_executes_with_token(monkeypatch):
    import dcx.apply.snowflake as apply_module
    captured = {}

    def fake(contract, **kw):
        captured.update(kw)
        return {"dry_run": False, "sql": "ALTER TABLE ...;", "statements_executed": 1, "account": "ACME"}

    monkeypatch.setattr(apply_module, "apply_snowflake_oauth", fake)
    r = _api_client().post(
        "/apply/snowflake",
        headers={"Authorization": "Bearer tok-xyz"},
        json={"contract": _API_CONTRACT, "options": {"include_quality": False}},
    )
    assert r.status_code == 200, r.text
    assert captured["token"] == "tok-xyz"
    assert captured["ddl_mode"] == apply_module.DdlMode.auto   # auto default
    assert captured["include_quality"] is False
    assert r.json()["statements_executed"] == 1


def test_api_apply_error_is_502(monkeypatch):
    import dcx.apply.snowflake as apply_module

    def boom(contract, **kw):
        raise apply_module.ApplyError("Snowflake connection failed: bad token")

    monkeypatch.setattr(apply_module, "apply_snowflake_oauth", boom)
    r = _api_client().post(
        "/apply/snowflake",
        headers={"Authorization": "Bearer tok"},
        json={"contract": _API_CONTRACT, "options": {}},
    )
    assert r.status_code == 502
    assert "bad token" in r.json()["detail"]
