import textwrap

import pytest
from open_data_contract_standard.model import OpenDataContractStandard
from typer.testing import CliRunner

from dcx.apply.snowflake import (
    ApplyError,
    _resolve_connection_params,
    configure_secondary_roles,
    normalize_secondary_roles,
)
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
    def __init__(self, conn):
        self._conn = conn
        self.closed = False

    def execute(self, sql):
        self._conn.record(sql)

    def fetchall(self):
        return []

    def close(self):
        self.closed = True


class _MockConn:
    """Fake connection that records every statement dcx executes.

    Deliberately does NOT implement `execute_string` — dcx splits the script itself
    and runs one statement per cursor, so a call to it would be a regression.

    `errors` maps a substring to an exception raised the first time a matching
    statement runs, for exercising the idempotent DMF paths.
    """

    def __init__(self, parsed_statements, errors=None):
        self._parsed = parsed_statements
        self.closed = False
        self.errors = dict(errors or {})
        self.cursors: list[_MockCursor] = []
        parsed_statements.setdefault("executed", [])

    def record(self, sql):
        stmt = sql.strip()
        for needle, exc in list(self.errors.items()):
            if needle.lower() in stmt.lower():
                del self.errors[needle]
                raise exc
        self._parsed["executed"].append(stmt)
        self._parsed["captured_sql"] = ";\n".join(self._parsed["executed"])

    def cursor(self):
        cur = _MockCursor(self)
        self.cursors.append(cur)
        return cur

    def close(self):
        self.closed = True


def test_configure_secondary_roles_normalizes_and_closes_cursor():
    state: dict = {"executed": []}
    conn = _MockConn(state)

    configure_secondary_roles(conn, "all")

    assert state["executed"] == ["USE SECONDARY ROLES ALL"]
    assert conn.cursors[0].closed is True


def test_configure_secondary_roles_none_is_explicit_and_unset_is_a_noop():
    state: dict = {"executed": []}
    conn = _MockConn(state)

    configure_secondary_roles(conn, None)
    configure_secondary_roles(conn, "none")

    assert state["executed"] == ["USE SECONDARY ROLES NONE"]


@pytest.mark.parametrize(("value", "expected"), [
    (" ALL ", "ALL"),
    ("none", "NONE"),
    ("DATA_READER", "DATA_READER"),
    ("DATA_READER, DATA_STEWARD", "DATA_READER,DATA_STEWARD"),
    ('"Finance Reader"', '"Finance Reader"'),
    ('"Finance ""Steward"""', '"Finance ""Steward"""'),
])
def test_normalize_secondary_roles_accepts_snowflake_syntax(value, expected):
    assert normalize_secondary_roles(value) == expected


def test_configure_secondary_roles_accepts_named_roles():
    state: dict = {"executed": []}
    conn = _MockConn(state)

    configure_secondary_roles(conn, 'DATA_READER, "Finance Reader"')

    assert state["executed"] == ['USE SECONDARY ROLES DATA_READER,"Finance Reader"']


def test_configure_secondary_roles_rejects_invalid_value_without_sql():
    state: dict = {"executed": []}
    conn = _MockConn(state)

    with pytest.raises(ValueError, match="only Snowflake role names"):
        configure_secondary_roles(conn, "NONE; DROP TABLE x")

    assert state["executed"] == []
    assert conn.cursors == []


def test_normalize_secondary_roles_rejects_non_string_values():
    with pytest.raises(ValueError, match="must be a string"):
        normalize_secondary_roles(1)


@pytest.fixture
def mock_snowflake_connector(monkeypatch):
    """Patch snowflake.connector.connect to capture its kwargs and return a fake conn."""
    state: dict = {"connect_kwargs": None, "captured_sql": None}

    def fake_connect(**kwargs):
        state["connect_kwargs"] = kwargs
        return _MockConn(state, errors=state.get("errors"))

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


def test_apply_configures_secondary_roles_before_drift_and_ddl(
    mock_snowflake_connector, monkeypatch,
):
    from dcx.apply.snowflake import apply_snowflake

    monkeypatch.setenv("SNOWFLAKE_USER", "me")
    apply_snowflake(_contract(), secondary_roles="none")

    executed = mock_snowflake_connector["executed"]
    assert executed.count("USE SECONDARY ROLES NONE") == 1
    assert executed[0] == "USE SECONDARY ROLES NONE"


def test_apply_secondary_role_failure_stops_before_drift_and_ddl(
    mock_snowflake_connector, monkeypatch,
):
    from dcx.apply.snowflake import apply_snowflake

    monkeypatch.setenv("SNOWFLAKE_USER", "me")
    mock_snowflake_connector["errors"] = {"USE SECONDARY ROLES ALL": RuntimeError("not allowed")}

    with pytest.raises(ApplyError, match="session configuration failed: not allowed"):
        apply_snowflake(_contract(), secondary_roles="ALL")

    assert mock_snowflake_connector["executed"] == []


def test_apply_rejects_invalid_secondary_roles_before_connect(mock_snowflake_connector, monkeypatch):
    from dcx.apply.snowflake import apply_snowflake

    monkeypatch.setenv("SNOWFLAKE_USER", "me")

    with pytest.raises(ApplyError, match="only Snowflake role names"):
        apply_snowflake(_contract(), secondary_roles="NONE; DROP TABLE x")

    assert mock_snowflake_connector["connect_kwargs"] is None


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


@pytest.mark.parametrize("flag", ["--database", "--schema"])
def test_no_database_or_schema_cli_flags(tmp_path, flag):
    """Dropped deliberately: the generated SQL is fully qualified from the contract's
    server block, so these could never retarget an apply — they only pointed the
    drift check at a different database than the one being written to."""
    contract_path = tmp_path / "datacontract.yaml"
    contract_path.write_text(CONTRACT_YAML)
    result = runner.invoke(app, [
        "apply", "snowflake", str(contract_path), flag, "OTHER",
    ])
    assert result.exit_code != 0
    assert "no such option" in result.output.lower()


def test_apply_targets_contract_server_block_only(mock_snowflake_connector, monkeypatch):
    """Session context follows the contract, so drift checks and DDL agree."""
    monkeypatch.delenv("SNOWFLAKE_DATABASE", raising=False)
    monkeypatch.delenv("SNOWFLAKE_SCHEMA", raising=False)
    monkeypatch.setenv("SNOWFLAKE_USER", "me")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "s3cret")
    from dcx.apply.snowflake import apply_snowflake

    apply_snowflake(_contract())
    kwargs = mock_snowflake_connector["connect_kwargs"]
    assert (kwargs["database"], kwargs["schema"]) == ("MY_DB", "LOAD")
    assert "MY_DB.LOAD.orders" in mock_snowflake_connector["captured_sql"]


def test_drift_check_ignores_diverging_session_env(mock_snowflake_connector, monkeypatch):
    """SNOWFLAKE_DATABASE must not steer the drift check away from the contract.

    The generated SQL is qualified from the contract's server block, so a drift
    check qualified from the *connection* would compare MY_DB.LOAD.orders (what we
    write) against OTHER_DB.OTHER_SCHEMA.orders (what the env points at).
    """
    monkeypatch.setenv("SNOWFLAKE_USER", "me")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "s3cret")
    monkeypatch.setenv("SNOWFLAKE_DATABASE", "OTHER_DB")
    monkeypatch.setenv("SNOWFLAKE_SCHEMA", "OTHER_SCHEMA")

    described = []
    import dcx.apply.snowflake as apply_module

    def spy(conn, contract, database, schema):
        described.append((database, schema))
        return []

    monkeypatch.setattr(apply_module, "_detect_drift", spy)
    apply_module.apply_snowflake(_contract())

    # Session context still follows the env (that is what those vars are for)...
    assert mock_snowflake_connector["connect_kwargs"]["database"] == "OTHER_DB"
    # ...but drift is checked against the contract, matching the SQL we emit.
    assert described == [("MY_DB", "LOAD")]
    assert "MY_DB.LOAD.orders" in mock_snowflake_connector["captured_sql"]


def test_drift_check_follows_named_server(mock_snowflake_connector, monkeypatch):
    """With several server blocks, --server picks the one drift compares against."""
    contract = OpenDataContractStandard.from_string(textwrap.dedent("""\
        apiVersion: v3.1.0
        kind: DataContract
        id: orders
        name: Orders
        version: 1.0.0
        servers:
          - server: prod
            type: snowflake
            account: ENTERPRISE
            database: PROD_DB
            schema: LOAD
          - server: dev
            type: snowflake
            account: ENTERPRISE
            database: DEV_DB
            schema: SANDBOX
        schema:
          - name: orders
            properties:
              - name: id
                logicalType: integer
    """))
    monkeypatch.setenv("SNOWFLAKE_USER", "me")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "s3cret")

    described = []
    import dcx.apply.snowflake as apply_module
    monkeypatch.setattr(
        apply_module, "_detect_drift",
        lambda conn, c, database, schema: described.append((database, schema)) or [],
    )
    apply_module.apply_snowflake(contract, server_name="dev")

    assert described == [("DEV_DB", "SANDBOX")]
    assert "DEV_DB.SANDBOX.orders" in mock_snowflake_connector["captured_sql"]


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


# === Apply with caller-supplied credentials (API path) ======================


def _rsa_pem(passphrase=None):
    """A throwaway RSA private key as PEM text, optionally encrypted."""
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    encryption = (
        serialization.BestAvailableEncryption(passphrase.encode())
        if passphrase
        else serialization.NoEncryption()
    )
    return key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, encryption
    ).decode()


def test_apply_api_auto_creates_if_not_exists_by_default(mock_snowflake_connector):
    from dcx.apply.snowflake import apply_snowflake_api
    from dcx.snowflake_auth import OAuthAuth

    result = apply_snowflake_api(_contract(), auth=OAuthAuth(type="oauth", token="tok123"))
    assert result["dry_run"] is False
    kwargs = mock_snowflake_connector["connect_kwargs"]
    assert kwargs["authenticator"] == "oauth"
    assert kwargs["token"] == "tok123"
    assert kwargs["account"] == "ENTERPRISE"   # from contract server block
    assert "password" not in kwargs
    # auto default → create-if-missing + govern existing
    assert "CREATE TABLE IF NOT EXISTS" in mock_snowflake_connector["captured_sql"]


def test_apply_api_key_pair_sends_der_not_pem(mock_snowflake_connector):
    from dcx.apply.snowflake import apply_snowflake_api
    from dcx.snowflake_auth import KeyPairAuth

    apply_snowflake_api(
        _contract(),
        auth=KeyPairAuth(
            type="key_pair", user="SVC", private_key=_rsa_pem(passphrase="pw"),
            private_key_passphrase="pw",
        ),
    )
    kwargs = mock_snowflake_connector["connect_kwargs"]
    assert kwargs["authenticator"] == "SNOWFLAKE_JWT"
    assert kwargs["user"] == "SVC"
    assert isinstance(kwargs["private_key"], bytes) and b"BEGIN" not in kwargs["private_key"]
    assert "private_key_file" not in kwargs      # no server-side path, ever


def test_apply_api_password(mock_snowflake_connector):
    from dcx.apply.snowflake import apply_snowflake_api
    from dcx.snowflake_auth import PasswordAuth

    apply_snowflake_api(
        _contract(), auth=PasswordAuth(type="password", user="SVC", password="hunter2"),
    )
    kwargs = mock_snowflake_connector["connect_kwargs"]
    assert kwargs["user"] == "SVC"
    assert kwargs["password"] == "hunter2"
    assert "authenticator" not in kwargs         # connector default


def test_apply_api_dry_run_needs_no_credentials():
    from dcx.apply.snowflake import apply_snowflake_api
    result = apply_snowflake_api(_contract(), auth=None, dry_run=True)
    assert result["dry_run"] is True
    assert "CREATE TABLE IF NOT EXISTS" in result["sql"]   # auto default
    assert result["account"] == "ENTERPRISE"


def test_apply_api_execute_requires_credentials():
    from dcx.apply.snowflake import apply_snowflake_api, ApplyError
    with pytest.raises(ApplyError, match="Credentials are required"):
        apply_snowflake_api(_contract(), auth=None, dry_run=False)


def test_apply_api_execute_requires_nonempty_token():
    from dcx.apply.snowflake import apply_snowflake_api
    from dcx.snowflake_auth import OAuthAuth, SnowflakeAuthError

    with pytest.raises(SnowflakeAuthError, match="OAuth token is required"):
        apply_snowflake_api(_contract(), auth=OAuthAuth(type="oauth", token=""))


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


def test_api_apply_execute_requires_credentials():
    r = _api_client().post("/apply/snowflake", json={"contract": _API_CONTRACT, "options": {}})
    assert r.status_code == 401
    assert "auth" in r.json()["detail"] and "Bearer" in r.json()["detail"]


def test_api_apply_executes_with_bearer_token(monkeypatch):
    import dcx.apply.snowflake as apply_module
    captured = {}

    def fake(contract, **kw):
        captured.update(kw)
        return {"dry_run": False, "sql": "ALTER TABLE ...;", "statements_executed": 1, "account": "ACME"}

    monkeypatch.setattr(apply_module, "apply_snowflake_api", fake)
    r = _api_client().post(
        "/apply/snowflake",
        headers={"Authorization": "Bearer tok-xyz"},
        json={
            "contract": _API_CONTRACT,
            "options": {"include_quality": False, "secondary_roles": " data_reader,DATA_STEWARD "},
        },
    )
    assert r.status_code == 200, r.text
    assert captured["auth"].token.get_secret_value() == "tok-xyz"
    assert captured["ddl_mode"] == apply_module.DdlMode.auto   # auto default
    assert captured["include_quality"] is False
    assert captured["secondary_roles"] == "data_reader,DATA_STEWARD"
    assert r.json()["statements_executed"] == 1


def test_api_apply_rejects_invalid_secondary_roles_before_core_call(monkeypatch):
    import dcx.apply.snowflake as apply_module

    called = False

    def fake(*args, **kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(apply_module, "apply_snowflake_api", fake)
    r = _api_client().post(
        "/apply/snowflake",
        headers={"Authorization": "Bearer tok-xyz"},
        json={
            "contract": _API_CONTRACT,
            "options": {"secondary_roles": "NONE; DROP TABLE x"},
        },
    )

    assert r.status_code == 422
    assert called is False


def test_api_apply_rejects_non_string_secondary_roles():
    r = _api_client().post(
        "/apply/snowflake",
        headers={"Authorization": "Bearer tok-xyz"},
        json={"contract": _API_CONTRACT, "options": {"secondary_roles": 1}},
    )

    assert r.status_code == 422


def test_api_apply_error_is_502(monkeypatch):
    import dcx.apply.snowflake as apply_module

    def boom(contract, **kw):
        raise apply_module.ApplyError("Snowflake connection failed: bad token")

    monkeypatch.setattr(apply_module, "apply_snowflake_api", boom)
    r = _api_client().post(
        "/apply/snowflake",
        headers={"Authorization": "Bearer tok"},
        json={"contract": _API_CONTRACT, "options": {}},
    )
    assert r.status_code == 502
    assert "bad token" in r.json()["detail"]


def test_api_apply_key_pair_auth(monkeypatch):
    import dcx.apply.snowflake as apply_module
    captured = {}

    def fake(contract, **kw):
        captured.update(kw)
        return {"dry_run": False, "sql": "ALTER TABLE ...;", "statements_executed": 1, "account": "ACME"}

    monkeypatch.setattr(apply_module, "apply_snowflake_api", fake)
    r = _api_client().post(
        "/apply/snowflake",
        json={
            "contract": _API_CONTRACT, "options": {},
            "auth": {"type": "key_pair", "user": "SVC", "private_key": _rsa_pem()},
        },
    )
    assert r.status_code == 200, r.text
    assert captured["auth"].user == "SVC"
    assert "BEGIN" not in repr(captured["auth"])      # SecretStr keeps it out of logs


def test_api_apply_config_auth_disabled_by_default(monkeypatch):
    """The gate must hold on the write path too, before any SQL is executed."""
    from dcx.snowflake_auth import ALLOW_LOCAL_CREDENTIALS_ENV

    monkeypatch.delenv(ALLOW_LOCAL_CREDENTIALS_ENV, raising=False)
    r = _api_client().post(
        "/apply/snowflake",
        json={
            "contract": _API_CONTRACT, "options": {},
            "auth": {"type": "config", "connection_name": "dev"},
        },
    )
    assert r.status_code == 403
    assert "--allow-local-credentials" in r.json()["detail"]


def test_api_apply_dry_run_skips_credential_checks(monkeypatch):
    """dry_run never connects, so credentials are not evaluated at all — a profile
    that a real apply would reject with 403 still returns SQL here. Consistent
    with dry_run needing no credentials in the first place."""
    from dcx.snowflake_auth import ALLOW_LOCAL_CREDENTIALS_ENV

    monkeypatch.delenv(ALLOW_LOCAL_CREDENTIALS_ENV, raising=False)
    r = _api_client().post(
        "/apply/snowflake",
        json={
            "contract": _API_CONTRACT, "options": {"dry_run": True},
            "auth": {"type": "config", "connection_name": "dev"},
        },
    )
    assert r.status_code == 200, r.text
    assert r.json()["dry_run"] is True


# === Default connection profile fallback ====================================


def test_default_profile_used_when_nothing_else_identifies_a_connection(
    mock_snowflake_connector, monkeypatch,
):
    """`~/.snowflake` already says who you are — don't demand a second copy."""
    import dcx.apply.snowflake as apply_module

    for var in ("SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_ACCOUNT"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setattr(apply_module, "default_connection_name", lambda: "my_default")

    apply_module.apply_snowflake(_contract())
    kwargs = mock_snowflake_connector["connect_kwargs"]
    assert kwargs["connection_name"] == "my_default"
    # The contract still names the account, so the profile can't cross accounts.
    assert kwargs["account"] == "ENTERPRISE"
    assert "password" not in kwargs        # the connector reads the profile itself


def test_default_profile_not_used_when_env_identifies_a_connection(
    mock_snowflake_connector, monkeypatch,
):
    import dcx.apply.snowflake as apply_module

    monkeypatch.setenv("SNOWFLAKE_USER", "me")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "s3cret")
    monkeypatch.setattr(apply_module, "default_connection_name", lambda: "my_default")

    apply_module.apply_snowflake(_contract())
    kwargs = mock_snowflake_connector["connect_kwargs"]
    assert "connection_name" not in kwargs
    assert kwargs["user"] == "me"


def test_original_error_kept_when_no_default_profile_exists(monkeypatch):
    import dcx.apply.snowflake as apply_module

    monkeypatch.delenv("SNOWFLAKE_USER", raising=False)
    monkeypatch.setattr(apply_module, "default_connection_name", lambda: None)

    with pytest.raises(ApplyError, match="Cannot determine Snowflake user"):
        apply_module.apply_snowflake(_contract())


def test_explicit_connection_name_still_wins(mock_snowflake_connector, monkeypatch):
    import dcx.apply.snowflake as apply_module

    monkeypatch.setattr(apply_module, "default_connection_name", lambda: "my_default")
    apply_module.apply_snowflake(_contract(), connection_name="dev")
    assert mock_snowflake_connector["connect_kwargs"]["connection_name"] == "dev"


def test_default_connection_name_survives_missing_config(monkeypatch):
    """No ~/.snowflake at all must yield None, not an exception."""
    from snowflake.connector.config_manager import CONFIG_MANAGER

    def boom(key):
        raise Exception("no config file")

    monkeypatch.setattr(CONFIG_MANAGER, "__getitem__", boom)
    from dcx.apply.snowflake import default_connection_name
    assert default_connection_name() is None


# === `dcx api --snowflake-config` ===========================================


def _sfhome(tmp_path, filename="config.toml"):
    d = tmp_path / "sfhome"
    d.mkdir()
    f = d / filename
    f.write_text('default_connection_name = "dev"\n[connections.dev]\naccount = "A"\n')
    f.chmod(0o600)          # Snowflake warns loudly on a world-readable config
    return d


def test_api_snowflake_config_sets_snowflake_home(tmp_path, monkeypatch):
    """The flag points the connector at the given config via its own SNOWFLAKE_HOME."""
    d = _sfhome(tmp_path)
    started = {}
    monkeypatch.setattr("uvicorn.run", lambda **kw: started.update(kw))
    monkeypatch.delenv("SNOWFLAKE_HOME", raising=False)

    result = runner.invoke(app, [
        "api", "--allow-local-credentials", "--snowflake-config", str(d),
    ])
    assert result.exit_code == 0, result.output
    assert started                                     # the server did start
    import os
    assert os.environ["SNOWFLAKE_HOME"] == str(d)


def test_api_snowflake_config_accepts_the_file_itself(tmp_path, monkeypatch):
    d = _sfhome(tmp_path, "connections.toml")
    monkeypatch.setattr("uvicorn.run", lambda **kw: None)
    monkeypatch.delenv("SNOWFLAKE_HOME", raising=False)

    result = runner.invoke(app, [
        "api", "--allow-local-credentials", "--snowflake-config", str(d / "connections.toml"),
    ])
    assert result.exit_code == 0, result.output
    import os
    assert os.environ["SNOWFLAKE_HOME"] == str(d)      # the parent directory


def test_api_snowflake_config_requires_the_gate(tmp_path, monkeypatch):
    """Without --allow-local-credentials the server never reads a config, so a
    silently-ignored flag would be worse than an error."""
    d = _sfhome(tmp_path)
    monkeypatch.setattr("uvicorn.run", lambda **kw: pytest.fail("should not start"))

    result = runner.invoke(app, ["api", "--snowflake-config", str(d)])
    assert result.exit_code == 2
    assert "--allow-local-credentials" in result.output


def test_api_snowflake_config_rejects_missing_path(tmp_path, monkeypatch):
    """A typo must fail loudly: the connector ignores a missing SNOWFLAKE_HOME."""
    monkeypatch.setattr("uvicorn.run", lambda **kw: pytest.fail("should not start"))
    result = runner.invoke(app, [
        "api", "--allow-local-credentials", "--snowflake-config", str(tmp_path / "nope"),
    ])
    assert result.exit_code == 2
    assert "No such file or directory" in result.output


def test_api_snowflake_config_rejects_unknown_filename(tmp_path, monkeypatch):
    """Only config.toml / connections.toml are read; any other name is a no-op."""
    f = tmp_path / "snowflake.toml"
    f.write_text("[connections.dev]\n")
    monkeypatch.setattr("uvicorn.run", lambda **kw: pytest.fail("should not start"))
    result = runner.invoke(app, [
        "api", "--allow-local-credentials", "--snowflake-config", str(f),
    ])
    assert result.exit_code == 2
    assert "config.toml" in result.output


def test_api_snowflake_config_rejects_directory_without_config(tmp_path, monkeypatch):
    empty = tmp_path / "empty"
    empty.mkdir()
    monkeypatch.setattr("uvicorn.run", lambda **kw: pytest.fail("should not start"))
    result = runner.invoke(app, [
        "api", "--allow-local-credentials", "--snowflake-config", str(empty),
    ])
    assert result.exit_code == 2
    assert "neither config.toml nor connections.toml" in result.output


def test_api_logs_the_snowflake_config_it_loaded(tmp_path, monkeypatch):
    """Startup must say which config is in play — the default location is not obvious."""
    d = _sfhome(tmp_path)
    monkeypatch.setattr("uvicorn.run", lambda **kw: None)

    result = runner.invoke(app, [
        "api", "--allow-local-credentials", "--snowflake-config", str(d),
    ])
    assert result.exit_code == 0, result.output
    assert str(d / "config.toml") in result.output
    assert "1 profile: dev" in result.output
    assert "default: dev" in result.output
    assert "auth.type: config" in result.output       # the warning


def test_api_logs_nothing_about_snowflake_without_the_gate(monkeypatch):
    """No config is read when profiles are off, so there is nothing to report."""
    monkeypatch.setattr("uvicorn.run", lambda **kw: None)
    result = runner.invoke(app, ["api"])
    assert result.exit_code == 0, result.output
    assert "Snowflake config" not in result.output


def test_api_warns_when_the_config_has_no_profiles(tmp_path, monkeypatch):
    d = tmp_path / "sfhome"
    d.mkdir()
    (d / "config.toml").write_text("# no connections here\n")
    (d / "config.toml").chmod(0o600)
    monkeypatch.setattr("uvicorn.run", lambda **kw: None)

    result = runner.invoke(app, [
        "api", "--allow-local-credentials", "--snowflake-config", str(d),
    ])
    assert result.exit_code == 0, result.output          # a warning, not a failure
    assert "defines none" in result.output


def test_api_warns_when_the_config_is_unreadable(tmp_path, monkeypatch):
    d = _sfhome(tmp_path)
    monkeypatch.setattr("uvicorn.run", lambda **kw: None)

    import dcx.cli as cli_module
    monkeypatch.setattr(
        cli_module, "_report_snowflake_config",
        lambda: cli_module.typer.secho("Warning: could not be read (boom).", err=True),
    )
    result = runner.invoke(app, [
        "api", "--allow-local-credentials", "--snowflake-config", str(d),
    ])
    assert result.exit_code == 0
    assert "could not be read" in result.output
