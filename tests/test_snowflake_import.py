import pytest
from open_data_contract_standard.model import OpenDataContractStandard
from typer.testing import CliRunner

from dcx.cli import app
from dcx.importers.snowflake import (
    SnowflakeImportError,
    _fetch_metadata,
    _fetch_tags,
    _map_type,
    _physical_type,
    build_snowflake_contract,
    import_snowflake,
)

runner = CliRunner()


# === Type mapping ===========================================================


def test_map_type_number_scale():
    assert _map_type("NUMBER", 0) == ("integer", None)
    assert _map_type("NUMBER", 2) == ("number", None)


def test_map_type_common():
    assert _map_type("TEXT", None) == ("string", None)
    assert _map_type("BOOLEAN", None) == ("boolean", None)
    assert _map_type("TIMESTAMP_NTZ", None) == ("timestamp", None)
    assert _map_type("BINARY", None) == ("string", "binary")
    assert _map_type("VARIANT", None) == ("object", None)
    assert _map_type("ARRAY", None) == ("array", None)
    assert _map_type("SOMETHING_NEW", None) == ("string", None)  # safe fallback


def test_physical_type_reconstruction():
    assert _physical_type("TEXT", 255, None, None) == "VARCHAR(255)"
    assert _physical_type("TEXT", None, None, None) == "VARCHAR"
    assert _physical_type("NUMBER", None, 38, 0) == "NUMBER(38,0)"
    assert _physical_type("NUMBER", None, 38, 2) == "NUMBER(38,2)"
    assert _physical_type("TIMESTAMP_NTZ", None, None, None) == "TIMESTAMP_NTZ"


# === Pure contract builder ==================================================


def _cols():
    return [
        {"table": "customer", "name": "id", "data_type": "NUMBER", "nullable": False,
         "comment": "Surrogate key", "char_len": None, "precision": 38, "scale": 0},
        {"table": "customer", "name": "email", "data_type": "TEXT", "nullable": False,
         "comment": None, "char_len": 255, "precision": None, "scale": None},
        {"table": "customer", "name": "amount", "data_type": "NUMBER", "nullable": True,
         "comment": None, "char_len": None, "precision": 38, "scale": 2},
        {"table": "customer", "name": "payload", "data_type": "VARIANT", "nullable": True,
         "comment": None, "char_len": None, "precision": None, "scale": None},
    ]


def _build(**kw):
    return build_snowflake_contract(
        server_info={"account": "ACME", "database": "DB", "schema": "SCH", "warehouse": "WH"},
        columns=kw.get("columns", _cols()),
        primary_keys=kw.get("primary_keys", {"customer": {"id"}}),
        table_comments=kw.get("table_comments", {"customer": "Customer master"}),
    )


def _props(contract, idx=0):
    return {p.name: p for p in contract.schema_[idx].properties}


def test_build_server_and_schema():
    c = _build()
    srv = c.servers[0]
    assert srv.type == "snowflake"
    assert srv.account == "ACME"
    assert srv.database == "DB"
    assert srv.schema_ == "SCH"
    assert srv.warehouse == "WH"
    assert c.schema_[0].name == "customer"
    assert c.schema_[0].description == "Customer master"
    assert c.id == "db.sch"


def test_build_default_server_name():
    assert _build().servers[0].server == "production"


def test_build_custom_server_name():
    c = build_snowflake_contract(
        server_info={"account": "A", "database": "DB", "schema": "SCH", "warehouse": None},
        columns=_cols(), primary_keys={}, table_comments={}, server_name="prod_eu",
    )
    assert c.servers[0].server == "prod_eu"


def test_import_passes_server_name(monkeypatch):
    import dcx.importers.snowflake as si
    monkeypatch.setattr(si, "_connect", lambda import_args: _FakeConn(_fake_data()))
    c = import_snowflake({"database": "DB", "schema": "SCH", "account": "A", "server_name": "staging"})
    assert c.servers[0].server == "staging"


def test_build_column_types_and_constraints():
    p = _props(_build())
    assert p["id"].physicalType == "NUMBER(38,0)"
    assert p["id"].logicalType == "integer"
    assert p["id"].required is True
    assert p["id"].primaryKey is True
    assert p["id"].unique is True           # single-column PK
    assert p["id"].description == "Surrogate key"

    assert p["email"].physicalType == "VARCHAR(255)"
    assert p["email"].logicalType == "string"
    assert p["email"].required is True
    assert p["email"].logicalTypeOptions == {"maxLength": 255}

    assert p["amount"].physicalType == "NUMBER(38,2)"
    assert p["amount"].logicalType == "number"
    assert p["amount"].required is None

    assert p["payload"].logicalType == "object"


def test_composite_pk_not_unique():
    cols = [
        {"table": "t", "name": "a", "data_type": "NUMBER", "nullable": False,
         "comment": None, "char_len": None, "precision": 38, "scale": 0},
        {"table": "t", "name": "b", "data_type": "NUMBER", "nullable": False,
         "comment": None, "char_len": None, "precision": 38, "scale": 0},
    ]
    c = _build(columns=cols, primary_keys={"t": {"a", "b"}}, table_comments={})
    p = _props(c)
    assert p["a"].primaryKey is True and p["a"].required is True
    assert p["a"].unique is None            # composite PK ⇒ no per-column uniqueness
    assert p["b"].unique is None


def test_multiple_tables_grouped_in_order():
    cols = _cols() + [
        {"table": "orders", "name": "id", "data_type": "NUMBER", "nullable": False,
         "comment": None, "char_len": None, "precision": 38, "scale": 0},
    ]
    c = _build(columns=cols, primary_keys={"customer": {"id"}, "orders": {"id"}},
               table_comments={})
    assert [o.name for o in c.schema_] == ["customer", "orders"]


# === Metadata fetch (mocked connection) =====================================


class _FakeCursor:
    def __init__(self, data):
        self.data = data
        self._rows = []
        self.description = []

    def _tags_for(self, sql):
        for table, pair in self.data.get("tags", {}).items():
            if f"{table}'" in sql:
                return pair
        return ([], [])

    def execute(self, sql, params=None):
        if "TAG_REFERENCES_ALL_COLUMNS" in sql:
            if self.data.get("tags_raise"):
                raise RuntimeError("Insufficient privileges to operate on tag")
            self.description = [
                ("COLUMN_NAME",), ("TAG_DATABASE",), ("TAG_SCHEMA",),
                ("TAG_NAME",), ("TAG_VALUE",), ("LEVEL",),
            ]
            self._rows = self._tags_for(sql)[0]
        elif "TAG_REFERENCES(" in sql:
            if self.data.get("tags_raise"):
                raise RuntimeError("Insufficient privileges to operate on tag")
            self.description = [
                ("TAG_DATABASE",), ("TAG_SCHEMA",), ("TAG_NAME",), ("TAG_VALUE",), ("LEVEL",),
            ]
            self._rows = self._tags_for(sql)[1]
        elif "INFORMATION_SCHEMA.VIEWS" in sql:
            self._rows = self.data.get("views", [])
        elif "INFORMATION_SCHEMA.COLUMNS" in sql:
            self.description = []
            self._rows = self.data["columns"]
        elif "INFORMATION_SCHEMA.TABLES" in sql:
            self._rows = self.data["tables"]
        elif "SHOW COLUMNS" in sql:
            self.description = [
                ("table_name",), ("schema_name",), ("column_name",), ("data_type",),
            ]
            self._rows = self.data.get("show_columns", [])
        elif "SHOW PRIMARY KEYS" in sql:
            self.description = [
                ("created_on",), ("database_name",), ("schema_name",),
                ("table_name",), ("column_name",), ("key_sequence",),
            ]
            self._rows = self.data["pks"]

    def fetchall(self):
        return self._rows

    def close(self):
        pass


class _FakeConn:
    # The real connector exposes the resolved account on the connection; that is
    # the only way a `connection_name` import learns which account it reached.
    account = "PROFILE_ACCT"

    def __init__(self, data):
        self.data = data
        self.closed = False

    def cursor(self):
        return _FakeCursor(self.data)

    def close(self):
        self.closed = True


def _fake_data():
    return {
        # (TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COMMENT, CHAR_MAX, NUM_PREC, NUM_SCALE)
        "columns": [
            ("CUSTOMER", "ID", "NUMBER", "NO", "key", None, 38, 0),
            ("CUSTOMER", "EMAIL", "TEXT", "NO", None, 255, None, None),
            ("ORDERS", "ID", "NUMBER", "NO", None, None, 38, 0),
            ("CUSTOMER", "EMBEDDING", "VECTOR", "YES", None, None, None, None),
            ("CUSTOMER", "EMBEDDING_I", "VECTOR", "YES", None, None, None, None),
        ],
        # (TABLE_NAME, COMMENT, TABLE_TYPE)
        "tables": [
            ("CUSTOMER", "Customers", "BASE TABLE"),
            ("ORDERS", None, "VIEW"),
        ],
        # (TABLE_NAME, VIEW_DEFINITION) for views
        "views": [("ORDERS", "SELECT id FROM raw_orders")],
        # SHOW COLUMNS rows: the only place a VECTOR's element type + dimension appear.
        # Payloads captured verbatim from Snowflake — note the element type is nested
        # under `vectorElementType` and uses internal names (REAL=FLOAT, FIXED=INT).
        "show_columns": [
            ("CUSTOMER", "SCH", "EMBEDDING",
             '{"type":"VECTOR","nullable":true,'
             '"vectorElementType":{"type":"REAL","nullable":false},"dimension":256}'),
            ("CUSTOMER", "SCH", "EMBEDDING_I",
             '{"type":"VECTOR","nullable":true,'
             '"vectorElementType":{"type":"FIXED","precision":38,"scale":0,'
             '"nullable":false},"dimension":3}'),
            ("CUSTOMER", "SCH", "EMAIL",
             '{"type":"TEXT","length":64,"byteLength":256,"nullable":true,"fixed":false}'),
            ("CUSTOMER", "SCH", "ID",
             '{"type":"FIXED","precision":38,"scale":0,"nullable":true}'),
        ],
        # SHOW PRIMARY KEYS rows in description order
        "pks": [
            ("t", "DB", "SCH", "CUSTOMER", "ID", 1),
            ("t", "DB", "SCH", "ORDERS", "ID", 1),
        ],
        # per-table (column_tag_rows, table_tag_rows); tag rows carry their namespace
        # (TAG_DATABASE, TAG_SCHEMA) so the importer can fully-qualify them.
        "tags": {
            "CUSTOMER": (
                [("EMAIL", "GOVERNANCE", "TAGS", "DATA_CLASSIFICATION", "PD_DATA", "COLUMN")],
                [("GOVERNANCE", "TAGS", "OWNER", "data-eng", "TABLE")],
            ),
            "ORDERS": ([], []),
        },
    }


def test_fetch_metadata_shapes():
    conn = _FakeConn(_fake_data())
    columns, pks, comments, types, vdefs, full_types = _fetch_metadata(conn, "db", "sch", None)
    assert len(columns) == 5
    assert columns[0] == {
        "table": "CUSTOMER", "name": "ID", "data_type": "NUMBER", "nullable": False,
        "comment": "key", "char_len": None, "precision": 38, "scale": 0,
    }
    assert pks == {"CUSTOMER": {"ID"}, "ORDERS": {"ID"}}
    assert comments == {"CUSTOMER": "Customers", "ORDERS": None}
    assert types == {"CUSTOMER": "BASE TABLE", "ORDERS": "VIEW"}
    assert vdefs == {"ORDERS": "SELECT id FROM raw_orders"}
    # Only types INFORMATION_SCHEMA can't express are captured; TEXT is left alone.
    assert full_types == {
        ("CUSTOMER", "EMBEDDING"): "VECTOR(FLOAT, 256)",
        ("CUSTOMER", "EMBEDDING_I"): "VECTOR(INT, 3)",
    }


def test_import_sets_physical_type_from_table_type(monkeypatch):
    import dcx.importers.snowflake as si
    monkeypatch.setattr(si, "_connect", lambda import_args: _FakeConn(_fake_data()))
    contract = import_snowflake({"database": "DB", "schema": "SCH", "account": "ACME"})
    by_name = {o.name: o for o in contract.schema_}
    assert by_name["CUSTOMER"].physicalType == "table"
    assert by_name["ORDERS"].physicalType == "view"


def test_import_captures_view_definition(monkeypatch):
    import dcx.importers.snowflake as si
    monkeypatch.setattr(si, "_connect", lambda import_args: _FakeConn(_fake_data()))
    contract = import_snowflake({"database": "DB", "schema": "SCH", "account": "ACME"})
    by_name = {o.name: o for o in contract.schema_}
    view_cp = {cp.property: cp.value for cp in (by_name["ORDERS"].customProperties or [])}
    assert view_cp["viewDefinition"] == "SELECT id FROM raw_orders"
    # tables carry no viewDefinition
    assert not any(
        cp.property == "viewDefinition" for cp in (by_name["CUSTOMER"].customProperties or [])
    )


def test_fetch_metadata_table_filter():
    conn = _FakeConn(_fake_data())
    columns, _, _, _, _, _ = _fetch_metadata(conn, "db", "sch", ["customer"])
    assert {c["table"] for c in columns} == {"CUSTOMER"}


def test_import_snowflake_end_to_end(monkeypatch):
    import dcx.importers.snowflake as si
    monkeypatch.setattr(si, "_connect", lambda import_args: _FakeConn(_fake_data()))
    contract = import_snowflake({"database": "DB", "schema": "SCH", "account": "ACME"})
    assert [o.name for o in contract.schema_] == ["CUSTOMER", "ORDERS"]
    assert _props(contract)["ID"].primaryKey is True


# === Tag import =============================================================


def test_build_applies_tags():
    c = build_snowflake_contract(
        server_info={"account": "A", "database": "DB", "schema": "SCH", "warehouse": None},
        columns=_cols(),
        primary_keys={"customer": {"id"}},
        table_comments={},
        column_tags={("customer", "email"): ["DATA_CLASSIFICATION=PD_DATA"]},
        table_tags={"customer": ["OWNER=data-eng"]},
    )
    assert _props(c)["email"].tags == ["DATA_CLASSIFICATION=PD_DATA"]
    assert _props(c)["id"].tags is None
    assert c.schema_[0].tags == ["OWNER=data-eng"]


def test_fetch_tags_shapes():
    conn = _FakeConn(_fake_data())
    column_tags, table_tags = _fetch_tags(conn, "db", "sch", ["CUSTOMER", "ORDERS"])
    # Fully qualified with the tag's own DB.SCHEMA namespace.
    assert column_tags == {("CUSTOMER", "EMAIL"): ["GOVERNANCE.TAGS.DATA_CLASSIFICATION=PD_DATA"]}
    assert table_tags == {"CUSTOMER": ["GOVERNANCE.TAGS.OWNER=data-eng"]}


def test_fetch_tags_graceful_on_error(capsys):
    data = _fake_data()
    data["tags_raise"] = True
    conn = _FakeConn(data)
    column_tags, table_tags = _fetch_tags(conn, "db", "sch", ["CUSTOMER"])
    assert column_tags == {} and table_tags == {}
    assert "Could not read Snowflake tags" in capsys.readouterr().err


def test_import_end_to_end_includes_tags(monkeypatch):
    import dcx.importers.snowflake as si
    monkeypatch.setattr(si, "_connect", lambda import_args: _FakeConn(_fake_data()))
    contract = import_snowflake({"database": "DB", "schema": "SCH", "account": "ACME"})
    props = {p.name: p for p in contract.schema_[0].properties}  # CUSTOMER
    assert props["EMAIL"].tags == ["GOVERNANCE.TAGS.DATA_CLASSIFICATION=PD_DATA"]
    assert contract.schema_[0].tags == ["GOVERNANCE.TAGS.OWNER=data-eng"]


def test_import_no_tags_skips_tag_queries(monkeypatch):
    import dcx.importers.snowflake as si
    data = _fake_data()
    data["tags_raise"] = True  # would blow up if tag queries ran
    monkeypatch.setattr(si, "_connect", lambda import_args: _FakeConn(data))
    contract = import_snowflake({"database": "DB", "schema": "SCH", "account": "ACME", "tags": False})
    # no crash, and no tags applied
    assert all(p.tags is None for p in contract.schema_[0].properties)


def test_import_requires_db_and_schema(monkeypatch):
    monkeypatch.delenv("SNOWFLAKE_DATABASE", raising=False)
    monkeypatch.delenv("SNOWFLAKE_SCHEMA", raising=False)
    with pytest.raises(SnowflakeImportError, match="--database and --schema are required"):
        import_snowflake({"account": "ACME"})


# === CLI / shim bypass ======================================================


def test_cli_schema_flag_not_rewritten(monkeypatch):
    """`--schema` must reach the command (the migration shim must not rewrite it)."""
    from datacontract.data_contract import DataContract

    captured = {}

    def fake(format, source=None, **kw):
        captured["format"] = format
        captured.update(kw)
        return OpenDataContractStandard(
            apiVersion="v3.1.0", kind="DataContract", id="x", name="X", version="1.0.0",
        )

    monkeypatch.setattr(DataContract, "import_from_source", staticmethod(fake))

    result = runner.invoke(app, [
        "import", "snowflake",
        "--database", "PROD_DB", "--schema", "LOAD",
        "--table", "A", "--table", "B",
    ])
    assert result.exit_code == 0, result.output
    assert captured["format"] == "snowflake"
    assert captured["database"] == "PROD_DB"
    assert captured["schema"] == "LOAD"        # not rewritten to json_schema
    assert captured["tables"] == ["A", "B"]
    assert captured["tags"] is True            # default on
    assert captured["server_name"] == "production"  # default


def test_cli_server_name_flag(monkeypatch):
    from datacontract.data_contract import DataContract
    captured = {}

    def fake(format, source=None, **kw):
        captured.update(kw)
        return OpenDataContractStandard(
            apiVersion="v3.1.0", kind="DataContract", id="x", name="X", version="1.0.0",
        )

    monkeypatch.setattr(DataContract, "import_from_source", staticmethod(fake))
    result = runner.invoke(app, [
        "import", "snowflake", "--database", "D", "--schema", "S", "--server-name", "prod_eu",
    ])
    assert result.exit_code == 0, result.output
    assert captured["server_name"] == "prod_eu"


def test_cli_no_tags_flag(monkeypatch):
    from datacontract.data_contract import DataContract
    captured = {}

    def fake(format, source=None, **kw):
        captured.update(kw)
        return OpenDataContractStandard(
            apiVersion="v3.1.0", kind="DataContract", id="x", name="X", version="1.0.0",
        )

    monkeypatch.setattr(DataContract, "import_from_source", staticmethod(fake))
    result = runner.invoke(app, [
        "import", "snowflake", "--database", "D", "--schema", "S", "--no-tags",
    ])
    assert result.exit_code == 0, result.output
    assert captured["tags"] is False


def test_cli_no_password_flag(monkeypatch):
    result = runner.invoke(app, [
        "import", "snowflake", "--database", "D", "--schema", "S", "--password", "x",
    ])
    assert result.exit_code != 0
    assert "password" in result.output.lower()


def test_cli_snowflake_import_error_is_user_facing(monkeypatch, strip_ansi):
    from datacontract.data_contract import DataContract

    error = SnowflakeImportError(
        "No accessible columns found in D.S. The schema may be empty, or the active "
        "primary/secondary roles may lack privileges to view its tables and columns."
    )
    monkeypatch.setattr(
        DataContract,
        "import_from_source",
        staticmethod(lambda *args, **kwargs: (_ for _ in ()).throw(error)),
    )

    result = runner.invoke(app, ["import", "snowflake", "--database", "D", "--schema", "S"])

    output = strip_ansi(result.output)
    assert result.exit_code == 1
    assert str(error) in output
    assert "Traceback" not in output


def test_cli_snowflake_import_error_reraises_in_debug_mode(monkeypatch):
    from datacontract.data_contract import DataContract

    error = SnowflakeImportError("Snowflake metadata query failed: test failure")
    monkeypatch.setattr(
        DataContract,
        "import_from_source",
        staticmethod(lambda *args, **kwargs: (_ for _ in ()).throw(error)),
    )

    with pytest.raises(SnowflakeImportError, match="test failure"):
        runner.invoke(
            app,
            ["import", "snowflake", "--database", "D", "--schema", "S", "--debug"],
            catch_exceptions=False,
        )


def test_cli_quiets_botocore_credential_noise(monkeypatch):
    import logging
    from datacontract.data_contract import DataContract

    logging.getLogger("botocore.credentials").setLevel(logging.WARNING)  # reset

    def fake(format, source=None, **kw):
        return OpenDataContractStandard(
            apiVersion="v3.1.0", kind="DataContract", id="x", name="X", version="1.0.0",
        )

    monkeypatch.setattr(DataContract, "import_from_source", staticmethod(fake))
    runner.invoke(app, ["import", "snowflake", "--database", "D", "--schema", "S"])
    assert logging.getLogger("botocore.credentials").level == logging.ERROR


def test_cli_debug_leaves_botocore_noise(monkeypatch):
    import logging
    from datacontract.data_contract import DataContract

    logging.getLogger("botocore.credentials").setLevel(logging.WARNING)  # reset

    def fake(format, source=None, **kw):
        return OpenDataContractStandard(
            apiVersion="v3.1.0", kind="DataContract", id="x", name="X", version="1.0.0",
        )

    monkeypatch.setattr(DataContract, "import_from_source", staticmethod(fake))
    runner.invoke(app, ["import", "snowflake", "--database", "D", "--schema", "S", "--debug"])
    assert logging.getLogger("botocore.credentials").level == logging.WARNING  # untouched


def test_snowflake_import_in_api_with_dedicated_endpoint():
    from dcx.api import build_dcx_api_app
    paths = {getattr(r, "path", "") for r in build_dcx_api_app().routes}
    assert "/import/snowflake" in paths   # dedicated OAuth endpoint
    assert "/import/json" in paths        # file-based importers still mirrored
    assert "/import/kafka" not in paths   # kafka remains CLI-only for now


# === API import path (caller-supplied credentials) ==========================


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


def _capture_connect(monkeypatch):
    """Patch connector.connect and return the dict its kwargs land in."""
    import snowflake.connector as connector

    captured = {}

    def fake_connect(**kw):
        captured.update(kw)
        return _FakeConn(_fake_data())

    monkeypatch.setattr(connector, "connect", fake_connect)
    return captured


def test_import_connect_configures_env_secondary_roles_before_metadata(monkeypatch):
    import dcx.importers.snowflake as si
    import snowflake.connector as connector

    executed: list[str] = []

    class RecordingCursor(_FakeCursor):
        def execute(self, sql, params=None):
            executed.append(sql)
            return super().execute(sql, params)

    class RecordingConn(_FakeConn):
        def cursor(self):
            return RecordingCursor(self.data)

    monkeypatch.setenv("SNOWFLAKE_SECONDARY_ROLES", "all")
    monkeypatch.setattr(connector, "connect", lambda **kwargs: RecordingConn(_fake_data()))

    conn = si._connect({"account": "ACME", "user": "SVC"})
    try:
        assert executed == ["USE SECONDARY ROLES ALL"]
    finally:
        conn.close()


def test_import_connect_invalid_secondary_roles_executes_no_sql(monkeypatch):
    import dcx.importers.snowflake as si
    import snowflake.connector as connector

    calls: list[dict] = []
    monkeypatch.setattr(connector, "connect", lambda **kwargs: calls.append(kwargs))

    with pytest.raises(SnowflakeImportError, match="only Snowflake role names"):
        si._connect({"account": "ACME", "user": "SVC", "secondary_roles": "NONE; DROP TABLE x"})

    assert calls == []


def test_import_connect_rejects_mixed_secondary_role_modes_before_connect(monkeypatch):
    import dcx.importers.snowflake as si
    import snowflake.connector as connector

    calls: list[dict] = []
    monkeypatch.setattr(connector, "connect", lambda **kwargs: calls.append(kwargs))

    with pytest.raises(SnowflakeImportError, match="ALL and NONE must be used alone"):
        si._connect({"account": "ACME", "user": "SVC", "secondary_roles": "ALL, ROLE_A"})

    assert calls == []


def test_import_connect_accepts_named_secondary_roles(monkeypatch):
    import dcx.importers.snowflake as si
    import snowflake.connector as connector

    executed: list[str] = []

    class RecordingCursor(_FakeCursor):
        def execute(self, sql, params=None):
            executed.append(sql)
            return super().execute(sql, params)

    class RecordingConn(_FakeConn):
        def cursor(self):
            return RecordingCursor(self.data)

    monkeypatch.setattr(connector, "connect", lambda **kwargs: RecordingConn(_fake_data()))
    conn = si._connect({
        "account": "ACME", "user": "SVC", "secondary_roles": 'DATA_READER, "Finance Reader"',
    })
    try:
        assert executed == ['USE SECONDARY ROLES DATA_READER,"Finance Reader"']
    finally:
        conn.close()


def test_import_connect_secondary_role_failure_stops_before_metadata(monkeypatch):
    import dcx.importers.snowflake as si
    import snowflake.connector as connector

    class FailingCursor(_FakeCursor):
        def execute(self, sql, params=None):
            if sql == "USE SECONDARY ROLES ALL":
                raise RuntimeError("not allowed")
            return super().execute(sql, params)

    class FailingConn(_FakeConn):
        def cursor(self):
            return FailingCursor(self.data)

    conn = FailingConn(_fake_data())
    monkeypatch.setattr(connector, "connect", lambda **kwargs: conn)

    with pytest.raises(SnowflakeImportError, match="session configuration failed: not allowed"):
        si._connect({"account": "ACME", "user": "SVC", "secondary_roles": "ALL"})

    assert conn.closed is True


def test_import_snowflake_api_uses_oauth_token(monkeypatch):
    from dcx.importers.snowflake import import_snowflake_api
    from dcx.snowflake_auth import OAuthAuth

    captured = _capture_connect(monkeypatch)
    contract = import_snowflake_api(
        auth=OAuthAuth(type="oauth", token="tok123"),
        account="ACME", database="DB", schema="SCH", tables=["CUSTOMER"],
    )
    assert captured["authenticator"] == "oauth"
    assert captured["token"] == "tok123"
    assert captured["account"] == "ACME"
    assert "password" not in captured        # never falls back to other secrets
    assert [o.name for o in contract.schema_] == ["CUSTOMER"]


def test_import_snowflake_api_uses_key_pair(monkeypatch):
    from dcx.importers.snowflake import import_snowflake_api
    from dcx.snowflake_auth import KeyPairAuth

    captured = _capture_connect(monkeypatch)
    import_snowflake_api(
        auth=KeyPairAuth(
            type="key_pair", user="SVC", private_key=_rsa_pem(passphrase="pw"),
            private_key_passphrase="pw",
        ),
        account="ACME", database="DB", schema="SCH",
    )
    assert captured["authenticator"] == "SNOWFLAKE_JWT"
    assert captured["user"] == "SVC"
    # Handed to the connector as decrypted DER bytes, never the PEM we were sent.
    assert isinstance(captured["private_key"], bytes)
    assert b"BEGIN" not in captured["private_key"]
    assert "private_key_file" not in captured    # no server-side path, ever


def test_import_snowflake_api_uses_password(monkeypatch):
    from dcx.importers.snowflake import import_snowflake_api
    from dcx.snowflake_auth import PasswordAuth

    captured = _capture_connect(monkeypatch)
    import_snowflake_api(
        auth=PasswordAuth(type="password", user="SVC", password="hunter2"),
        account="ACME", database="DB", schema="SCH",
    )
    assert captured["user"] == "SVC"
    assert captured["password"] == "hunter2"
    assert "authenticator" not in captured       # connector default


def test_import_snowflake_api_connection_profile_needs_no_account(monkeypatch):
    """A profile carries its own account; we read it back off the connection."""
    from dcx.importers.snowflake import import_snowflake_api
    from dcx.snowflake_auth import ALLOW_LOCAL_CREDENTIALS_ENV, ConfigAuth

    monkeypatch.setenv(ALLOW_LOCAL_CREDENTIALS_ENV, "1")
    captured = _capture_connect(monkeypatch)
    contract = import_snowflake_api(
        auth=ConfigAuth(type="config", connection_name="dev"),
        database="DB", schema="SCH",
    )
    assert captured["connection_name"] == "dev"
    assert "account" not in captured
    assert contract.servers[0].account == _FakeConn.account


def test_import_snowflake_api_requires_account(monkeypatch):
    from dcx.importers.snowflake import import_snowflake_api
    from dcx.snowflake_auth import OAuthAuth

    with pytest.raises(SnowflakeImportError, match="account is required"):
        import_snowflake_api(
            auth=OAuthAuth(type="oauth", token="t"), database="D", schema="S",
        )


def test_import_snowflake_api_requires_token():
    from dcx.importers.snowflake import import_snowflake_api
    from dcx.snowflake_auth import OAuthAuth, SnowflakeAuthError

    with pytest.raises(SnowflakeAuthError, match="OAuth token is required"):
        import_snowflake_api(
            auth=OAuthAuth(type="oauth", token=""), account="A", database="D", schema="S",
        )


# === API endpoint ===========================================================


def _client():
    from fastapi.testclient import TestClient
    from dcx.api import build_dcx_api_app
    return TestClient(build_dcx_api_app())


def test_api_snowflake_requires_credentials():
    r = _client().post("/import/snowflake", json={"account": "A", "database": "D", "schema": "S"})
    assert r.status_code == 401
    assert "auth" in r.json()["detail"] and "Bearer" in r.json()["detail"]


def test_api_snowflake_works(monkeypatch):
    import dcx.importers.snowflake as si
    captured = {}

    def fake(**kw):
        captured.update(kw)
        return OpenDataContractStandard(
            apiVersion="v3.1.0", kind="DataContract", id="x", name="X", version="1.0.0",
        )

    monkeypatch.setattr(si, "import_snowflake_api", fake)
    r = _client().post(
        "/import/snowflake",
        headers={"Authorization": "Bearer tok-xyz"},
        json={
            "account": "ACME", "database": "DB", "schema": "SCH", "tables": ["T"],
            "secondary_roles": "NONE",
        },
    )
    assert r.status_code == 200, r.text
    assert captured["auth"].token.get_secret_value() == "tok-xyz"
    assert captured["account"] == "ACME"
    assert captured["schema"] == "SCH"       # body "schema" → schema_ → schema kwarg
    assert captured["tables"] == ["T"]
    assert captured["secondary_roles"] == "NONE"
    assert captured["quality"] is False      # opt-in; off unless requested


def test_api_snowflake_quality_flag_passes_through(monkeypatch):
    import dcx.importers.snowflake as si
    captured = {}

    def fake(**kw):
        captured.update(kw)
        return OpenDataContractStandard(
            apiVersion="v3.1.0", kind="DataContract", id="x", name="X", version="1.0.0",
        )

    monkeypatch.setattr(si, "import_snowflake_api", fake)
    r = _client().post(
        "/import/snowflake",
        headers={"Authorization": "Bearer tok"},
        json={"account": "A", "database": "D", "schema": "S", "quality": True},
    )
    assert r.status_code == 200, r.text
    assert captured["quality"] is True


def test_api_snowflake_error_is_502(monkeypatch):
    import dcx.importers.snowflake as si

    def boom(**kw):
        raise si.SnowflakeImportError("Snowflake connection failed: bad token")

    monkeypatch.setattr(si, "import_snowflake_api", boom)
    r = _client().post(
        "/import/snowflake",
        headers={"Authorization": "Bearer tok"},
        json={"account": "A", "database": "D", "schema": "S"},
    )
    assert r.status_code == 502
    assert "bad token" in r.json()["detail"]


def test_vector_column_round_trips_into_valid_ddl(monkeypatch):
    """The regression this fixes: INFORMATION_SCHEMA reports a bare `VECTOR`, which is
    not valid DDL, so the generated CREATE TABLE was one Snowflake refuses to parse."""
    import dcx.importers.snowflake as si
    from dcx.exporters.snowflake import to_snowflake_full_sql

    monkeypatch.setattr(si, "_connect", lambda import_args: _FakeConn(_fake_data()))
    contract = import_snowflake({"database": "DB", "schema": "SCH", "account": "ACME"})

    props = {p.name: p for p in contract.schema_[0].properties}
    assert props["EMBEDDING"].physicalType == "VECTOR(FLOAT, 256)"
    assert props["EMBEDDING_I"].physicalType == "VECTOR(INT, 3)"
    ddl = to_snowflake_full_sql(contract)
    assert "VECTOR(FLOAT, 256)" in ddl
    assert "VECTOR(INT, 3)" in ddl


def test_show_columns_failure_is_not_fatal(monkeypatch):
    """SHOW COLUMNS needs its own privileges; without it the import must still succeed,
    just with the less precise INFORMATION_SCHEMA type."""
    import dcx.importers.snowflake as si

    class _NoShowColumns(_FakeCursor):
        def execute(self, sql, params=None):
            if "SHOW COLUMNS" in sql:
                raise RuntimeError("Insufficient privileges")
            return super().execute(sql, params)

    class _Conn(_FakeConn):
        def cursor(self):
            return _NoShowColumns(self.data)

    monkeypatch.setattr(si, "_connect", lambda import_args: _Conn(_fake_data()))
    contract = import_snowflake({"database": "DB", "schema": "SCH", "account": "ACME"})
    embedding = {p.name: p for p in contract.schema_[0].properties}["EMBEDDING"]
    assert embedding.physicalType == "VECTOR"


# === Data quality round trip ================================================

# Column subset of DATA_METRIC_FUNCTION_REFERENCES that the importer reads. Lookup is
# by name, so the real view's remaining columns (METRIC_DATA_TYPE, REF_ID,
# SCHEDULE_STATUS, PROPERTIES, ...) are irrelevant here.
_DMF_COLUMNS = [
    "metric_database_name", "metric_schema_name", "metric_name",
    "metric_signature", "ref_entity_name", "ref_arguments", "schedule", "ref_id",
]
# Expectations come from their own table function, joined on ref_id. Fixture rows
# declare the expectation inline as a last element and the fake connection splits it
# across the two calls, exactly as Snowflake reports it.
_EXPECTATION_COLUMNS = ["ref_id", "expectation_name", "expectation_expression"]

# Rows captured verbatim from a live account (SNOWFLAKE.CORE metrics on LOAD.CUSTOMERS).
# Note REF_ARGUMENTS mixes COLUMN and VALUES domains, and SCHEDULE carries a trailing
# timezone with no `USING CRON` prefix.
_REAL_ROWS = [
    ("SNOWFLAKE", "CORE", "ACCEPTED_VALUES", "TABLE(NUMBER)", "CUSTOMER",
     '[{"domain":"COLUMN","id":"591995302","name":"EMAIL"},'
     '{"domain":"VALUES","name":"EMAIL IN (\'a\', \'b\')"}]', "0 */1 * * * UTC", "VALUE = 0"),
    ("SNOWFLAKE", "CORE", "FRESHNESS", "", "CUSTOMER", "[]", "0 */1 * * * UTC", "VALUE <= 14400"),
    ("SNOWFLAKE", "CORE", "NULL_COUNT", "TABLE(NUMBER)", "CUSTOMER",
     '[{"domain":"COLUMN","id":"591995298","name":"ID"}]', "0 */1 * * * UTC", "VALUE = 0"),
]


def _import_with_dmfs(monkeypatch, rows):
    import dcx.importers.snowflake as si

    class _Cur(_FakeCursor):
        def execute(self, sql, params=None):
            matching = [
                (f"ref{i}", r) for i, r in enumerate(rows) if f".{r[4]}'" in sql.upper()
            ]
            if "DATA_METRIC_FUNCTION_REFERENCES" in sql:
                self.description = [(c,) for c in _DMF_COLUMNS]
                self._rows = [tuple(r[:7]) + (ref_id,) for ref_id, r in matching]
                return
            if "DATA_METRIC_FUNCTION_EXPECTATIONS" in sql:
                self.description = [(c,) for c in _EXPECTATION_COLUMNS]
                self._rows = [
                    (ref_id, "EXP__DCX__X", r[7]) for ref_id, r in matching if len(r) > 7 and r[7]
                ]
                return
            return super().execute(sql, params)

    class _Conn(_FakeConn):
        def cursor(self):
            return _Cur(self.data)

    monkeypatch.setattr(si, "_connect", lambda import_args: _Conn(_fake_data()))
    return import_snowflake({"database": "DB", "schema": "SCH", "account": "ACME", "quality": True})


def _obj(contract, name="CUSTOMER"):
    return {o.name: o for o in contract.schema_}[name]


def test_core_dmfs_import_as_quality_rules(monkeypatch):
    contract = _import_with_dmfs(monkeypatch, _REAL_ROWS)
    customer = _obj(contract)

    id_rule = {p.name: p for p in customer.properties}["ID"].quality[0]
    assert id_rule.type == "library"
    assert id_rule.metric == "nullValues"
    # `0 */1 * * * UTC` has no USING CRON prefix and a trailing timezone.
    assert id_rule.schedule == "0 */1 * * *"
    assert id_rule.scheduler == "cron"


def test_accepted_values_recovers_its_allowed_set(monkeypatch):
    contract = _import_with_dmfs(monkeypatch, _REAL_ROWS)
    rule = {p.name: p for p in _obj(contract).properties}["EMAIL"].quality[0]
    assert rule.metric == "invalidValues"
    assert rule.arguments == {"validValues": ["a", "b"]}


def test_values_domain_entry_is_not_mistaken_for_a_column(monkeypatch):
    """REF_ARGUMENTS mixes domains; taking every `name` would treat the predicate text
    as a column name."""
    contract = _import_with_dmfs(monkeypatch, _REAL_ROWS)
    columns_with_quality = [p.name for p in _obj(contract).properties if p.quality]
    assert columns_with_quality == ["ID", "EMAIL"]


def test_non_in_predicate_is_preserved_as_custom(monkeypatch):
    """`AGE BETWEEN 0 AND 150` has no ODCS equivalent — flattening it into a bare
    `invalidValues` would assert something different from what Snowflake enforces."""
    contract = _import_with_dmfs(monkeypatch, [
        ("SNOWFLAKE", "CORE", "ACCEPTED_VALUES", "TABLE(NUMBER)", "CUSTOMER",
         '[{"domain":"COLUMN","name":"EMAIL"},'
         '{"domain":"VALUES","name":"AGE BETWEEN 0 AND 150"}]', None, None),
    ])
    rule = {p.name: p for p in _obj(contract).properties}["EMAIL"].quality[0]
    assert rule.type == "custom"
    assert rule.engine == "snowflake"
    assert rule.implementation["condition"] == "AGE BETWEEN 0 AND 150"


def test_freshness_imports_as_an_sla_not_a_quality_rule(monkeypatch):
    contract = _import_with_dmfs(monkeypatch, _REAL_ROWS)
    assert contract.slaProperties[0].property == "latency"
    assert contract.slaProperties[0].element == "DB.SCH.CUSTOMER"
    assert not _obj(contract).quality


def test_blank_count_imports_as_a_check_tagged_sql_rule(monkeypatch):
    contract = _import_with_dmfs(monkeypatch, [
        ("SNOWFLAKE", "CORE", "BLANK_COUNT", "TABLE(NUMBER)", "CUSTOMER",
         '[{"domain":"COLUMN","name":"EMAIL"}]', None, None),
    ])
    rule = {p.name: p for p in _obj(contract).properties}["EMAIL"].quality[0]
    assert rule.type == "sql"
    assert rule.customProperties[0].property == "check"
    assert rule.customProperties[0].value == "blankCount"
    assert "TRIM(CAST(${column} AS STRING)) = ''" in rule.query


def test_user_defined_metric_imports_as_odcs_custom(monkeypatch):
    """A DMF outside SNOWFLAKE.CORE is engine-specific — including one that shadows a
    built-in name, which is why the namespace is part of the identity."""
    contract = _import_with_dmfs(monkeypatch, [
        ("MY_DB", "GOV", "NULL_COUNT", "TABLE(NUMBER)", "CUSTOMER", "[]", None, None),
    ])
    rule = _obj(contract).quality[0]
    assert rule.type == "custom"
    assert rule.engine == "snowflake"
    assert rule.implementation == "MY_DB.GOV.NULL_COUNT"


@pytest.mark.parametrize("import_args,expected_queries", [
    ({}, 0),                       # default: off, for import speed
    ({"quality": False}, 0),       # explicit --no-quality
    ({"quality": True}, 2),        # opt in: references + expectations, per table
])
def test_quality_queries_are_opt_in(monkeypatch, import_args, expected_queries):
    """Quality import costs two extra per-table round trips, so it is off unless asked
    for. `_fake_data` has one table, so the count is the per-table cost."""
    import dcx.importers.snowflake as si
    seen: list = []

    class _Cur(_FakeCursor):
        def execute(self, sql, params=None):
            if "DATA_METRIC_FUNCTION_" in sql:
                seen.append(sql)
                self.description = [("ref_id",)]
                self._rows = []
                return
            return super().execute(sql, params)

    class _Conn(_FakeConn):
        def cursor(self):
            return _Cur(self.data)

    monkeypatch.setattr(si, "_connect", lambda import_args: _Conn(_fake_data()))
    base = {"database": "DB", "schema": "SCH", "account": "ACME"}
    import_snowflake({**base, **import_args})
    # _fake_data has CUSTOMER and ORDERS, so two tables x the per-table cost.
    assert len(seen) == expected_queries * 2


def test_no_quality_flag_skips_the_dmf_query(monkeypatch):
    import dcx.importers.snowflake as si
    calls: list = []

    class _Cur(_FakeCursor):
        def execute(self, sql, params=None):
            if "DATA_METRIC_FUNCTION_REFERENCES" in sql:
                calls.append(sql)
            return super().execute(sql, params)

    class _Conn(_FakeConn):
        def cursor(self):
            return _Cur(self.data)

    monkeypatch.setattr(si, "_connect", lambda import_args: _Conn(_fake_data()))
    import_snowflake({"database": "DB", "schema": "SCH", "account": "ACME", "quality": False})
    assert calls == []


def test_applied_quality_survives_a_full_round_trip(monkeypatch):
    """export → (Snowflake) → import must reproduce the same ODCS constructs."""
    from dcx.exporters.snowflake import to_snowflake_full_sql

    contract = _import_with_dmfs(monkeypatch, _REAL_ROWS)
    sql = to_snowflake_full_sql(contract, include_quality=True, include_ddl=False)
    assert "SNOWFLAKE.CORE.NULL_COUNT ON (ID)" in sql
    assert "SNOWFLAKE.CORE.FRESHNESS ON ()" in sql
    assert "VALUE <= 14400" in sql
    assert "SNOWFLAKE.CORE.ACCEPTED_VALUES ON (EMAIL, EMAIL -> EMAIL IN ('a', 'b'))" in sql
    assert "SET DATA_METRIC_SCHEDULE = 'USING CRON 0 */1 * * * UTC';" in sql


# === Expectations → ODCS operators ==========================================


@pytest.mark.parametrize("expression,expected", [
    ("VALUE = 0", ("mustBe", 0)),
    ("VALUE <> 5", ("mustNotBe", 5)),
    ("VALUE > 0", ("mustBeGreaterThan", 0)),
    ("VALUE >= 10", ("mustBeGreaterOrEqualTo", 10)),
    ("VALUE < 3", ("mustBeLessThan", 3)),
    ("VALUE <= 14400", ("mustBeLessOrEqualTo", 14400)),
    ("10 <= VALUE AND VALUE <= 20", ("mustBeBetween", [10, 20])),
    ("VALUE < 1 OR VALUE > 9", ("mustNotBeBetween", [1, 9])),
    ("(VALUE = 0)", ("mustBe", 0)),          # parenthesised, as Snowflake stores it
    ("VALUE <= 4.5", ("mustBeLessOrEqualTo", 4.5)),
    ("VALUE IS NOT NULL", None),             # unparseable predicate
    (None, None),
])
def test_operator_parsed_from_expectation(expression, expected):
    from dcx.importers.snowflake import _operator_from_expectation
    assert _operator_from_expectation(expression) == expected


def test_expectation_restores_the_rule_threshold(monkeypatch):
    contract = _import_with_dmfs(monkeypatch, [
        ("SNOWFLAKE", "CORE", "NULL_COUNT", "TABLE(NUMBER)", "CUSTOMER",
         '[{"domain":"COLUMN","name":"ID"}]', None, "VALUE = 0"),
    ])
    rule = {p.name: p for p in _obj(contract).properties}["ID"].quality[0]
    assert rule.metric == "nullValues"
    assert rule.mustBe == 0


def test_freshness_expectation_becomes_the_sla_value(monkeypatch):
    contract = _import_with_dmfs(monkeypatch, [
        ("SNOWFLAKE", "CORE", "FRESHNESS", "", "CUSTOMER", "[]", None, "VALUE <= 14400"),
    ])
    sla = contract.slaProperties[0]
    assert (sla.property, sla.value, sla.unit) == ("latency", 14400, "s")


def test_missing_expectation_leaves_the_rule_without_a_threshold(monkeypatch):
    """Better than inventing one: the rule records what is attached, and the warning
    says it cannot fail anything yet."""
    contract = _import_with_dmfs(monkeypatch, [
        ("SNOWFLAKE", "CORE", "NULL_COUNT", "TABLE(NUMBER)", "CUSTOMER",
         '[{"domain":"COLUMN","name":"ID"}]', None, None),
    ])
    rule = {p.name: p for p in _obj(contract).properties}["ID"].quality[0]
    assert rule.mustBe is None


def test_operators_survive_a_full_round_trip(monkeypatch):
    """The gap this closes: export → Snowflake → import → export must reproduce the
    same EXPECTATION, not just the same metric."""
    from dcx.exporters.snowflake import to_snowflake_full_sql

    contract = _import_with_dmfs(monkeypatch, [
        ("SNOWFLAKE", "CORE", "NULL_COUNT", "TABLE(NUMBER)", "CUSTOMER",
         '[{"domain":"COLUMN","name":"ID"}]', None, "VALUE = 0"),
        ("SNOWFLAKE", "CORE", "ROW_COUNT", "", "CUSTOMER", "[]", None, "VALUE > 0"),
        ("SNOWFLAKE", "CORE", "FRESHNESS", "", "CUSTOMER", "[]", None, "VALUE <= 14400"),
    ])
    sql = to_snowflake_full_sql(contract, include_quality=True, include_ddl=False)
    assert "EXPECTATION EXP__DCX__ID__NONULLS (VALUE = 0);" in sql
    assert "EXPECTATION EXP__DCX__ROW_COUNT__GREATERTHAN0 (VALUE > 0);" in sql
    assert "EXPECTATION EXP__DCX__FRESHNESS__LESSTHANOREQUALTO14400 (VALUE <= 14400);" in sql


def test_expectation_is_joined_on_ref_id(monkeypatch):
    """Two metrics on the same table must each get their own expectation — the join key
    is ref_id, not the table."""
    contract = _import_with_dmfs(monkeypatch, [
        ("SNOWFLAKE", "CORE", "NULL_COUNT", "TABLE(NUMBER)", "CUSTOMER",
         '[{"domain":"COLUMN","name":"ID"}]', None, "VALUE = 0"),
        ("SNOWFLAKE", "CORE", "ROW_COUNT", "", "CUSTOMER", "[]", None, "VALUE > 100"),
    ])
    customer = _obj(contract)
    assert {p.name: p for p in customer.properties}["ID"].quality[0].mustBe == 0
    assert customer.quality[0].mustBeGreaterThan == 100


def test_expectations_query_failure_is_not_fatal(monkeypatch):
    """The expectations table function needs its own privileges; without it the metrics
    must still import, just without thresholds."""
    import dcx.importers.snowflake as si

    class _Cur(_FakeCursor):
        def execute(self, sql, params=None):
            if "DATA_METRIC_FUNCTION_EXPECTATIONS" in sql:
                raise RuntimeError("Insufficient privileges")
            if "DATA_METRIC_FUNCTION_REFERENCES" in sql:
                self.description = [(c,) for c in _DMF_COLUMNS]
                self._rows = [("SNOWFLAKE", "CORE", "NULL_COUNT", "TABLE(NUMBER)",
                               "CUSTOMER", '[{"domain":"COLUMN","name":"ID"}]', None, "r0")]
                return
            return super().execute(sql, params)

    class _Conn(_FakeConn):
        def cursor(self):
            return _Cur(self.data)

    monkeypatch.setattr(si, "_connect", lambda import_args: _Conn(_fake_data()))
    contract = import_snowflake({"database": "DB", "schema": "SCH", "account": "ACME", "quality": True})
    rule = {p.name: p for p in _obj(contract).properties}["ID"].quality[0]
    assert rule.metric == "nullValues"
    assert rule.mustBe is None


def test_real_expectation_expressions_parse_verbatim():
    """Snowflake returns expectation_expression exactly as written — no normalising,
    no substituting the metric name for VALUE. Strings captured from a live account."""
    from dcx.importers.snowflake import _operator_from_expectation
    assert _operator_from_expectation("VALUE = 0") == ("mustBe", 0)
    assert _operator_from_expectation("VALUE < 86400") == ("mustBeLessThan", 86400)


def test_second_expectation_on_one_association_is_reported(monkeypatch, capsys):
    """Real state on a live table: one association carried both a hand-made
    EXP__CUSTOMER_ID__NONULLS and a second expectation. ODCS holds one operator."""
    import dcx.importers.snowflake as si

    class _Cur(_FakeCursor):
        def execute(self, sql, params=None):
            mine = ".CUSTOMER'" in sql.upper()
            if "DATA_METRIC_FUNCTION_EXPECTATIONS" in sql:
                self.description = [(c,) for c in _EXPECTATION_COLUMNS]
                self._rows = [
                    ("523a27cf", "EXP__CUSTOMER_ID__NONULLS", "VALUE = 0"),
                    ("523a27cf", "EXP__DCX__PROBE", "VALUE = 0"),
                ] if mine else []
                return
            if "DATA_METRIC_FUNCTION_REFERENCES" in sql:
                self.description = [(c,) for c in _DMF_COLUMNS]
                self._rows = [("SNOWFLAKE", "CORE", "NULL_COUNT", "TABLE(NUMBER)",
                               "CUSTOMER", '[{"domain":"COLUMN","name":"ID"}]',
                               None, "523a27cf")] if mine else []
                return
            return super().execute(sql, params)

    class _Conn(_FakeConn):
        def cursor(self):
            return _Cur(self.data)

    monkeypatch.setattr(si, "_connect", lambda import_args: _Conn(_fake_data()))
    contract = import_snowflake({"database": "DB", "schema": "SCH", "account": "ACME", "quality": True})
    rule = {p.name: p for p in _obj(contract).properties}["ID"].quality[0]
    assert rule.mustBe == 0
    assert "2 expectations" in capsys.readouterr().err


# === Query failures surface as SnowflakeImportError, not a 500 ==============
# `import_snowflake_api` wrapped only `connect()`, so anything that failed during
# the metadata queries escaped as a raw ProgrammingError and the API answered 500
# with a traceback. Snowflake's own message is passed through unchanged.


class _RaisingCursor:
    def __init__(self, exc):
        self.exc = exc
        self.description = []

    def execute(self, sql, params=None):
        raise self.exc

    def fetchall(self):
        return []

    def close(self):
        pass


class _RaisingConn:
    def __init__(self, exc):
        self.exc = exc
        self.closed = False

    def cursor(self):
        return _RaisingCursor(self.exc)

    def close(self):
        self.closed = True


NO_WAREHOUSE = (
    "000606 (57P03): 01c66fcb-0002-800c: No active warehouse selected in the "
    "current session.  Select an active warehouse with the 'use warehouse' command."
)


def test_metadata_failure_becomes_import_error_not_raw_exception():
    from dcx.importers.snowflake import _contract_from_connection

    conn = _RaisingConn(RuntimeError(NO_WAREHOUSE))
    with pytest.raises(SnowflakeImportError) as excinfo:
        _contract_from_connection(
            conn, database="DB", schema="SCH", tables=None, fetch_tags=False,
            server_info={"account": "A", "database": "DB", "schema": "SCH", "warehouse": None},
            server_name="production",
        )
    assert NO_WAREHOUSE in str(excinfo.value)   # Snowflake's own text, unaltered


def test_metadata_failure_closes_the_connection(monkeypatch):
    """The `finally: conn.close()` in the import entry points must survive the raise."""
    import dcx.importers.snowflake as si

    conn = _RaisingConn(RuntimeError(NO_WAREHOUSE))
    monkeypatch.setattr(si, "_connect", lambda import_args: conn)
    with pytest.raises(SnowflakeImportError):
        si.import_snowflake({"database": "DB", "schema": "SCH", "account": "A"})
    assert conn.closed is True


def test_api_returns_502_with_snowflake_text_not_500(monkeypatch):
    import dcx.importers.snowflake as si

    def boom(**kw):
        raise SnowflakeImportError(f"Snowflake metadata query failed: {NO_WAREHOUSE}")

    monkeypatch.setattr(si, "import_snowflake_api", boom)
    r = _client().post(
        "/import/snowflake",
        headers={"Authorization": "Bearer tok"},
        json={"account": "A", "database": "D", "schema": "S"},
    )
    assert r.status_code == 502
    assert "No active warehouse selected" in r.json()["detail"]


# === API endpoint: credential methods =======================================


def _fake_import(monkeypatch, captured):
    """Intercept the importer so endpoint tests never touch the network."""
    import dcx.importers.snowflake as si

    def fake(**kw):
        captured.update(kw)
        return OpenDataContractStandard(
            apiVersion="v3.1.0", kind="DataContract", id="x", name="X", version="1.0.0",
        )

    monkeypatch.setattr(si, "import_snowflake_api", fake)


def test_api_snowflake_key_pair_auth(monkeypatch):
    captured = {}
    _fake_import(monkeypatch, captured)
    r = _client().post(
        "/import/snowflake",
        json={
            "account": "ACME", "database": "DB", "schema": "SCH",
            "auth": {
                "type": "key_pair", "user": "SVC",
                "private_key": _rsa_pem(), "private_key_passphrase": None,
            },
        },
    )
    assert r.status_code == 200, r.text
    auth = captured["auth"]
    assert auth.user == "SVC"
    # SecretStr: the key never shows up in a repr, log line, or error echo.
    assert "BEGIN" not in repr(auth)


def test_api_snowflake_password_auth(monkeypatch):
    captured = {}
    _fake_import(monkeypatch, captured)
    r = _client().post(
        "/import/snowflake",
        json={
            "account": "ACME", "database": "DB", "schema": "SCH",
            "auth": {"type": "password", "user": "SVC", "password": "hunter2"},
        },
    )
    assert r.status_code == 200, r.text
    assert captured["auth"].password.get_secret_value() == "hunter2"
    assert "hunter2" not in repr(captured["auth"])


def test_api_snowflake_body_auth_beats_bearer_header(monkeypatch):
    captured = {}
    _fake_import(monkeypatch, captured)
    r = _client().post(
        "/import/snowflake",
        headers={"Authorization": "Bearer header-tok"},
        json={
            "account": "A", "database": "D", "schema": "S",
            "auth": {"type": "oauth", "token": "body-tok"},
        },
    )
    assert r.status_code == 200, r.text
    assert captured["auth"].token.get_secret_value() == "body-tok"


def test_api_snowflake_bad_private_key_is_400(monkeypatch):
    # No importer stub here: credential handling is what's under test, and it
    # has to reject the request before `connect()` is ever reached.
    connected = _capture_connect(monkeypatch)
    r = _client().post(
        "/import/snowflake",
        json={
            "account": "A", "database": "D", "schema": "S",
            "auth": {"type": "key_pair", "user": "SVC", "private_key": "not-a-key"},
        },
    )
    assert r.status_code == 400
    assert "private_key" in r.json()["detail"]
    assert connected == {}          # never dialled Snowflake


def test_api_snowflake_wrong_passphrase_is_400_without_key_material(monkeypatch):
    connected = _capture_connect(monkeypatch)
    r = _client().post(
        "/import/snowflake",
        json={
            "account": "A", "database": "D", "schema": "S",
            "auth": {
                "type": "key_pair", "user": "SVC",
                "private_key": _rsa_pem(passphrase="right"),
                "private_key_passphrase": "wrong",
            },
        },
    )
    assert r.status_code == 400
    assert "BEGIN" not in r.text            # the error never quotes the key
    assert connected == {}


def test_api_config_auth_disabled_by_default(monkeypatch):
    from dcx.snowflake_auth import ALLOW_LOCAL_CREDENTIALS_ENV

    monkeypatch.delenv(ALLOW_LOCAL_CREDENTIALS_ENV, raising=False)
    connected = _capture_connect(monkeypatch)
    r = _client().post(
        "/import/snowflake",
        json={
            "database": "D", "schema": "S",
            "auth": {"type": "config", "connection_name": "dev"},
        },
    )
    assert r.status_code == 403
    assert "--allow-local-credentials" in r.json()["detail"]
    assert connected == {}          # the gate holds before any connection


def test_api_config_auth_when_enabled(monkeypatch):
    from dcx.snowflake_auth import ALLOW_LOCAL_CREDENTIALS_ENV

    monkeypatch.setenv(ALLOW_LOCAL_CREDENTIALS_ENV, "1")
    connected = _capture_connect(monkeypatch)
    r = _client().post(
        "/import/snowflake",
        json={
            "database": "D", "schema": "S",       # no account: the profile has one
            "auth": {"type": "config", "connection_name": "dev"},
        },
    )
    assert r.status_code == 200, r.text
    assert connected["connection_name"] == "dev"
    assert "account" not in connected


def test_api_snowflake_rejects_server_side_key_path(monkeypatch):
    """A path would make the server read its own filesystem for the caller."""
    _capture_connect(monkeypatch)
    r = _client().post(
        "/import/snowflake",
        json={
            "account": "A", "database": "D", "schema": "S",
            "auth": {
                "type": "key_pair", "user": "SVC",
                "private_key_file": "/home/someone/.ssh/rsa_key.p8",
            },
        },
    )
    assert r.status_code == 422       # extra="forbid" + private_key missing


# === Default connection profile fallback ====================================


def test_import_falls_back_to_default_profile(monkeypatch):
    """No flags, no env — use Snowflake's own default profile rather than erroring."""
    import dcx.importers.snowflake as si

    for var in ("SNOWFLAKE_USER", "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_PASSWORD"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setattr(si, "default_connection_name", lambda: "my_default")
    captured = _capture_connect(monkeypatch)

    si.import_snowflake({"database": "DB", "schema": "SCH"})
    assert captured["connection_name"] == "my_default"
    # --database/--schema name what to read, so they override the profile context.
    assert (captured["database"], captured["schema"]) == ("DB", "SCH")


def test_import_original_error_kept_without_default_profile(monkeypatch):
    import dcx.importers.snowflake as si

    for var in ("SNOWFLAKE_USER", "SNOWFLAKE_ACCOUNT"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setattr(si, "default_connection_name", lambda: None)

    with pytest.raises(SnowflakeImportError, match="Cannot determine Snowflake account"):
        si.import_snowflake({"database": "DB", "schema": "SCH"})


def test_import_default_profile_not_used_when_env_is_set(monkeypatch):
    import dcx.importers.snowflake as si

    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "ACME")
    monkeypatch.setenv("SNOWFLAKE_USER", "me")
    monkeypatch.setattr(si, "default_connection_name", lambda: "my_default")
    captured = _capture_connect(monkeypatch)

    si.import_snowflake({"database": "DB", "schema": "SCH"})
    assert "connection_name" not in captured
    assert captured["account"] == "ACME"


def test_allow_local_credentials_does_not_change_other_auth_methods(monkeypatch):
    """Enabling server-side profiles unlocks `connection_name` and nothing else."""
    from dcx.snowflake_auth import ALLOW_LOCAL_CREDENTIALS_ENV

    monkeypatch.setenv(ALLOW_LOCAL_CREDENTIALS_ENV, "1")
    monkeypatch.setenv("SNOWFLAKE_USER", "server-ambient-user")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "server-ambient-pw")
    connected = _capture_connect(monkeypatch)

    r = _client().post(
        "/import/snowflake",
        json={
            "account": "CALLER_ACCT", "database": "DB", "schema": "SCH",
            "auth": {"type": "oauth", "token": "caller-token"},
        },
    )
    assert r.status_code == 200, r.text
    assert connected["token"] == "caller-token"
    assert connected["account"] == "CALLER_ACCT"
    assert "connection_name" not in connected           # the server profile is untouched
    assert "server-ambient-user" not in connected.values()
    assert "password" not in connected


def test_api_never_falls_back_to_the_servers_default_profile(monkeypatch):
    """The CLI's default-profile fallback must not exist on the API path: it would
    make the server connect as itself for a caller who supplied no account."""
    import dcx.importers.snowflake as si
    from dcx.snowflake_auth import ALLOW_LOCAL_CREDENTIALS_ENV

    monkeypatch.setenv(ALLOW_LOCAL_CREDENTIALS_ENV, "1")
    consulted = []
    monkeypatch.setattr(
        si, "default_connection_name", lambda: consulted.append(1) or "server_default",
    )
    connected = _capture_connect(monkeypatch)

    r = _client().post(
        "/import/snowflake",
        json={
            "database": "DB", "schema": "SCH",      # no account
            "auth": {"type": "oauth", "token": "caller-token"},
        },
    )
    assert r.status_code == 502
    assert "account is required" in r.json()["detail"]
    assert consulted == []
    assert connected == {}


def test_api_config_auth_name_may_be_omitted_for_the_default(monkeypatch):
    """`{"type": "config"}` means the server's default_connection_name."""
    import dcx.snowflake_auth as sa
    from dcx.snowflake_auth import ALLOW_LOCAL_CREDENTIALS_ENV

    monkeypatch.setenv(ALLOW_LOCAL_CREDENTIALS_ENV, "1")
    monkeypatch.setattr(sa, "default_connection_name", lambda: "server_default")
    connected = _capture_connect(monkeypatch)

    r = _client().post(
        "/import/snowflake",
        json={"database": "D", "schema": "S", "auth": {"type": "config"}},
    )
    assert r.status_code == 200, r.text
    assert connected["connection_name"] == "server_default"


def test_api_config_auth_without_a_default_is_400(monkeypatch):
    import dcx.snowflake_auth as sa
    from dcx.snowflake_auth import ALLOW_LOCAL_CREDENTIALS_ENV

    monkeypatch.setenv(ALLOW_LOCAL_CREDENTIALS_ENV, "1")
    monkeypatch.setattr(sa, "default_connection_name", lambda: None)
    connected = _capture_connect(monkeypatch)

    r = _client().post(
        "/import/snowflake",
        json={"database": "D", "schema": "S", "auth": {"type": "config"}},
    )
    assert r.status_code == 400
    assert "default_connection_name" in r.json()["detail"]
    assert connected == {}


def test_api_gate_checked_before_the_default_is_looked_up(monkeypatch):
    """A disabled server must 403 without revealing whether it has a default."""
    import dcx.snowflake_auth as sa
    from dcx.snowflake_auth import ALLOW_LOCAL_CREDENTIALS_ENV

    monkeypatch.delenv(ALLOW_LOCAL_CREDENTIALS_ENV, raising=False)
    looked_up = []
    monkeypatch.setattr(
        sa, "default_connection_name", lambda: looked_up.append(1) or "secret_default",
    )
    r = _client().post(
        "/import/snowflake",
        json={"database": "D", "schema": "S", "auth": {"type": "config"}},
    )
    assert r.status_code == 403
    assert looked_up == []
    assert "secret_default" not in r.text


def test_tilde_private_key_path_error_explains_itself(monkeypatch):
    """The connector does not expand `~` in private_key_file; say so."""
    import snowflake.connector as connector
    import dcx.importers.snowflake as si

    def boom(**kw):
        raise FileNotFoundError(
            2, "No such file or directory", "~/keys/rsa_key.p8"
        )

    monkeypatch.setattr(connector, "connect", boom)
    with pytest.raises(si.SnowflakeImportError) as excinfo:
        si.import_snowflake({"database": "D", "schema": "S", "account": "A",
                             "user": "u", "connection_name": "dev"})
    msg = str(excinfo.value)
    assert "does not expand" in msg
    assert "absolute path" in msg


def test_ordinary_connection_error_gets_no_tilde_hint(monkeypatch):
    import snowflake.connector as connector
    import dcx.importers.snowflake as si

    monkeypatch.setattr(connector, "connect", lambda **kw: (_ for _ in ()).throw(
        Exception("Incorrect username or password was specified.")))
    with pytest.raises(si.SnowflakeImportError) as excinfo:
        si.import_snowflake({"database": "D", "schema": "S", "account": "A",
                             "user": "u", "connection_name": "dev"})
    assert "does not expand" not in str(excinfo.value)
    assert "Incorrect username" in str(excinfo.value)
