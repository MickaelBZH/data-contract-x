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


# === OAuth import path ======================================================


def test_import_snowflake_oauth_uses_token(monkeypatch):
    import snowflake.connector as connector
    from dcx.importers.snowflake import import_snowflake_oauth

    captured = {}

    def fake_connect(**kw):
        captured.update(kw)
        return _FakeConn(_fake_data())

    monkeypatch.setattr(connector, "connect", fake_connect)
    contract = import_snowflake_oauth(
        token="tok123", account="ACME", database="DB", schema="SCH", tables=["CUSTOMER"],
    )
    assert captured["authenticator"] == "oauth"
    assert captured["token"] == "tok123"
    assert captured["account"] == "ACME"
    assert "password" not in captured        # never falls back to other secrets
    assert [o.name for o in contract.schema_] == ["CUSTOMER"]


def test_import_snowflake_oauth_requires_token():
    from dcx.importers.snowflake import import_snowflake_oauth
    with pytest.raises(SnowflakeImportError, match="OAuth token is required"):
        import_snowflake_oauth(token="", account="A", database="D", schema="S")


# === API endpoint ===========================================================


def _client():
    from fastapi.testclient import TestClient
    from dcx.api import build_dcx_api_app
    return TestClient(build_dcx_api_app())


def test_api_snowflake_requires_bearer_token():
    r = _client().post("/import/snowflake", json={"account": "A", "database": "D", "schema": "S"})
    assert r.status_code == 401
    assert "Bearer" in r.json()["detail"]


def test_api_snowflake_works(monkeypatch):
    import dcx.importers.snowflake as si
    captured = {}

    def fake(**kw):
        captured.update(kw)
        return OpenDataContractStandard(
            apiVersion="v3.1.0", kind="DataContract", id="x", name="X", version="1.0.0",
        )

    monkeypatch.setattr(si, "import_snowflake_oauth", fake)
    r = _client().post(
        "/import/snowflake",
        headers={"Authorization": "Bearer tok-xyz"},
        json={"account": "ACME", "database": "DB", "schema": "SCH", "tables": ["T"]},
    )
    assert r.status_code == 200, r.text
    assert captured["token"] == "tok-xyz"
    assert captured["account"] == "ACME"
    assert captured["schema"] == "SCH"       # body "schema" → schema_ → schema kwarg
    assert captured["tables"] == ["T"]


def test_api_snowflake_error_is_502(monkeypatch):
    import dcx.importers.snowflake as si

    def boom(**kw):
        raise si.SnowflakeImportError("Snowflake connection failed: bad token")

    monkeypatch.setattr(si, "import_snowflake_oauth", boom)
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
    return import_snowflake({"database": "DB", "schema": "SCH", "account": "ACME"})


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
    contract = import_snowflake({"database": "DB", "schema": "SCH", "account": "ACME"})
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
    contract = import_snowflake({"database": "DB", "schema": "SCH", "account": "ACME"})
    rule = {p.name: p for p in _obj(contract).properties}["ID"].quality[0]
    assert rule.mustBe == 0
    assert "2 expectations" in capsys.readouterr().err
