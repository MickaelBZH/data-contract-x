import yaml as yamllib
import pytest
from fastapi.testclient import TestClient

from dcx.api import build_dcx_api_app


@pytest.fixture
def client():
    return TestClient(build_dcx_api_app())


MINIMAL_CONTRACT = {
    "apiVersion": "v3.1.0",
    "kind": "DataContract",
    "id": "test",
    "name": "Test",
    "version": "1.0.0",
    "schema": [
        {
            "name": "orders",
            "physicalType": "table",
            "properties": [
                {"name": "id", "logicalType": "integer"},
                {"name": "created_at", "logicalType": "timestamp"},
            ],
        }
    ],
}


def _props(contract_dict, idx=0):
    return {p["name"]: p for p in contract_dict["schema"][idx]["properties"]}


def test_all_33_target_endpoints_registered():
    app = build_dcx_api_app()
    target_paths = {
        r.path for r in app.routes if getattr(r, "path", "").startswith("/target/")
    }
    assert len(target_paths) == 33


def test_combined_serve_app_has_upstream_and_dcx_routes():
    """`dcx.serve:app` (what `dcx api` runs) exposes both upstream + dcx routes."""
    from dcx.serve import app as combined_app
    paths = {getattr(r, "path", "") for r in combined_app.routes}
    assert "/lint" in paths
    assert "/test" in paths
    assert "/changelog" in paths
    assert "/export" in paths
    assert "/target/snowflake" in paths
    assert "/target/kafka" in paths
    assert "/target/api" in paths
    assert "/info" in paths
    assert "/import/json" in paths
    assert "/export/sql" in paths


def test_combined_app_dcx_endpoint_works():
    """Hit a dcx /target endpoint on the combined serve app via TestClient."""
    from dcx.serve import app as combined_app
    client = TestClient(combined_app)
    response = client.post(
        "/target/snowflake",
        json={
            "contract": MINIMAL_CONTRACT,
            "options": {"account": "xy", "database": "DW", "schema": "S"},
        },
    )
    assert response.status_code == 200, response.text
    assert response.json()["contract"]["servers"][0]["type"] == "snowflake"


def test_combined_app_upstream_endpoint_still_works():
    """The upstream /lint route is still functional after we mount our routes."""
    from dcx.serve import app as combined_app
    client = TestClient(combined_app)
    yaml_body = yamllib.safe_dump(MINIMAL_CONTRACT)
    response = client.post(
        "/lint",
        content=yaml_body,
        headers={"Content-Type": "application/yaml"},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert "result" in body and "checks" in body


def test_dcx_api_command_registered_and_replaces_upstream():
    """`dcx api` is registered with our help text, not the upstream one."""
    from dcx.cli import app as cli_app
    api_cmds = [c for c in cli_app.registered_commands if c.name == "api"]
    assert len(api_cmds) == 1, "should have exactly one `api` command (ours)"
    callback = api_cmds[0].callback
    assert callback.__module__ == "dcx.cli"
    assert "dcx REST API" in (callback.__doc__ or "")


# === /info ==================================================================


def test_info_endpoint_returns_versions(client):
    response = client.get("/info")
    assert response.status_code == 200
    data = response.json()
    assert "dcx" in data
    assert "datacontract_cli" in data
    assert data["dcx"].count(".") >= 2


# === /import/* ==============================================================


def test_import_endpoints_registered():
    app = build_dcx_api_app()
    import_paths = {
        r.path for r in app.routes if getattr(r, "path", "").startswith("/import/")
    }
    # 16 file-based importers (generic mirror) + the dedicated OAuth snowflake one.
    assert len(import_paths) == 17
    assert "/import/snowflake" in import_paths


def test_import_json_from_inline_content(client):
    inline = '{"order_id": 42, "amount": 19.99, "placed_at": "2024-01-15T10:30:00Z"}'
    response = client.post(
        "/import/json",
        json={"source_content": inline, "options": {}},
    )
    assert response.status_code == 200, response.text
    contract = response.json()["contract"]
    assert contract["kind"] == "DataContract"
    assert contract["schema"][0]["properties"]
    prop_names = {p["name"] for p in contract["schema"][0]["properties"]}
    assert {"order_id", "amount", "placed_at"} <= prop_names


def test_import_sql_from_inline_content(client):
    inline = "CREATE TABLE orders (id INTEGER PRIMARY KEY, amount NUMERIC(18,2), placed_at TIMESTAMP);"
    response = client.post(
        "/import/sql",
        json={
            "source_content": inline,
            "options": {"dialect": "postgres"},
        },
    )
    assert response.status_code == 200, response.text
    contract = response.json()["contract"]
    schema = contract["schema"][0]
    assert schema["name"] == "orders"
    prop_names = {p["name"] for p in schema["properties"]}
    assert {"id", "amount", "placed_at"} <= prop_names


def test_import_jsonschema_from_inline_content(client):
    inline = """{
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "title": "Order",
      "properties": {
        "id": {"type": "integer"},
        "amount": {"type": "number"}
      }
    }"""
    response = client.post(
        "/import/jsonschema",
        json={"source_content": inline, "options": {}},
    )
    assert response.status_code == 200, response.text
    contract = response.json()["contract"]
    assert contract["kind"] == "DataContract"


def test_import_returns_yaml_via_format_query(client):
    inline = '{"x": 1}'
    response = client.post(
        "/import/json?format=yaml",
        json={"source_content": inline, "options": {}},
    )
    assert response.status_code == 200
    assert "text/yaml" in response.headers["content-type"]
    parsed = yamllib.safe_load(response.text)
    assert parsed["kind"] == "DataContract"


# === /export/* ==============================================================


CONTRACT_FOR_EXPORT = {
    "apiVersion": "v3.1.0",
    "kind": "DataContract",
    "id": "ex",
    "name": "Ex",
    "version": "1.0.0",
    "status": "draft",
    "servers": [
        {"server": "production", "type": "snowflake",
         "account": "xy", "database": "DW", "schema": "S"},
    ],
    "schema": [
        {
            "name": "orders",
            "physicalType": "table",
            "properties": [
                {"name": "id", "logicalType": "integer", "primaryKey": True,
                 "physicalType": "NUMBER"},
                {"name": "amount", "logicalType": "number", "physicalType": "NUMBER(18,2)"},
            ],
        }
    ],
}


def test_export_registered_for_most_formats():
    app = build_dcx_api_app()
    paths = {r.path for r in app.routes if getattr(r, "path", "").startswith("/export/")}
    assert len(paths) >= 25
    for name in ("sql", "html", "odcs", "jsonschema", "markdown", "excel"):
        assert f"/export/{name}" in paths


def test_export_sql_returns_text(client):
    response = client.post(
        "/export/sql",
        json={"contract": CONTRACT_FOR_EXPORT, "options": {}},
    )
    assert response.status_code == 200, response.text
    assert "text/plain" in response.headers["content-type"]
    assert "CREATE TABLE" in response.text
    assert "orders" in response.text


def test_export_jsonschema_returns_application_json(client):
    response = client.post(
        "/export/jsonschema",
        json={"contract": CONTRACT_FOR_EXPORT, "options": {}},
    )
    assert response.status_code == 200, response.text
    assert "application/json" in response.headers["content-type"]
    schema = response.json()
    assert schema.get("$schema") or schema.get("type")


def test_export_odcs_returns_yaml_by_default(client):
    response = client.post(
        "/export/odcs",
        json={"contract": CONTRACT_FOR_EXPORT, "options": {}},
    )
    assert response.status_code == 200, response.text
    assert "text/plain" in response.headers["content-type"]
    parsed = yamllib.safe_load(response.text)
    assert parsed["id"] == "ex"


def test_export_odcs_returns_json_with_format_query(client):
    response = client.post(
        "/export/odcs?format=json",
        json={"contract": CONTRACT_FOR_EXPORT, "options": {}},
    )
    assert response.status_code == 200, response.text
    assert "application/json" in response.headers["content-type"]
    body = response.json()
    assert body["contract"]["id"] == "ex"


def test_export_html_returns_text(client):
    response = client.post(
        "/export/html",
        json={"contract": CONTRACT_FOR_EXPORT, "options": {}},
    )
    assert response.status_code == 200, response.text
    assert "text/plain" in response.headers["content-type"]
    assert "<html" in response.text.lower() or "<!doctype" in response.text.lower()


def test_export_accepts_yaml_string_for_contract(client):
    yaml_str = yamllib.safe_dump(CONTRACT_FOR_EXPORT)
    response = client.post(
        "/export/sql",
        json={"contract": yaml_str, "options": {}},
    )
    assert response.status_code == 200, response.text
    assert "CREATE TABLE" in response.text


# === Format negotiation matrix ==============================================


def test_target_yaml_body_in_via_contract_string_json_out(client):
    yaml_contract = yamllib.safe_dump(MINIMAL_CONTRACT)
    response = client.post(
        "/target/snowflake",
        json={
            "contract": yaml_contract,
            "options": {"account": "xy", "database": "DW", "schema": "S"},
        },
    )
    assert response.status_code == 200
    assert "application/json" in response.headers["content-type"]
    assert response.json()["contract"]["servers"][0]["type"] == "snowflake"


def test_target_yaml_body_in_via_contract_string_yaml_out(client):
    yaml_contract = yamllib.safe_dump(MINIMAL_CONTRACT)
    response = client.post(
        "/target/snowflake",
        json={
            "contract": yaml_contract,
            "options": {"account": "xy", "database": "DW", "schema": "S"},
        },
        headers={"Accept": "text/yaml"},
    )
    assert response.status_code == 200
    assert "text/yaml" in response.headers["content-type"]
    assert yamllib.safe_load(response.text)["servers"][0]["type"] == "snowflake"


def test_import_chains_with_target_via_api(client):
    """End-to-end: import a JSON sample, then target snowflake — both via API."""
    inline = '{"id": 1, "amount": 19.99, "name": "widget"}'

    imp = client.post("/import/json", json={"source_content": inline, "options": {}})
    assert imp.status_code == 200, imp.text
    contract = imp.json()["contract"]

    tgt = client.post(
        "/target/snowflake",
        json={
            "contract": contract,
            "options": {
                "account": "xy",
                "database": "DW",
                "schema": "SALES",
                "overwrite": True,
            },
        },
    )
    assert tgt.status_code == 200, tgt.text
    final = tgt.json()["contract"]
    assert final["servers"][0]["type"] == "snowflake"
    assert final["servers"][0]["account"] == "xy"
    props = {p["name"]: p for p in final["schema"][0]["properties"]}
    assert props["id"]["physicalType"] == "NUMBER"
    assert props["amount"]["physicalType"] == "NUMBER"
    assert props["name"]["physicalType"] == "STRING"


def test_snowflake_returns_json_with_resolved_physical_types(client):
    response = client.post(
        "/target/snowflake",
        json={
            "contract": MINIMAL_CONTRACT,
            "options": {
                "account": "xy",
                "database": "DW",
                "schema": "SALES",
            },
        },
    )
    assert response.status_code == 200, response.text
    contract = response.json()["contract"]
    server = contract["servers"][0]
    assert server["type"] == "snowflake"
    assert server["account"] == "xy"
    assert server["database"] == "DW"
    assert server["schema"] == "SALES"
    props = _props(contract)
    assert props["id"]["physicalType"] == "NUMBER"
    assert props["created_at"]["physicalType"] == "TIMESTAMP_TZ"


def test_kafka_avro_resolves_avro_types(client):
    response = client.post(
        "/target/kafka",
        json={
            "contract": MINIMAL_CONTRACT,
            "options": {"host": "kafka:9092", "format": "avro"},
        },
    )
    assert response.status_code == 200, response.text
    contract = response.json()["contract"]
    assert contract["servers"][0]["format"] == "avro"
    props = _props(contract)
    assert props["id"]["physicalType"] == "long"
    assert props["created_at"]["physicalType"] == "timestamp-micros"


def test_accepts_yaml_string_in_contract_field(client):
    yaml_contract = yamllib.safe_dump(MINIMAL_CONTRACT)
    response = client.post(
        "/target/snowflake",
        json={
            "contract": yaml_contract,
            "options": {
                "account": "xy",
                "database": "DW",
                "schema": "SALES",
            },
        },
    )
    assert response.status_code == 200, response.text
    contract = response.json()["contract"]
    assert contract["servers"][0]["type"] == "snowflake"


def test_returns_yaml_via_format_query(client):
    response = client.post(
        "/target/snowflake?format=yaml",
        json={
            "contract": MINIMAL_CONTRACT,
            "options": {"account": "xy", "database": "DW", "schema": "SALES"},
        },
    )
    assert response.status_code == 200
    assert "text/yaml" in response.headers["content-type"]
    parsed = yamllib.safe_load(response.text)
    assert parsed["servers"][0]["type"] == "snowflake"


def test_returns_yaml_via_accept_header(client):
    response = client.post(
        "/target/snowflake",
        json={
            "contract": MINIMAL_CONTRACT,
            "options": {"account": "xy", "database": "DW", "schema": "SALES"},
        },
        headers={"Accept": "text/yaml"},
    )
    assert response.status_code == 200
    assert "text/yaml" in response.headers["content-type"]


def test_type_conflict_returns_409(client):
    contract_with_postgres = {
        **MINIMAL_CONTRACT,
        "servers": [
            {"server": "production", "type": "postgres",
             "host": "pg", "database": "app", "schema": "public"},
        ],
    }
    response = client.post(
        "/target/snowflake",
        json={
            "contract": contract_with_postgres,
            "options": {
                "server_name": "production",
                "account": "xy", "database": "DW", "schema": "S",
            },
        },
    )
    assert response.status_code == 409
    detail = response.json()["detail"]
    assert "already exists with type 'postgres'" in detail


def test_overwrite_resolves_type_conflict(client):
    contract_with_postgres = {
        **MINIMAL_CONTRACT,
        "servers": [
            {"server": "production", "type": "postgres",
             "host": "pg", "database": "app", "schema": "public"},
        ],
    }
    response = client.post(
        "/target/snowflake",
        json={
            "contract": contract_with_postgres,
            "options": {
                "server_name": "production",
                "account": "xy", "database": "DW", "schema": "S",
                "overwrite": True,
            },
        },
    )
    assert response.status_code == 200, response.text
    result = response.json()["contract"]
    assert len(result["servers"]) == 1
    assert result["servers"][0]["type"] == "snowflake"


def test_invalid_contract_type_rejected(client):
    """Pydantic catches non-dict/non-string contracts at body validation (422)."""
    response = client.post(
        "/target/snowflake",
        json={
            "contract": 123,
            "options": {"account": "xy", "database": "DW", "schema": "S"},
        },
    )
    assert response.status_code == 422


def test_missing_options_returns_422(client):
    """`options` is required at the root."""
    response = client.post(
        "/target/snowflake",
        json={"contract": MINIMAL_CONTRACT},  # missing `options`
    )
    assert response.status_code == 422


def test_missing_required_option_returns_422(client):
    """Required fields inside `options` are validated."""
    response = client.post(
        "/target/snowflake",
        json={"contract": MINIMAL_CONTRACT, "options": {}},  # missing account/database/schema
    )
    assert response.status_code == 422


def test_round_trip_cli_and_api_match(tmp_path):
    """The API result should equal the CLI result for the same operation."""
    client = TestClient(build_dcx_api_app())
    api_response = client.post(
        "/target/snowflake",
        json={
            "contract": MINIMAL_CONTRACT,
            "options": {
                "account": "xy", "database": "DW", "schema": "SALES",
                "warehouse": "PROD_WH",
            },
        },
    )
    assert api_response.status_code == 200
    api_contract = api_response.json()["contract"]

    from typer.testing import CliRunner
    from dcx.cli import app
    contract_file = tmp_path / "contract.yaml"
    contract_file.write_text(yamllib.safe_dump(MINIMAL_CONTRACT))
    cli_result = CliRunner().invoke(app, [
        "target", "snowflake", str(contract_file),
        "--account", "xy", "--database", "DW", "--schema", "SALES",
        "--warehouse", "PROD_WH",
        "--output", str(contract_file),
    ])
    assert cli_result.exit_code == 0, cli_result.output
    cli_contract = yamllib.safe_load(contract_file.read_text())

    assert api_contract["servers"] == cli_contract["servers"]
    api_props = _props(api_contract)
    cli_props = _props(cli_contract)
    for name in api_props:
        assert api_props[name].get("physicalType") == cli_props[name].get("physicalType")


def test_serve_registers_custom_export_endpoint_in_isolation():
    """A fresh uvicorn worker imports only `dcx.serve` — not `dcx.cli`. The custom
    `export snowflake-full` endpoint must still register (regression: it depended on
    dcx.cli importing the command module)."""
    import json
    import subprocess
    import sys

    code = (
        "import json, dcx.serve as s;"
        "p = s.app.openapi()['paths'];"
        "print(json.dumps(["
        "  '/export/snowflake-full' in p,"
        "  '/import/snowflake' in p,"
        "  '/apply/snowflake' in p,"
        "]))"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    assert json.loads(out.stdout.strip()) == [True, True, True]
