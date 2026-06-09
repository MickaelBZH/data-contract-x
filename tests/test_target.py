import textwrap

import yaml
from typer.testing import CliRunner

from dcx.cli import app

runner = CliRunner()


MINIMAL_CONTRACT = textwrap.dedent(
    """\
    apiVersion: v3.1.0
    kind: DataContract
    id: test-contract
    name: Test
    version: 1.0.0
    schema:
      - name: orders
        physicalType: table
        properties:
          - name: id
            logicalType: integer
            primaryKey: true
          - name: created_at
            logicalType: timestamp
          - name: items
            logicalType: array
            items:
              logicalType: string
          - name: meta
            logicalType: object
            properties:
              - name: source
                logicalType: string
              - name: imported_at
                logicalType: timestamp
    """
)


def _write_contract(path, content=MINIMAL_CONTRACT):
    path.write_text(content)


def _props_by_name(data, schema_index=0):
    return {p["name"]: p for p in data["schema"][schema_index]["properties"]}


def _custom_properties(prop):
    return {cp["property"]: cp["value"] for cp in prop.get("customProperties") or []}


def _target_inplace(args, contract_path):
    """Invoke `dcx target ...` and persist the result to `contract_path`.

    Default behavior is stdout, so tests that read the file back must pass
    `--output <contract_path>` to overwrite in place.
    """
    return runner.invoke(app, args + ["--output", str(contract_path)])


def test_target_snowflake_writes_server_block_and_physical_types(tmp_path):
    contract = tmp_path / "contract.yaml"
    _write_contract(contract)

    result = _target_inplace([
        "target", "snowflake", str(contract),
        "--account", "xy12345",
        "--database", "ANALYTICS",
        "--schema", "SALES",
        "--warehouse", "PROD_WH",
    ], contract)
    assert result.exit_code == 0, result.output

    data = yaml.safe_load(contract.read_text())
    server = data["servers"][0]
    assert server["server"] == "production"
    assert server["type"] == "snowflake"
    assert server["account"] == "xy12345"
    assert server["database"] == "ANALYTICS"
    assert server["schema"] == "SALES"
    assert server["warehouse"] == "PROD_WH"

    props = _props_by_name(data)
    assert props["id"]["physicalType"] == "NUMBER"
    assert props["created_at"]["physicalType"] == "TIMESTAMP_TZ"


def test_default_writes_modified_contract_to_stdout(tmp_path):
    contract = tmp_path / "contract.yaml"
    original_text = MINIMAL_CONTRACT
    _write_contract(contract)

    result = runner.invoke(app, [
        "target", "snowflake", str(contract),
        "--account", "xy", "--database", "DB", "--schema", "S",
    ])
    assert result.exit_code == 0, result.output

    # Input file is untouched
    assert contract.read_text() == original_text

    # Modified contract is printed to stdout
    data = yaml.safe_load(result.output)
    assert data["servers"][0]["type"] == "snowflake"
    assert data["servers"][0]["account"] == "xy"


def test_nested_properties_get_physical_types_recursively(tmp_path):
    contract = tmp_path / "contract.yaml"
    _write_contract(contract)

    result = _target_inplace([
        "target", "snowflake", str(contract),
        "--account", "xy", "--database", "DB", "--schema", "S",
    ], contract)
    assert result.exit_code == 0, result.output

    data = yaml.safe_load(contract.read_text())
    props = _props_by_name(data)

    assert props["items"]["items"]["physicalType"] == "STRING"
    nested = {p["name"]: p for p in props["meta"]["properties"]}
    assert nested["source"]["physicalType"] == "STRING"
    assert nested["imported_at"]["physicalType"] == "TIMESTAMP_TZ"


def test_multi_environment_same_dialect_upserts_servers(tmp_path):
    contract = tmp_path / "contract.yaml"
    _write_contract(contract)

    for env, acct, db in [
        ("prod", "prod-acct", "PROD_DB"),
        ("staging", "stg-acct", "STG_DB"),
        ("dev", "dev-acct", "DEV_DB"),
    ]:
        result = _target_inplace([
            "target", "snowflake", str(contract),
            "--server-name", env, "--environment", env,
            "--account", acct, "--database", db, "--schema", "SALES",
        ], contract)
        assert result.exit_code == 0, result.output

    data = yaml.safe_load(contract.read_text())
    servers = {s["server"]: s for s in data["servers"]}
    assert set(servers) == {"prod", "staging", "dev"}
    assert servers["prod"]["account"] == "prod-acct"
    assert servers["staging"]["account"] == "stg-acct"
    assert servers["dev"]["account"] == "dev-acct"

    props = _props_by_name(data)
    assert props["created_at"].get("physicalType") == "TIMESTAMP_TZ"
    assert "snowflakeType" not in _custom_properties(props["created_at"])


def test_upsert_replaces_existing_server_with_same_name(tmp_path):
    contract = tmp_path / "contract.yaml"
    _write_contract(contract)

    _target_inplace([
        "target", "snowflake", str(contract),
        "--server-name", "prod",
        "--account", "old-acct", "--database", "OLD", "--schema", "S",
    ], contract)
    result = _target_inplace([
        "target", "snowflake", str(contract),
        "--server-name", "prod",
        "--account", "new-acct", "--database", "NEW", "--schema", "S",
    ], contract)
    assert result.exit_code == 0, result.output

    data = yaml.safe_load(contract.read_text())
    assert len(data["servers"]) == 1
    assert data["servers"][0]["account"] == "new-acct"
    assert data["servers"][0]["database"] == "NEW"


def test_cross_type_collision_errors_without_overwrite(tmp_path):
    contract = tmp_path / "contract.yaml"
    _write_contract(contract)

    _target_inplace([
        "target", "postgres", str(contract),
        "--server-name", "production",
        "--host", "pg", "--database", "app", "--schema", "public",
    ], contract)
    result = _target_inplace([
        "target", "snowflake", str(contract),
        "--server-name", "production",  # same name, but postgres → snowflake
        "--account", "xy", "--database", "DW", "--schema", "S",
    ], contract)
    assert result.exit_code == 1
    assert "already exists with type 'postgres'" in result.output
    assert "--overwrite" in result.output

    data = yaml.safe_load(contract.read_text())
    assert data["servers"][0]["type"] == "postgres"


def test_cross_type_collision_succeeds_with_overwrite(tmp_path):
    contract = tmp_path / "contract.yaml"
    _write_contract(contract)

    _target_inplace([
        "target", "postgres", str(contract),
        "--server-name", "production",
        "--host", "pg", "--database", "app", "--schema", "public",
    ], contract)
    result = _target_inplace([
        "target", "snowflake", str(contract),
        "--server-name", "production",
        "--account", "xy", "--database", "DW", "--schema", "S",
        "--overwrite",
    ], contract)
    assert result.exit_code == 0, result.output

    data = yaml.safe_load(contract.read_text())
    assert len(data["servers"]) == 1
    assert data["servers"][0]["type"] == "snowflake"
    assert data["servers"][0]["account"] == "xy"


def test_multi_dialect_writes_custom_property_keys(tmp_path):
    contract = tmp_path / "contract.yaml"
    _write_contract(contract)

    _target_inplace([
        "target", "postgres", str(contract),
        "--server-name", "source",
        "--host", "pg.prod", "--database", "app", "--schema", "public",
    ], contract)
    result = _target_inplace([
        "target", "snowflake", str(contract),
        "--server-name", "warehouse",
        "--account", "xy", "--database", "DW", "--schema", "ANALYTICS",
    ], contract)
    assert result.exit_code == 0, result.output

    data = yaml.safe_load(contract.read_text())
    server_names = {s["server"] for s in data["servers"]}
    assert server_names == {"source", "warehouse"}

    props = _props_by_name(data)
    cps = _custom_properties(props["created_at"])
    assert cps.get("snowflakeType") == "TIMESTAMP_TZ"


def test_kafka_avro_resolves_avro_physical_types(tmp_path):
    contract = tmp_path / "contract.yaml"
    _write_contract(contract)

    result = _target_inplace([
        "target", "kafka", str(contract),
        "--host", "kafka:9092", "--format", "avro",
    ], contract)
    assert result.exit_code == 0, result.output

    data = yaml.safe_load(contract.read_text())
    assert data["servers"][0]["type"] == "kafka"
    assert data["servers"][0]["format"] == "avro"

    props = _props_by_name(data)
    assert props["id"]["physicalType"] == "long"
    assert props["created_at"]["physicalType"] == "timestamp-micros"
    # Nested
    nested = {p["name"]: p for p in props["meta"]["properties"]}
    assert nested["source"]["physicalType"] == "string"
    assert nested["imported_at"]["physicalType"] == "timestamp-micros"
    # Array items
    assert props["items"]["items"]["physicalType"] == "string"


def test_kafka_protobuf_resolves_protobuf_physical_types(tmp_path):
    contract = tmp_path / "contract.yaml"
    _write_contract(contract)

    result = _target_inplace([
        "target", "kafka", str(contract),
        "--host", "kafka:9092", "--format", "protobuf",
    ], contract)
    assert result.exit_code == 0, result.output

    props = _props_by_name(yaml.safe_load(contract.read_text()))
    assert props["id"]["physicalType"] == "int64"
    assert props["created_at"]["physicalType"] == "google.protobuf.Timestamp"
    assert props["meta"]["physicalType"] == "message"
    assert props["items"]["physicalType"] == "repeated"


def test_s3_parquet_resolves_parquet_physical_types(tmp_path):
    contract = tmp_path / "contract.yaml"
    _write_contract(contract)

    result = _target_inplace([
        "target", "s3", str(contract),
        "--location", "s3://bucket/orders/", "--format", "parquet",
    ], contract)
    assert result.exit_code == 0, result.output

    data = yaml.safe_load(contract.read_text())
    assert data["servers"][0]["type"] == "s3"
    assert data["servers"][0]["format"] == "parquet"

    props = _props_by_name(data)
    assert props["id"]["physicalType"] == "INT64"
    assert props["created_at"]["physicalType"] == "TIMESTAMP_MICROS"
    assert props["meta"]["physicalType"] == "STRUCT"
    assert props["items"]["physicalType"] == "LIST"


def test_local_json_resolves_jsonschema_physical_types(tmp_path):
    contract = tmp_path / "contract.yaml"
    _write_contract(contract)

    result = _target_inplace([
        "target", "local", str(contract),
        "--path", "/data/orders.json", "--format", "json",
    ], contract)
    assert result.exit_code == 0, result.output

    props = _props_by_name(yaml.safe_load(contract.read_text()))
    assert props["id"]["physicalType"] == "integer"
    assert props["created_at"]["physicalType"] == "string"  # JSON has no native timestamp
    assert props["meta"]["physicalType"] == "object"
    assert props["items"]["physicalType"] == "array"


def test_kafka_xml_format_skips_resolution_with_warning(tmp_path):
    contract = tmp_path / "contract.yaml"
    _write_contract(contract)

    result = _target_inplace([
        "target", "kafka", str(contract),
        "--host", "kafka:9092", "--format", "xml",
    ], contract)
    assert result.exit_code == 0, result.output
    assert "No physical-type resolver for format 'xml'" in result.output

    props = _props_by_name(yaml.safe_load(contract.read_text()))
    assert "physicalType" not in props["created_at"]


def test_s3_csv_format_skips_resolution_with_warning(tmp_path):
    contract = tmp_path / "contract.yaml"
    _write_contract(contract)

    result = _target_inplace([
        "target", "s3", str(contract),
        "--location", "s3://bucket/orders/", "--format", "csv",
    ], contract)
    assert result.exit_code == 0, result.output
    assert "No physical-type resolver for format 'csv'" in result.output

    props = _props_by_name(yaml.safe_load(contract.read_text()))
    assert "physicalType" not in props["created_at"]


def test_invalid_server_type_errors(tmp_path):
    contract = tmp_path / "contract.yaml"
    _write_contract(contract)

    result = runner.invoke(app, ["target", "not-a-type", str(contract)])
    assert result.exit_code == 2
    assert "No such command" in result.output


def test_invalid_format_for_type_errors(tmp_path):
    contract = tmp_path / "contract.yaml"
    _write_contract(contract)

    result = runner.invoke(app, [
        "target", "kafka", str(contract),
        "--host", "k:9092", "--format", "parquet",  # parquet not in KafkaFormat enum
    ])
    assert result.exit_code == 2
    assert "Invalid value for '--format'" in result.output


EXPECTED_SUBCOMMANDS = {
    # cloud warehouses + SQL DBs
    "snowflake", "postgres", "postgresql", "mysql", "redshift", "databricks",
    "bigquery", "sqlserver", "synapse", "oracle", "trino", "duckdb", "presto",
    # other SQL (server block only)
    "clickhouse", "db2", "denodo", "dremio", "hive", "impala", "informix",
    "vertica", "cloudsql", "zen",
    # streaming
    "kafka", "kinesis", "pubsub",
    # object stores / files
    "s3", "azure", "sftp", "local", "glue",
    # specials
    "athena", "api",
}


def test_help_lists_all_supported_server_types():
    """Every ODCS server type except `custom` must be a target subcommand."""
    assert len(EXPECTED_SUBCOMMANDS) == 33
    result = runner.invoke(app, ["target", "--help"])
    assert result.exit_code == 0
    for cmd in EXPECTED_SUBCOMMANDS:
        assert cmd in result.output, f"missing subcommand: {cmd}"


def test_oracle_resolves_physical_types(tmp_path):
    contract = tmp_path / "contract.yaml"
    _write_contract(contract)
    result = _target_inplace([
        "target", "oracle", str(contract),
        "--host", "ora.prod", "--service-name", "ORCL",
    ], contract)
    assert result.exit_code == 0, result.output
    data = yaml.safe_load(contract.read_text())
    assert data["servers"][0]["type"] == "oracle"
    assert data["servers"][0]["serviceName"] == "ORCL"
    props = _props_by_name(data)
    # Oracle converter exists, so physical types should be resolved.
    assert "physicalType" in props["created_at"]


def test_trino_resolves_physical_types(tmp_path):
    contract = tmp_path / "contract.yaml"
    _write_contract(contract)
    result = _target_inplace([
        "target", "trino", str(contract),
        "--host", "trino.prod", "--catalog", "hive", "--schema", "default",
    ], contract)
    assert result.exit_code == 0, result.output
    data = yaml.safe_load(contract.read_text())
    assert data["servers"][0]["type"] == "trino"
    assert data["servers"][0]["catalog"] == "hive"
    assert data["servers"][0]["schema"] == "default"
    props = _props_by_name(data)
    assert "physicalType" in props["created_at"]


def test_no_converter_type_writes_server_block_only(tmp_path):
    """Server types without a SQL converter (e.g. clickhouse) update the server block but skip types."""
    contract = tmp_path / "contract.yaml"
    _write_contract(contract)
    result = _target_inplace([
        "target", "clickhouse", str(contract),
        "--host", "ch.prod", "--database", "metrics",
    ], contract)
    assert result.exit_code == 0, result.output
    assert "No physical-type resolver" in result.output or "not supported" in result.output
    data = yaml.safe_load(contract.read_text())
    assert data["servers"][0]["type"] == "clickhouse"
    props = _props_by_name(data)
    assert "physicalType" not in props["created_at"]


def test_azure_parquet_resolves_parquet_physical_types(tmp_path):
    contract = tmp_path / "contract.yaml"
    _write_contract(contract)
    result = _target_inplace([
        "target", "azure", str(contract),
        "--location", "abfss://container@account.dfs.core.windows.net/orders/",
        "--format", "parquet",
    ], contract)
    assert result.exit_code == 0, result.output
    data = yaml.safe_load(contract.read_text())
    assert data["servers"][0]["type"] == "azure"
    props = _props_by_name(data)
    assert props["id"]["physicalType"] == "INT64"
    assert props["created_at"]["physicalType"] == "TIMESTAMP_MICROS"


def test_snowflake_help_shows_only_snowflake_options():
    result = runner.invoke(app, ["target", "snowflake", "--help"])
    assert result.exit_code == 0
    assert "--account" in result.output
    assert "--warehouse" in result.output
    assert "--project" not in result.output
    assert "--dataset" not in result.output
    assert "--bootstrap" not in result.output
    assert "--region-name" not in result.output
