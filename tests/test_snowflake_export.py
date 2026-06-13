import textwrap

import yaml as yamllib
from open_data_contract_standard.model import OpenDataContractStandard
from typer.testing import CliRunner

from dcx.cli import app
from dcx.exporters.snowflake import to_snowflake_full_sql

runner = CliRunner()


CONTRACT_YAML = textwrap.dedent(
    """\
    apiVersion: v3.1.0
    kind: DataContract
    id: orders-contract
    name: Orders
    version: 1.0.0
    status: draft
    tags:
      - transactional
    servers:
      - server: production
        type: snowflake
        account: xy12345
        database: SALES_DB
        schema: SALES
    schema:
      - name: orders
        physicalType: table
        tags:
          - critical
        properties:
          - name: id
            logicalType: integer
            physicalType: NUMBER
            primaryKey: true
            quality:
              - type: library
                metric: nullValues
                mustBe: 0
                severity: error
          - name: email
            logicalType: string
            physicalType: VARCHAR(255)
            classification: PII
            tags:
              - customer
        quality:
          - type: library
            metric: rowCount
            mustBeGreaterThan: 0
    """
)


def _load_contract() -> OpenDataContractStandard:
    return OpenDataContractStandard.from_string(CONTRACT_YAML)


# === Pure function tests ====================================================


def test_ddl_section_always_present():
    sql = to_snowflake_full_sql(_load_contract())
    assert "-- ===== DDL =====" in sql
    # Table is fully-qualified using the contract's server db.schema
    assert "CREATE TABLE SALES_DB.SALES.orders" in sql
    assert "NUMBER" in sql
    assert "VARCHAR(255)" in sql


def test_tags_section_default_on():
    sql = to_snowflake_full_sql(_load_contract())
    assert "-- ===== Tags =====" in sql
    # Schema-level tag (table qualified by db.schema)
    assert "ALTER TABLE SALES_DB.SALES.orders SET TAG critical = 'critical';" in sql
    # Column classification
    assert (
        "ALTER TABLE SALES_DB.SALES.orders MODIFY COLUMN email "
        "SET TAG classification = 'PII';"
    ) in sql
    # Column tag
    assert (
        "ALTER TABLE SALES_DB.SALES.orders MODIFY COLUMN email "
        "SET TAG customer = 'customer';"
    ) in sql


def test_quality_section_off_by_default():
    sql = to_snowflake_full_sql(_load_contract())
    assert "DATA METRIC FUNCTION" not in sql
    assert "Data Quality" not in sql


def test_quality_section_when_enabled():
    sql = to_snowflake_full_sql(_load_contract(), include_quality=True)
    assert "-- ===== Data Quality (Data Metric Functions) =====" in sql
    assert "Snowflake Enterprise feature" in sql
    assert "ALTER TABLE SALES_DB.SALES.orders SET DATA_METRIC_SCHEDULE" in sql
    assert (
        "ALTER TABLE SALES_DB.SALES.orders ADD DATA METRIC FUNCTION "
        "SNOWFLAKE.CORE.ROW_COUNT ON ();"
    ) in sql
    # `nullValues` maps to NULL_COUNT
    assert (
        "ALTER TABLE SALES_DB.SALES.orders ADD DATA METRIC FUNCTION "
        "SNOWFLAKE.CORE.NULL_COUNT ON (id);"
    ) in sql


def test_create_tags_emits_create_tag_if_not_exists():
    sql = to_snowflake_full_sql(_load_contract(), create_tags=True)
    assert "CREATE TAG IF NOT EXISTS critical;" in sql
    assert "CREATE TAG IF NOT EXISTS customer;" in sql
    assert "CREATE TAG IF NOT EXISTS classification;" in sql


def test_tag_namespace_qualifies_references():
    sql = to_snowflake_full_sql(
        _load_contract(), create_tags=True, tag_namespace="GOV.TAGS",
    )
    assert "CREATE TAG IF NOT EXISTS GOV.TAGS.critical;" in sql
    assert "SET TAG GOV.TAGS.critical = 'critical';" in sql
    assert "SET TAG GOV.TAGS.classification = 'PII';" in sql


def test_fully_qualified_tag_not_double_qualified():
    """A namespaced tag (e.g. imported as DB.SCHEMA.NAME) keeps its own namespace;
    --tag-namespace only qualifies bare tags."""
    yaml_fq = textwrap.dedent(
        """\
        apiVersion: v3.1.0
        kind: DataContract
        id: ex
        name: Ex
        version: 1.0.0
        status: draft
        schema:
          - name: customers
            physicalType: table
            properties:
              - name: email
                physicalType: STRING
                tags:
                  - GOVERNANCE.TAGS.DATA_CLASSIFICATION=PD_DATA   # already qualified
                  - sensitive                                    # bare
        """
    )
    contract = OpenDataContractStandard.from_string(yaml_fq)
    sql = to_snowflake_full_sql(contract, tag_namespace="EXTRA.NS")
    # FQ tag keeps its own namespace, not EXTRA.NS.GOVERNANCE.TAGS...
    assert "SET TAG GOVERNANCE.TAGS.DATA_CLASSIFICATION = 'PD_DATA';" in sql
    assert "EXTRA.NS.GOVERNANCE" not in sql
    # ...while the bare tag is still qualified by --tag-namespace.
    assert "SET TAG EXTRA.NS.sensitive = 'sensitive';" in sql


def test_no_tags_when_disabled():
    sql = to_snowflake_full_sql(_load_contract(), include_tags=False)
    assert "SET TAG" not in sql
    assert "Tags" not in sql


def test_unmappable_quality_emits_todo():
    yaml_with_unknown = textwrap.dedent(
        """\
        apiVersion: v3.1.0
        kind: DataContract
        id: ex
        name: Ex
        version: 1.0.0
        status: draft
        schema:
          - name: orders
            physicalType: table
            properties:
              - name: id
                logicalType: integer
                physicalType: NUMBER
            quality:
              - type: custom
                name: business_rule
                query: SELECT * FROM orders WHERE bad_state = true
        """
    )
    contract = OpenDataContractStandard.from_string(yaml_with_unknown)
    sql = to_snowflake_full_sql(contract, include_quality=True)
    assert "-- TODO: unmappable quality rule 'business_rule'" in sql
    assert "type=custom" in sql


def test_custom_metric_schedule():
    sql = to_snowflake_full_sql(
        _load_contract(),
        include_quality=True,
        metric_schedule="USING CRON 0 */6 * * * UTC",
    )
    assert "USING CRON 0 */6 * * * UTC" in sql


# === NAME=VALUE tag convention ==============================================


CONTRACT_WITH_NAMED_TAGS = textwrap.dedent(
    """\
    apiVersion: v3.1.0
    kind: DataContract
    id: governed-customers
    name: Customers
    version: 1.0.0
    status: draft
    servers:
      - server: production
        type: snowflake
        account: xy12345
        database: SALES_DB
        schema: SALES
    schema:
      - name: customers
        physicalType: table
        properties:
          - name: email
            logicalType: string
            physicalType: STRING
            tags:
              - DATA_CLASSIFICATION=PD_DATA
              - CIA_CONFIDENTIALITY=CIA_CONF_CONF
              - TRADE_SECRET_CLASSIFICATION=NON_TS_DATA
          - name: customer_id
            logicalType: integer
            physicalType: NUMBER
            tags:
              - DATA_CLASSIFICATION=PD_DATA
              - sensitive   # plain tag still works
    """
)


def test_name_equals_value_tag_splits_correctly():
    contract = OpenDataContractStandard.from_string(CONTRACT_WITH_NAMED_TAGS)
    sql = to_snowflake_full_sql(contract)
    # Named tags split on `=`
    assert "MODIFY COLUMN email SET TAG DATA_CLASSIFICATION = 'PD_DATA';" in sql
    assert "MODIFY COLUMN email SET TAG CIA_CONFIDENTIALITY = 'CIA_CONF_CONF';" in sql
    assert "MODIFY COLUMN email SET TAG TRADE_SECRET_CLASSIFICATION = 'NON_TS_DATA';" in sql
    # Plain tags still work (value = name)
    assert "MODIFY COLUMN customer_id SET TAG sensitive = 'sensitive';" in sql


def test_create_tags_dedupes_by_name_not_value():
    """Multiple values for the same TAG_NAME → one CREATE TAG IF NOT EXISTS."""
    contract = OpenDataContractStandard.from_string(CONTRACT_WITH_NAMED_TAGS)
    sql = to_snowflake_full_sql(contract, create_tags=True)
    # `DATA_CLASSIFICATION=PD_DATA` appears on two columns, but should only
    # produce one `CREATE TAG IF NOT EXISTS DATA_CLASSIFICATION;`
    create_lines = [line for line in sql.splitlines() if line.startswith("CREATE TAG")]
    names = [line.split()[-1].rstrip(";") for line in create_lines]
    assert names.count("DATA_CLASSIFICATION") == 1
    assert "CIA_CONFIDENTIALITY" in names
    assert "TRADE_SECRET_CLASSIFICATION" in names
    assert "sensitive" in names


def test_named_tag_with_namespace():
    contract = OpenDataContractStandard.from_string(CONTRACT_WITH_NAMED_TAGS)
    sql = to_snowflake_full_sql(
        contract, create_tags=True, tag_namespace="GOVERNANCE_DB.DATA_CLASSIFICATION",
    )
    assert (
        "CREATE TAG IF NOT EXISTS GOVERNANCE_DB.DATA_CLASSIFICATION.DATA_CLASSIFICATION;"
        in sql
    )
    assert (
        "MODIFY COLUMN email SET TAG "
        "GOVERNANCE_DB.DATA_CLASSIFICATION.DATA_CLASSIFICATION = 'PD_DATA';"
    ) in sql


def test_named_tag_with_special_chars_in_value():
    """Single quotes in values are SQL-escaped."""
    yaml = textwrap.dedent(
        """\
        apiVersion: v3.1.0
        kind: DataContract
        id: ex
        name: Ex
        version: 1.0.0
        status: draft
        schema:
          - name: t
            physicalType: table
            properties:
              - name: c
                logicalType: string
                physicalType: STRING
                tags:
                  - "OWNER=Alice O'Brien"
        """
    )
    contract = OpenDataContractStandard.from_string(yaml)
    sql = to_snowflake_full_sql(contract)
    assert "SET TAG OWNER = 'Alice O''Brien';" in sql


def test_apostrophe_in_description_is_escaped_for_snowflake():
    """Descriptions with `'` must double-up the quote so COMMENT '...' is valid SQL."""
    yaml = textwrap.dedent(
        """\
        apiVersion: v3.1.0
        kind: DataContract
        id: ex
        name: Ex
        version: 1.0.0
        status: draft
        schema:
          - name: t
            physicalType: table
            description: "It's a test table"
            properties:
              - name: email
                logicalType: string
                physicalType: STRING
                description: "The customer's email address"
        """
    )
    contract = OpenDataContractStandard.from_string(yaml)
    sql = to_snowflake_full_sql(contract)
    # Column-level COMMENT 'customer''s email' (Snowflake escape: double single quote)
    assert "COMMENT 'The customer''s email address'" in sql
    # Table-level COMMENT='It''s a test table'
    assert "COMMENT='It''s a test table'" in sql
    # And no broken (unescaped) form remains
    assert "customer's email" not in sql
    assert "It's a test" not in sql


def test_empty_value_in_named_tag():
    """`tags: [NAME=]` produces an empty-string value."""
    yaml = textwrap.dedent(
        """\
        apiVersion: v3.1.0
        kind: DataContract
        id: ex
        name: Ex
        version: 1.0.0
        status: draft
        schema:
          - name: t
            physicalType: table
            properties:
              - name: c
                logicalType: string
                physicalType: STRING
                tags:
                  - "FLAG="
        """
    )
    contract = OpenDataContractStandard.from_string(yaml)
    sql = to_snowflake_full_sql(contract)
    assert "SET TAG FLAG = '';" in sql


# === CLI tests ==============================================================


def test_cli_export_snowflake_full_to_stdout(tmp_path):
    contract_path = tmp_path / "datacontract.yaml"
    contract_path.write_text(CONTRACT_YAML)
    result = runner.invoke(app, [
        "export", "snowflake-full", str(contract_path),
    ])
    assert result.exit_code == 0, result.output
    assert "CREATE TABLE IF NOT EXISTS SALES_DB.SALES.orders" in result.output  # auto default
    assert "ALTER TABLE SALES_DB.SALES.orders SET TAG critical" in result.output
    # quality off by default
    assert "DATA METRIC FUNCTION" not in result.output


def test_cli_export_snowflake_full_with_quality(tmp_path):
    contract_path = tmp_path / "datacontract.yaml"
    contract_path.write_text(CONTRACT_YAML)
    result = runner.invoke(app, [
        "export", "snowflake-full", str(contract_path),
        "--include-quality",
    ])
    assert result.exit_code == 0, result.output
    assert (
        "ALTER TABLE SALES_DB.SALES.orders ADD DATA METRIC FUNCTION "
        "SNOWFLAKE.CORE.NULL_COUNT ON (id);"
    ) in result.output


def test_cli_export_to_file(tmp_path):
    contract_path = tmp_path / "datacontract.yaml"
    contract_path.write_text(CONTRACT_YAML)
    out_path = tmp_path / "setup.sql"
    result = runner.invoke(app, [
        "export", "snowflake-full", str(contract_path),
        "--output", str(out_path),
        "--include-quality", "--create-tags",
        "--tag-namespace", "GOV.TAGS",
    ])
    assert result.exit_code == 0, result.output
    sql = out_path.read_text()
    assert "CREATE TABLE IF NOT EXISTS SALES_DB.SALES.orders" in sql  # auto default
    assert "CREATE TAG IF NOT EXISTS GOV.TAGS.critical" in sql
    assert "SET TAG GOV.TAGS.classification = 'PII'" in sql
    assert "ADD DATA METRIC FUNCTION" in sql


# === API integration test ===================================================


def test_api_export_snowflake_full(tmp_path):
    """The new export shows up in the API mirror automatically."""
    from fastapi.testclient import TestClient
    from dcx.api import build_dcx_api_app
    client = TestClient(build_dcx_api_app())

    response = client.post(
        "/export/snowflake-full",
        json={
            "contract": yamllib.safe_load(CONTRACT_YAML),
            "options": {"include_quality": True, "create_tags": True},
        },
    )
    assert response.status_code == 200, response.text
    assert "text/plain" in response.headers["content-type"]
    body = response.text
    assert "CREATE TABLE IF NOT EXISTS SALES_DB.SALES.orders" in body  # auto default
    assert "CREATE TAG IF NOT EXISTS critical;" in body
    assert "ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT" in body


def test_no_prefix_when_server_lacks_db_schema(tmp_path):
    """No database/schema in server block → no prefix on table references."""
    yaml = textwrap.dedent(
        """\
        apiVersion: v3.1.0
        kind: DataContract
        id: ex
        name: Ex
        version: 1.0.0
        status: draft
        servers:
          - server: production
            type: snowflake
            account: xy12345
        schema:
          - name: orders
            physicalType: table
            properties:
              - name: id
                logicalType: integer
                physicalType: NUMBER
        """
    )
    contract = OpenDataContractStandard.from_string(yaml)
    sql = to_snowflake_full_sql(contract)
    # Bare table name (no prefix)
    assert "CREATE TABLE orders" in sql
    assert "SALES_DB" not in sql


def test_prefix_uses_correct_server_when_multiple(tmp_path):
    """With multiple servers, --server picks which one drives the prefix."""
    yaml = textwrap.dedent(
        """\
        apiVersion: v3.1.0
        kind: DataContract
        id: ex
        name: Ex
        version: 1.0.0
        status: draft
        servers:
          - server: prod
            type: snowflake
            account: prodacct
            database: PROD_DB
            schema: PROD_SCHEMA
          - server: dev
            type: snowflake
            account: devacct
            database: DEV_DB
            schema: DEV_SCHEMA
        schema:
          - name: orders
            physicalType: table
            properties:
              - name: id
                logicalType: integer
                physicalType: NUMBER
        """
    )
    contract = OpenDataContractStandard.from_string(yaml)
    sql_prod = to_snowflake_full_sql(contract, server="prod")
    sql_dev = to_snowflake_full_sql(contract, server="dev")
    assert "CREATE TABLE PROD_DB.PROD_SCHEMA.orders" in sql_prod
    assert "DEV_DB" not in sql_prod
    assert "CREATE TABLE DEV_DB.DEV_SCHEMA.orders" in sql_dev
    assert "PROD_DB" not in sql_dev


# === Snowflake-native types upstream's converter doesn't recognize ==========

_VARIANT_CONTRACT = textwrap.dedent(
    """\
    apiVersion: v3.1.0
    kind: DataContract
    id: events
    name: Events
    version: 1.0.0
    status: draft
    servers:
      - server: prod
        type: snowflake
        account: ACME
        database: DB
        schema: LOAD
    schema:
      - name: EVENTS
        physicalType: table
        properties:
          - name: id
            logicalType: integer
            physicalType: NUMBER
          - name: payload
            logicalType: object
            physicalType: VARIANT
          - name: geo
            logicalType: string
            physicalType: GEOGRAPHY
          - name: context
            logicalType: object
            physicalType: OBJECT
    """
)


def test_variant_renders_without_cannot_map_warning(caplog):
    """VARIANT/GEOGRAPHY (Snowflake-native, unknown to upstream) map cleanly, no warning."""
    contract = OpenDataContractStandard.from_string(_VARIANT_CONTRACT)
    with caplog.at_level("WARNING", logger="datacontract.export.sql_type_converter"):
        sql = to_snowflake_full_sql(contract, include_ddl=True)
    assert "payload VARIANT" in sql
    assert "geo GEOGRAPHY" in sql
    assert "context OBJECT" in sql  # already-mapped types still fine
    assert "Cannot map type" not in caplog.text


def test_pin_unmapped_types_respects_user_snowflake_type():
    """A user-supplied snowflakeType custom property is not overridden."""
    from dcx.exporters.snowflake import _pin_unmapped_snowflake_types

    contract = OpenDataContractStandard.from_string(textwrap.dedent(
        """\
        apiVersion: v3.1.0
        kind: DataContract
        id: events
        name: Events
        version: 1.0.0
        schema:
          - name: EVENTS
            properties:
              - name: payload
                physicalType: VARIANT
                customProperties:
                  - property: snowflakeType
                    value: OBJECT
        """
    ))
    _pin_unmapped_snowflake_types(contract)
    cps = contract.schema_[0].properties[0].customProperties
    snowflake_types = [cp.value for cp in cps if cp.property == "snowflakeType"]
    assert snowflake_types == ["OBJECT"]  # untouched, not duplicated


# === Structured types (opt-in) ==============================================

_NESTED_CONTRACT = textwrap.dedent(
    """\
    apiVersion: v3.1.0
    kind: DataContract
    id: events
    name: Events
    version: 1.0.0
    status: draft
    servers:
      - server: prod
        type: snowflake
        account: ACME
        database: DB
        schema: LOAD
    schema:
      - name: EVENTS
        physicalType: table
        properties:
          - name: id
            logicalType: integer
            physicalType: NUMBER
          - name: context
            logicalType: object
            physicalType: OBJECT
            properties:
              - name: app_version
                logicalType: string
                physicalType: VARCHAR
              - name: device
                logicalType: object
                physicalType: OBJECT
                properties:
                  - name: os
                    logicalType: string
                    physicalType: VARCHAR
          - name: labels
            logicalType: array
            physicalType: ARRAY
            items:
              name: label
              logicalType: string
              physicalType: VARCHAR
          - name: payload
            logicalType: object
            physicalType: VARIANT
    """
)


def test_structured_types_off_by_default_renders_bare():
    contract = OpenDataContractStandard.from_string(_NESTED_CONTRACT)
    sql = to_snowflake_full_sql(contract, include_ddl=True)
    assert "context OBJECT," in sql or "context OBJECT " in sql
    assert "labels ARRAY," in sql or "labels ARRAY " in sql
    assert "OBJECT(" not in sql and "ARRAY(" not in sql


def test_structured_types_renders_nested_shape():
    contract = OpenDataContractStandard.from_string(_NESTED_CONTRACT)
    sql = to_snowflake_full_sql(contract, include_ddl=True, structured_types=True)
    # nested object recurses; array gets its element type
    assert "context OBJECT(app_version VARCHAR, device OBJECT(os VARCHAR))" in sql
    assert "labels ARRAY(VARCHAR)" in sql
    # free-form VARIANT (no properties) stays untyped
    assert "payload VARIANT" in sql
    assert "payload OBJECT(" not in sql


# === ddl-mode / apply parity (export shares apply's SQL-generation knobs) =====

def test_export_ddl_modes(tmp_path):
    contract_path = tmp_path / "datacontract.yaml"
    contract_path.write_text(CONTRACT_YAML)

    auto = runner.invoke(app, ["export", "snowflake-full", str(contract_path)])
    assert auto.exit_code == 0, auto.output
    assert "CREATE TABLE IF NOT EXISTS SALES_DB.SALES.orders" in auto.output  # default

    always = runner.invoke(app, ["export", "snowflake-full", str(contract_path), "--ddl-mode", "always"])
    assert "CREATE TABLE SALES_DB.SALES.orders" in always.output
    assert "IF NOT EXISTS" not in always.output

    never = runner.invoke(app, ["export", "snowflake-full", str(contract_path), "--ddl-mode", "never"])
    assert "CREATE TABLE" not in never.output          # alter/govern only
    assert "SET TAG" in never.output                   # tags still emitted


def test_export_snowflake_full_matches_apply_dry_run(tmp_path):
    """`export snowflake-full` produces the same SQL as `apply snowflake --dry-run`."""
    contract_path = tmp_path / "datacontract.yaml"
    contract_path.write_text(CONTRACT_YAML)

    exp = runner.invoke(app, ["export", "snowflake-full", str(contract_path), "--include-quality"])
    dry = runner.invoke(app, ["apply", "snowflake", str(contract_path), "--dry-run", "--include-quality"])
    assert exp.exit_code == 0 and dry.exit_code == 0, (exp.output, dry.output)
    assert exp.output.strip() == dry.output.strip()
