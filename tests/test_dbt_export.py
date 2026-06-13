import textwrap

import yaml as yamllib
from open_data_contract_standard.model import OpenDataContractStandard
from typer.testing import CliRunner

from dcx.cli import app
from dcx.exporters.dbt import DbtKind, DbtMetaKeyStyle, to_dbt_yaml

runner = CliRunner()


CONTRACT_YAML = textwrap.dedent(
    """\
    apiVersion: v3.1.0
    kind: DataContract
    id: customers
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
      - name: CUSTOMERS
        physicalType: table
        tags:
          - SCHEMA_LEVEL_TAG
          - TABLE_CLASSIFICATION=RESTRICTED
        properties:
          - name: customer_id
            logicalType: integer
            physicalType: NUMBER
            primaryKey: true
            businessName: Customer Identifier
            criticalDataElement: true
          - name: email
            logicalType: string
            physicalType: VARCHAR(255)
            classification: PII
            tags:
              - sensitive
              - DATA_CLASSIFICATION=PD_DATA
    """
)


def _load() -> OpenDataContractStandard:
    return OpenDataContractStandard.from_string(CONTRACT_YAML)


def _models() -> dict:
    return yamllib.safe_load(to_dbt_yaml(_load(), kind=DbtKind.models, server="production"))


def _column(doc: dict, name: str, kind: str = "models") -> dict:
    node = doc["models"][0] if kind == "models" else doc["sources"][0]["tables"][0]
    return next(c for c in node["columns"] if c["name"] == name)


# === Column governance → config.meta / config.tags ==========================


def test_name_value_tag_goes_to_config_meta():
    col = _column(_models(), "email")
    assert col["config"]["meta"]["data_classification"] == "PD_DATA"
    # ...and not as a literal dbt selection tag
    assert "DATA_CLASSIFICATION=PD_DATA" not in col["config"].get("tags", [])


def test_bare_tag_goes_to_config_tags():
    col = _column(_models(), "email")
    assert col["config"]["tags"] == ["sensitive"]


def test_classification_and_data_classification_are_separate_keys():
    meta = _column(_models(), "email")["config"]["meta"]
    assert meta["classification"] == "PII"            # from `classification`
    assert meta["data_classification"] == "PD_DATA"   # from the NAME=VALUE tag


def test_business_name_and_critical_data_element_in_meta():
    meta = _column(_models(), "customer_id")["config"]["meta"]
    assert meta["business_name"] == "Customer Identifier"
    assert meta["critical_data_element"] is True


def test_no_top_level_meta_or_tags_on_column():
    """Governance lives under `config`, not legacy top-level keys."""
    col = _column(_models(), "email")
    assert "meta" not in col
    assert "tags" not in col


# === Schema-object (table) level tags → model config ========================


def test_schema_level_tags_mapped_to_model_config():
    model = _models()["models"][0]
    assert model["config"]["tags"] == ["SCHEMA_LEVEL_TAG"]
    assert model["config"]["meta"]["table_classification"] == "RESTRICTED"
    # upstream model meta is preserved
    assert model["config"]["meta"]["data_contract"] == "customers"


# === Meta-key style for fully-qualified tag names ===========================


_FQ_CONTRACT = textwrap.dedent(
    """\
    apiVersion: v3.1.0
    kind: DataContract
    id: c
    name: C
    version: 1.0.0
    status: draft
    schema:
      - name: T
        physicalType: table
        properties:
          - name: email
            physicalType: STRING
            tags:
              - GOVERNANCE.TAGS.DATA_CLASSIFICATION=PD_DATA
    """
)


def _fq_meta(style: DbtMetaKeyStyle) -> dict:
    doc = yamllib.safe_load(
        to_dbt_yaml(OpenDataContractStandard.from_string(_FQ_CONTRACT), kind=DbtKind.models, meta_key_style=style)
    )
    return doc["models"][0]["columns"][0]["config"]["meta"]


def test_meta_key_style_full_keeps_dotted_namespace():
    assert _fq_meta(DbtMetaKeyStyle.full) == {"governance.tags.data_classification": "PD_DATA"}


def test_meta_key_style_sanitized_replaces_dots():
    assert _fq_meta(DbtMetaKeyStyle.sanitized) == {"governance_tags_data_classification": "PD_DATA"}


def test_meta_key_style_short_uses_last_segment():
    assert _fq_meta(DbtMetaKeyStyle.short) == {"data_classification": "PD_DATA"}


def test_meta_key_style_default_is_full():
    # to_dbt_yaml default + the CLI default both keep the namespace.
    assert _fq_meta(DbtMetaKeyStyle.full) == _fq_meta(DbtMetaKeyStyle("full"))


def test_meta_key_style_namespace_collision_tradeoff():
    """Same short name across two namespaces: full keeps both, short collapses them."""
    yaml_two = textwrap.dedent(
        """\
        apiVersion: v3.1.0
        kind: DataContract
        id: c
        name: C
        version: 1.0.0
        status: draft
        schema:
          - name: T
            physicalType: table
            properties:
              - name: email
                physicalType: STRING
                tags:
                  - GOV.TAGS.CLASSIFICATION=A
                  - SEC.AUDIT.CLASSIFICATION=B
        """
    )
    contract = OpenDataContractStandard.from_string(yaml_two)

    full = yamllib.safe_load(to_dbt_yaml(contract, kind=DbtKind.models, meta_key_style=DbtMetaKeyStyle.full))
    full_meta = full["models"][0]["columns"][0]["config"]["meta"]
    assert full_meta == {"gov.tags.classification": "A", "sec.audit.classification": "B"}

    short = yamllib.safe_load(to_dbt_yaml(contract, kind=DbtKind.models, meta_key_style=DbtMetaKeyStyle.short))
    short_meta = short["models"][0]["columns"][0]["config"]["meta"]
    assert short_meta == {"classification": "B"}  # second wins — the documented risk


def test_cli_export_dbt_meta_key_style_short(tmp_path):
    path = tmp_path / "datacontract.yaml"
    path.write_text(_FQ_CONTRACT)
    result = runner.invoke(app, ["export", "dbt", str(path), "--meta-key-style", "short"])
    assert result.exit_code == 0, result.output
    doc = yamllib.safe_load(result.output)
    assert doc["models"][0]["columns"][0]["config"]["meta"] == {"data_classification": "PD_DATA"}


# === Unmapped type bug fix ==================================================


def test_unmapped_type_emits_single_prefix_test_with_real_type():
    yaml_unmapped = textwrap.dedent(
        """\
        apiVersion: v3.1.0
        kind: DataContract
        id: c
        name: C
        version: 1.0.0
        status: draft
        schema:
          - name: T
            physicalType: table
            properties:
              - name: unmapped
                logicalType: weirdlogical
              - name: notype
        """
    )
    contract = OpenDataContractStandard.from_string(yaml_unmapped)
    sql = to_dbt_yaml(contract, kind=DbtKind.models)
    # Single (not doubled) dbt_expectations namespace, and a real expected type.
    assert "dbt_expectations.dbt_expectations" not in sql
    assert "dbt_expectations.expect_column_values_to_be_of_type" in sql
    assert "column_type: weirdlogical" in sql
    assert "column_type: null" not in sql

    doc = yamllib.safe_load(sql)
    # A column with no resolvable type at all gets no spurious type test.
    notype = _column(doc, "notype")
    assert "data_tests" not in notype


# === Sources share the same column treatment ================================


def test_sources_columns_get_config_meta():
    doc = yamllib.safe_load(to_dbt_yaml(_load(), kind=DbtKind.sources, server="production"))
    meta = _column(doc, "email", kind="sources")["config"]["meta"]
    assert meta["data_classification"] == "PD_DATA"
    assert meta["classification"] == "PII"
    # source database/schema still resolved from the server block
    assert doc["sources"][0]["database"] == "SALES_DB"


# === Staging ================================================================


def test_staging_returns_select():
    sql = to_dbt_yaml(_load(), kind=DbtKind.staging, schema_name="all")
    assert "select" in sql
    assert "customer_id" in sql and "email" in sql
    assert "source('customers', 'CUSTOMERS')" in sql


# === CLI ====================================================================


def test_cli_export_dbt_models(tmp_path):
    path = tmp_path / "datacontract.yaml"
    path.write_text(CONTRACT_YAML)
    result = runner.invoke(app, ["export", "dbt", str(path), "--kind", "models", "--server", "production"])
    assert result.exit_code == 0, result.output
    doc = yamllib.safe_load(result.output)
    assert doc["models"][0]["columns"][1]["config"]["meta"]["data_classification"] == "PD_DATA"


def test_cli_export_dbt_default_kind_is_models(tmp_path):
    path = tmp_path / "datacontract.yaml"
    path.write_text(CONTRACT_YAML)
    result = runner.invoke(app, ["export", "dbt", str(path)])
    assert result.exit_code == 0, result.output
    assert "models:" in result.output


def test_cli_export_dbt_to_file(tmp_path):
    path = tmp_path / "datacontract.yaml"
    path.write_text(CONTRACT_YAML)
    out = tmp_path / "schema.yml"
    result = runner.invoke(app, ["export", "dbt", str(path), "--output", str(out)])
    assert result.exit_code == 0, result.output
    assert "models:" in out.read_text()


# === API ====================================================================


def _api_client():
    from fastapi.testclient import TestClient
    from dcx.api import build_dcx_api_app
    return TestClient(build_dcx_api_app())


def test_api_export_dbt():
    r = _api_client().post(
        "/export/dbt",
        json={"contract": CONTRACT_YAML, "options": {"kind": "models", "server": "production"}},
    )
    assert r.status_code == 200, r.text
    doc = yamllib.safe_load(r.text)
    assert doc["models"][0]["columns"][1]["config"]["meta"]["data_classification"] == "PD_DATA"


# === Back-compat ============================================================


def test_upstream_dbt_commands_still_present():
    import datacontract.cli  # noqa: F401  full init
    import dcx.cli  # noqa: F401  registers the dcx dbt command
    from datacontract.command_export import export_app

    names = {c.name for c in export_app.registered_commands}
    assert {"dbt-models", "dbt-sources", "dbt-staging-sql", "dbt"} <= names
