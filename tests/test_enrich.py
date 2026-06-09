import textwrap

import pytest
from fastapi.testclient import TestClient
from open_data_contract_standard.model import OpenDataContractStandard
from typer.testing import CliRunner

from dcx.api import build_dcx_api_app
from dcx.cli import app
from dcx.enrich import (
    DEFAULT_MODEL,
    EnrichError,
    EnrichSettings,
    enrich_all_contract,
    enrich_columns_contract,
    enrich_quality_contract,
    enrich_tags_contract,
    load_tag_catalog_file,
    parse_tag_catalog,
)
from dcx.enrich.base import _collect_properties
from dcx.enrich.columns import _valid_options
from dcx.enrich.quality import _build_quality_rule, _validated_operator

runner = CliRunner()


CONTRACT_YAML = textwrap.dedent(
    """\
    apiVersion: v3.1.0
    kind: DataContract
    id: customers
    name: Customers
    version: 1.0.0
    status: draft
    schema:
      - name: customers
        physicalType: table
        properties:
          - name: id
            logicalType: integer
            primaryKey: true
          - name: email
            logicalType: string
            examples:
              - jane@example.com
          - name: signup_date
            logicalType: date
            description: Existing description kept as-is.
    """
)


def _contract():
    return OpenDataContractStandard.from_string(CONTRACT_YAML)


def _props(contract, idx=0):
    return {p.name: p for p in contract.schema_[idx].properties}


# A fake completion that returns canned enrichment keyed by the column ids the
# core assigns (depth-first order: id, email, signup_date -> 0, 1, 2).
def _fake_complete_factory(by_id):
    captured = {}

    def fake(messages, tools, settings, max_tokens):
        captured["messages"] = messages
        captured["tools"] = tools
        captured["max_tokens"] = max_tokens
        return {"columns": [{"id": i, **e} for i, e in by_id.items()]}

    fake.captured = captured
    return fake


# === Pure helpers ===========================================================


def test_collect_properties_flattens_nested():
    yaml = textwrap.dedent(
        """\
        apiVersion: v3.1.0
        kind: DataContract
        id: x
        name: X
        version: 1.0.0
        schema:
          - name: t
            properties:
              - name: addr
                logicalType: object
                properties:
                  - name: city
                    logicalType: string
        """
    )
    contract = OpenDataContractStandard.from_string(yaml)
    pairs = _collect_properties(contract.schema_[0].properties)
    paths = [p for p, _ in pairs]
    assert paths == ["addr", "addr.city"]


def test_valid_options_filters_by_logical_type():
    # string allows format/maxLength; minimum is for numbers -> dropped
    out = _valid_options("string", {"format": "email", "maxLength": 320, "minimum": 0})
    assert out == {"format": "email", "maxLength": 320}


def test_valid_options_unknown_type_drops_all():
    assert _valid_options("boolean", {"format": "x"}) == {}
    assert _valid_options(None, {"format": "x"}) == {}


def test_valid_options_strips_presence_proxy_minlength():
    # minLength 0/1 is a presence proxy -> dropped; a real bound is kept.
    assert _valid_options("string", {"minLength": 1, "maxLength": 50}) == {"maxLength": 50}
    assert _valid_options("string", {"minLength": 0}) == {}
    assert _valid_options("string", {"minLength": 3}) == {"minLength": 3}


def _contract_with_format_custom_property():
    yaml = textwrap.dedent(
        """\
        apiVersion: v3.1.0
        kind: DataContract
        id: x
        name: X
        version: 1.0.0
        schema:
          - name: t
            properties:
              - name: email
                logicalType: string
                customProperties:
                  - property: format
                    value: email
                  - property: pii
                    value: "true"
        """
    )
    return OpenDataContractStandard.from_string(yaml)


def test_format_custom_property_migrated_to_logical_type_options():
    contract = _contract_with_format_custom_property()
    # Model returns nothing for format; migration must still happen.
    fake = _fake_complete_factory({0: {"description": "Email."}})
    result = enrich_columns_contract(contract, EnrichSettings(), complete=fake)
    prop = _props(result)["email"]
    assert prop.logicalTypeOptions == {"format": "email"}
    # The redundant `format` custom property is gone; unrelated ones remain.
    remaining = {cp.property for cp in (prop.customProperties or [])}
    assert remaining == {"pii"}


def test_format_dedup_when_model_also_returns_it():
    contract = _contract_with_format_custom_property()
    fake = _fake_complete_factory({0: {"logicalTypeOptions": {"format": "email"}}})
    result = enrich_columns_contract(contract, EnrichSettings(), complete=fake)
    prop = _props(result)["email"]
    assert prop.logicalTypeOptions == {"format": "email"}
    assert all(cp.property != "format" for cp in (prop.customProperties or []))


def test_format_migration_skipped_when_type_options_off():
    contract = _contract_with_format_custom_property()
    fake = _fake_complete_factory({0: {"description": "Email."}})
    result = enrich_columns_contract(
        contract, EnrichSettings(enrich_type_options=False), complete=fake,
    )
    prop = _props(result)["email"]
    # Untouched: still on customProperties, not migrated.
    assert {cp.property for cp in prop.customProperties} == {"format", "pii"}


# === Core enrichment ========================================================


def test_enrich_fills_descriptions_and_options():
    contract = _contract()
    fake = _fake_complete_factory({
        0: {"description": "Surrogate key.", "logicalTypeOptions": {"minimum": 1}},
        1: {"description": "Customer email.", "logicalTypeOptions": {"format": "email"}},
        2: {"description": "Should NOT overwrite.", "logicalTypeOptions": {"format": "%Y-%m-%d"}},
    })
    result = enrich_columns_contract(contract, EnrichSettings(), complete=fake)
    props = _props(result)

    assert props["id"].description == "Surrogate key."
    assert props["id"].logicalTypeOptions == {"minimum": 1}
    assert props["email"].description == "Customer email."
    assert props["email"].logicalTypeOptions == {"format": "email"}
    # signup_date already had a description -> preserved (no overwrite)
    assert props["signup_date"].description == "Existing description kept as-is."
    # but its options were empty -> filled
    assert props["signup_date"].logicalTypeOptions == {"format": "%Y-%m-%d"}


def test_overwrite_replaces_existing_description():
    contract = _contract()
    fake = _fake_complete_factory({
        0: {"description": "A"},
        1: {"description": "B"},
        2: {"description": "Replaced!"},
    })
    result = enrich_columns_contract(
        contract, EnrichSettings(overwrite=True), complete=fake,
    )
    assert _props(result)["signup_date"].description == "Replaced!"


def test_invalid_options_are_dropped():
    contract = _contract()
    # email is a string; `minimum` is invalid for strings and must be dropped.
    fake = _fake_complete_factory({
        1: {"logicalTypeOptions": {"format": "email", "minimum": 5}},
    })
    result = enrich_columns_contract(contract, EnrichSettings(), complete=fake)
    assert _props(result)["email"].logicalTypeOptions == {"format": "email"}


def test_required_and_unique_populated():
    contract = _contract()
    fake = _fake_complete_factory({
        0: {"required": True, "unique": True},   # id
        1: {"required": True, "unique": True},   # email
        2: {"required": False},                  # signup_date
    })
    result = enrich_columns_contract(contract, EnrichSettings(), complete=fake)
    props = _props(result)
    assert props["id"].required is True
    assert props["id"].unique is True
    assert props["email"].required is True
    assert props["email"].unique is True
    # false is the ODCS default — we don't write it (keeps the contract clean)
    assert props["signup_date"].required is None


def test_required_unique_skipped_when_type_options_off():
    contract = _contract()
    fake = _fake_complete_factory({0: {"required": True, "unique": True}})
    result = enrich_columns_contract(
        contract, EnrichSettings(enrich_type_options=False), complete=fake,
    )
    assert _props(result)["id"].required is None
    assert _props(result)["id"].unique is None


def test_existing_required_not_clobbered_without_overwrite():
    yaml = textwrap.dedent(
        """\
        apiVersion: v3.1.0
        kind: DataContract
        id: x
        name: X
        version: 1.0.0
        schema:
          - name: t
            properties:
              - name: a
                logicalType: string
                required: false
        """
    )
    contract = OpenDataContractStandard.from_string(yaml)
    fake = _fake_complete_factory({0: {"required": True}})
    result = enrich_columns_contract(contract, EnrichSettings(), complete=fake)
    # already explicitly set -> preserved
    assert _props(result)["a"].required is False
    # but overwrite replaces it
    contract2 = OpenDataContractStandard.from_string(yaml)
    result2 = enrich_columns_contract(contract2, EnrichSettings(overwrite=True), complete=fake)
    assert _props(result2)["a"].required is True


def test_descriptions_only_skips_options():
    contract = _contract()
    fake = _fake_complete_factory({
        0: {"description": "key", "logicalTypeOptions": {"minimum": 1}},
    })
    result = enrich_columns_contract(
        contract, EnrichSettings(enrich_type_options=False), complete=fake,
    )
    assert _props(result)["id"].description == "key"
    assert _props(result)["id"].logicalTypeOptions is None


def test_nothing_enabled_errors():
    with pytest.raises(EnrichError, match="Nothing to enrich"):
        enrich_columns_contract(
            _contract(),
            EnrichSettings(enrich_descriptions=False, enrich_type_options=False),
            complete=lambda *a: {},
        )


def test_skips_llm_call_when_nothing_to_fill():
    yaml = textwrap.dedent(
        """\
        apiVersion: v3.1.0
        kind: DataContract
        id: x
        name: X
        version: 1.0.0
        schema:
          - name: t
            properties:
              - name: a
                logicalType: string
                description: done
                logicalTypeOptions:
                  maxLength: 10
        """
    )
    contract = OpenDataContractStandard.from_string(yaml)
    calls = {"n": 0}

    def fake(*args):
        calls["n"] += 1
        return {"columns": []}

    enrich_columns_contract(contract, EnrichSettings(), complete=fake)
    assert calls["n"] == 0


def test_missing_columns_array_errors():
    def fake(*args):
        return {"not_columns": []}

    with pytest.raises(EnrichError, match="missing a 'columns' array"):
        enrich_columns_contract(_contract(), EnrichSettings(), complete=fake)


def test_max_tokens_scales_with_columns():
    contract = _contract()
    fake = _fake_complete_factory({0: {"description": "x"}})
    enrich_columns_contract(contract, EnrichSettings(), complete=fake)
    # 3 columns -> 250*3 + 512 = 1262
    assert fake.captured["max_tokens"] == 1262


def test_instructions_forwarded_to_prompt():
    contract = _contract()
    fake = _fake_complete_factory({0: {"description": "x"}})
    enrich_columns_contract(
        contract, EnrichSettings(instructions="Treat email as PII."), complete=fake,
    )
    user_msg = fake.captured["messages"][-1]["content"]
    assert "Treat email as PII." in user_msg


# === Tag manager ============================================================


CATALOG = {
    "tags": [
        {
            "name": "DATA_CLASSIFICATION",
            "description": "Sensitivity of the column.",
            "multiple": False,
            "values": [
                {"value": "PD_DATA", "description": "Personal data.", "examples": ["email"]},
                {"value": "PUBLIC", "description": "Non-sensitive."},
            ],
        },
        {
            "name": "DOMAIN",
            "multiple": True,
            "values": ["SALES", "MARKETING"],
        },
    ]
}


def _tags_complete_factory(by_id):
    def fake(messages, tools, settings, max_tokens):
        # sanity: tags tool is the one being forced
        assert tools[0]["function"]["name"] == "submit_column_tags"
        return {"columns": [{"id": i, "tags": t} for i, t in by_id.items()]}

    return fake


def test_parse_catalog_shorthand_and_full():
    cat = parse_tag_catalog(CATALOG)
    assert cat.names() == {"DATA_CLASSIFICATION", "DOMAIN"}
    assert cat.get("DATA_CLASSIFICATION").multiple is False
    assert cat.get("DOMAIN").multiple is True
    # string shorthand value parsed
    assert cat.get("DOMAIN").allowed_values() == {"SALES", "MARKETING"}


def test_parse_catalog_accepts_yaml_string():
    cat = parse_tag_catalog("tags:\n  - name: X\n    values: [a, b]\n")
    assert cat.get("X").allowed_values() == {"a", "b"}


def test_parse_catalog_errors():
    with pytest.raises(EnrichError, match="non-empty 'tags' list"):
        parse_tag_catalog({"tags": []})
    with pytest.raises(EnrichError, match="needs a 'name'"):
        parse_tag_catalog({"tags": [{"values": ["a"]}]})
    with pytest.raises(EnrichError, match="at least one value"):
        parse_tag_catalog({"tags": [{"name": "X", "values": []}]})


def test_load_catalog_file(tmp_path):
    p = tmp_path / "catalog.yaml"
    p.write_text("tags:\n  - name: PII\n    values: ['yes', 'no']\n")  # quoted: stay strings
    cat = load_tag_catalog_file(p)
    assert cat.get("PII").allowed_values() == {"yes", "no"}


def test_tags_applied_as_name_value():
    contract = _contract()
    fake = _tags_complete_factory({
        1: [{"name": "DATA_CLASSIFICATION", "value": "PD_DATA"},
            {"name": "DOMAIN", "value": "SALES"}],
    })
    result = enrich_tags_contract(contract, EnrichSettings(), parse_tag_catalog(CATALOG), complete=fake)
    assert _props(result)["email"].tags == ["DATA_CLASSIFICATION=PD_DATA", "DOMAIN=SALES"]


def test_tags_invalid_name_or_value_dropped():
    contract = _contract()
    fake = _tags_complete_factory({
        0: [{"name": "DATA_CLASSIFICATION", "value": "NOT_IN_CATALOG"},  # bad value
            {"name": "UNKNOWN_TAG", "value": "x"},                       # bad name
            {"name": "PUBLIC"}],                                         # malformed (no value field semantics)
    })
    result = enrich_tags_contract(contract, EnrichSettings(), parse_tag_catalog(CATALOG), complete=fake)
    assert _props(result)["id"].tags is None


def test_single_value_tag_enforced():
    contract = _contract()
    fake = _tags_complete_factory({
        0: [{"name": "DATA_CLASSIFICATION", "value": "PD_DATA"},
            {"name": "DATA_CLASSIFICATION", "value": "PUBLIC"}],  # 2nd dropped (not multiple)
    })
    result = enrich_tags_contract(contract, EnrichSettings(), parse_tag_catalog(CATALOG), complete=fake)
    assert _props(result)["id"].tags == ["DATA_CLASSIFICATION=PD_DATA"]


def test_multiple_value_tag_allows_several():
    contract = _contract()
    fake = _tags_complete_factory({
        0: [{"name": "DOMAIN", "value": "SALES"}, {"name": "DOMAIN", "value": "MARKETING"}],
    })
    result = enrich_tags_contract(contract, EnrichSettings(), parse_tag_catalog(CATALOG), complete=fake)
    assert _props(result)["id"].tags == ["DOMAIN=SALES", "DOMAIN=MARKETING"]


def test_existing_catalog_tag_preserved_then_overwritten():
    yaml = textwrap.dedent(
        """\
        apiVersion: v3.1.0
        kind: DataContract
        id: x
        name: X
        version: 1.0.0
        schema:
          - name: t
            properties:
              - name: a
                logicalType: string
                tags:
                  - DATA_CLASSIFICATION=PUBLIC
                  - MANUAL_TAG=keep
        """
    )
    fake = _tags_complete_factory({0: [{"name": "DATA_CLASSIFICATION", "value": "PD_DATA"}]})

    # no overwrite: existing single-value catalog tag preserved; non-catalog tag kept
    c1 = OpenDataContractStandard.from_string(yaml)
    r1 = enrich_tags_contract(c1, EnrichSettings(), parse_tag_catalog(CATALOG), complete=fake)
    assert _props(r1)["a"].tags == ["DATA_CLASSIFICATION=PUBLIC", "MANUAL_TAG=keep"]

    # overwrite: catalog tag replaced, non-catalog tag still kept
    c2 = OpenDataContractStandard.from_string(yaml)
    r2 = enrich_tags_contract(c2, EnrichSettings(overwrite=True), parse_tag_catalog(CATALOG), complete=fake)
    assert set(_props(r2)["a"].tags) == {"MANUAL_TAG=keep", "DATA_CLASSIFICATION=PD_DATA"}


CATALOG_WITH_DEFAULT = {
    "tags": [{
        "name": "DATA_CLASSIFICATION",
        "multiple": False,
        "values": [
            {"value": "PD_DATA", "description": "Personal data.", "examples": ["email"]},
            {"value": "PUBLIC", "description": "Non-sensitive.", "default": True},
        ],
    }]
}


def test_default_value_parsed():
    cat = parse_tag_catalog(CATALOG_WITH_DEFAULT)
    assert cat.get("DATA_CLASSIFICATION").default_value() == "PUBLIC"


def test_more_than_one_default_errors():
    with pytest.raises(EnrichError, match="more than one default"):
        parse_tag_catalog({"tags": [{"name": "X", "values": [
            {"value": "a", "default": True}, {"value": "b", "default": True},
        ]}]})


def test_default_applied_when_model_assigns_nothing():
    contract = _contract()  # id, email, signup_date
    # model only classifies email; id and signup_date get the default
    fake = _tags_complete_factory({
        1: [{"name": "DATA_CLASSIFICATION", "value": "PD_DATA"}],
    })
    cat = parse_tag_catalog(CATALOG_WITH_DEFAULT)
    result = enrich_tags_contract(contract, EnrichSettings(), cat, complete=fake)
    props = _props(result)
    assert props["email"].tags == ["DATA_CLASSIFICATION=PD_DATA"]   # model value wins
    assert props["id"].tags == ["DATA_CLASSIFICATION=PUBLIC"]       # default fills gap
    assert props["signup_date"].tags == ["DATA_CLASSIFICATION=PUBLIC"]


def test_default_applied_even_when_column_omitted_by_model():
    contract = _contract()
    # model returns no entry for any column at all
    fake = _tags_complete_factory({})
    cat = parse_tag_catalog(CATALOG_WITH_DEFAULT)
    result = enrich_tags_contract(contract, EnrichSettings(), cat, complete=fake)
    for prop in _props(result).values():
        assert prop.tags == ["DATA_CLASSIFICATION=PUBLIC"]


def test_default_does_not_override_existing():
    yaml = textwrap.dedent(
        """\
        apiVersion: v3.1.0
        kind: DataContract
        id: x
        name: X
        version: 1.0.0
        schema:
          - name: t
            properties:
              - name: a
                logicalType: string
                tags:
                  - DATA_CLASSIFICATION=PD_DATA
        """
    )
    contract = OpenDataContractStandard.from_string(yaml)
    cat = parse_tag_catalog(CATALOG_WITH_DEFAULT)
    # already fully classified -> skipped, default must not clobber
    fake = _tags_complete_factory({})
    result = enrich_tags_contract(contract, EnrichSettings(), cat, complete=fake)
    assert _props(result)["a"].tags == ["DATA_CLASSIFICATION=PD_DATA"]


def test_tags_skips_when_fully_classified():
    yaml = textwrap.dedent(
        """\
        apiVersion: v3.1.0
        kind: DataContract
        id: x
        name: X
        version: 1.0.0
        schema:
          - name: t
            properties:
              - name: a
                logicalType: string
                tags:
                  - DATA_CLASSIFICATION=PUBLIC
                  - DOMAIN=SALES
        """
    )
    contract = OpenDataContractStandard.from_string(yaml)
    calls = {"n": 0}

    def fake(*a):
        calls["n"] += 1
        return {"columns": []}

    enrich_tags_contract(contract, EnrichSettings(), parse_tag_catalog(CATALOG), complete=fake)
    assert calls["n"] == 0


# === Quality suite ==========================================================


def _op(name, value):
    return {"name": name, "value": value}


def test_validated_operator_rules():
    assert _validated_operator(_op("mustBe", 0)) == {"mustBe": 0}
    assert _validated_operator(_op("mustBeGreaterThan", 0)) == {"mustBeGreaterThan": 0}
    assert _validated_operator(_op("mustBeBetween", [0, 120])) == {"mustBeBetween": [0, 120]}
    # invalid: numeric op with non-number
    assert _validated_operator(_op("mustBeGreaterThan", "x")) is None
    # invalid: between with wrong arity
    assert _validated_operator(_op("mustBeBetween", [1])) is None
    # invalid: unknown operator
    assert _validated_operator(_op("mustBeWeird", 1)) is None
    # invalid: bool sneaking in as number
    assert _validated_operator(_op("mustBeGreaterThan", True)) is None


def test_build_quality_rule_library():
    rule = _build_quality_rule({
        "name": "id not null", "dimension": "completeness", "type": "library",
        "metric": "nullValues", "severity": "error", "operator": _op("mustBe", 0),
    })
    assert rule is not None
    assert rule.type == "library"
    assert rule.metric == "nullValues"
    assert rule.mustBe == 0
    assert rule.dimension == "completeness"
    assert rule.severity == "error"


def test_build_quality_rule_sql():
    rule = _build_quality_rule({
        "name": "created before updated", "dimension": "consistency", "type": "sql",
        "query": "SELECT COUNT(*) FROM ${table} WHERE created_at > updated_at",
        "operator": _op("mustBe", 0),
    })
    assert rule.type == "sql"
    assert "${table}" in rule.query
    assert rule.mustBe == 0


def test_build_quality_rule_dimension_synonym_mapped():
    rule = _build_quality_rule({
        "name": "x", "dimension": "validity", "type": "library",
        "metric": "invalidValues", "operator": _op("mustBe", 0),
    })
    assert rule.dimension == "conformity"  # validity -> conformity


def test_build_quality_rule_invalid_dropped():
    # library without a valid metric
    assert _build_quality_rule({
        "name": "x", "dimension": "completeness", "type": "library",
        "metric": "notAMetric", "operator": _op("mustBe", 0),
    }) is None
    # sql without a query
    assert _build_quality_rule({
        "name": "x", "dimension": "consistency", "type": "sql", "operator": _op("mustBe", 0),
    }) is None
    # missing operator
    assert _build_quality_rule({
        "name": "x", "dimension": "completeness", "type": "library", "metric": "rowCount",
    }) is None


def _quality_complete_factory(table_rules, by_id):
    def fake(messages, tools, settings, max_tokens):
        assert tools[0]["function"]["name"] == "submit_quality_suite"
        return {
            "table_rules": table_rules,
            "columns": [{"id": i, "rules": r} for i, r in by_id.items()],
        }

    return fake


def test_quality_applies_table_and_column_rules():
    contract = _contract()
    fake = _quality_complete_factory(
        table_rules=[{
            "name": "non-empty", "dimension": "coverage", "type": "library",
            "metric": "rowCount", "operator": _op("mustBeGreaterThan", 0),
        }],
        by_id={
            0: [{"name": "id not null", "dimension": "completeness", "type": "library",
                 "metric": "nullValues", "severity": "error", "operator": _op("mustBe", 0)},
                {"name": "id unique", "dimension": "uniqueness", "type": "library",
                 "metric": "duplicateValues", "operator": _op("mustBe", 0)}],
        },
    )
    result = enrich_quality_contract(contract, EnrichSettings(), complete=fake)
    table = result.schema_[0]
    assert table.quality[0].metric == "rowCount"
    assert table.quality[0].mustBeGreaterThan == 0
    idprop = _props(result)["id"]
    assert [q.metric for q in idprop.quality] == ["nullValues", "duplicateValues"]


def test_quality_drops_invalid_rules_only():
    contract = _contract()
    fake = _quality_complete_factory(
        table_rules=[],
        by_id={0: [
            {"name": "good", "dimension": "completeness", "type": "library",
             "metric": "nullValues", "operator": _op("mustBe", 0)},
            {"name": "bad", "dimension": "completeness", "type": "library",
             "metric": "garbage", "operator": _op("mustBe", 0)},
        ]},
    )
    result = enrich_quality_contract(contract, EnrichSettings(), complete=fake)
    rules = _props(result)["id"].quality
    assert [r.name for r in rules] == ["good"]


def test_quality_preserves_existing_unless_overwrite():
    yaml = textwrap.dedent(
        """\
        apiVersion: v3.1.0
        kind: DataContract
        id: x
        name: X
        version: 1.0.0
        schema:
          - name: t
            quality:
              - type: library
                metric: rowCount
                mustBeGreaterThan: 100
            properties:
              - name: a
                logicalType: integer
                quality:
                  - type: library
                    metric: nullValues
                    mustBe: 0
        """
    )
    fake = _quality_complete_factory(
        table_rules=[{"name": "vol", "dimension": "coverage", "type": "library",
                      "metric": "rowCount", "operator": _op("mustBeGreaterThan", 0)}],
        by_id={0: [{"name": "new", "dimension": "completeness", "type": "library",
                    "metric": "nullValues", "operator": _op("mustBe", 0)}]},
    )

    # no overwrite: table already had quality AND the only column has quality -> skipped entirely
    c1 = OpenDataContractStandard.from_string(yaml)
    calls = {"n": 0}

    def counting_fake(*a):
        calls["n"] += 1
        return fake(*a)

    enrich_quality_contract(c1, EnrichSettings(), complete=counting_fake)
    assert calls["n"] == 0
    assert c1.schema_[0].quality[0].mustBeGreaterThan == 100  # untouched

    # overwrite: regenerated
    c2 = OpenDataContractStandard.from_string(yaml)
    enrich_quality_contract(c2, EnrichSettings(overwrite=True), complete=fake)
    assert c2.schema_[0].quality[0].mustBeGreaterThan == 0


# === enrich all =============================================================


def _combined_complete(messages, tools, settings, max_tokens):
    """One fake that answers each stage by inspecting the forced tool name."""
    tool = tools[0]["function"]["name"]
    if tool == "submit_column_enrichment":
        return {"columns": [{"id": i, "description": f"col{i}", "required": True}
                            for i in range(3)]}
    if tool == "submit_column_tags":
        return {"columns": [{"id": 1, "tags": [{"name": "DATA_CLASSIFICATION", "value": "PD_DATA"}]}]}
    if tool == "submit_quality_suite":
        return {
            "table_rules": [{"name": "vol", "dimension": "coverage", "type": "library",
                             "metric": "rowCount", "operator": {"name": "mustBeGreaterThan", "value": 0}}],
            "columns": [{"id": 0, "rules": [{"name": "nn", "dimension": "completeness",
                         "type": "library", "metric": "nullValues",
                         "operator": {"name": "mustBe", "value": 0}}]}],
        }
    raise AssertionError(f"unexpected tool {tool}")


def test_enrich_all_runs_three_stages():
    contract = _contract()
    enrich_all_contract(
        contract, EnrichSettings(), parse_tag_catalog(CATALOG), complete=_combined_complete,
    )
    props = _props(contract)
    # columns stage
    assert props["id"].description == "col0"
    assert props["id"].required is True
    # tags stage (email is column id 1)
    assert props["email"].tags == ["DATA_CLASSIFICATION=PD_DATA"]
    # quality stage
    assert contract.schema_[0].quality[0].metric == "rowCount"
    assert props["id"].quality[0].metric == "nullValues"


def test_enrich_all_skips_tags_without_catalog():
    contract = _contract()
    enrich_all_contract(contract, EnrichSettings(), None, complete=_combined_complete)
    props = _props(contract)
    assert props["id"].description == "col0"          # columns ran
    assert props["email"].tags is None                # tags skipped (no catalog)
    assert contract.schema_[0].quality[0].metric == "rowCount"  # quality ran


def test_enrich_all_endpoint_works(monkeypatch):
    import dcx.enrich.base as enrich_mod
    monkeypatch.setattr(enrich_mod, "_llm_complete", _combined_complete)
    client = TestClient(build_dcx_api_app())
    response = client.post(
        "/enrich/all",
        json={
            "contract": {
                "apiVersion": "v3.1.0", "kind": "DataContract", "id": "c",
                "name": "C", "version": "1.0.0",
                "schema": [{"name": "customers", "properties": [
                    {"name": "id", "logicalType": "integer"},
                    {"name": "email", "logicalType": "string"},
                    {"name": "x", "logicalType": "string"},
                ]}],
            },
            "catalog": CATALOG,
            "options": {"model": "gpt-4o"},
        },
    )
    assert response.status_code == 200, response.text
    body = response.json()
    props = {p["name"]: p for p in body["contract"]["schema"][0]["properties"]}
    assert props["id"]["description"] == "col0"
    assert props["email"]["tags"] == ["DATA_CLASSIFICATION=PD_DATA"]
    assert body["contract"]["schema"][0]["quality"][0]["metric"] == "rowCount"


def test_enrich_all_endpoint_without_catalog(monkeypatch):
    import dcx.enrich.base as enrich_mod
    monkeypatch.setattr(enrich_mod, "_llm_complete", _combined_complete)
    client = TestClient(build_dcx_api_app())
    response = client.post(
        "/enrich/all",
        json={
            "contract": {
                "apiVersion": "v3.1.0", "kind": "DataContract", "id": "c",
                "name": "C", "version": "1.0.0",
                "schema": [{"name": "t", "properties": [
                    {"name": "id", "logicalType": "integer"},
                    {"name": "email", "logicalType": "string"},
                    {"name": "x", "logicalType": "string"},
                ]}],
            },
            "options": {},
        },
    )
    assert response.status_code == 200, response.text
    props = {p["name"]: p for p in response.json()["contract"]["schema"][0]["properties"]}
    assert props["email"].get("tags") is None


# === CLI ====================================================================


def test_cli_enriches_via_monkeypatched_llm(tmp_path, monkeypatch):
    # Patch the litellm-backed completion so no network/key is needed.
    import dcx.enrich.base as enrich_mod
    monkeypatch.setattr(
        enrich_mod, "_llm_complete",
        lambda messages, tools, settings, max_tokens: {
            "columns": [{"id": 0, "description": "Surrogate key."}],
        },
    )
    contract_path = tmp_path / "datacontract.yaml"
    contract_path.write_text(CONTRACT_YAML)
    out_path = tmp_path / "out.yaml"

    result = runner.invoke(app, [
        "enrich", "columns", str(contract_path),
        "--output", str(out_path),
    ])
    assert result.exit_code == 0, result.output
    enriched = OpenDataContractStandard.from_file(str(out_path))
    assert _props(enriched)["id"].description == "Surrogate key."


def test_cli_no_api_key_flag(tmp_path):
    contract_path = tmp_path / "datacontract.yaml"
    contract_path.write_text(CONTRACT_YAML)
    result = runner.invoke(app, [
        "enrich", "columns", str(contract_path), "--api-key", "secret",
    ])
    assert result.exit_code != 0
    assert "api-key" in result.output.lower() or "api_key" in result.output.lower()


def test_cli_quality_via_monkeypatched_llm(tmp_path, monkeypatch):
    import dcx.enrich.base as enrich_mod
    monkeypatch.setattr(
        enrich_mod, "_llm_complete",
        lambda messages, tools, settings, max_tokens: {
            "table_rules": [{"name": "vol", "dimension": "coverage", "type": "library",
                             "metric": "rowCount", "operator": {"name": "mustBeGreaterThan", "value": 0}}],
            "columns": [{"id": 0, "rules": [{"name": "id nn", "dimension": "completeness",
                         "type": "library", "metric": "nullValues",
                         "operator": {"name": "mustBe", "value": 0}}]}],
        },
    )
    contract_path = tmp_path / "datacontract.yaml"
    contract_path.write_text(CONTRACT_YAML)
    out_path = tmp_path / "out.yaml"
    result = runner.invoke(app, [
        "enrich", "quality", str(contract_path), "--output", str(out_path),
    ])
    assert result.exit_code == 0, result.output
    enriched = OpenDataContractStandard.from_file(str(out_path))
    assert enriched.schema_[0].quality[0].metric == "rowCount"
    assert _props(enriched)["id"].quality[0].metric == "nullValues"


def test_cli_tags_via_monkeypatched_llm(tmp_path, monkeypatch):
    import dcx.enrich.base as enrich_mod
    monkeypatch.setattr(
        enrich_mod, "_llm_complete",
        lambda messages, tools, settings, max_tokens: {
            "columns": [{"id": 1, "tags": [{"name": "DATA_CLASSIFICATION", "value": "PD_DATA"}]}],
        },
    )
    contract_path = tmp_path / "datacontract.yaml"
    contract_path.write_text(CONTRACT_YAML)
    catalog_path = tmp_path / "catalog.yaml"
    catalog_path.write_text(
        "tags:\n  - name: DATA_CLASSIFICATION\n    values:\n      - value: PD_DATA\n      - value: PUBLIC\n"
    )
    out_path = tmp_path / "out.yaml"

    result = runner.invoke(app, [
        "enrich", "tags", str(contract_path),
        "--catalog", str(catalog_path),
        "--output", str(out_path),
    ])
    assert result.exit_code == 0, result.output
    enriched = OpenDataContractStandard.from_file(str(out_path))
    assert _props(enriched)["email"].tags == ["DATA_CLASSIFICATION=PD_DATA"]


def test_cli_all_via_monkeypatched_llm(tmp_path, monkeypatch):
    import dcx.enrich.base as enrich_mod
    monkeypatch.setattr(enrich_mod, "_llm_complete", _combined_complete)
    contract_path = tmp_path / "datacontract.yaml"
    contract_path.write_text(CONTRACT_YAML)
    catalog_path = tmp_path / "catalog.yaml"
    catalog_path.write_text(
        "tags:\n  - name: DATA_CLASSIFICATION\n    values:\n      - value: PD_DATA\n      - value: PUBLIC\n"
    )
    out_path = tmp_path / "out.yaml"
    result = runner.invoke(app, [
        "enrich", "all", str(contract_path),
        "--catalog", str(catalog_path), "--output", str(out_path),
    ])
    assert result.exit_code == 0, result.output
    enriched = OpenDataContractStandard.from_file(str(out_path))
    props = _props(enriched)
    assert props["id"].description == "col0"
    assert props["email"].tags == ["DATA_CLASSIFICATION=PD_DATA"]
    assert enriched.schema_[0].quality[0].metric == "rowCount"


def test_cli_tags_missing_catalog_errors(tmp_path):
    contract_path = tmp_path / "datacontract.yaml"
    contract_path.write_text(CONTRACT_YAML)
    result = runner.invoke(app, [
        "enrich", "tags", str(contract_path),
        "--catalog", str(tmp_path / "does-not-exist.yaml"),
    ])
    assert result.exit_code != 0
    assert "not found" in result.output.lower()


def test_enrich_in_dcx_commands():
    from dcx.cli import DCX_COMMANDS
    assert "enrich" in DCX_COMMANDS


def test_default_model_is_set():
    assert DEFAULT_MODEL.startswith("anthropic/")


# === API ====================================================================


def test_enrich_endpoint_registered():
    app_ = build_dcx_api_app()
    paths = {getattr(r, "path", "") for r in app_.routes}
    assert "/enrich/columns" in paths
    assert "/enrich/quality" in paths
    assert "/enrich/tags" in paths
    assert "/enrich/all" in paths


def test_enrich_quality_endpoint_works(monkeypatch):
    import dcx.enrich.base as enrich_mod
    monkeypatch.setattr(
        enrich_mod, "_llm_complete",
        lambda *a: {
            "table_rules": [],
            "columns": [{"id": 0, "rules": [{"name": "nn", "dimension": "completeness",
                         "type": "library", "metric": "nullValues",
                         "operator": {"name": "mustBe", "value": 0}}]}],
        },
    )
    client = TestClient(build_dcx_api_app())
    response = client.post(
        "/enrich/quality",
        json={
            "contract": {
                "apiVersion": "v3.1.0", "kind": "DataContract", "id": "c",
                "name": "C", "version": "1.0.0",
                "schema": [{"name": "t", "properties": [{"name": "id", "logicalType": "integer"}]}],
            },
            "options": {"model": "gpt-4o"},
        },
    )
    assert response.status_code == 200, response.text
    props = {p["name"]: p for p in response.json()["contract"]["schema"][0]["properties"]}
    assert props["id"]["quality"][0]["metric"] == "nullValues"


def test_enrich_tags_endpoint_works(monkeypatch):
    import dcx.enrich.base as enrich_mod
    monkeypatch.setattr(
        enrich_mod, "_llm_complete",
        lambda *a: {"columns": [{"id": 0, "tags": [{"name": "PII", "value": "yes"}]}]},
    )
    client = TestClient(build_dcx_api_app())
    response = client.post(
        "/enrich/tags",
        json={
            "contract": {
                "apiVersion": "v3.1.0", "kind": "DataContract", "id": "c",
                "name": "C", "version": "1.0.0",
                "schema": [{"name": "t", "properties": [{"name": "email", "logicalType": "string"}]}],
            },
            "catalog": {"tags": [{"name": "PII", "values": ["yes", "no"]}]},
            "options": {"model": "gpt-4o"},
        },
    )
    assert response.status_code == 200, response.text
    props = {p["name"]: p for p in response.json()["contract"]["schema"][0]["properties"]}
    assert props["email"]["tags"] == ["PII=yes"]


def test_enrich_tags_endpoint_bad_catalog(monkeypatch):
    client = TestClient(build_dcx_api_app())
    response = client.post(
        "/enrich/tags",
        json={
            "contract": {
                "apiVersion": "v3.1.0", "kind": "DataContract", "id": "c",
                "name": "C", "version": "1.0.0",
                "schema": [{"name": "t", "properties": [{"name": "email", "logicalType": "string"}]}],
            },
            "catalog": {"tags": []},  # invalid
            "options": {},
        },
    )
    assert response.status_code == 400
    assert "tags" in response.text.lower()


def test_enrich_endpoint_works(monkeypatch):
    import dcx.enrich.base as enrich_mod
    monkeypatch.setattr(
        enrich_mod, "_llm_complete",
        lambda messages, tools, settings, max_tokens: {
            "columns": [
                {"id": 0, "description": "Key."},
                {"id": 1, "description": "Email.", "logicalTypeOptions": {"format": "email"}},
            ],
        },
    )
    client = TestClient(build_dcx_api_app())
    response = client.post(
        "/enrich/columns",
        json={
            "contract": {
                "apiVersion": "v3.1.0",
                "kind": "DataContract",
                "id": "c",
                "name": "C",
                "version": "1.0.0",
                "schema": [
                    {
                        "name": "customers",
                        "properties": [
                            {"name": "id", "logicalType": "integer"},
                            {"name": "email", "logicalType": "string"},
                        ],
                    }
                ],
            },
            "options": {"model": "gpt-4o"},
        },
    )
    assert response.status_code == 200, response.text
    body = response.json()
    props = {p["name"]: p for p in body["contract"]["schema"][0]["properties"]}
    assert props["id"]["description"] == "Key."
    assert props["email"]["logicalTypeOptions"] == {"format": "email"}


def test_enrich_endpoint_yaml_response(monkeypatch):
    import dcx.enrich.base as enrich_mod
    monkeypatch.setattr(
        enrich_mod, "_llm_complete",
        lambda *a: {"columns": [{"id": 0, "description": "Key."}]},
    )
    client = TestClient(build_dcx_api_app())
    response = client.post(
        "/enrich/columns?format=yaml",
        json={
            "contract": {
                "apiVersion": "v3.1.0", "kind": "DataContract", "id": "c",
                "name": "C", "version": "1.0.0",
                "schema": [{"name": "t", "properties": [{"name": "id", "logicalType": "integer"}]}],
            },
            "options": {},
        },
    )
    assert response.status_code == 200, response.text
    assert "text/yaml" in response.headers["content-type"]
    assert "description: Key." in response.text


def test_enrich_endpoint_propagates_llm_error(monkeypatch):
    import dcx.enrich.base as enrich_mod

    def boom(*a):
        raise enrich_mod.EnrichError("LLM call failed: boom")

    monkeypatch.setattr(enrich_mod, "_llm_complete", boom)
    client = TestClient(build_dcx_api_app())
    response = client.post(
        "/enrich/columns",
        json={
            "contract": {
                "apiVersion": "v3.1.0", "kind": "DataContract", "id": "c",
                "name": "C", "version": "1.0.0",
                "schema": [{"name": "t", "properties": [{"name": "id", "logicalType": "integer"}]}],
            },
            "options": {},
        },
    )
    assert response.status_code == 502
    assert "boom" in response.text
