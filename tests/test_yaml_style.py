"""`dcx.yaml_style` renders multi-line strings as readable block scalars."""

import yaml
from open_data_contract_standard.model import (
    CustomProperty,
    OpenDataContractStandard,
    SchemaObject,
)

import dcx.yaml_style  # noqa: F401  registers the representer


def _contract(view_body: str) -> OpenDataContractStandard:
    contract = OpenDataContractStandard(
        apiVersion="v3.1.0", kind="DataContract", id="db.sch", name="SCH", version="1.0.0",
    )
    contract.schema_ = [
        SchemaObject(
            name="V_BRAND",
            physicalType="view",
            customProperties=[CustomProperty(property="viewDefinition", value=view_body)],
        )
    ]
    return contract


def test_view_definition_dumps_as_block_scalar():
    body = "select a\n,b\nfrom t"
    out = _contract(body).to_yaml()

    assert "value: |-" in out
    assert "\\n" not in out              # no escaped newlines
    assert "    select a" in out         # SQL readable in place


def test_block_scalar_round_trips():
    body = "select a\n,b\nfrom t"
    reloaded = yaml.safe_load(_contract(body).to_yaml())
    assert reloaded["schema"][0]["customProperties"][0]["value"] == body


def test_unrepresentable_body_falls_back_to_quoted_style():
    """Trailing whitespace can't survive a block scalar; PyYAML must fall back."""
    body = "select a   \n,b from t"
    out = _contract(body).to_yaml()

    assert "value: |-" not in out
    assert yaml.safe_load(out)["schema"][0]["customProperties"][0]["value"] == body


def test_single_line_strings_are_untouched():
    assert "name: SCH\n" in _contract("select 1").to_yaml()
