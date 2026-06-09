"""`dcx enrich quality` — generate an executable, portable ODCS data quality suite.

Library metrics are preferred (portable + exportable to DMFs etc.); SQL with
`${table}`/`${column}` placeholders covers what library can't (consistency,
freshness, regex).
"""

import json
from pathlib import Path
from typing import Any, Optional

import typer
from open_data_contract_standard.model import DataQuality, OpenDataContractStandard
from typing_extensions import Annotated

from dcx.enrich import base
from dcx.enrich.base import (
    DEFAULT_MODEL,
    CompleteFn,
    EnrichError,
    EnrichSettings,
    _collect_properties,
    _column_payload,
    _enrich_capture_var,
    _err,
    _max_tokens_for,
    _read_contract_or_exit,
    _write_contract,
    enrich_app,
)

# ODCS DataQuality enums (from the ODCS JSON schema).
QUALITY_DIMENSIONS = {
    "accuracy", "completeness", "conformity", "consistency",
    "coverage", "timeliness", "uniqueness",
}
# Map common synonyms the model might emit onto the ODCS dimension vocabulary.
_DIMENSION_SYNONYMS = {
    "validity": "conformity",
    "valid": "conformity",
    "freshness": "timeliness",
    "integrity": "consistency",
    "volume": "coverage",
}
LIBRARY_METRICS = {"nullValues", "missingValues", "invalidValues", "duplicateValues", "rowCount"}
QUALITY_OPERATORS = {
    "mustBe", "mustNotBe", "mustBeGreaterThan", "mustBeGreaterOrEqualTo",
    "mustBeLessThan", "mustBeLessOrEqualTo", "mustBeBetween", "mustNotBeBetween",
}
_NUMERIC_OPERATORS = {
    "mustBeGreaterThan", "mustBeGreaterOrEqualTo", "mustBeLessThan", "mustBeLessOrEqualTo",
}
_RANGE_OPERATORS = {"mustBeBetween", "mustNotBeBetween"}
QUALITY_SEVERITIES = {"info", "warning", "error"}

_QUALITY_TOOL_NAME = "submit_quality_suite"

_QUALITY_SYSTEM_PROMPT = """\
You are a senior data quality engineer authoring an EXECUTABLE, portable data
quality test suite for an Open Data Contract Standard (ODCS) schema.

Produce a COMPLETE suite covering every relevant ODCS quality dimension:
- completeness: required columns must not be null (metric `nullValues` mustBe 0).
- uniqueness: keys / unique columns must have no duplicates (`duplicateValues` mustBe 0).
- conformity & accuracy: values must match their domain — allowed value sets,
  formats (email, uuid, ISO codes), patterns, and numeric/length ranges.
- consistency: cross-column invariants (e.g. created_at <= updated_at).
- timeliness: freshness of event/audit timestamps.
- coverage: table volume / expected cardinality (metric `rowCount`).

Rule construction (best practices):
- PREFER `type: library` with a predefined `metric` (nullValues, missingValues,
  invalidValues, duplicateValues, rowCount) — these are portable and exportable.
  For allowed-value/format checks use `metric: invalidValues` with `arguments`
  (e.g. {"validValues": ["FR","DE"]} or {"format": "email"}) and `mustBe` 0.
- Use `type: sql` ONLY when no library metric fits (cross-column consistency,
  freshness, complex regex). Write portable SQL using the placeholders
  `${table}` and `${column}`; the query MUST return a single comparable number.
- Every rule MUST include exactly one `operator` describing the PASS condition:
  one of mustBe, mustNotBe, mustBeGreaterThan, mustBeGreaterOrEqualTo,
  mustBeLessThan, mustBeLessOrEqualTo, mustBeBetween, mustNotBeBetween.
  For mustBeBetween/mustNotBeBetween, value is a 2-number array [min, max].
- Set `dimension` (accuracy, completeness, conformity, consistency, coverage,
  timeliness, uniqueness), a clear unique `name`, a precise `description`,
  `severity` (error for keys/required/critical data, warning otherwise), and
  `unit` ("rows" or "percent") where meaningful.

Placement:
- Put COLUMN-level rules under each column id (nullValues, duplicateValues,
  invalidValues, per-column sql checks).
- Put TABLE-level rules in `table_rules` (rowCount, cross-column sql checks).

Ground every rule in the provided metadata (logicalType, examples, required,
unique, logicalTypeOptions such as minimum/maximum/pattern/format, tags). Do not
invent constraints with no basis. Aim for completeness: a nullValues check for
every required column, a duplicateValues check for every unique/key column,
validity checks wherever a domain is known, a rowCount check for the table, and
obvious consistency/timeliness checks.
"""


def _rule_schema() -> dict:
    return {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "description": {"type": "string"},
            "dimension": {"type": "string", "enum": sorted(QUALITY_DIMENSIONS)},
            "type": {"type": "string", "enum": ["library", "sql"]},
            "severity": {"type": "string", "enum": sorted(QUALITY_SEVERITIES)},
            "unit": {"type": "string"},
            "metric": {"type": "string", "enum": sorted(LIBRARY_METRICS),
                       "description": "Required when type=library."},
            "arguments": {"type": "object", "description": "Parameters for the metric (e.g. validValues)."},
            "query": {"type": "string",
                      "description": "Required when type=sql. Use ${table}/${column}; return one number."},
            "operator": {
                "type": "object",
                "description": "The pass condition.",
                "properties": {
                    "name": {"type": "string", "enum": sorted(QUALITY_OPERATORS)},
                    "value": {"description": "Number, value, or [min,max] for between operators."},
                },
                "required": ["name", "value"],
            },
        },
        "required": ["name", "dimension", "type", "operator"],
    }


def _build_quality_tool_schema() -> list[dict]:
    rule = _rule_schema()
    return [
        {
            "type": "function",
            "function": {
                "name": _QUALITY_TOOL_NAME,
                "description": "Submit the data quality suite: table-level rules and per-column rules.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "table_rules": {"type": "array", "items": rule},
                        "columns": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "id": {"type": "integer"},
                                    "rules": {"type": "array", "items": rule},
                                },
                                "required": ["id"],
                            },
                        },
                    },
                    "required": ["columns"],
                },
            },
        }
    ]


def _norm_dimension(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    v = value.strip().lower()
    v = _DIMENSION_SYNONYMS.get(v, v)
    return v if v in QUALITY_DIMENSIONS else None


def _validated_operator(op: Any) -> Optional[dict]:
    """Return a single-key operator kwargs dict, or None if invalid."""
    if not isinstance(op, dict):
        return None
    name = op.get("name")
    if name not in QUALITY_OPERATORS or "value" not in op:
        return None
    value = op.get("value")
    if name in _RANGE_OPERATORS:
        if not (isinstance(value, list) and len(value) == 2
                and all(isinstance(x, (int, float)) and not isinstance(x, bool) for x in value)):
            return None
        return {name: value}
    if name in _NUMERIC_OPERATORS:
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            return None
        return {name: value}
    # mustBe / mustNotBe accept any non-null value
    if value is None:
        return None
    return {name: value}


def _build_quality_rule(raw: Any) -> Optional[DataQuality]:
    """Validate one model rule into an ODCS DataQuality, or None to drop it."""
    if not isinstance(raw, dict):
        return None
    operator_kwargs = _validated_operator(raw.get("operator"))
    if operator_kwargs is None:
        return None

    name = raw.get("name") if isinstance(raw.get("name"), str) else None
    description = raw.get("description") if isinstance(raw.get("description"), str) else None
    dimension = _norm_dimension(raw.get("dimension"))
    severity = raw.get("severity") if raw.get("severity") in QUALITY_SEVERITIES else None
    unit = raw.get("unit") if isinstance(raw.get("unit"), str) else None
    common = dict(name=name, description=description, dimension=dimension,
                  severity=severity, unit=unit)

    rtype = raw.get("type")
    if rtype == "sql":
        query = raw.get("query")
        if not isinstance(query, str) or not query.strip():
            return None
        return DataQuality(type="sql", query=query, **operator_kwargs, **common)

    # default: library
    metric = raw.get("metric")
    if metric not in LIBRARY_METRICS:
        return None
    arguments = raw.get("arguments") if isinstance(raw.get("arguments"), dict) else None
    return DataQuality(
        type="library", metric=metric, arguments=arguments, **operator_kwargs, **common,
    )


def _build_quality_messages(
    contract: OpenDataContractStandard,
    schema_obj_name: Optional[str],
    schema_obj_description: Optional[str],
    columns: list[dict],
    settings: EnrichSettings,
) -> list[dict]:
    context: dict[str, Any] = {}
    if contract.name:
        context["contractName"] = contract.name
    if schema_obj_name:
        context["table"] = schema_obj_name
    if schema_obj_description:
        context["tableDescription"] = schema_obj_description

    user_text = (
        "Author a complete ODCS data quality suite for the following table.\n\n"
        + json.dumps({"context": context, "columns": columns}, indent=2, default=str)
    )
    if settings.instructions:
        user_text += f"\n\nAdditional guidance from the data owner:\n{settings.instructions}"

    return [
        {"role": "system", "content": _QUALITY_SYSTEM_PROMPT},
        {"role": "user", "content": user_text},
    ]


def _quality_needs_work(schema_obj, pairs, overwrite: bool) -> bool:
    if overwrite:
        return True
    if not schema_obj.quality:
        return True
    return any(not prop.quality for _, prop in pairs)


def enrich_quality_contract(
    contract: OpenDataContractStandard,
    settings: EnrichSettings,
    *,
    complete: Optional[CompleteFn] = None,
) -> OpenDataContractStandard:
    """Generate an ODCS data quality suite for each schema object in place.

    Table-level rules go on `schema_obj.quality`; column-level rules on each
    property's `quality`. Fills gaps by default (existing suites are preserved);
    `--overwrite` regenerates. One LLM request per schema object.
    """
    complete = complete or base._llm_complete
    tools = _build_quality_tool_schema()

    for schema_obj in contract.schema_ or []:
        if settings.schema_name != "all" and schema_obj.name != settings.schema_name:
            continue

        pairs = _collect_properties(schema_obj.properties or [])
        if not pairs:
            continue
        if not _quality_needs_work(schema_obj, pairs, settings.overwrite):
            continue

        columns = [
            _column_payload(i, path, prop, include_constraints=True)
            for i, (path, prop) in enumerate(pairs)
        ]
        messages = _build_quality_messages(
            contract, schema_obj.name, schema_obj.description, columns, settings,
        )
        result = complete(messages, tools, settings, _max_tokens_for(len(columns), per_item=600))

        entries = result.get("columns") if isinstance(result, dict) else None
        if not isinstance(entries, list):
            raise EnrichError(
                f"Model response for table '{schema_obj.name}' missing a 'columns' array."
            )

        # Table-level rules.
        table_rules = [r for r in (_build_quality_rule(x) for x in (result.get("table_rules") or [])) if r]
        if table_rules and (settings.overwrite or not schema_obj.quality):
            schema_obj.quality = table_rules

        # Column-level rules.
        by_id = {e["id"]: e for e in entries if isinstance(e, dict) and "id" in e}
        for i, (_path, prop) in enumerate(pairs):
            entry = by_id.get(i)
            if not entry:
                continue
            rules = [r for r in (_build_quality_rule(x) for x in (entry.get("rules") or [])) if r]
            if rules and (settings.overwrite or not prop.quality):
                prop.quality = rules

    return contract


# ---------------------------------------------------------------------------
# CLI wrapper + command
# ---------------------------------------------------------------------------


def apply_enrich_quality(
    *,
    settings: EnrichSettings,
    contract_path: Path,
    output: Optional[Path] = None,
) -> None:
    """CLI wrapper for `enrich quality`. Shares the `_enrich_capture_var` so the
    generic API mirror can drive it (the handler dispatches to the quality core)."""
    capture = _enrich_capture_var.get()
    if capture is not None:
        capture["settings"] = settings
        return

    contract = _read_contract_or_exit(contract_path)
    try:
        contract = enrich_quality_contract(contract, settings)
    except EnrichError as exc:
        _err(f"Error: {exc}")
        raise typer.Exit(1)
    _write_contract(contract, output)


@enrich_app.command("quality")
def enrich_quality_command(
    location: Annotated[
        str, typer.Argument(help="Path to the data contract."),
    ] = "datacontract.yaml",
    model: Annotated[
        str,
        typer.Option(
            help="LLM in litellm form, e.g. `anthropic/claude-sonnet-4-5`, `gpt-4o`, `ollama/llama3`.",
        ),
    ] = DEFAULT_MODEL,
    base_url: Annotated[
        Optional[str],
        typer.Option(help="Override the API base URL (proxy, Azure, self-hosted, Ollama)."),
    ] = None,
    schema_name: Annotated[
        str, typer.Option(help="Contract schema (table) to add quality rules to (default: all)."),
    ] = "all",
    overwrite: Annotated[
        bool, typer.Option(help="Replace existing quality suites instead of only filling gaps."),
    ] = False,
    instructions: Annotated[
        Optional[str],
        typer.Option(help="Extra guidance passed to the model (e.g. SLAs, known domains)."),
    ] = None,
    debug: Annotated[
        bool, typer.Option(help="Turn on litellm debug logging (prints the resolved request URL)."),
    ] = False,
    output: Annotated[
        Optional[Path],
        typer.Option(help="Write the contract with quality rules here. Default: stdout."),
    ] = None,
) -> None:
    """Generate an executable ODCS data quality suite across all dimensions.

    Prefers portable `library` metrics (nullValues, duplicateValues, rowCount,
    invalidValues, ...) and falls back to `sql` (with ${table}/${column}
    placeholders) for consistency/freshness/regex checks. Existing quality rules
    are preserved unless you pass --overwrite. API key read from the environment.
    """
    settings = EnrichSettings(
        model=model,
        base_url=base_url,
        schema_name=schema_name,
        overwrite=overwrite,
        instructions=instructions,
        debug=debug,
    )
    apply_enrich_quality(settings=settings, contract_path=Path(location), output=output)
