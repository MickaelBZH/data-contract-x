"""`dcx enrich quality` — generate an executable, portable ODCS data quality suite.

Library metrics are preferred (portable + exportable to DMFs etc.); SQL with
`${table}`/`${column}` placeholders covers what library can't (consistency,
freshness, regex).
"""

import json
from pathlib import Path
from typing import Any, Optional

import typer
from open_data_contract_standard.model import (
    CustomProperty,
    DataQuality,
    OpenDataContractStandard,
)
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
# Bronze scope: the minimum set of checks for streaming/bronze
# assets, split by how the CONTRACT represents them so the output stays valid
# against the UNMODIFIED ODCS schema (no schema patching) and stays engine-neutral:
#
# * STANDARD_LIBRARY_METRICS — names in the ODCS `quality.metric` enum. Authored as
#   `type: library`; portable and understood by any ODCS consumer.
# * CHECK_METRICS — checks with no ODCS metric name (blankCount, freshness).
#   Authored as `type: sql` (a portable query) plus a `{property: "check", value:
#   <name>}` tag. Any SQL engine can run the query; an adapter that recognises the
#   name may accelerate it into a native check (e.g. a Snowflake DMF). No engine
#   specifics live here — that mapping belongs to each exporter/adapter.
STANDARD_LIBRARY_METRICS = {
    "rowCount",            # volume / coverage (table)
    "nullValues",          # completeness (column)
    "duplicateValues",     # uniqueness — duplicate values (column)
    "invalidValues",       # conformity — value outside an allowed set (column; arguments.validValues)
}

# customProperties key naming the abstract check a `type: sql` rule implements. This
# engine-neutral name is the ONLY contract between enrich and an adapter; adapters
# read it to upgrade the sql check to a native one (see dcx/exporters/snowflake.py).
CHECK_PROPERTY = "check"

# Portable `type: sql` query for each check metric (returns one comparable number).
# `${table}`/`${column}` are the ODCS placeholders. The set of keys defines the
# extended (non-ODCS-enum) checks the model may pick from.
_CHECK_QUERY = {
    "blankCount": (
        "SELECT COUNT(*) FROM ${table} "
        "WHERE ${column} IS NOT NULL AND TRIM(CAST(${column} AS STRING)) = ''"
    ),
    "freshness": (
        "SELECT TIMEDIFF(second, MAX(last_altered), CURRENT_TIMESTAMP()) "
        "FROM information_schema.tables WHERE table_name = UPPER('${table}')"
    ),
}
CHECK_METRICS = set(_CHECK_QUERY)  # {"blankCount", "freshness"}
# The full vocabulary the model may pick from (drives the tool-schema `metric` enum).
LIBRARY_METRICS = STANDARD_LIBRARY_METRICS | CHECK_METRICS
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
You are a senior data quality engineer authoring an EXECUTABLE data quality test
suite for an Open Data Contract Standard (ODCS) schema, scoped to the
"bronze" standard for streaming/ingested assets.

Cover ONLY the bronze dimensions below. Do NOT propose accuracy or consistency
library checks — steer those to `type: sql` if truly needed.
- completeness:         required columns must not be null (`nullValues` mustBe 0);
                        string columns must not be blank/empty (`blankCount` mustBe 0).
- conformity (types):   categorical/enum columns must hold only allowed values
                        (`invalidValues` mustBe 0, with the allowed set in
                        `arguments.validValues`).
- uniqueness:           columns expected to be unique must not have duplicates
                        (`duplicateValues` mustBe 0).
- coverage (volume):    table must have data (`rowCount` mustBeGreaterThan 0).
- timeliness (freshness): the table must have been updated recently (`freshness`,
                        TABLE-level, mustBeLessThan a threshold in seconds since the
                        last DML on the table).

Rule construction (best practices):
- PREFER `type: library` with a supported `metric`. Library metrics are portable and
  directly executable by dcx engine adapters (e.g. Snowflake DMFs).
  You may ONLY use these bronze library metrics (anything else MUST be `type: sql`):
    Volume:      rowCount            (table)
    Freshness:   freshness           (table; seconds since last DML)
    Completeness (per column):  nullValues, blankCount
    Conformity/types (per column):  invalidValues
    Uniqueness (per column):  duplicateValues
- `invalidValues` checks a column against a fixed allowed set (it counts rows whose
  value is OUTSIDE the set). ONLY use it when the
  metadata makes the set explicit (logicalTypeOptions.enum, an enum-like tag, or a
  short, well-known categorical from examples). Put the allowed set in
  `arguments.validValues` as an array (e.g. {"validValues": ["Pending", "Dispatched"]});
  use mustBe 0 (rows outside the set). Do NOT guess the set for free-text columns.
- Do NOT invent metric names. Length checks, regex/format checks, and cross-column
  consistency have NO bronze DMF — use
  `type: sql` for those (only when genuinely required). Write portable SQL using
  the placeholders `${table}` and `${column}`; the query MUST return a single
  comparable number.
- Every rule MUST include exactly one `operator` describing the PASS condition:
  one of mustBe, mustNotBe, mustBeGreaterThan, mustBeGreaterOrEqualTo,
  mustBeLessThan, mustBeLessOrEqualTo, mustBeBetween, mustNotBeBetween.
  For mustBeBetween/mustNotBeBetween, value is a 2-number array [min, max].
- Set `dimension` (one of: completeness, conformity, coverage, timeliness,
  uniqueness), a clear unique `name`, a precise `description`, `severity` (error
  for required/critical data, warning otherwise), and `unit` where meaningful.

Placement:
- Put COLUMN-level rules under each column id (nullValues, blankCount,
  duplicateValues, invalidValues, plus per-column sql).
- Put TABLE-level rules in `table_rules` (rowCount, freshness).

Ground every rule in the provided metadata (logicalType, examples, required,
unique, logicalTypeOptions such as minimum/maximum/pattern/format, tags). Do not
invent constraints with no basis. Stay within the bronze scope: a nullValues
check for every required column, a blankCount check for required string columns,
a rowCount check for the table, and a
table-level freshness check (seconds since last DML).
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

    # library-family: the model picked one of the offered metric names.
    metric = raw.get("metric")
    if metric not in LIBRARY_METRICS:
        return None
    if metric in CHECK_METRICS:
        # Extended check → portable `type: sql` + a neutral `check` tag naming the
        # metric. Valid against the unmodified ODCS schema; an engine adapter may
        # upgrade it to a native check.
        return DataQuality(
            type="sql",
            query=_CHECK_QUERY[metric],
            customProperties=[CustomProperty(property=CHECK_PROPERTY, value=metric)],
            **operator_kwargs,
            **common,
        )
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

    Prefers portable `library` metrics that map to Snowflake system DMFs
    (nullValues, duplicateValues, rowCount, freshness, uniqueCount, nullPercent,
    ...) and falls back to `sql` (with ${table}/${column} placeholders) for
    allowed-value/regex/consistency checks that have no DMF. Existing quality
    rules are preserved unless you pass --overwrite. API key read from the
    environment.
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
