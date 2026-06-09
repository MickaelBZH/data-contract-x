"""`dcx enrich columns` — use an LLM to enrich an ODCS schema.

For every column (property) in the contract's schema, the model proposes:
- `description` — a concise business description, and
- `logicalTypeOptions` — ODCS validation constraints (e.g. `format`, `maxLength`,
  `minimum`) inferred *only* when supported by the column name / logical type /
  example values.

**LLM-quality practices baked in:**
- *Forced tool-calling* for structured output — the model must call
  `submit_column_enrichment`, so the response is always schema-valid JSON, never
  prose we have to scrape.
- `temperature=0` for determinism; automatic retries on transient errors.
- Each column is grounded with its name, logical/physical type, current
  description, examples, and key/uniqueness flags, plus the table + contract
  context, so descriptions are specific rather than generic.
- The model is instructed not to fabricate constraints, and we *additionally*
  validate every returned `logicalTypeOptions` against the keys ODCS allows for
  that logical type — anything else is dropped.
- Idempotent by default: existing descriptions/options are preserved; pass
  `--overwrite` to replace them.
"""

import json
from pathlib import Path
from typing import Any, Optional

import typer
from open_data_contract_standard.model import OpenDataContractStandard, SchemaProperty
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

# ODCS v3 `logicalTypeOptions` keys, per logical type. Anything the model returns
# outside the set for a column's logicalType is dropped (defense against
# hallucinated/ill-typed constraints). Keys absent here (e.g. `boolean`) get no
# options at all.
_LOGICAL_TYPE_OPTION_KEYS: dict[str, set[str]] = {
    "string":  {"minLength", "maxLength", "pattern", "format"},
    "integer": {"minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf", "format"},
    "number":  {"minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf", "format"},
    "date":    {"format"},
    "array":   {"minItems", "maxItems", "uniqueItems"},
    "object":  {"minProperties", "maxProperties", "required"},
}

_TOOL_NAME = "submit_column_enrichment"

_SYSTEM_PROMPT = """\
You are a senior data steward enriching an Open Data Contract Standard (ODCS) schema.

For each column you are given, produce:
1. `description`: a clear, concise (1-2 sentence) business description of what the
   column represents and how it is used. Be factual and specific to the table's
   domain. Do not merely restate the column name. Be PII-aware and vendor-neutral.
2. `required`: true if the column must always carry a value (non-null) — e.g.
   identifiers, keys, names, audit timestamps such as created_at. Set false (or
   omit) when a value can legitimately be absent (optional/nullable fields).
3. `unique`: true if every row must have a distinct value — e.g. surrogate keys
   and natural keys like email or username. Omit otherwise.
4. `logicalTypeOptions`: ODCS *value* constraints you can infer with HIGH
   confidence from the column name, its logical type, and any example values.
   Omit entirely when you are not confident. Never fabricate ranges.

CRITICAL: Do NOT use `minLength` or `minimum` as a stand-in for presence or
uniqueness. Express "must be present" with `required: true` and "must be
distinct" with `unique: true`. Reserve `minLength`/`minimum` for genuine domain
value bounds (e.g. age >= 0), never merely to say "not empty".
- NEVER emit `minLength: 1` (or `minLength: 0`). "Non-empty" is `required`'s job.
  Only emit `minLength`/`maxLength` for a real, specific length (e.g. a 3-char code).
- For categorical/enum-like fields (status, channel, gender, ...), only emit a
  `pattern` if the examples make the FULL allowed set clear. If you cannot
  enumerate every value with confidence, omit logicalTypeOptions — do not guess
  a partial pattern or a placeholder `minLength`.

Only use `logicalTypeOptions` keys that are valid for the column's logicalType:
- string:  minLength, maxLength, pattern, format (e.g. email, uuid, uri, date-time, ipv4)
- integer: minimum, maximum, exclusiveMinimum, exclusiveMaximum, multipleOf
- number:  minimum, maximum, exclusiveMinimum, exclusiveMaximum, multipleOf
- date:    format (a strftime-style pattern)
- array:   minItems, maxItems, uniqueItems
- object:  minProperties, maxProperties, required

Guidance:
- Prefer recognised string formats (email, uuid, uri, ipv4) when the name or
  examples clearly indicate them.
- Only set min/max bounds when example values or the column's meaning give clear
  evidence; otherwise omit them. Do not assume non-negativity for monetary
  balances (overdrafts exist) unless the domain clearly forbids it.
- Return an entry for EVERY column you were given, identified by its integer `id`.
"""


# ---------------------------------------------------------------------------
# Property checks
# ---------------------------------------------------------------------------


def _needs_enrichment(prop: SchemaProperty, settings: EnrichSettings) -> bool:
    """Whether this column would have anything filled in for the given settings."""
    if settings.overwrite:
        return True
    if settings.enrich_descriptions and not prop.description:
        return True
    if settings.enrich_type_options and not prop.logicalTypeOptions:
        return True
    return False


# ---------------------------------------------------------------------------
# Validation of model output
# ---------------------------------------------------------------------------


def _valid_options(logical_type: Optional[str], options: Any) -> dict:
    """Filter returned logicalTypeOptions to ODCS-valid keys for the logical type.

    Also strips `minLength <= 1`: that is a presence proxy, and presence is the
    `required` boolean's job (see the system prompt). Kept deterministic here so
    the constraint holds regardless of model compliance.
    """
    allowed = _LOGICAL_TYPE_OPTION_KEYS.get((logical_type or "").lower())
    if not allowed or not isinstance(options, dict):
        return {}
    filtered = {k: v for k, v in options.items() if k in allowed and v is not None}
    if isinstance(filtered.get("minLength"), int) and filtered["minLength"] <= 1:
        filtered.pop("minLength")
    return filtered


def _apply_enrichment(
    prop: SchemaProperty, entry: dict, settings: EnrichSettings,
) -> None:
    """Apply one model entry onto a property, respecting enable flags and overwrite."""
    if settings.enrich_descriptions:
        desc = entry.get("description")
        if isinstance(desc, str) and desc.strip():
            if settings.overwrite or not prop.description:
                prop.description = desc.strip()

    if settings.enrich_type_options:
        opts = _valid_options(prop.logicalType, entry.get("logicalTypeOptions"))
        if opts:
            if settings.overwrite or not prop.logicalTypeOptions:
                # Merge so existing keys win unless overwriting outright.
                existing = {} if settings.overwrite else dict(prop.logicalTypeOptions or {})
                prop.logicalTypeOptions = {**opts, **existing}

        # `required`/`unique` are first-class ODCS booleans. Only ever write the
        # meaningful `true` (false is the ODCS default — don't clutter the
        # contract). Fill gaps unless overwriting.
        if entry.get("required") is True and (settings.overwrite or prop.required is None):
            prop.required = True
        if entry.get("unique") is True and (settings.overwrite or prop.unique is None):
            prop.unique = True


def _migrate_format_custom_property(prop: SchemaProperty) -> None:
    """Move a non-standard `customProperties.format` into `logicalTypeOptions.format`.

    Importers (e.g. JSON) emit `format` as a customProperty, which no ODCS tool
    acts on. `logicalTypeOptions.format` is the standard home. We migrate the
    value (standard location wins if both exist) and drop the redundant custom
    property — eliminating the duplication enrichment would otherwise leave.
    """
    if not prop.customProperties:
        return
    fmt_props = [cp for cp in prop.customProperties if cp.property == "format"]
    if not fmt_props:
        return
    allowed = _LOGICAL_TYPE_OPTION_KEYS.get((prop.logicalType or "").lower())
    if not allowed or "format" not in allowed:
        return  # format isn't representable for this logical type — leave as-is

    opts = dict(prop.logicalTypeOptions or {})
    if "format" not in opts:  # standard location wins; only fill when absent
        opts["format"] = fmt_props[0].value
        prop.logicalTypeOptions = opts

    remaining = [cp for cp in prop.customProperties if cp.property != "format"]
    prop.customProperties = remaining or None


# ---------------------------------------------------------------------------
# Prompt + tool schema
# ---------------------------------------------------------------------------


def _build_tool_schema() -> list[dict]:
    return [
        {
            "type": "function",
            "function": {
                "name": _TOOL_NAME,
                "description": "Submit enrichment for every column you were given.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "columns": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "id": {"type": "integer", "description": "The column id you were given."},
                                    "description": {"type": "string"},
                                    "required": {
                                        "type": "boolean",
                                        "description": "True if the column must always carry a value (non-null).",
                                    },
                                    "unique": {
                                        "type": "boolean",
                                        "description": "True if every row must have a distinct value.",
                                    },
                                    "logicalTypeOptions": {
                                        "type": "object",
                                        "description": "ODCS value constraints valid for this column's logicalType.",
                                    },
                                },
                                "required": ["id"],
                            },
                        }
                    },
                    "required": ["columns"],
                },
            },
        }
    ]


def _build_messages(
    contract: OpenDataContractStandard,
    schema_obj_name: Optional[str],
    schema_obj_description: Optional[str],
    columns: list[dict],
    settings: EnrichSettings,
) -> list[dict]:
    context: dict[str, Any] = {}
    if contract.name:
        context["contractName"] = contract.name
    if contract.description and getattr(contract.description, "purpose", None):
        context["contractPurpose"] = contract.description.purpose
    if contract.domain:
        context["domain"] = contract.domain
    if schema_obj_name:
        context["table"] = schema_obj_name
    if schema_obj_description:
        context["tableDescription"] = schema_obj_description

    user_payload = {"context": context, "columns": columns}
    user_text = (
        "Enrich the following ODCS columns. Use the context to make descriptions "
        "specific. Return one entry per column id via the tool.\n\n"
        + json.dumps(user_payload, indent=2, default=str)
    )
    if settings.instructions:
        user_text += f"\n\nAdditional domain instructions from the data owner:\n{settings.instructions}"

    return [
        {"role": "system", "content": _SYSTEM_PROMPT},
        {"role": "user", "content": user_text},
    ]


# ---------------------------------------------------------------------------
# Core (LLM IO, but no file IO)
# ---------------------------------------------------------------------------


def enrich_columns_contract(
    contract: OpenDataContractStandard,
    settings: EnrichSettings,
    *,
    complete: Optional[CompleteFn] = None,
) -> OpenDataContractStandard:
    """Enrich every column of the contract's schema(s) in place and return it.

    One LLM request per schema object (table) — this gives the model cross-column
    context and keeps requests token-efficient. Tables that already have nothing
    left to fill (given `settings`) are skipped without an API call.

    `complete` lets callers/tests inject the model call; defaults to litellm.
    """
    if not settings.enrich_descriptions and not settings.enrich_type_options:
        raise EnrichError(
            "Nothing to enrich: both descriptions and type-options are disabled."
        )

    complete = complete or base._llm_complete
    tools = _build_tool_schema()

    for schema_obj in contract.schema_ or []:
        if settings.schema_name != "all" and schema_obj.name != settings.schema_name:
            continue

        pairs = _collect_properties(schema_obj.properties or [])
        if not pairs:
            continue

        # Skip the call entirely if nothing needs work (and we're not overwriting).
        if not any(_needs_enrichment(prop, settings) for _, prop in pairs):
            continue

        # Send all columns for context; map results back by id.
        columns = [_column_payload(i, path, prop) for i, (path, prop) in enumerate(pairs)]
        messages = _build_messages(
            contract, schema_obj.name, schema_obj.description, columns, settings,
        )
        result = complete(messages, tools, settings, _max_tokens_for(len(columns)))

        entries = result.get("columns") if isinstance(result, dict) else None
        if not isinstance(entries, list):
            raise EnrichError(
                f"Model response for table '{schema_obj.name}' missing a 'columns' array."
            )

        by_id = {e["id"]: e for e in entries if isinstance(e, dict) and "id" in e}
        for i, (_path, prop) in enumerate(pairs):
            entry = by_id.get(i)
            if entry:
                _apply_enrichment(prop, entry, settings)
            if settings.enrich_type_options:
                _migrate_format_custom_property(prop)

    return contract


# ---------------------------------------------------------------------------
# CLI wrapper + command
# ---------------------------------------------------------------------------


def apply_enrich(
    *,
    settings: EnrichSettings,
    contract_path: Path,
    output: Optional[Path] = None,
) -> None:
    """CLI wrapper: read contract → enrich via LLM → write to file or stdout.

    In API mode (`_enrich_capture_var` set) captures `settings` and returns
    without touching the filesystem or calling the model.
    """
    capture = _enrich_capture_var.get()
    if capture is not None:
        capture["settings"] = settings
        return

    contract = _read_contract_or_exit(contract_path)
    try:
        contract = enrich_columns_contract(contract, settings)
    except EnrichError as exc:
        _err(f"Error: {exc}")
        raise typer.Exit(1)
    _write_contract(contract, output)


@enrich_app.command("columns")
def enrich_columns_command(
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
        str, typer.Option(help="Contract schema (table) to enrich (default: all)."),
    ] = "all",
    descriptions: Annotated[
        bool, typer.Option(help="Generate column descriptions."),
    ] = True,
    type_options: Annotated[
        bool,
        typer.Option(
            help="Infer constraints: logicalTypeOptions plus the `required` and `unique` flags.",
        ),
    ] = True,
    overwrite: Annotated[
        bool, typer.Option(help="Replace existing descriptions/options instead of only filling gaps."),
    ] = False,
    instructions: Annotated[
        Optional[str],
        typer.Option(help="Extra domain guidance passed to the model (e.g. GDPR/PII notes)."),
    ] = None,
    debug: Annotated[
        bool, typer.Option(help="Turn on litellm debug logging (prints the resolved request URL)."),
    ] = False,
    output: Annotated[
        Optional[Path],
        typer.Option(help="Write the enriched contract here. Default: stdout."),
    ] = None,
) -> None:
    """Enrich every column with an LLM-generated description and logicalTypeOptions.

    The provider API key is read from the environment (e.g. ANTHROPIC_API_KEY,
    OPENAI_API_KEY) — there is no --api-key flag by design. Existing values are
    preserved unless you pass --overwrite.
    """
    settings = EnrichSettings(
        model=model,
        base_url=base_url,
        schema_name=schema_name,
        overwrite=overwrite,
        enrich_descriptions=descriptions,
        enrich_type_options=type_options,
        instructions=instructions,
        debug=debug,
    )
    apply_enrich(settings=settings, contract_path=Path(location), output=output)
