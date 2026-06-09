"""`dcx enrich tags` — controlled-vocabulary column tagging via an LLM.

The user supplies a catalog of tag names, each with allowed values (plus
descriptions/examples to guide classification). The LLM may only assign values
that exist in the catalog. Tags are written using the `NAME=VALUE` convention
that the snowflake-full export already consumes.
"""

import json
from dataclasses import dataclass, field
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
    _err,
    _max_tokens_for,
    _read_contract_or_exit,
    _write_contract,
    enrich_app,
)


@dataclass
class TagValueDef:
    """One allowed value of a tag, with hints that help the LLM classify."""

    value: str
    description: Optional[str] = None
    examples: list = field(default_factory=list)
    default: bool = False  # assigned automatically when the LLM picks no value for this tag


@dataclass
class TagDefinition:
    """A tag name and the closed set of values it may take."""

    name: str
    description: Optional[str] = None
    multiple: bool = False  # may a single column carry more than one value of this tag?
    values: list[TagValueDef] = field(default_factory=list)

    def allowed_values(self) -> set[str]:
        return {v.value for v in self.values}

    def default_value(self) -> Optional[str]:
        return next((v.value for v in self.values if v.default), None)


@dataclass
class TagCatalog:
    """The user's controlled tag vocabulary."""

    tags: list[TagDefinition]

    def names(self) -> set[str]:
        return {t.name for t in self.tags}

    def get(self, name: Optional[str]) -> Optional[TagDefinition]:
        return next((t for t in self.tags if t.name == name), None)


def parse_tag_catalog(data: Any) -> TagCatalog:
    """Build a TagCatalog from a dict or a YAML/JSON string.

    Expected shape::

        tags:
          - name: DATA_CLASSIFICATION
            description: Sensitivity of the column.
            multiple: false
            values:
              - value: CONFIDENTIAL
                description: Personal or sensitive business data; need-to-know access.
                examples: [email, phone, full_name]
              - value: PUBLIC
                description: Non-sensitive, shareable data.
    """
    if isinstance(data, str):
        import yaml
        data = yaml.safe_load(data)
    if not isinstance(data, dict):
        raise EnrichError("Tag catalog must be a mapping with a 'tags' list.")

    raw_tags = data.get("tags")
    if not isinstance(raw_tags, list) or not raw_tags:
        raise EnrichError("Tag catalog must define a non-empty 'tags' list.")

    tags: list[TagDefinition] = []
    for raw in raw_tags:
        if not isinstance(raw, dict):
            raise EnrichError("Each catalog tag must be a mapping.")
        name = raw.get("name")
        if not name:
            raise EnrichError("Each catalog tag needs a 'name'.")

        values: list[TagValueDef] = []
        for rv in raw.get("values") or []:
            if isinstance(rv, dict):
                if "value" not in rv:
                    raise EnrichError(f"Tag '{name}' has a value entry without 'value'.")
                values.append(TagValueDef(
                    value=str(rv["value"]),
                    description=rv.get("description"),
                    examples=list(rv.get("examples") or []),
                    default=bool(rv.get("default", False)),
                ))
            elif isinstance(rv, (str, int, float, bool)):
                # Scalars are coerced to str. Note: unquoted YAML `yes`/`no`/`on`
                # become booleans -> "True"/"False"; quote them in the catalog to
                # keep the literal text.
                values.append(TagValueDef(value=str(rv)))
            else:
                raise EnrichError(f"Tag '{name}' has an invalid value entry.")

        if not values:
            raise EnrichError(f"Tag '{name}' must define at least one value.")

        if sum(1 for v in values if v.default) > 1:
            raise EnrichError(f"Tag '{name}' has more than one default value.")

        tags.append(TagDefinition(
            name=str(name),
            description=raw.get("description"),
            multiple=bool(raw.get("multiple", False)),
            values=values,
        ))

    return TagCatalog(tags=tags)


def load_tag_catalog_file(path: Path) -> TagCatalog:
    if not path.exists():
        raise EnrichError(f"Tag catalog file not found: {path}")
    import yaml
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise EnrichError(f"Could not parse tag catalog {path}: {exc}")
    return parse_tag_catalog(data)


_TAGS_TOOL_NAME = "submit_column_tags"

_TAGS_SYSTEM_PROMPT = """\
You are a data-governance classifier. You assign tags to the columns of an Open
Data Contract (ODCS) using ONLY the controlled tag catalog you are given.

For each column, decide which catalog tag values apply, based on the column's
name, type, description and examples, and the catalog's value descriptions and
examples.

Rules:
- Choose `name`/`value` pairs ONLY from the catalog. Never invent a tag name or
  a value, and use the catalog's exact strings.
- For a tag whose `multiple` is false, assign at most ONE value to a column.
- If no value of a tag applies to a column, do not assign that tag.
- Assigning no tags at all to a column is correct when nothing applies.
- Return an entry for every column id (use an empty `tags` list when none apply).
"""


def _build_tags_tool_schema() -> list[dict]:
    return [
        {
            "type": "function",
            "function": {
                "name": _TAGS_TOOL_NAME,
                "description": "Assign catalog tag values to each column.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "columns": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "id": {"type": "integer", "description": "The column id you were given."},
                                    "tags": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "name": {"type": "string"},
                                                "value": {"type": "string"},
                                            },
                                            "required": ["name", "value"],
                                        },
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


def _catalog_payload(catalog: TagCatalog) -> dict:
    return {
        "tags": [
            {
                "name": td.name,
                "description": td.description,
                "multiple": td.multiple,
                "values": [
                    {"value": vd.value, "description": vd.description, "examples": vd.examples}
                    for vd in td.values
                ],
            }
            for td in catalog.tags
        ]
    }


def _build_tags_messages(
    contract: OpenDataContractStandard,
    schema_obj_name: Optional[str],
    schema_obj_description: Optional[str],
    columns: list[dict],
    catalog: TagCatalog,
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
        "Classify the following ODCS columns against the tag catalog.\n\n"
        "TAG CATALOG:\n"
        + json.dumps(_catalog_payload(catalog), indent=2, default=str)
        + "\n\nCOLUMNS:\n"
        + json.dumps({"context": context, "columns": columns}, indent=2, default=str)
    )
    if settings.instructions:
        user_text += f"\n\nAdditional classification guidance from the data owner:\n{settings.instructions}"

    return [
        {"role": "system", "content": _TAGS_SYSTEM_PROMPT},
        {"role": "user", "content": user_text},
    ]


def _parse_tag_name(tag: str) -> str:
    """Tag name is everything before the first `=` (NAME=VALUE convention)."""
    return tag.split("=", 1)[0]


def _column_needs_tags(prop: SchemaProperty, catalog: TagCatalog, overwrite: bool) -> bool:
    if overwrite:
        return True
    existing = {_parse_tag_name(t) for t in (prop.tags or [])}
    return any(td.name not in existing for td in catalog.tags)


def _validated_pairs(chosen: Any, catalog: TagCatalog) -> list[tuple[str, str]]:
    """Keep only (name, value) pairs that exist in the catalog; enforce single-value tags."""
    if not isinstance(chosen, list):
        return []
    out: list[tuple[str, str]] = []
    seen_single: set[str] = set()
    for item in chosen:
        if not isinstance(item, dict):
            continue
        name = item.get("name")
        value = item.get("value")
        td = catalog.get(name)
        if td is None or str(value) not in td.allowed_values():
            continue
        if not td.multiple:
            if name in seen_single:
                continue
            seen_single.add(name)
        out.append((str(name), str(value)))
    return out


def _apply_tags(
    prop: SchemaProperty, entry: dict, catalog: TagCatalog, overwrite: bool,
) -> None:
    """Merge validated catalog tags onto a property's `tags` list (NAME=VALUE).

    Only catalog-managed tag names are touched — tags from other sources are
    preserved. Without overwrite: single-value tags already present are kept;
    multi-value tags gain new values. With overwrite: catalog tags are replaced.
    """
    chosen = _validated_pairs(entry.get("tags"), catalog)
    catalog_names = catalog.names()
    existing = list(prop.tags or [])

    if overwrite:
        result = [t for t in existing if _parse_tag_name(t) not in catalog_names]
    else:
        result = list(existing)

    result_tags = set(result)
    result_names = {_parse_tag_name(t) for t in result}

    for name, value in chosen:
        td = catalog.get(name)
        tag = f"{name}={value}"
        if tag in result_tags:
            continue
        if not overwrite and td is not None and not td.multiple and name in result_names:
            continue  # single-value tag already set — preserve it
        result.append(tag)
        result_tags.add(tag)
        result_names.add(name)

    # Defaults: any catalog tag with a default value that the column still has no
    # value for gets the default assigned.
    for td in catalog.tags:
        default = td.default_value()
        if default is not None and td.name not in result_names:
            tag = f"{td.name}={default}"
            result.append(tag)
            result_names.add(td.name)

    prop.tags = result or None


def enrich_tags_contract(
    contract: OpenDataContractStandard,
    settings: EnrichSettings,
    catalog: TagCatalog,
    *,
    complete: Optional[CompleteFn] = None,
) -> OpenDataContractStandard:
    """Classify each column against `catalog` and write NAME=VALUE tags in place.

    One LLM request per schema object. Tables whose columns are already fully
    classified (for every catalog tag name) are skipped unless `--overwrite`.
    """
    if not catalog.tags:
        raise EnrichError("Tag catalog is empty.")

    complete = complete or base._llm_complete
    tools = _build_tags_tool_schema()

    for schema_obj in contract.schema_ or []:
        if settings.schema_name != "all" and schema_obj.name != settings.schema_name:
            continue

        pairs = _collect_properties(schema_obj.properties or [])
        if not pairs:
            continue
        if not any(_column_needs_tags(prop, catalog, settings.overwrite) for _, prop in pairs):
            continue

        columns = [_column_payload(i, path, prop) for i, (path, prop) in enumerate(pairs)]
        messages = _build_tags_messages(
            contract, schema_obj.name, schema_obj.description, columns, catalog, settings,
        )
        result = complete(messages, tools, settings, _max_tokens_for(len(columns)))

        entries = result.get("columns") if isinstance(result, dict) else None
        if not isinstance(entries, list):
            raise EnrichError(
                f"Model response for table '{schema_obj.name}' missing a 'columns' array."
            )

        by_id = {e["id"]: e for e in entries if isinstance(e, dict) and "id" in e}
        for i, (_path, prop) in enumerate(pairs):
            # Always call: even columns the model returned no entry for must get
            # default tags applied. `_apply_tags` is a no-op when there's nothing
            # to add and no default fills a gap.
            _apply_tags(prop, by_id.get(i) or {}, catalog, settings.overwrite)

    return contract


# ---------------------------------------------------------------------------
# CLI wrapper + command
# ---------------------------------------------------------------------------


def apply_enrich_tags(
    *,
    settings: EnrichSettings,
    catalog: TagCatalog,
    contract_path: Path,
    output: Optional[Path] = None,
) -> None:
    """CLI wrapper for `enrich tags`: read → classify against catalog → write.

    Unlike `apply_enrich`, this has no capture contextvar: the API path needs
    the catalog inline in the request, so it builds settings + catalog itself
    and calls `enrich_tags_contract` directly.
    """
    contract = _read_contract_or_exit(contract_path)
    try:
        contract = enrich_tags_contract(contract, settings, catalog)
    except EnrichError as exc:
        _err(f"Error: {exc}")
        raise typer.Exit(1)
    _write_contract(contract, output)


@enrich_app.command("tags")
def enrich_tags_command(
    location: Annotated[
        str, typer.Argument(help="Path to the data contract."),
    ] = "datacontract.yaml",
    catalog: Annotated[
        Path,
        typer.Option(
            "--catalog",
            help=(
                "Tag catalog file (YAML/JSON): tag names, allowed values, and "
                "per-value descriptions/examples that guide classification."
            ),
        ),
    ] = ...,
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
        str, typer.Option(help="Contract schema (table) to tag (default: all)."),
    ] = "all",
    overwrite: Annotated[
        bool, typer.Option(help="Replace existing catalog tags instead of only filling gaps."),
    ] = False,
    instructions: Annotated[
        Optional[str],
        typer.Option(help="Extra classification guidance passed to the model."),
    ] = None,
    debug: Annotated[
        bool, typer.Option(help="Turn on litellm debug logging (prints the resolved request URL)."),
    ] = False,
    output: Annotated[
        Optional[Path],
        typer.Option(help="Write the tagged contract here. Default: stdout."),
    ] = None,
) -> None:
    """Classify each column against a controlled tag catalog (a tag manager).

    The LLM may only assign values defined in --catalog; anything else is
    dropped. Tags are written as `NAME=VALUE` (the convention the snowflake-full
    export consumes). The provider API key is read from the environment.
    """
    try:
        catalog_obj = load_tag_catalog_file(Path(catalog))
    except EnrichError as exc:
        _err(f"Error: {exc}")
        raise typer.Exit(1)

    settings = EnrichSettings(
        model=model,
        base_url=base_url,
        schema_name=schema_name,
        overwrite=overwrite,
        instructions=instructions,
        debug=debug,
    )
    apply_enrich_tags(
        settings=settings, catalog=catalog_obj,
        contract_path=Path(location), output=output,
    )
