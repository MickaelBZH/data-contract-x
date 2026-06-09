"""Shared machinery for the `dcx enrich` subcommands.

Holds everything more than one subcommand needs: the error type, the run
`EnrichSettings`, property traversal, the litellm-backed completion call, the
output-token budget, the `enrich_app` Typer group, and the CLI read/write
helpers. Each subcommand module (`columns`, `tags`, `quality`, `all`) imports
from here and registers its command on `enrich_app`.

**Provider-agnostic.** All LLM calls go through `litellm`, so any model it
supports works via `--model` (e.g. `anthropic/claude-sonnet-4-5`, `gpt-4o`,
`gemini/gemini-2.0-flash`, `bedrock/...`, `ollama/llama3`, an Azure deployment).
`--base-url` points at proxies / self-hosted / Ollama / Azure.

**Secrets via env only** (mirrors [[design-dcx-apply-auth]]): there is no
`--api-key` flag. litellm auto-reads the provider's standard env var
(`ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, `GEMINI_API_KEY`, ...).

Note: subcommand cores call `base._llm_complete` *module-qualified* (rather than
importing the name) so a test that monkeypatches `dcx.enrich.base._llm_complete`
patches a single point that every subcommand observes.
"""

import contextvars
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

import typer
from open_data_contract_standard.model import OpenDataContractStandard, SchemaProperty


class EnrichError(Exception):
    """An enrichment failure with a user-actionable message."""


DEFAULT_MODEL = "anthropic/claude-sonnet-4-5"

# Per-request ceiling (seconds) on the LLM call so a hung provider connection
# can't block a worker indefinitely (and, via the API, exhaust the threadpool).
# Generous enough for large single-table enrichments; tune here if needed.
LLM_TIMEOUT_SECONDS = 300


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------


@dataclass
class EnrichSettings:
    """Knobs for an enrichment run. CLI flags and API options both map here."""

    model: str = DEFAULT_MODEL
    base_url: Optional[str] = None
    schema_name: str = "all"
    overwrite: bool = False
    enrich_descriptions: bool = True
    enrich_type_options: bool = True
    instructions: Optional[str] = None
    debug: bool = False


# A pluggable completion function: (messages, tools, settings, max_tokens) -> parsed tool args.
# Defaults to the litellm-backed implementation; tests inject a fake.
CompleteFn = Callable[[list, list, EnrichSettings, int], dict]


# ---------------------------------------------------------------------------
# Property traversal
# ---------------------------------------------------------------------------


def _collect_properties(
    props: list[SchemaProperty], path_prefix: str = "",
) -> list[tuple[str, SchemaProperty]]:
    """Flatten a (possibly nested) property tree to (dotted-path, property) pairs."""
    out: list[tuple[str, SchemaProperty]] = []
    for prop in props:
        path = f"{path_prefix}{prop.name}" if prop.name else f"{path_prefix}?"
        out.append((path, prop))
        if prop.properties:
            out.extend(_collect_properties(prop.properties, path + "."))
        if prop.items:
            out.extend(_collect_properties([prop.items], path + "[]."))
    return out


def _column_payload(
    idx: int, path: str, prop: SchemaProperty, *, include_constraints: bool = False,
) -> dict:
    """Build the per-column context object sent to the model (None/empty dropped).

    `include_constraints` additionally exposes logicalTypeOptions and tags —
    useful for quality-rule generation, which needs the column's domain bounds.
    """
    payload: dict[str, Any] = {"id": idx, "path": path, "name": prop.name}
    if prop.logicalType:
        payload["logicalType"] = prop.logicalType
    if prop.physicalType:
        payload["physicalType"] = prop.physicalType
    if prop.description:
        payload["currentDescription"] = prop.description
    if prop.examples:
        payload["examples"] = prop.examples[:5]
    if prop.primaryKey:
        payload["primaryKey"] = True
    if prop.required:
        payload["required"] = True
    if prop.unique:
        payload["unique"] = True
    if include_constraints:
        if prop.logicalTypeOptions:
            payload["logicalTypeOptions"] = prop.logicalTypeOptions
        if prop.tags:
            payload["tags"] = prop.tags
    return payload


# ---------------------------------------------------------------------------
# litellm-backed completion (default implementation)
# ---------------------------------------------------------------------------


def _llm_complete(messages: list, tools: list, settings: EnrichSettings, max_tokens: int) -> dict:
    """Call the model via litellm with forced tool use; return the parsed tool args."""
    try:
        import litellm
    except ImportError:
        raise EnrichError(
            "litellm is not installed. Install it via `pip install litellm`."
        )

    if settings.debug:
        # Recent litellm ignores LITELLM_LOG=DEBUG; this is the supported switch.
        # Prints the resolved request URL, model, and payload — invaluable for
        # diagnosing 404 / base-URL / deployment-name issues.
        litellm._turn_on_debug()

    forced_tool = tools[0]["function"]["name"]
    try:
        response = litellm.completion(
            model=settings.model,
            messages=messages,
            tools=tools,
            tool_choice={"type": "function", "function": {"name": forced_tool}},
            temperature=0,
            max_tokens=max_tokens,
            num_retries=2,
            timeout=LLM_TIMEOUT_SECONDS,
            api_base=settings.base_url,
        )
    except Exception as exc:
        raise EnrichError(f"LLM call failed: {exc}")

    try:
        message = response.choices[0].message
        tool_calls = getattr(message, "tool_calls", None)
        if not tool_calls:
            raise EnrichError("Model returned no tool call; cannot parse enrichment.")
        raw = tool_calls[0].function.arguments
        return json.loads(raw) if isinstance(raw, str) else raw
    except EnrichError:
        raise
    except Exception as exc:
        raise EnrichError(f"Could not parse model response: {exc}")


def _max_tokens_for(n_columns: int, per_item: int = 250) -> int:
    """Budget output tokens by column count, clamped to a sane window.

    `per_item` is larger for verbose outputs like quality suites.
    """
    return max(1024, min(16384, per_item * n_columns + 512))


# ---------------------------------------------------------------------------
# Typer sub-app (subcommands register themselves on this)
# ---------------------------------------------------------------------------

enrich_app = typer.Typer(
    help="Use an LLM to enrich a data contract (descriptions, type options, tags, quality).",
    no_args_is_help=True,
)


# ---------------------------------------------------------------------------
# CLI helpers (with API-capture contextvar, mirroring target.apply_target)
# ---------------------------------------------------------------------------

# When set (by the API mirror), the `apply_*` wrappers capture the settings
# instead of doing file IO; the API handler then runs the core with the contract
# from the request body. Contextvar keeps this safe across async requests.
_enrich_capture_var: contextvars.ContextVar[Optional[dict]] = contextvars.ContextVar(
    "_enrich_capture_var", default=None,
)


def _err(msg: str) -> None:
    typer.secho(msg, err=True, fg=typer.colors.RED)


def _read_contract_or_exit(contract_path: Path) -> OpenDataContractStandard:
    if not contract_path.exists():
        _err(f"Contract file not found: {contract_path}")
        raise typer.Exit(1)
    return OpenDataContractStandard.from_file(str(contract_path))


def _write_contract(contract: OpenDataContractStandard, output: Optional[Path]) -> None:
    yaml_content = contract.to_yaml()
    if output is None:
        typer.echo(yaml_content, nl=False)
    else:
        output.write_text(yaml_content, encoding="utf-8")
        typer.echo(f"Wrote {output}", err=True)
