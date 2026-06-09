"""`dcx enrich all` — run columns → tags → quality in sequence.

Order matters: descriptions/types/constraints from `columns` ground the
classification in `tags`, and both ground the `quality` suite. Each stage is
independently idempotent.
"""

from pathlib import Path
from typing import Optional

import typer
from open_data_contract_standard.model import OpenDataContractStandard
from typing_extensions import Annotated

from dcx.enrich.base import (
    DEFAULT_MODEL,
    CompleteFn,
    EnrichError,
    EnrichSettings,
    _err,
    _read_contract_or_exit,
    _write_contract,
    enrich_app,
)
from dcx.enrich.columns import enrich_columns_contract
from dcx.enrich.quality import enrich_quality_contract
from dcx.enrich.tags import TagCatalog, enrich_tags_contract, load_tag_catalog_file


def enrich_all_contract(
    contract: OpenDataContractStandard,
    settings: EnrichSettings,
    catalog: Optional[TagCatalog] = None,
    *,
    complete: Optional[CompleteFn] = None,
) -> OpenDataContractStandard:
    """Run columns → tags → quality in order on the same contract.

    Order matters: descriptions/types/constraints from `columns` ground the
    classification in `tags`, and both ground the `quality` suite. `tags` is
    skipped when no `catalog` is given. Each stage is independently idempotent.
    """
    if settings.enrich_descriptions or settings.enrich_type_options:
        enrich_columns_contract(contract, settings, complete=complete)
    if catalog is not None:
        enrich_tags_contract(contract, settings, catalog, complete=complete)
    enrich_quality_contract(contract, settings, complete=complete)
    return contract


# ---------------------------------------------------------------------------
# CLI wrapper + command
# ---------------------------------------------------------------------------


def apply_enrich_all(
    *,
    settings: EnrichSettings,
    contract_path: Path,
    catalog: Optional[TagCatalog] = None,
    output: Optional[Path] = None,
) -> None:
    """CLI wrapper for `enrich all`: read → columns+tags+quality → write.

    No capture contextvar (like `apply_enrich_tags`): the catalog is optional and
    inline for the API path, which builds settings + catalog and calls the core.
    """
    contract = _read_contract_or_exit(contract_path)
    try:
        contract = enrich_all_contract(contract, settings, catalog)
    except EnrichError as exc:
        _err(f"Error: {exc}")
        raise typer.Exit(1)
    _write_contract(contract, output)


@enrich_app.command("all")
def enrich_all_command(
    location: Annotated[
        str, typer.Argument(help="Path to the data contract."),
    ] = "datacontract.yaml",
    catalog: Annotated[
        Optional[Path],
        typer.Option(
            "--catalog",
            help="Tag catalog file (YAML/JSON). Omit to skip the tagging stage.",
        ),
    ] = None,
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
        typer.Option(help="Infer constraints: logicalTypeOptions plus `required`/`unique`."),
    ] = True,
    overwrite: Annotated[
        bool, typer.Option(help="Replace existing values instead of only filling gaps."),
    ] = False,
    instructions: Annotated[
        Optional[str],
        typer.Option(help="Extra domain guidance passed to the model at every stage."),
    ] = None,
    debug: Annotated[
        bool, typer.Option(help="Turn on litellm debug logging (prints the resolved request URL)."),
    ] = False,
    output: Annotated[
        Optional[Path],
        typer.Option(help="Write the fully enriched contract here. Default: stdout."),
    ] = None,
) -> None:
    """Run the full enrichment: columns, then tags (if --catalog), then quality.

    Stages run in order so each grounds the next (descriptions/types inform
    tagging; both inform the quality suite). Every stage is idempotent — already
    enriched parts are left alone unless you pass --overwrite. API key from env.
    """
    catalog_obj: Optional[TagCatalog] = None
    if catalog is not None:
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
        enrich_descriptions=descriptions,
        enrich_type_options=type_options,
        instructions=instructions,
        debug=debug,
    )
    apply_enrich_all(
        settings=settings, catalog=catalog_obj,
        contract_path=Path(location), output=output,
    )
