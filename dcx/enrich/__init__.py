"""`dcx enrich` — LLM enrichment of a contract (columns, tags, data quality).

One file per subcommand: `columns`, `tags`, `quality`, `all` (plus shared
machinery in `base`). Importing the four subcommand modules registers their
commands on `enrich_app`; the import order here fixes the order they appear in
`dcx enrich --help`. This package re-exports the public surface the CLI assembly
layer and the API mirror consume. White-box access to a subcommand's internals
imports that submodule directly (e.g. `from dcx.enrich.quality import ...`).

Note: `_llm_complete` is deliberately *not* re-exported — it lives in
`dcx.enrich.base` and is the single monkeypatch point for tests.
"""

from dcx.enrich.base import (
    DEFAULT_MODEL,
    EnrichError,
    EnrichSettings,
    enrich_app,
)
from dcx.enrich.base import _enrich_capture_var  # noqa: F401  re-exported for the API mirror
from dcx.enrich.columns import enrich_columns_contract
from dcx.enrich.quality import enrich_quality_contract
from dcx.enrich.tags import (
    enrich_tags_contract,
    load_tag_catalog_file,
    parse_tag_catalog,
)
from dcx.enrich.all import enrich_all_contract

__all__ = [
    "DEFAULT_MODEL",
    "EnrichError",
    "EnrichSettings",
    "enrich_app",
    "enrich_columns_contract",
    "enrich_tags_contract",
    "enrich_quality_contract",
    "enrich_all_contract",
    "parse_tag_catalog",
    "load_tag_catalog_file",
]
