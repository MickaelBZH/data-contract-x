"""`dcx export snowflake-full` — Snowflake DDL + tags + (optional) DQ rules.

Generates a single SQL script with up to three sections:

1. **DDL** — `CREATE TABLE` statements (reuses upstream `to_sql_ddl` for the Snowflake dialect).
   Carries table/column comments inline. In *alter-only* mode (no DDL), descriptions
   are instead emitted as standalone `COMMENT ON TABLE/COLUMN ... IS '...'` so they
   reach tables that already exist.
2. **Tags** — `ALTER TABLE ... SET TAG` and `ALTER TABLE ... MODIFY COLUMN ... SET TAG`
   from each ODCS object's `tags` list and each column's `classification`/`tags`.
3. **Data quality** — `ALTER TABLE ... SET DATA_METRIC_SCHEDULE` + `ADD DATA METRIC FUNCTION`
   from each `quality` entry. **Off by default** because Snowflake Data Metric Functions are
   an Enterprise-tier feature. Enable with `--include-quality`.

Tag convention:
- Each ODCS tag becomes a Snowflake tag. Two forms are supported in the `tags`
  list:
    1. Plain name (e.g. `"sensitive"`) — Snowflake requires a non-null value, so
       we emit `SET TAG sensitive = 'sensitive'` (self-referential).
    2. `NAME=VALUE` (e.g. `"DATA_CLASSIFICATION=CONFIDENTIAL"`) — split on the first
       `=` to produce `SET TAG DATA_CLASSIFICATION = 'CONFIDENTIAL'`. This is the
       lightweight extension used to represent Snowflake's TAG_NAME/TAG_VALUE
       pairing (which ODCS doesn't model natively — its `tags` field is
       `list[str]`).
- `classification: "PII"` on a column becomes `SET TAG classification = 'PII'`.
- Use `--tag-namespace DB.SCHEMA` to qualify tag references; `--create-tags` to
  also emit `CREATE TAG IF NOT EXISTS` for each distinct tag used.

Quality convention: ODCS `library`-type quality entries are mapped to
`SNOWFLAKE.CORE.*` system DMFs by their `metric` name. Custom/unmappable rules
emit a `-- TODO` comment instead of breaking the script.
"""

import logging
import re
from contextlib import contextmanager
from enum import Enum
from typing import Any, Iterable, Optional

import datacontract.export.sql_exporter as _upstream_sql_exporter
from datacontract.export.exporter import Exporter
from datacontract.export.exporter_factory import exporter_factory
from datacontract.export.sql_exporter import to_sql_ddl
from datacontract.export.sql_type_converter import convert_to_snowflake
from open_data_contract_standard.model import (
    CustomProperty,
    DataQuality,
    OpenDataContractStandard,
    SchemaObject,
    ServiceLevelAgreementProperty,
)

# ODCS-standard `library` metrics → Snowflake CORE DMF (and where it applies).
# Each entry: metric_name → (snowflake_dmf, scope) where scope is "column" or "table".
#
# These names are part of the ODCS spec's `quality.metric` enum, so a `type: library`
# rule using them validates against the UNMODIFIED ODCS schema (no schema patching)
# and is understood by other ODCS consumers (e.g. Collate). Non-standard checks that
# still map to a DMF are handled via `_CHECK_TO_DMF` / the `check` tag below,
# keeping the contract portable while dcx still emits a native DMF.
_LIBRARY_METRIC_TO_DMF: dict[str, tuple[str, str]] = {
    "rowCount":        ("SNOWFLAKE.CORE.ROW_COUNT", "table"),
    "nullValues":      ("SNOWFLAKE.CORE.NULL_COUNT", "column"),
    "missingValues":   ("SNOWFLAKE.CORE.NULL_COUNT", "column"),
    "duplicateValues": ("SNOWFLAKE.CORE.DUPLICATE_COUNT", "column"),
    # `invalidValues` counts rows whose value is NOT in the allowed set supplied in
    # the rule's `arguments.validValues`; it maps to SNOWFLAKE.CORE.ACCEPTED_VALUES.
    # See `_dmf_binding` for the `ON (col, col -> col IN (...))` association form.
    "invalidValues":   ("SNOWFLAKE.CORE.ACCEPTED_VALUES", "column"),
}

# customProperties key naming the abstract check a `type: sql` rule implements (see
# dcx.enrich.quality.CHECK_PROPERTY). Held locally so this adapter has no dependency
# on the enricher — the shared contract is just the engine-neutral check name.
_CHECK_PROPERTY = "check"

# Checks this adapter can accelerate into a native Snowflake DMF, keyed by the
# engine-neutral name carried in a rule's `check` tag. name -> (dmf, scope). Checks
# not listed here simply run as their portable `sql` query.
_CHECK_TO_DMF: dict[str, tuple[str, str]] = {
    "blankCount": ("SNOWFLAKE.CORE.BLANK_COUNT", "column"),
}

# ODCS `slaProperties[].property` values this adapter enforces natively. ODCS models
# freshness as an SLA, not a quality rule, so it is read from `slaProperties` rather
# than from `quality` — expressing it as a quality rule needs a query that is
# Snowflake-specific in all but name.
#
# `SNOWFLAKE.CORE.FRESHNESS ON ()` measures seconds since the last DML on the table.
# The column form (`ON (col)`) accepts only DATE/TIMESTAMP_LTZ/TIMESTAMP_TZ — not
# TIMESTAMP_NTZ, Snowflake's default TIMESTAMP — so the table form is used, matching
# Snowsight's "last table update" freshness.
_SLA_PROPERTY_TO_DMF: dict[str, tuple[str, str]] = {
    "latency":   ("SNOWFLAKE.CORE.FRESHNESS", "table"),
    "freshness": ("SNOWFLAKE.CORE.FRESHNESS", "table"),
}

# ODCS SLA `unit` → seconds. FRESHNESS reports seconds, so an SLA expressed in hours
# or days is converted rather than rejected. ISO-ish abbreviations per the ODCS field
# description; unknown units are skipped (with a `-- TODO`) rather than guessed.
_SLA_UNIT_SECONDS: dict[str, int] = {
    "s": 1, "sec": 1, "secs": 1, "second": 1, "seconds": 1,
    "m": 60, "min": 60, "mins": 60, "minute": 60, "minutes": 60,
    "h": 3600, "hr": 3600, "hrs": 3600, "hour": 3600, "hours": 3600,
    "d": 86400, "day": 86400, "days": 86400,
}


class DdlMode(str, Enum):
    """How the snowflake-full SQL handles table creation when the table may or may
    not exist. Shared by `dcx apply snowflake` and `dcx export snowflake-full`.

    - `auto`   — `CREATE TABLE IF NOT EXISTS`: create missing tables, govern
      existing ones (comments/tags/DQ). The default; safe when existence is unknown.
    - `always` — plain `CREATE TABLE`: fails if the table already exists. Use when
      you intend to create fresh tables and want a loud error otherwise.
    - `never`  — no `CREATE TABLE`: alter-only governance of existing tables.
    """

    auto = "auto"
    always = "always"
    never = "never"

    def to_sql_kwargs(self) -> dict[str, bool]:
        """Map to the `to_snowflake_full_sql` DDL flags."""
        return {
            DdlMode.auto:   {"include_ddl": True, "ddl_if_not_exists": True},
            DdlMode.always: {"include_ddl": True, "ddl_if_not_exists": False},
            DdlMode.never:  {"include_ddl": False, "ddl_if_not_exists": False},
        }[self]


class SnowflakeFullExporter(Exporter):
    """Exporter for `snowflake-full` format — registered in the upstream factory."""

    def export(
        self,
        data_contract: OpenDataContractStandard,
        schema_name: str,
        server: str,
        sql_server_type: str,
        export_args: dict,
    ) -> str:
        return to_snowflake_full_sql(
            data_contract,
            include_ddl=bool(export_args.get("include_ddl", True)),
            ddl_if_not_exists=bool(export_args.get("ddl_if_not_exists", False)),
            structured_types=bool(export_args.get("structured_types", False)),
            include_comments=bool(export_args.get("include_comments", True)),
            include_tags=bool(export_args.get("include_tags", True)),
            include_quality=bool(export_args.get("include_quality", False)),
            create_tags=bool(export_args.get("create_tags", False)),
            tag_namespace=export_args.get("tag_namespace"),
            tag_namespace_filter=export_args.get("tag_namespace_filter"),
            metric_schedule=export_args.get("metric_schedule", "USING CRON 0 0 * * * UTC"),
            server=server,
        )


@contextmanager
def _snowflake_correct_escape():
    """Temporarily swap upstream `_escape` for a Snowflake-correct version.

    The upstream helper at `datacontract.export.sql_exporter._escape` only escapes
    double quotes (for Databricks' double-quoted COMMENT). When the same helper is
    reused inside Snowflake's single-quoted COMMENT, descriptions containing `'`
    produce broken SQL like `COMMENT 'customer's email'`. Within this context we
    replace `_escape` with one that doubles single quotes (Snowflake's standard
    escape) AND keeps the double-quote behavior, then restore on exit so other
    code paths (e.g. Databricks export via the same function) are unaffected.
    """
    original = _upstream_sql_exporter._escape

    def _escape(text):
        if text is None:
            return None
        return text.replace("'", "''").replace('"', '\\"')

    _upstream_sql_exporter._escape = _escape
    try:
        yield
    finally:
        _upstream_sql_exporter._escape = original


def _snowflake_table_prefix(
    contract: OpenDataContractStandard, server_name: Optional[str],
) -> str:
    """Return the `DB.SCHEMA.` prefix from the contract's Snowflake server, or ''.

    Mirrors upstream's `to_sql_ddl` behavior for Databricks (which already prefixes
    `catalog.schema.`) — we provide the equivalent for Snowflake, which upstream
    omits. If `server_name` is given, only that named server is considered.
    """
    servers = contract.servers or []
    if server_name:
        servers = [s for s in servers if s.server == server_name]
    for srv in servers:
        if srv.type != "snowflake":
            continue
        db, schema = srv.database, srv.schema_
        if db and schema:
            return f"{db}.{schema}."
        if db:
            return f"{db}."
        if schema:
            return f"{schema}."
    return ""


def _leaf_snowflake_type(prop) -> str:
    """Snowflake column type for a leaf property (no nested properties/items)."""
    return convert_to_snowflake(prop) or prop.physicalType or "VARIANT"


def _snowflake_structured_type(prop) -> Optional[str]:
    """Render a Snowflake *structured* type from a property's nested shape.

    - object with `properties`  → ``OBJECT(<field> <type>, ...)``
    - array with typed `items`  → ``ARRAY(<item type>)``

    Returns None for a leaf, or for an object/array with no defined inner shape
    (e.g. a free-form VARIANT), which keeps its bare semi-structured type. Recurses,
    so nested structs/arrays nest in the type. Per-subfield comments aren't
    representable inside a Snowflake structured type, so they stay in the contract.
    """
    if prop.properties:
        fields = []
        for child in prop.properties:
            cname = child.physicalName or child.name
            if not cname:
                continue
            ctype = _snowflake_structured_type(child) or _leaf_snowflake_type(child)
            fields.append(f"{cname} {ctype}")
        return f"OBJECT({', '.join(fields)})" if fields else None
    if prop.items:
        item_type = _snowflake_structured_type(prop.items) or _leaf_snowflake_type(prop.items)
        return f"ARRAY({item_type})"
    return None


def _pin_structured_types(contract: OpenDataContractStandard) -> None:
    """Pin ``OBJECT(...)``/``ARRAY(...)`` structured types (via `snowflakeType`) for
    top-level columns that have a defined nested shape, so the CREATE TABLE captures
    the structure. Free-form columns (VARIANT, or object/array with no inner shape)
    are left bare. Honors a user-supplied snowflakeType/physicalType. Idempotent.

    Only top-level columns are pinned — Snowflake DDL renders only those.
    """
    logger = logging.getLogger("datacontract.export.sql_type_converter")
    prev_level = logger.level
    logger.setLevel(logging.ERROR)  # silence warnings while resolving leaf types
    try:
        for schema_obj in contract.schema_ or []:
            for prop in schema_obj.properties or []:
                pinned = {cp.property for cp in (prop.customProperties or [])}
                if "snowflakeType" in pinned or "physicalType" in pinned:
                    continue
                structured = _snowflake_structured_type(prop)
                if structured is None:
                    continue
                prop.customProperties = (prop.customProperties or []) + [
                    CustomProperty(property="snowflakeType", value=structured)
                ]
    finally:
        logger.setLevel(prev_level)


def _pin_unmapped_snowflake_types(contract: OpenDataContractStandard) -> None:
    """Pin Snowflake-native column types that upstream's converter doesn't recognize.

    Upstream's snowflake type table maps OBJECT/ARRAY but not VARIANT (nor
    GEOGRAPHY/GEOMETRY/VECTOR/TIMESTAMP_LTZ/...). For such a column it logs a
    `Cannot map type ...` warning and falls back to the raw physicalType verbatim —
    correct, but noisy. We probe upstream's own converter and, for any top-level
    column it can't map, add a `snowflakeType` custom property equal to the
    physicalType; the converter honors that first, so the DDL maps cleanly with no
    warning. Self-maintaining (uses upstream's logic) and idempotent.

    Only top-level columns are pinned — Snowflake DDL renders only those; nested
    fields live inside an OBJECT/VARIANT/ARRAY column.
    """
    logger = logging.getLogger("datacontract.export.sql_type_converter")
    prev_level = logger.level
    logger.setLevel(logging.ERROR)  # silence the probe's would-be warning
    try:
        for schema_obj in contract.schema_ or []:
            for prop in schema_obj.properties or []:
                if not prop.physicalType:
                    continue
                pinned = {cp.property for cp in (prop.customProperties or [])}
                if "snowflakeType" in pinned or "physicalType" in pinned:
                    continue  # already pinned (by the user or a previous pass)
                if convert_to_snowflake(prop) is not None:
                    continue  # upstream maps it fine — leave it alone
                prop.customProperties = (prop.customProperties or []) + [
                    CustomProperty(property="snowflakeType", value=prop.physicalType)
                ]
    finally:
        logger.setLevel(prev_level)


def _qualify_ddl_tables(
    ddl: str, prefix: str, contract: OpenDataContractStandard, *, if_not_exists: bool = False,
) -> str:
    """Qualify `CREATE TABLE <name>` with the db.schema prefix in upstream DDL, and
    optionally make it `CREATE TABLE IF NOT EXISTS` (idempotent create-if-missing)."""
    ine = "IF NOT EXISTS " if if_not_exists else ""
    for schema_obj in contract.schema_ or []:
        name = schema_obj.name
        if not name:
            continue
        ddl = ddl.replace(
            f"CREATE TABLE {name} (", f"CREATE TABLE {ine}{prefix}{name} (",
        )
    return ddl


def _object_kind(schema_obj: SchemaObject) -> str:
    """Snowflake object keyword for ALTER/COMMENT statements, from `physicalType`.

    A `view` physicalType yields `VIEW` (so governance emits `ALTER VIEW` /
    `COMMENT ON VIEW`); everything else defaults to `TABLE`. Materialized/external
    tables are currently governed as tables.
    """
    return "VIEW" if (schema_obj.physicalType or "").strip().lower() == "view" else "TABLE"


def _view_definition(schema_obj: SchemaObject) -> Optional[str]:
    """The view's SELECT body from the `viewDefinition` custom property, or None.

    Captured on import (Snowflake `INFORMATION_SCHEMA.VIEWS.VIEW_DEFINITION`). With it,
    a view can be (re)created — the only way to apply view *column* comments, which
    Snowflake permits solely in the `CREATE VIEW` column list (no `ALTER` path).
    """
    for cp in schema_obj.customProperties or []:
        if cp.property == "viewDefinition" and cp.value:
            return str(cp.value)
    return None


# Strips a leading `create [or replace] [secure] [recursive] view <name> [(...)] … as`
# header, leaving the SELECT body. DOTALL so the header can span lines.
_VIEW_HEADER_RE = re.compile(
    r"^\s*create\s+(?:or\s+replace\s+)?(?:secure\s+)?(?:recursive\s+)?view\b.*?\bas\b\s*",
    re.IGNORECASE | re.DOTALL,
)


def _view_select_body(definition: Optional[str]) -> Optional[str]:
    """Reduce a stored view definition to its clean SELECT body.

    Snowflake's `VIEW_DEFINITION` returns the *full* `create [or replace] view <name> …
    as <body>`. We keep only `<body>` so it can be re-wrapped with the contract's own
    column comments, and trim trailing whitespace / blank edges so it stores and renders
    tidily (a YAML block scalar). A value that's already a bare body passes through.
    """
    if not definition:
        return definition
    text = definition.strip()
    match = _VIEW_HEADER_RE.match(text)
    if match:
        text = text[match.end():]
    lines = [line.rstrip() for line in text.splitlines()]
    while lines and not lines[0].strip():
        lines.pop(0)
    while lines and not lines[-1].strip():
        lines.pop()
    return "\n".join(lines)


def _generate_view_ddl(
    contract: OpenDataContractStandard, *, table_prefix: str = "", ddl_if_not_exists: bool = False,
) -> str:
    """`CREATE [OR REPLACE | IF NOT EXISTS] VIEW` for each view that carries a definition.

    Column comments are embedded in the column list and the view comment via `COMMENT =`
    (the only way Snowflake persists view column comments — mirrors what Snowsight does).
    `ddl_if_not_exists` (auto) → `CREATE VIEW IF NOT EXISTS`; otherwise (always) →
    `CREATE OR REPLACE VIEW`, which redefines and so reliably updates comments. Views with
    no stored definition are skipped here (governed via `COMMENT ON VIEW` / tags instead).
    """
    blocks: list[str] = []
    for schema_obj in contract.schema_ or []:
        if _object_kind(schema_obj) != "VIEW":
            continue
        definition = _view_definition(schema_obj)
        name = schema_obj.name
        if not definition or not name:
            continue
        keyword = "CREATE VIEW IF NOT EXISTS" if ddl_if_not_exists else "CREATE OR REPLACE VIEW"
        qualified = f"{table_prefix}{name}"

        col_defs: list[str] = []
        for prop in schema_obj.properties or []:
            col = prop.physicalName or prop.name
            if not col:
                continue
            if prop.description:
                col_defs.append(f"  {col} COMMENT '{_sql_escape(prop.description)}'")
            else:
                col_defs.append(f"  {col}")
        col_clause = " (\n" + ",\n".join(col_defs) + "\n)" if col_defs else ""

        comment_clause = (
            f"\nCOMMENT = '{_sql_escape(schema_obj.description)}'" if schema_obj.description else ""
        )
        body = (_view_select_body(definition) or "").rstrip().rstrip(";").rstrip()
        blocks.append(f"{keyword} {qualified}{col_clause}{comment_clause}\nAS\n{body};")
    return "\n\n".join(blocks)


def to_snowflake_full_sql(
    contract: OpenDataContractStandard,
    *,
    include_ddl: bool = True,
    ddl_if_not_exists: bool = False,
    structured_types: bool = False,
    include_comments: bool = True,
    include_tags: bool = True,
    include_quality: bool = False,
    create_tags: bool = False,
    tag_namespace: Optional[str] = None,
    tag_namespace_filter: Optional[list[str]] = None,
    metric_schedule: str = "USING CRON 0 0 * * * UTC",
    server: Optional[str] = None,
) -> str:
    """Render the full Snowflake setup script for a contract.

    `include_ddl=False` gives an *alter-only* script (comments + tags + data
    quality on pre-existing tables). `ddl_if_not_exists=True` makes the DDL
    `CREATE TABLE IF NOT EXISTS`, so a single script both creates missing tables
    and governs existing ones — used by `apply`'s default "auto" mode.

    Comment placement: a plain `CREATE TABLE` carries comments inline, so the
    standalone `COMMENT ON` section is skipped there. In alter-only **and** in
    `IF NOT EXISTS` mode it is emitted, so descriptions reach tables that already
    exist (which a skipped `CREATE TABLE IF NOT EXISTS` never touches).

    `structured_types=True` renders nested columns with a defined shape as Snowflake
    structured types (`OBJECT(field type, ...)`, `ARRAY(type)`) instead of the bare
    semi-structured `OBJECT`/`ARRAY`. Free-form `VARIANT` columns stay untyped.
    """
    sections: list[str] = []

    # Fully-qualify table references with `<database>.<schema>.` from the Snowflake
    # server block when available, so the script is self-contained (no USE DATABASE
    # required before running it).
    prefix = _snowflake_table_prefix(contract, server)

    # DDL — tables via upstream `to_sql_ddl` (apostrophe-safe for Snowflake's
    # single-quoted COMMENT clauses), then qualified. Views are generated by dcx from
    # their stored `viewDefinition` (upstream knows nothing of view bodies); views with
    # no definition emit no CREATE and are governed via COMMENT ON VIEW / tags instead.
    if include_ddl:
        if structured_types:
            _pin_structured_types(contract)
        _pin_unmapped_snowflake_types(contract)
        all_objs = contract.schema_
        contract.schema_ = [o for o in (all_objs or []) if _object_kind(o) == "TABLE"]
        try:
            with _snowflake_correct_escape():
                table_ddl = to_sql_ddl(contract, server_type="snowflake", server=server).strip()
            table_ddl = _qualify_ddl_tables(table_ddl, prefix, contract, if_not_exists=ddl_if_not_exists)
        finally:
            contract.schema_ = all_objs
        view_ddl = _generate_view_ddl(
            contract, table_prefix=prefix, ddl_if_not_exists=ddl_if_not_exists,
        )
        ddl_blocks = [b for b in (table_ddl, view_ddl) if b]
        if ddl_blocks:
            sections.append("-- ===== DDL =====")
            sections.append("\n\n".join(ddl_blocks))

    # Standalone COMMENT ON. A plain `CREATE TABLE` (always mode) carries table+column
    # comments inline, so those are skipped here; IF-NOT-EXISTS/alter-only tables need
    # standalone comments to reach existing objects. A view whose CREATE was emitted
    # above (has a definition) already carries its comments, so it's skipped too.
    if include_comments:
        comment_sql = _generate_comment_sql(
            contract, table_prefix=prefix,
            inline_table_comments=(include_ddl and not ddl_if_not_exists),
            ddl_included=include_ddl,
        )
        if comment_sql:
            sections.append("\n-- ===== Comments =====")
            sections.append(comment_sql)

    if include_tags:
        tag_sql = _generate_tag_sql(
            contract,
            create_tags=create_tags,
            tag_namespace=tag_namespace,
            tag_namespace_filter=tag_namespace_filter,
            table_prefix=prefix,
        )
        if tag_sql:
            sections.append("\n-- ===== Tags =====")
            sections.append(tag_sql)

    if include_quality:
        quality_sql = _generate_quality_sql(
            contract, metric_schedule=metric_schedule, table_prefix=prefix,
        )
        if quality_sql:
            sections.append("\n-- ===== Data Quality (Data Metric Functions) =====")
            sections.append(
                "-- NOTE: Data Metric Functions are a Snowflake Enterprise feature."
            )
            sections.append(quality_sql)

    return "\n".join(sections).strip() + "\n"


# === Tags ===================================================================


def _qualify_tag(tag: str, namespace: Optional[str]) -> str:
    # `--tag-namespace` qualifies *bare* tag names. A tag that's already namespaced
    # (e.g. imported as DB.SCHEMA.NAME) is left untouched, so it never gets
    # double-qualified into NAMESPACE.DB.SCHEMA.NAME.
    if namespace and "." not in tag:
        return f"{namespace}.{tag}"
    return tag


def _sql_escape(value: str) -> str:
    """Escape single quotes for embedding inside a SQL string literal."""
    return value.replace("'", "''")


def _parse_tag(tag_str: str) -> tuple[str, str]:
    """Split an ODCS tag string into (name, value).

    Supports two forms:
      - `"name"`              → ("name", "name")          — plain flag-style tag
      - `"NAME=VALUE"`        → ("NAME", "VALUE")         — Snowflake TAG_NAME/TAG_VALUE
    Splits on the first `=` only; subsequent `=` characters remain in the value.
    """
    if "=" in tag_str:
        name, _, value = tag_str.partition("=")
        return name.strip(), value.strip()
    return tag_str, tag_str


def _tag_namespace(tag: str) -> Optional[str]:
    """The `DB.SCHEMA` namespace of a tag — everything before the last dot of its
    name — or None for an un-namespaced tag. `GOV.TAGS.NAME=VALUE` → `GOV.TAGS`;
    `sensitive` → None."""
    name = _parse_tag(tag)[0]
    return name.rsplit(".", 1)[0] if "." in name else None


def _filter_tags_by_namespace(tags, namespaces) -> list:
    """Keep only tags whose namespace is in `namespaces` (a strict allow-list).

    With no `namespaces`, returns the tags unchanged. Un-namespaced tags have no
    namespace to match, so they're dropped when a filter is active.
    """
    if not namespaces:
        return list(tags or [])
    allowed = set(namespaces)
    return [t for t in (tags or []) if _tag_namespace(t) in allowed]


def _collect_all_tag_names(
    contract: OpenDataContractStandard,
    *,
    tag_namespace_filter: Optional[list[str]] = None,
) -> set[str]:
    """Distinct Snowflake tag *names* used in the contract (for CREATE TAG IF NOT EXISTS).

    Only the TAG_NAME side of each entry is collected — multiple TAG_VALUEs for the
    same TAG_NAME (the normal case) result in a single CREATE TAG statement. When a
    `tag_namespace_filter` is given, only tags in those namespaces are counted, so the
    emitted CREATE TAGs match the SET TAGs. `classification` is exempt (not a namespaced
    tag), like in `_generate_tag_sql`.
    """
    flt = lambda tags: _filter_tags_by_namespace(tags, tag_namespace_filter)  # noqa: E731
    names: set[str] = set()
    for tag in flt(contract.tags):
        names.add(_parse_tag(tag)[0])
    for schema_obj in contract.schema_ or []:
        for tag in flt(schema_obj.tags):
            names.add(_parse_tag(tag)[0])
        for prop in schema_obj.properties or []:
            for tag in flt(prop.tags):
                names.add(_parse_tag(tag)[0])
            if prop.classification:
                names.add("classification")
    return names


def _generate_comment_sql(
    contract: OpenDataContractStandard,
    *,
    table_prefix: str = "",
    inline_table_comments: bool = False,
    ddl_included: bool = False,
) -> str:
    """`COMMENT ON TABLE|VIEW/COLUMN` statements for object + top-level column descriptions.

    Idempotent (Snowflake `COMMENT ON ... IS` overwrites), so it is safe to (re-)run
    against objects that already exist. When `inline_table_comments` is set (plain
    `CREATE TABLE` mode), table objects are skipped here because the CREATE carries
    their comments inline — but views (which have no CREATE) are always emitted.

    **View columns:** Snowflake can't set a view's *column* comments via any `ALTER`
    statement — they exist only in the `CREATE VIEW` column list. So a view whose CREATE
    was emitted (it has a `viewDefinition`, and `ddl_included`) is skipped here entirely:
    that CREATE already carried its view + column comments. A view *without* a definition
    can't be recreated, so we emit its view-level `COMMENT ON VIEW` and skip the column
    comments with a note. Nested struct fields have no Snowflake column of their own, so
    only top-level column descriptions are applied here; they stay in `properties`.
    """
    lines: list[str] = []
    for schema_obj in contract.schema_ or []:
        table = schema_obj.name
        if not table:
            continue
        kind = _object_kind(schema_obj)
        if kind == "TABLE" and inline_table_comments:
            continue  # carried inline by the plain CREATE TABLE
        is_view = kind == "VIEW"
        if is_view and ddl_included and _view_definition(schema_obj):
            continue  # carried inline by the CREATE [OR REPLACE] VIEW above
        qualified = f"{table_prefix}{table}"
        if schema_obj.description:
            lines.append(
                f"COMMENT ON {kind} {qualified} IS '{_sql_escape(schema_obj.description)}';"
            )
        skipped_view_columns = False
        for prop in schema_obj.properties or []:
            col = prop.physicalName or prop.name
            if not col or not prop.description:
                continue
            if is_view:
                skipped_view_columns = True
                continue  # no ALTER path for view column comments; CREATE VIEW only
            lines.append(
                f"COMMENT ON COLUMN {qualified}.{col} IS '{_sql_escape(prop.description)}';"
            )
        if skipped_view_columns:
            lines.append(
                f"-- NOTE: column comments for view {qualified} can only be set in its "
                f"CREATE VIEW definition; skipped."
            )
    return "\n".join(lines)


def _generate_tag_sql(
    contract: OpenDataContractStandard,
    *,
    create_tags: bool,
    tag_namespace: Optional[str],
    tag_namespace_filter: Optional[list[str]] = None,
    table_prefix: str = "",
) -> str:
    lines: list[str] = []

    if create_tags:
        for name in sorted(_collect_all_tag_names(contract, tag_namespace_filter=tag_namespace_filter)):
            lines.append(f"CREATE TAG IF NOT EXISTS {_qualify_tag(name, tag_namespace)};")
        if lines:
            lines.append("")

    for schema_obj in contract.schema_ or []:
        table = schema_obj.name
        if not table:
            continue
        kind = _object_kind(schema_obj)
        qualified = f"{table_prefix}{table}"
        for tag in _filter_tags_by_namespace(schema_obj.tags, tag_namespace_filter):
            name, value = _parse_tag(tag)
            lines.append(
                f"ALTER {kind} {qualified} SET TAG "
                f"{_qualify_tag(name, tag_namespace)} = '{_sql_escape(value)}';"
            )
        for prop in schema_obj.properties or []:
            col = prop.physicalName or prop.name
            if not col:
                continue
            if prop.classification:
                lines.append(
                    f"ALTER {kind} {qualified} MODIFY COLUMN {col} "
                    f"SET TAG {_qualify_tag('classification', tag_namespace)} = "
                    f"'{_sql_escape(prop.classification)}';"
                )
            for tag in _filter_tags_by_namespace(prop.tags, tag_namespace_filter):
                name, value = _parse_tag(tag)
                lines.append(
                    f"ALTER {kind} {qualified} MODIFY COLUMN {col} "
                    f"SET TAG {_qualify_tag(name, tag_namespace)} = '{_sql_escape(value)}';"
                )

    return "\n".join(lines)


# === Quality ================================================================


def _quality_metric(q: DataQuality) -> Optional[str]:
    """Pull the metric name, supporting both `metric` and the deprecated `rule`."""
    return q.metric or q.rule


def _render_sql_literal(v: Any) -> str:
    """Render a Python value as a Snowflake SQL literal for an IN (...) list.

    Backslash is escaped as well as the quote: Snowflake interprets backslash escape
    sequences inside string literals by default, so a raw `\\` would corrupt the value.
    """
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return _fmt_num(v)
    return "'" + str(v).replace("\\", "\\\\").replace("'", "''") + "'"


def _accepted_values_list(q: DataQuality) -> Optional[str]:
    """Render the allowed-value set for `acceptedValues` from the rule's
    `arguments` (`validValues` / `acceptedValues` / `values`) as a comma-separated
    SQL literal list, or None if no non-empty set is present."""
    args = q.arguments or {}
    vals = None
    for key in ("validValues", "acceptedValues", "values"):
        candidate = args.get(key)
        if isinstance(candidate, (list, tuple)) and candidate:
            vals = candidate
            break
    if not vals:
        return None
    rendered = [_render_sql_literal(v) for v in vals if v is not None]
    return ", ".join(rendered) if rendered else None


def _check_name(q: DataQuality) -> Optional[str]:
    """Return the engine-neutral check name from the rule's `check` customProperties
    tag, or None.

    Lets a `type: sql`/`custom` rule that models a known check (e.g. blankCount,
    freshness) upgrade to a native DMF while the CONTRACT stays portable and
    engine-neutral (no Snowflake specifics stored in it)."""
    for cp in (q.customProperties or []):
        if getattr(cp, "property", None) == _CHECK_PROPERTY:
            val = getattr(cp, "value", None)
            if isinstance(val, str) and val.strip():
                return val.strip()
    return None


def _dmf_binding(
    q: DataQuality, *, column: Optional[str]
) -> Optional[str]:
    """Return the `<dmf> ON (...)` association for `ADD DATA METRIC FUNCTION`, or None.

    A DMF is resolved from EITHER an ODCS-standard `type: library` metric
    (`_LIBRARY_METRIC_TO_DMF`) OR the engine-neutral `check` tag on a `type: sql`/
    `custom` rule (`_check_name` → `_CHECK_TO_DMF`) — the latter keeps the contract
    portable while still emitting a native DMF for non-standard checks. For
    `acceptedValues` the form includes the `col -> col IN (...)` lambda.
    """
    metric = _quality_metric(q)
    is_library = (q.type or "").lower() == "library"
    mapping = _LIBRARY_METRIC_TO_DMF.get(metric) if (is_library and metric) else None
    if mapping is not None:
        dmf, scope = mapping
        if metric == "invalidValues":
            values = _accepted_values_list(q)
            if not column or not values:
                # Without a column + allowed set there is nothing to check.
                return None
            return f"{dmf} ON ({column}, {column} -> {column} IN ({values}))"
    else:
        # Non-standard check carrying a `check` tag (type: sql/custom). Resolve the
        # DMF + scope from this adapter's capability map; unknown checks stay as sql.
        check = _check_name(q)
        binding = _CHECK_TO_DMF.get(check) if check else None
        if binding is None:
            return None
        dmf, scope = binding
    if scope == "table":
        # Table-scope DMFs (ROW_COUNT, FRESHNESS) are added with an empty column
        # list. `ON ()` is the documented syntax for both ALTER TABLE and ALTER VIEW.
        return f"{dmf} ON ()"
    if scope == "column" and column:
        return f"{dmf} ON ({column})"
    return None


# ODCS operator field -> (SQL comparison template, Snowsight operator label). `VALUE`
# is the placeholder Snowflake substitutes with the DMF result; the expression must
# be TRUE when the data PASSES the check (matching ODCS semantics). Labels mirror
# Snowsight's expectation-name vocabulary (EQUALTO, GREATERTHANOREQUALTO, ...).
_OPERATOR_TO_EXPECTATION: dict[str, tuple[str, str]] = {
    "mustBe":                 ("VALUE = {v}",  "EQUALTO"),
    "mustNotBe":              ("VALUE <> {v}", "NOTEQUALTO"),
    "mustBeGreaterThan":      ("VALUE > {v}",  "GREATERTHAN"),
    "mustBeGreaterOrEqualTo": ("VALUE >= {v}", "GREATERTHANOREQUALTO"),
    "mustBeLessThan":         ("VALUE < {v}",  "LESSTHAN"),
    "mustBeLessOrEqualTo":    ("VALUE <= {v}", "LESSTHANOREQUALTO"),
}
_RANGE_OPERATOR_TO_EXPECTATION: dict[str, tuple[str, str]] = {
    "mustBeBetween":    ("{a} <= VALUE AND VALUE <= {b}", "BETWEEN"),
    "mustNotBeBetween": ("VALUE < {a} OR VALUE > {b}",     "NOTBETWEEN"),
}

# Snowsight friendly aliases for the common "= 0" completeness/uniqueness checks on a
# column, keyed by DMF short name. Used only when the rule is `mustBe 0`.
_ZERO_COUNT_ALIAS: dict[str, str] = {
    "NULL_COUNT":      "NONULLS",
    "BLANK_COUNT":     "NOBLANKS",
    "DUPLICATE_COUNT": "NODUPLICATES",
}

# Prefix marking a dcx-authored expectation, so it stays distinguishable from one a
# user created directly in Snowsight (which keeps its own plain `EXP__...` name) and
# groups all dcx expectations together in the Snowsight UI.
_EXP_PREFIX = "EXP__DCX__"


def _fmt_num(n: float | int) -> str:
    """Render a number without a trailing `.0` so `5.0` -> `5`."""
    if isinstance(n, float) and n.is_integer():
        return str(int(n))
    return str(n)


def _value_token(n: float | int) -> str:
    """Identifier-safe form of a threshold for an expectation name (`-` -> `NEG`,
    `.` -> `_`), so `10` -> `10`, `-3` -> `NEG3`, `5.5` -> `5_5`."""
    return _fmt_num(n).replace("-", "NEG").replace(".", "_")


def _expectation_name_and_expr(
    q: DataQuality, dmf: str, column: Optional[str]
) -> Optional[tuple[str, str]]:
    """Return `(expectation_name, expression)` for the rule's ODCS operator, or None
    if the rule carries no operator.

    Names follow Snowsight's convention under an `EXP__DCX__` provenance prefix:
    - a column `= 0` completeness/uniqueness check uses the friendly alias
      (`EXP__DCX__<COL>__NONULLS` / `NOBLANKS` / `NODUPLICATES`);
    - any other column check uses `EXP__DCX__<COL>__<DMF>__<OP><value>`;
    - a table check uses `EXP__DCX__<DMF>__<OP><value>`.
    Because the threshold is part of the name, a different threshold yields a distinct
    expectation added alongside the existing one (additive). `VALUE` is the DMF
    result; the expression is TRUE when the data passes."""
    # Take the function name (before the ` ON (...)` args) then its last dotted part,
    # so values containing periods in the args don't corrupt the label.
    dmf_short = dmf.split(" ")[0].split(".")[-1].upper()
    col = column.upper() if column else None

    for op, (template, label) in _OPERATOR_TO_EXPECTATION.items():
        v = getattr(q, op, None)
        if v is None:
            continue
        expr = template.format(v=_fmt_num(v))
        if col and op == "mustBe" and v == 0 and dmf_short in _ZERO_COUNT_ALIAS:
            return f"{_EXP_PREFIX}{col}__{_ZERO_COUNT_ALIAS[dmf_short]}", expr
        cond = f"{label}{_value_token(v)}"
        body = f"{col}__{dmf_short}__{cond}" if col else f"{dmf_short}__{cond}"
        return f"{_EXP_PREFIX}{body}", expr

    for op, (template, label) in _RANGE_OPERATOR_TO_EXPECTATION.items():
        rng = getattr(q, op, None)
        if not (rng and len(rng) == 2):
            continue
        expr = template.format(a=_fmt_num(rng[0]), b=_fmt_num(rng[1]))
        cond = f"{label}{_value_token(rng[0])}_{_value_token(rng[1])}"
        body = f"{col}__{dmf_short}__{cond}" if col else f"{dmf_short}__{cond}"
        return f"{_EXP_PREFIX}{body}", expr
    return None


def _expectation_for_quality(q: DataQuality, dmf: str, column: Optional[str]) -> Optional[str]:
    """Build a Snowflake `EXPECTATION <name> ( <expr> )` clause from the rule's ODCS
    operator, or None if the rule carries no operator."""
    parts = _expectation_name_and_expr(q, dmf, column)
    if parts is None:
        return None
    name, expr = parts
    return f"EXPECTATION {name} ({expr})"


def _quality_iter(
    schema_obj: SchemaObject,
) -> Iterable[tuple[Optional[str], DataQuality]]:
    """Yield (column_or_None, quality) for both table-level and column-level quality entries."""
    for q in schema_obj.quality or []:
        yield (None, q)
    for prop in schema_obj.properties or []:
        col = prop.physicalName or prop.name
        for q in prop.quality or []:
            yield (col, q)


def _to_dmf_schedule(value: str) -> str:
    """Snowflake DATA_METRIC_SCHEDULE clause from a stored schedule value.

    The contract stores the engine-neutral bare cron (e.g. `0 20 * * *`,
    `*/5 * * * *`), which becomes `USING CRON <cron> UTC` — Snowflake's CRON accepts
    step expressions, so minute cadences ride the same path. A value already in
    Snowflake clause form (`USING CRON ...`, `<n> MINUTE`, `TRIGGER_ON_CHANGES`)
    is used as-is, so the `metric_schedule` fallback default passes through.
    """
    clause = value.strip()
    upper = clause.upper()
    if (
        upper.startswith("USING CRON")
        or upper.endswith("MINUTE")
        or upper == "TRIGGER_ON_CHANGES"
    ):
        return clause
    return f"USING CRON {clause} UTC"


def _resolve_table_schedule(
    entries: list[tuple[Optional[str], DataQuality]],
    slas: list[ServiceLevelAgreementProperty],
    default: str,
    *,
    qualified: str = "",
    warnings: Optional[list[str]] = None,
) -> str:
    """Snowflake DATA_METRIC_SCHEDULE for a table, from its quality rules / SLAs.

    `DATA_METRIC_SCHEDULE` is a property of the TABLE, not of an individual metric, so
    a table whose rules disagree cannot be represented. The first schedule found wins
    and every differing one is reported through `warnings` — silently dropping a
    cadence the contract asked for is exactly the kind of thing nobody notices.
    If nothing carries one, the `metric_schedule` default (CLI `--metric-schedule` /
    API field) applies.
    """
    found: list[str] = [
        cron
        for cron in ((q.schedule or "").strip() for _column, q in entries)
        if cron
    ]
    found += [cron for cron in ((s.schedule or "").strip() for s in slas) if cron]
    if not found:
        return _to_dmf_schedule(default)
    chosen = found[0]
    ignored = sorted({c for c in found[1:] if c != chosen})
    if ignored and warnings is not None:
        warnings.append(
            f"{qualified or 'table'}: conflicting data-quality schedules "
            f"{[chosen, *ignored]}; DATA_METRIC_SCHEDULE is per-table, so '{chosen}' "
            f"is used and the rest are ignored."
        )
    return _to_dmf_schedule(chosen)


def _sla_seconds(sla: ServiceLevelAgreementProperty) -> Optional[int]:
    """The SLA's value converted to seconds, or None if not a usable number/unit.

    `SNOWFLAKE.CORE.FRESHNESS` reports seconds, so an SLA written in hours or days is
    converted rather than refused. An unknown unit yields None (and a `-- TODO`) rather
    than a guess — silently treating `4` weeks as `4` seconds would be worse than
    emitting nothing.
    """
    value = sla.value
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    factor = _SLA_UNIT_SECONDS.get((sla.unit or "s").strip().lower())
    if factor is None:
        return None
    return int(value * factor)


def _sla_by_object(contract: OpenDataContractStandard) -> dict[str, list]:
    """Group natively-enforceable `slaProperties` by the schema object they target.

    ODCS `element` uses path notation, so it may be bare (`ORDERS`), qualified
    (`DB.SCHEMA.ORDERS`) or column-scoped (`DB.SCHEMA.ORDERS.CREATED_AT`). An entry is
    attributed to a schema object when that object's name appears as one of the path
    segments. SLA properties with no `element`, or ones this adapter has no DMF for,
    are left alone — they remain documentation, which is all ODCS asks of them.
    """
    grouped: dict[str, list] = {}
    names = [obj.name for obj in (contract.schema_ or []) if obj.name]
    for sla in contract.slaProperties or []:
        if (sla.property or "").strip().lower() not in _SLA_PROPERTY_TO_DMF:
            continue
        segments = {seg.upper() for seg in (sla.element or "").split(".") if seg}
        for name in names:
            if name.upper() in segments:
                grouped.setdefault(name, []).append(sla)
    return grouped


def _sla_dmf_line(
    sla: ServiceLevelAgreementProperty, *, kind: str, qualified: str
) -> str:
    """One `ADD DATA METRIC FUNCTION` for an SLA, or a `-- TODO` if unenforceable.

    An SLA value is an upper bound (`latency: 4 h` means "at most 4 hours old"), so the
    expectation is `VALUE <= <seconds>`. ODCS SLA entries carry no operator field.
    """
    dmf, _scope = _SLA_PROPERTY_TO_DMF[(sla.property or "").strip().lower()]
    seconds = _sla_seconds(sla)
    if seconds is None:
        return (
            f"-- TODO: SLA '{sla.property}' on {qualified} is not enforceable "
            f"(value={sla.value!r}, unit={sla.unit!r})"
        )
    short = dmf.split(".")[-1].upper()
    name = f"{_EXP_PREFIX}{short}__LESSTHANOREQUALTO{_value_token(seconds)}"
    return (
        f"ALTER {kind} {qualified} ADD DATA METRIC FUNCTION {dmf} ON ()\n"
        f"  EXPECTATION {name} (VALUE <= {seconds});"
    )


def _generate_quality_sql(
    contract: OpenDataContractStandard,
    *,
    metric_schedule: str,
    table_prefix: str = "",
    warnings: Optional[list[str]] = None,
) -> str:
    sla_by_object = _sla_by_object(contract)
    lines: list[str] = []
    for schema_obj in contract.schema_ or []:
        table = schema_obj.name
        if not table:
            continue
        entries = list(_quality_iter(schema_obj))
        slas = sla_by_object.get(table, [])
        if not entries and not slas:
            continue
        kind = _object_kind(schema_obj)
        qualified = f"{table_prefix}{table}"
        # Collected locally so a conflict is always visible as a `-- WARNING:` in the
        # script itself (dry-run, export, review), and additionally handed to a caller
        # that wants it programmatically.
        schedule_warnings: list[str] = []
        schedule_clause = _resolve_table_schedule(
            entries, slas, metric_schedule,
            qualified=qualified, warnings=schedule_warnings,
        )
        for warning in schedule_warnings:
            lines.append(f"-- WARNING: {warning}")
        if warnings is not None:
            warnings.extend(schedule_warnings)
        lines.append(f"ALTER {kind} {qualified} SET DATA_METRIC_SCHEDULE = '{schedule_clause}';")
        for sla in slas:
            lines.append(_sla_dmf_line(sla, kind=kind, qualified=qualified))
        emitted: set[str] = set()
        for column, q in entries:
            add_dmf = _dmf_binding(q, column=column)
            label = q.name or _quality_metric(q) or "unnamed"
            if add_dmf:
                # A table-scope metric (rowCount, ...) authored under a COLUMN still
                # binds to the table: `ON ()`. Naming its expectation after that column
                # would be a lie, and would make N column-level rowCount rules look like
                # N distinct expectations on one metric.
                effective_column = None if add_dmf.rstrip().endswith("ON ()") else column
                expectation = _expectation_for_quality(q, add_dmf, effective_column)
                clause = f"\n  {expectation}" if expectation else ""
                # Emit a plain ADD with its value-named expectation. If the DMF is
                # already on the column, `apply` re-issues this as MODIFY ... ADD
                # EXPECTATION so a new threshold is added additively (Snowsight style).
                statement = (
                    f"ALTER {kind} {qualified} ADD DATA METRIC FUNCTION {add_dmf}{clause};"
                )
                # Several rules can collapse onto one association+expectation — most
                # often a `rowCount` repeated under every column. Snowflake would take
                # the first and treat the rest as already-present, so emitting them adds
                # only noise.
                if statement in emitted:
                    continue
                emitted.add(statement)
                lines.append(statement)
            else:
                target = f" on column {column}" if column else ""
                lines.append(f"-- TODO: unmappable quality rule '{label}'{target} (type={q.type})")
        lines.append("")  # blank line between tables

    return "\n".join(lines).rstrip()


# === Registration with upstream factory =====================================
exporter_factory.register_lazy_exporter(
    "snowflake-full", "dcx.exporters.snowflake", "SnowflakeFullExporter",
)
