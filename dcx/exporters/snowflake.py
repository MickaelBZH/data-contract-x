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
from contextlib import contextmanager
from enum import Enum
from typing import Iterable, Optional

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
)

# ODCS `metric` value → Snowflake CORE DMF name (and where it applies).
# Each entry: metric_name → (snowflake_dmf, scope) where scope is "column" or "table".
# Keys are the metric names allowed by the ODCS spec for library-type quality.
_LIBRARY_METRIC_TO_DMF: dict[str, tuple[str, str]] = {
    "rowCount":        ("SNOWFLAKE.CORE.ROW_COUNT", "table"),
    "nullValues":      ("SNOWFLAKE.CORE.NULL_COUNT", "column"),
    "missingValues":   ("SNOWFLAKE.CORE.NULL_COUNT", "column"),
    "duplicateValues": ("SNOWFLAKE.CORE.DUPLICATE_COUNT", "column"),
    # `invalidValues` has no direct Snowflake CORE DMF — falls through to TODO.
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

    # DDL — reuse upstream, with apostrophe-safe escaping for Snowflake's
    # single-quoted COMMENT '...' clauses, then qualify table names.
    if include_ddl:
        if structured_types:
            _pin_structured_types(contract)
        _pin_unmapped_snowflake_types(contract)
        with _snowflake_correct_escape():
            ddl = to_sql_ddl(contract, server_type="snowflake", server=server).strip()
        ddl = _qualify_ddl_tables(ddl, prefix, contract, if_not_exists=ddl_if_not_exists)
        if ddl:
            sections.append("-- ===== DDL =====")
            sections.append(ddl)

    # Emit standalone COMMENT ON unless this is a plain CREATE TABLE (which carries
    # comments inline). In alter-only and IF NOT EXISTS modes we emit them so
    # descriptions reach tables that already exist.
    if include_comments and not (include_ddl and not ddl_if_not_exists):
        comment_sql = _generate_comment_sql(contract, table_prefix=prefix)
        if comment_sql:
            sections.append("\n-- ===== Comments =====")
            sections.append(comment_sql)

    if include_tags:
        tag_sql = _generate_tag_sql(
            contract,
            create_tags=create_tags,
            tag_namespace=tag_namespace,
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
    return f"{namespace}.{tag}" if namespace else tag


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


def _collect_all_tag_names(contract: OpenDataContractStandard) -> set[str]:
    """Distinct Snowflake tag *names* used in the contract (for CREATE TAG IF NOT EXISTS).

    Only the TAG_NAME side of each entry is collected — multiple TAG_VALUEs for the
    same TAG_NAME (the normal case) result in a single CREATE TAG statement.
    """
    names: set[str] = set()
    for tag in contract.tags or []:
        names.add(_parse_tag(tag)[0])
    for schema_obj in contract.schema_ or []:
        for tag in schema_obj.tags or []:
            names.add(_parse_tag(tag)[0])
        for prop in schema_obj.properties or []:
            for tag in prop.tags or []:
                names.add(_parse_tag(tag)[0])
            if prop.classification:
                names.add("classification")
    return names


def _generate_comment_sql(
    contract: OpenDataContractStandard,
    *,
    table_prefix: str = "",
) -> str:
    """`COMMENT ON TABLE/COLUMN` statements for table + top-level column descriptions.

    Idempotent (Snowflake `COMMENT ON ... IS` overwrites), so it is safe to (re-)run
    against tables that already exist — the alter-only path for governing tables you
    didn't create from this contract. Nested struct fields have no Snowflake column
    of their own, so only top-level column descriptions are applied here; they remain
    captured in the contract's nested `properties`.
    """
    lines: list[str] = []
    for schema_obj in contract.schema_ or []:
        table = schema_obj.name
        if not table:
            continue
        qualified = f"{table_prefix}{table}"
        if schema_obj.description:
            lines.append(
                f"COMMENT ON TABLE {qualified} IS '{_sql_escape(schema_obj.description)}';"
            )
        for prop in schema_obj.properties or []:
            col = prop.physicalName or prop.name
            if not col or not prop.description:
                continue
            lines.append(
                f"COMMENT ON COLUMN {qualified}.{col} IS '{_sql_escape(prop.description)}';"
            )
    return "\n".join(lines)


def _generate_tag_sql(
    contract: OpenDataContractStandard,
    *,
    create_tags: bool,
    tag_namespace: Optional[str],
    table_prefix: str = "",
) -> str:
    lines: list[str] = []

    if create_tags:
        for name in sorted(_collect_all_tag_names(contract)):
            lines.append(f"CREATE TAG IF NOT EXISTS {_qualify_tag(name, tag_namespace)};")
        if lines:
            lines.append("")

    for schema_obj in contract.schema_ or []:
        table = schema_obj.name
        if not table:
            continue
        qualified = f"{table_prefix}{table}"
        for tag in schema_obj.tags or []:
            name, value = _parse_tag(tag)
            lines.append(
                f"ALTER TABLE {qualified} SET TAG "
                f"{_qualify_tag(name, tag_namespace)} = '{_sql_escape(value)}';"
            )
        for prop in schema_obj.properties or []:
            col = prop.physicalName or prop.name
            if not col:
                continue
            if prop.classification:
                lines.append(
                    f"ALTER TABLE {qualified} MODIFY COLUMN {col} "
                    f"SET TAG {_qualify_tag('classification', tag_namespace)} = "
                    f"'{_sql_escape(prop.classification)}';"
                )
            for tag in prop.tags or []:
                name, value = _parse_tag(tag)
                lines.append(
                    f"ALTER TABLE {qualified} MODIFY COLUMN {col} "
                    f"SET TAG {_qualify_tag(name, tag_namespace)} = '{_sql_escape(value)}';"
                )

    return "\n".join(lines)


# === Quality ================================================================


def _quality_metric(q: DataQuality) -> Optional[str]:
    """Pull the metric name, supporting both `metric` and the deprecated `rule`."""
    return q.metric or q.rule


def _dmf_for_quality(q: DataQuality, *, column: Optional[str]) -> Optional[str]:
    """Return the Snowflake DMF call (e.g. `SNOWFLAKE.CORE.NULL_COUNT ON (col)`), or None."""
    if (q.type or "").lower() != "library":
        return None
    metric = _quality_metric(q)
    if not metric:
        return None
    mapping = _LIBRARY_METRIC_TO_DMF.get(metric)
    if mapping is None:
        return None
    dmf, scope = mapping
    if scope == "table":
        return f"{dmf} ON ()"
    if scope == "column" and column:
        return f"{dmf} ON ({column})"
    return None


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


def _generate_quality_sql(
    contract: OpenDataContractStandard,
    *,
    metric_schedule: str,
    table_prefix: str = "",
) -> str:
    lines: list[str] = []
    for schema_obj in contract.schema_ or []:
        table = schema_obj.name
        if not table:
            continue
        entries = list(_quality_iter(schema_obj))
        if not entries:
            continue
        qualified = f"{table_prefix}{table}"
        lines.append(f"ALTER TABLE {qualified} SET DATA_METRIC_SCHEDULE = '{metric_schedule}';")
        for column, q in entries:
            dmf = _dmf_for_quality(q, column=column)
            label = q.name or _quality_metric(q) or "unnamed"
            if dmf:
                lines.append(f"ALTER TABLE {qualified} ADD DATA METRIC FUNCTION {dmf};")
            else:
                target = f" on column {column}" if column else ""
                lines.append(f"-- TODO: unmappable quality rule '{label}'{target} (type={q.type})")
        lines.append("")  # blank line between tables

    return "\n".join(lines).rstrip()


# === Registration with upstream factory =====================================
exporter_factory.register_lazy_exporter(
    "snowflake-full", "dcx.exporters.snowflake", "SnowflakeFullExporter",
)
