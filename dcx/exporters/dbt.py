"""`dcx export dbt` — unified dbt export (models · sources · staging) with
ODCS governance mapped into idiomatic dbt `config.meta` / `config.tags`.

Upstream `datacontract-cli` ships three separate dbt exporters (`dbt-models`,
`dbt-sources`, `dbt-staging-sql`). This module groups them under one command via
`--kind` and improves the models/sources output:

- **Tags → the right place.** A `NAME=VALUE` tag (the dcx convention, e.g.
  `DATA_CLASSIFICATION=PD_DATA`) becomes `config.meta.<name lowercased>`, because in
  dbt `meta` carries key/value metadata (surfaced in docs + manifest, consumed by
  catalogs) while `tags` are a *selection* mechanism (`dbt run --select tag:x`). A
  bare tag (e.g. `sensitive`) stays a real dbt tag under `config.tags`.
- **More ODCS fidelity.** `classification`, `businessName`, and `criticalDataElement`
  are surfaced into `config.meta`; **schema-object (table) level tags** — which the
  upstream models exporter drops entirely — are mapped onto the model's
  `config.meta` / `config.tags`.
- **Modern placement.** Column meta/tags go under `config:` (matching the model
  block), not the legacy top-level keys.
- **Bug fix.** Upstream's "unknown type" path emits a data test with a doubled
  `dbt_expectations.dbt_expectations.` namespace and `column_type: null`; here it is a
  single prefix with the contract's physical/logical type (or omitted if unknown).
- **Quality → data_tests.** ODCS `quality[]` (`nullValues`/`missingValues`,
  `duplicateValues`, `rowCount`, `type: sql`, `type: custom`) — never read by the
  upstream exporter — is mapped onto dbt `not_null`/`unique` or
  `dbt_utils.expression_is_true` tests. A rule that can't be mapped is surfaced as a
  `# WARNING:` comment in the returned YAML rather than silently dropped.

Type conversion (`convert_to_sql_type`) and the data-test mapping
(`field_to_data_tests`) are reused from upstream so they keep evolving with it;
staging SQL is reused wholesale (it has no governance to map).

Imported for its side effects (factory registration) by `dcx.exporters.command`.
"""

import logging
import re
from enum import Enum
from typing import Any, Optional

import yaml
from datacontract.export.dbt_exporter import (
    _get_description_str,
    _get_owner,
    _get_server_by_name,
    _supports_constraints,
    _to_dbt_model_type,
    to_dbt_staging_sql,
)
from datacontract.export.exporter import Exporter, _check_schema_name_for_export
from datacontract.export.exporter_factory import exporter_factory
from datacontract.export.sql_type_converter import convert_to_sql_type
from datacontract.integration.dbt_test_mapping import field_to_data_tests
from open_data_contract_standard.model import DataQuality, OpenDataContractStandard, SchemaObject, SchemaProperty

# The `NAME=VALUE` tag convention + namespace filtering are shared with the
# Snowflake exporters.
from dcx.exporters.snowflake import _filter_tags_by_namespace, _parse_tag

logger = logging.getLogger(__name__)


class DbtKind(str, Enum):
    """Which dbt artifact `dcx export dbt` produces.

    - `models`  — `schema.yml` model definitions (the default).
    - `sources` — `sources.yml` source-table definitions.
    - `staging` — a staging `SELECT` against the contract's source.
    """

    models = "models"
    sources = "sources"
    staging = "staging"


class DbtMetaKeyStyle(str, Enum):
    """How a `NAME=VALUE` tag's (possibly namespaced) name becomes a `config.meta` key.

    Tags imported from Snowflake are fully qualified (`DB.SCHEMA.NAME`). Only the
    *meta key* is affected — the Snowflake `SET TAG` path always uses the full name.

    - `full`      — keep the dotted name as-is (`db.schema.name`). Faithful; some
      catalog tools read `.` as nesting.
    - `sanitized` — replace dots with underscores (`db_schema_name`). Safe key,
      namespace preserved.
    - `short`     — last segment only (`name`). Cleanest, but two tags that share a
      short name across namespaces collide on one column.
    """

    full = "full"
    sanitized = "sanitized"
    short = "short"


def _meta_key(name: str, style: DbtMetaKeyStyle) -> str:
    """Derive the `config.meta` key for a tag name under the chosen style."""
    if style is DbtMetaKeyStyle.short:
        name = name.rsplit(".", 1)[-1]
    elif style is DbtMetaKeyStyle.sanitized:
        name = name.replace(".", "_")
    return name.lower()


def _adapter_type(odcs: OpenDataContractStandard, server: Optional[str]) -> Optional[str]:
    """Resolve a `--server` name to its adapter type (e.g. `snowflake`).

    Upstream's models exporter uses the server *name* directly as the SQL dialect;
    we look it up to its `.type` so `--server production` maps to the real adapter.
    Falls back to the raw value (then to `snowflake` downstream) when unknown.
    """
    if not server:
        return None
    found = _get_server_by_name(odcs, server)
    return found.type if found is not None else server


def _governance(
    src, meta_key_style: DbtMetaKeyStyle, tag_namespace_filter: Optional[list] = None,
) -> tuple[dict, list]:
    """Map an ODCS element's governance fields to dbt (meta dict, tags list).

    - `NAME=VALUE` tag → `meta[_meta_key(name)] = value` (key/value metadata)
    - bare tag         → `tags` list (a dbt selection label)
    - `classification` / `businessName` / `criticalDataElement` → `meta`

    With `tag_namespace_filter`, only tags in those namespaces survive (the dedicated
    fields above are exempt). Schema objects carry only `tags`; the `getattr` guards
    make the column-only fields no-ops there, so the same helper serves columns and tables.
    """
    meta: dict = {}
    tags: list = []
    for tag in _filter_tags_by_namespace(getattr(src, "tags", None), tag_namespace_filter):
        if "=" in tag:
            name, value = _parse_tag(tag)
            meta[_meta_key(name, meta_key_style)] = value
        else:
            tags.append(tag)
    classification = getattr(src, "classification", None)
    if classification is not None:
        meta["classification"] = classification
    business_name = getattr(src, "businessName", None)
    if business_name is not None:
        meta["business_name"] = business_name
    critical = getattr(src, "criticalDataElement", None)
    if critical is not None:
        meta["critical_data_element"] = critical
    return meta, tags


# === ODCS `quality[]` → dbt `data_tests` ====================================
# Mirrors `dcx.exporters.snowflake._LIBRARY_METRIC_TO_DMF`'s role for the Snowflake
# DMF export: the ODCS-standard library metrics + `type: sql`/`custom` rules map onto
# dbt's own quality vocabulary instead. `invalidValues` is excluded here — upstream's
# `field_to_data_tests` (via `_get_enum_values`) already turns it into an
# `accepted_values` test, so mapping it again here would just duplicate that test.
#
# ODCS operator → the trailing SQL comparison against a COUNT(*) (not a bare `VALUE`
# as in the Snowflake DMF mapping, since dbt has no expectation-style placeholder).
_COUNT_OP_SQL: dict[str, str] = {
    "mustBe": "= {v}",
    "mustNotBe": "<> {v}",
    "mustBeGreaterThan": "> {v}",
    "mustBeGreaterOrEqualTo": ">= {v}",
    "mustBeLessThan": "< {v}",
    "mustBeLessOrEqualTo": "<= {v}",
}
_COUNT_RANGE_OP_SQL: dict[str, str] = {
    "mustBeBetween": "between {a} and {b}",
    "mustNotBeBetween": "not between {a} and {b}",
}


def _fmt_num(n: Any) -> str:
    """Render a number without a trailing `.0` so `5.0` -> `5`."""
    if isinstance(n, float) and n.is_integer():
        return str(int(n))
    return str(n)


def _quality_operator(q: DataQuality) -> Optional[tuple[str, Any]]:
    """`(operator, value)` off whichever ODCS operator field the rule carries, or
    None if it carries no pass condition at all."""
    for op in _COUNT_OP_SQL:
        v = getattr(q, op, None)
        if v is not None:
            return op, v
    for op in _COUNT_RANGE_OP_SQL:
        v = getattr(q, op, None)
        if v:
            return op, v
    return None


def _count_condition(op: str, value: Any) -> Optional[str]:
    """The SQL comparison a `(select count(*) ...)` subquery must satisfy, or None
    if the operator has no COUNT-based reading (there is none such today, but this
    keeps the call sites honest about the possibility)."""
    if op in _COUNT_OP_SQL:
        return _COUNT_OP_SQL[op].format(v=_fmt_num(value))
    if op in _COUNT_RANGE_OP_SQL and isinstance(value, (list, tuple)) and len(value) == 2:
        return _COUNT_RANGE_OP_SQL[op].format(a=_fmt_num(value[0]), b=_fmt_num(value[1]))
    return None


def _substitute_placeholders(query: str, column: Optional[str]) -> Optional[str]:
    """`${table}`/`${column}` (the enricher's portable `type: sql` convention, see
    `dcx.enrich.quality`) rewritten for dbt: `${table}` -> `{{ this }}` (works for
    both a model and a source-table test — `this` always resolves to the relation
    under test); `${column}` -> the actual column name. None if `${column}` is used
    but no column is in scope (a table-level rule has none to substitute)."""
    text = query.replace("${table}", "{{ this }}")
    if "${column}" in text:
        if not column:
            return None
        text = text.replace("${column}", column)
    return text


def _quality_check_name(q: DataQuality) -> Optional[str]:
    """Return the engine-neutral `check` name carried by a SQL quality rule."""
    for custom_property in getattr(q, "customProperties", None) or []:
        if getattr(custom_property, "property", None) == "check":
            value = getattr(custom_property, "value", None)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return None


def _quality_to_tests(
    quality: Optional[list], *, column: Optional[str], label: str,
) -> tuple[list, list, list[str]]:
    """Map `SchemaProperty.quality` / `SchemaObject.quality` rules to dbt tests.

    Returns `(column_tests, model_tests, warnings)`. `column_tests` holds only bare
    presence/uniqueness tests (`not_null`/`unique`) scoped to `column`; anything
    carrying a numeric threshold becomes a MODEL-level `dbt_utils.expression_is_true`
    over a `(select count(*) ...)` subquery instead — an aggregate check doesn't fit
    a per-row column test, and `{{ this }}` makes the same expression work whether
    the caller is building a model or a source table. Rules this adapter can't
    represent in dbt append a human-readable entry to `warnings` instead of vanishing.
    """
    column_tests: list = []
    model_tests: list = []
    warnings: list[str] = []

    for q in quality or []:
        rtype = (getattr(q, "type", None) or "").lower()
        metric = getattr(q, "metric", None) if rtype == "library" else None
        if rtype == "sql":
            metric = _quality_check_name(q)
        op = _quality_operator(q)

        if metric == "blankCount":
            if column is None:
                warnings.append(f"{label}: blankCount quality rule has no column; skipped.")
                continue
            if op is None or op == ("mustBe", 0):
                column_tests.append("dbt_utils.not_empty_string")
                continue
            condition = _count_condition(*op)
            if condition is None:
                warnings.append(f"{label}: blankCount on {column} has an unsupported operator; skipped.")
                continue
            model_tests.append({"dbt_utils.expression_is_true": {
                "expression": (
                    f"(select count(*) from {{{{ this }}}} "
                    f"where {column} is not null and trim(cast({column} as string)) = '') {condition}"
                ),
            }})
            continue

        if metric in ("nullValues", "missingValues"):
            if column is None:
                warnings.append(f"{label}: {metric} quality rule has no column; skipped.")
                continue
            if op is None or op == ("mustBe", 0):
                column_tests.append("not_null")
                continue
            condition = _count_condition(*op)
            if condition is None:
                warnings.append(f"{label}: {metric} on {column} has an unsupported operator; skipped.")
                continue
            model_tests.append({"dbt_utils.expression_is_true": {
                "expression": f"(select count(*) from {{{{ this }}}} where {column} is null) {condition}",
            }})
            continue

        if metric == "duplicateValues":
            if column is not None:
                if op is None or op == ("mustBe", 0):
                    column_tests.append("unique")
                    continue
                condition = _count_condition(*op)
                if condition is None:
                    warnings.append(f"{label}: duplicateValues on {column} has an unsupported operator; skipped.")
                    continue
                model_tests.append({"dbt_utils.expression_is_true": {
                    "expression": (
                        f"(select count(*) from (select {column} from {{{{ this }}}} "
                        f"group by {column} having count(*) > 1) dcx_dupes) {condition}"
                    ),
                }})
                continue
            # Table-level duplicateValues has no implicit column; it only maps when
            # the rule names the composite key to check via `arguments.columns`.
            columns = (getattr(q, "arguments", None) or {}).get("columns")
            if not columns:
                warnings.append(f"{label}: table-level duplicateValues needs arguments.columns; skipped.")
                continue
            model_tests.append(
                {"dbt_expectations.expect_compound_columns_to_be_unique": {"column_list": columns}}
            )
            continue

        if metric == "rowCount":
            if op is None:
                warnings.append(f"{label}: rowCount quality rule has no operator; skipped.")
                continue
            condition = _count_condition(*op)
            if condition is None:
                warnings.append(f"{label}: rowCount has an unsupported operator; skipped.")
                continue
            model_tests.append({"dbt_utils.expression_is_true": {
                "expression": f"(select count(*) from {{{{ this }}}}) {condition}",
            }})
            continue

        if metric == "invalidValues":
            continue  # already surfaced as an `accepted_values` test by field_to_data_tests

        if rtype == "sql":
            query = getattr(q, "query", None)
            if not query:
                warnings.append(f"{label}: type: sql rule has no query; skipped.")
                continue
            expr = _substitute_placeholders(query, column)
            if expr is None:
                warnings.append(f"{label}: type: sql rule uses ${{column}} with no column in scope; skipped.")
                continue
            if op is not None:
                condition = _count_condition(*op)
                if condition is None:
                    warnings.append(f"{label}: type: sql rule has an unsupported operator; skipped.")
                    continue
                expr = f"({expr}) {condition}"
            model_tests.append({"dbt_utils.expression_is_true": {"expression": expr}})
            continue

        if rtype == "custom":
            engine = (getattr(q, "engine", None) or "").lower()
            impl = getattr(q, "implementation", None)
            if engine != "dbt" or impl is None:
                warnings.append(
                    f"{label}: type: custom rule (engine={engine or '?'}) has no dbt implementation; skipped."
                )
                continue
            if isinstance(impl, str):
                try:
                    impl = yaml.safe_load(impl)
                except yaml.YAMLError:
                    impl = None
            if not isinstance(impl, (dict, str)):
                warnings.append(f"{label}: type: custom implementation is not a usable dbt test block; skipped.")
                continue
            model_tests.append(impl)
            continue

        if metric:
            warnings.append(f"{label}: quality metric '{metric}' has no dbt mapping; skipped.")
        # No type/metric at all: nothing to report — not a gap this adapter created.

    return column_tests, model_tests, warnings


def _prepend_warnings(text: str, warnings: list[str]) -> str:
    """Surface unmappable quality rules as `# WARNING:` comments at the top of the
    returned YAML — visible to the caller without changing the export response shape
    (mirrors the `-- WARNING:` convention in the Snowflake SQL exporter) — and log
    them for server-side visibility.
    """
    if not warnings:
        return text
    for w in warnings:
        logger.warning(w)
    return "".join(f"# WARNING: {w}\n" for w in warnings) + text


def _to_dbt_column(
    odcs: OpenDataContractStandard,
    prop: SchemaProperty,
    supports_constraints: bool,
    adapter_type: Optional[str],
    is_primary_key: bool = False,
    is_single_pk: bool = False,
    *,
    meta_key_style: DbtMetaKeyStyle = DbtMetaKeyStyle.full,
    tag_namespace_filter: Optional[list] = None,
) -> tuple[dict, list, list[str]]:
    """Build a dbt column dict in a readable key order, routing governance to `config`.

    Returns `(column, model_tests, warnings)`: quality rules that need a MODEL-level
    test (a threshold on `nullValues`/`duplicateValues`, `rowCount`, `type: sql`/
    `custom`) can't live under this column, so they bubble up to the caller.
    """
    adapter_type = adapter_type or "snowflake"
    column: dict = {"name": prop.name}

    dbt_type = convert_to_sql_type(prop, adapter_type)
    data_tests: list = []
    if dbt_type is not None:
        column["data_type"] = dbt_type
    else:
        # Upstream emits a doubled `dbt_expectations.dbt_expectations.` namespace and
        # `column_type: null` here; use a single prefix and the contract's own type.
        expected_type = prop.physicalType or prop.logicalType
        if expected_type:
            data_tests.append(
                {"dbt_expectations.expect_column_values_to_be_of_type": {"column_type": expected_type}}
            )

    if prop.description is not None:
        column["description"] = prop.description.strip().replace("\n", " ")

    # not_null / unique go to `constraints` on materializations that support them;
    # otherwise field_to_data_tests emits them as data tests.
    if supports_constraints:
        if prop.required or is_primary_key:
            column.setdefault("constraints", []).append({"type": "not_null"})
        if prop.unique or (is_primary_key and is_single_pk):
            column.setdefault("constraints", []).append({"type": "unique"})

    meta, tags = _governance(prop, meta_key_style, tag_namespace_filter)
    config: dict = {}
    if meta:
        config["meta"] = meta
    if tags:
        config["tags"] = tags
    if config:
        column["config"] = config

    data_tests.extend(
        field_to_data_tests(
            prop,
            is_primary_key=is_primary_key,
            is_single_pk=is_single_pk,
            supports_constraints=supports_constraints,
            source_name=odcs.id,
        )
    )
    col_quality_tests, model_quality_tests, quality_warnings = _quality_to_tests(
        prop.quality, column=prop.name, label=f"column {prop.name}",
    )
    for t in col_quality_tests:
        if t not in data_tests:
            data_tests.append(t)
    if data_tests:
        column["data_tests"] = data_tests

    return column, model_quality_tests, quality_warnings


def _to_columns(
    odcs: OpenDataContractStandard,
    properties: list[SchemaProperty],
    supports_constraints: bool,
    adapter_type: Optional[str],
    primary_key_columns: Optional[list] = None,
    *,
    meta_key_style: DbtMetaKeyStyle = DbtMetaKeyStyle.full,
    tag_namespace_filter: Optional[list] = None,
) -> tuple[list, list, list[str]]:
    primary_key_columns = primary_key_columns or []
    is_single_pk = len(primary_key_columns) == 1
    columns: list = []
    model_tests: list = []
    warnings: list[str] = []
    for prop in properties:
        column, m_tests, warns = _to_dbt_column(
            odcs, prop, supports_constraints, adapter_type,
            prop.name in primary_key_columns, is_single_pk,
            meta_key_style=meta_key_style, tag_namespace_filter=tag_namespace_filter,
        )
        columns.append(column)
        model_tests.extend(m_tests)
        warnings.extend(warns)
    return columns, model_tests, warnings


def _to_dbt_model(
    schema_name: str, schema_object: SchemaObject, odcs: OpenDataContractStandard,
    adapter_type: Optional[str], meta_key_style: DbtMetaKeyStyle,
    tag_namespace_filter: Optional[list] = None,
) -> tuple[dict, list[str]]:
    model_type = _to_dbt_model_type(schema_object.physicalType)

    config: dict = {"meta": {"data_contract": odcs.id}}
    if model_type:
        config["materialized"] = model_type
    owner = _get_owner(odcs)
    if owner is not None:
        config["meta"]["owner"] = owner

    # Schema-object (table) level tags — dropped by the upstream models exporter —
    # land on the model's config.meta / config.tags.
    meta, tags = _governance(schema_object, meta_key_style, tag_namespace_filter)
    config["meta"].update(meta)
    if tags:
        config.setdefault("tags", []).extend(tags)

    if _supports_constraints(model_type):
        config["contract"] = {"enforced": True}

    dbt_model: dict = {"name": schema_name, "config": config}
    if schema_object.description is not None:
        dbt_model["description"] = schema_object.description.strip().replace("\n", " ")

    primary_key_columns = [
        p.name for p in (schema_object.properties or []) if p.primaryKey
    ]
    if len(primary_key_columns) > 1:
        dbt_model["data_tests"] = [
            {"dbt_utils.unique_combination_of_columns": {"combination_of_columns": primary_key_columns}}
        ]

    columns, quality_model_tests, warnings = _to_columns(
        odcs, schema_object.properties or [], _supports_constraints(model_type),
        adapter_type, primary_key_columns, meta_key_style=meta_key_style,
        tag_namespace_filter=tag_namespace_filter,
    )
    if columns:
        dbt_model["columns"] = columns

    _, table_model_tests, table_warnings = _quality_to_tests(
        schema_object.quality, column=None, label=f"table {schema_name}",
    )
    warnings.extend(table_warnings)
    for t in quality_model_tests + table_model_tests:
        existing = dbt_model.setdefault("data_tests", [])
        if t not in existing:
            existing.append(t)

    return dbt_model, warnings


def _to_models_yaml(
    odcs: OpenDataContractStandard, server: Optional[str], meta_key_style: DbtMetaKeyStyle,
    tag_namespace_filter: Optional[list] = None,
) -> str:
    adapter_type = _adapter_type(odcs, server)
    dbt = {"version": 2, "models": []}
    warnings: list[str] = []
    for schema_obj in odcs.schema_ or []:
        model, warns = _to_dbt_model(
            schema_obj.name, schema_obj, odcs, adapter_type, meta_key_style,
            tag_namespace_filter,
        )
        dbt["models"].append(model)
        warnings.extend(warns)
    text = yaml.safe_dump(dbt, indent=2, sort_keys=False, allow_unicode=True)
    return _prepend_warnings(text, warnings)


def _to_dbt_source_table(
    odcs: OpenDataContractStandard, model_key: str, model_value: SchemaObject,
    adapter_type: Optional[str], meta_key_style: DbtMetaKeyStyle,
    tag_namespace_filter: Optional[list] = None,
) -> tuple[dict, list[str]]:
    table: dict = {"name": model_key}
    if model_value.description is not None:
        table["description"] = model_value.description.strip().replace("\n", " ")

    meta, tags = _governance(model_value, meta_key_style, tag_namespace_filter)
    config: dict = {}
    if meta:
        config["meta"] = meta
    if tags:
        config["tags"] = tags
    if config:
        table["config"] = config

    columns, quality_model_tests, warnings = _to_columns(
        odcs, model_value.properties or [], False, adapter_type, meta_key_style=meta_key_style,
        tag_namespace_filter=tag_namespace_filter,
    )
    if columns:
        table["columns"] = columns

    _, table_model_tests, table_warnings = _quality_to_tests(
        model_value.quality, column=None, label=f"table {model_key}",
    )
    warnings.extend(table_warnings)
    for t in quality_model_tests + table_model_tests:
        existing = table.setdefault("data_tests", [])
        if t not in existing:
            existing.append(t)

    return table, warnings


def _source_name(odcs: OpenDataContractStandard) -> str:
    """A dbt-safe source name: `sources[].name` becomes the identifier used in
    `source('name', 'table')` calls, so it must be a plain slug — not the contract's
    dotted `id` (e.g. `dev_dp_db.collate`). Prefer the contract's `name` field
    (e.g. `COLLATE`), sanitized; fall back to a sanitized `id` if `name` is absent.
    """
    raw = (getattr(odcs, "name", None) or odcs.id or "").strip()
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", raw).strip("_").lower()
    return slug or re.sub(r"[^a-zA-Z0-9]+", "_", odcs.id).strip("_").lower()


def _to_sources_yaml(
    odcs: OpenDataContractStandard, server: Optional[str], meta_key_style: DbtMetaKeyStyle,
    tag_namespace_filter: Optional[list] = None,
) -> str:
    source: dict = {"name": _source_name(odcs)}
    dbt = {"version": 2, "sources": [source]}

    owner = _get_owner(odcs)
    if owner is not None:
        source["meta"] = {"owner": owner}
    desc_str = _get_description_str(odcs.description)
    if desc_str is not None:
        source["description"] = desc_str

    found_server = _get_server_by_name(odcs, server) if server else None
    adapter_type = None
    if found_server is not None:
        adapter_type = found_server.type
        if adapter_type == "bigquery":
            source["database"] = found_server.project
            source["schema"] = found_server.dataset
        else:
            source["database"] = found_server.database
            source["schema"] = found_server.schema_

    tables = []
    warnings: list[str] = []
    for schema_obj in odcs.schema_ or []:
        table, warns = _to_dbt_source_table(
            odcs, schema_obj.name, schema_obj, adapter_type, meta_key_style,
            tag_namespace_filter,
        )
        tables.append(table)
        warnings.extend(warns)
    source["tables"] = tables
    text = yaml.safe_dump(dbt, indent=2, sort_keys=False, allow_unicode=True)
    return _prepend_warnings(text, warnings)


def _to_staging_sql(odcs: OpenDataContractStandard, schema_name: str) -> str:
    model_name, model_value = _check_schema_name_for_export(odcs, schema_name, "dbt-staging-sql")
    return to_dbt_staging_sql(odcs, model_name, model_value)


def to_dbt_yaml(
    contract: OpenDataContractStandard,
    *,
    kind: DbtKind = DbtKind.models,
    server: Optional[str] = None,
    schema_name: str = "all",
    meta_key_style: DbtMetaKeyStyle = DbtMetaKeyStyle.full,
    tag_namespace_filter: Optional[list] = None,
) -> str:
    """Render the requested dbt artifact for a contract."""
    kind = DbtKind(kind)
    meta_key_style = DbtMetaKeyStyle(meta_key_style)
    if kind is DbtKind.models:
        return _to_models_yaml(contract, server, meta_key_style, tag_namespace_filter)
    if kind is DbtKind.sources:
        return _to_sources_yaml(contract, server, meta_key_style, tag_namespace_filter)
    return _to_staging_sql(contract, schema_name)


class DcxDbtExporter(Exporter):
    """Exporter for the `dbt` format — registered in the upstream factory."""

    def export(
        self,
        data_contract: OpenDataContractStandard,
        schema_name: str,
        server: str,
        sql_server_type: str,
        export_args: dict,
    ) -> str:
        return to_dbt_yaml(
            data_contract,
            kind=DbtKind(export_args.get("kind", "models")),
            server=server,
            schema_name=schema_name,
            meta_key_style=DbtMetaKeyStyle(export_args.get("meta_key_style", "full")),
            tag_namespace_filter=export_args.get("tag_namespace_filter"),
        )


# === Registration with upstream factory =====================================
exporter_factory.register_lazy_exporter(
    "dbt", "dcx.exporters.dbt", "DcxDbtExporter",
)
