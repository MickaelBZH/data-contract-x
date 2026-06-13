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

Type conversion (`convert_to_sql_type`) and the data-test mapping
(`field_to_data_tests`) are reused from upstream so they keep evolving with it;
staging SQL is reused wholesale (it has no governance to map).

Imported for its side effects (factory registration) by `dcx.exporters.command`.
"""

from enum import Enum
from typing import Optional

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
from open_data_contract_standard.model import OpenDataContractStandard, SchemaObject, SchemaProperty

# The `NAME=VALUE` tag convention is shared with the Snowflake exporters.
from dcx.exporters.snowflake import _parse_tag


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


def _governance(src, meta_key_style: DbtMetaKeyStyle) -> tuple[dict, list]:
    """Map an ODCS element's governance fields to dbt (meta dict, tags list).

    - `NAME=VALUE` tag → `meta[_meta_key(name)] = value` (key/value metadata)
    - bare tag         → `tags` list (a dbt selection label)
    - `classification` / `businessName` / `criticalDataElement` → `meta`

    Schema objects carry only `tags`; the `getattr` guards make the column-only
    fields no-ops there, so the same helper serves both columns and tables.
    """
    meta: dict = {}
    tags: list = []
    for tag in getattr(src, "tags", None) or []:
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


def _to_dbt_column(
    odcs: OpenDataContractStandard,
    prop: SchemaProperty,
    supports_constraints: bool,
    adapter_type: Optional[str],
    is_primary_key: bool = False,
    is_single_pk: bool = False,
    *,
    meta_key_style: DbtMetaKeyStyle = DbtMetaKeyStyle.full,
) -> dict:
    """Build a dbt column dict in a readable key order, routing governance to `config`."""
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

    meta, tags = _governance(prop, meta_key_style)
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
    if data_tests:
        column["data_tests"] = data_tests

    return column


def _to_columns(
    odcs: OpenDataContractStandard,
    properties: list[SchemaProperty],
    supports_constraints: bool,
    adapter_type: Optional[str],
    primary_key_columns: Optional[list] = None,
    *,
    meta_key_style: DbtMetaKeyStyle = DbtMetaKeyStyle.full,
) -> list:
    primary_key_columns = primary_key_columns or []
    is_single_pk = len(primary_key_columns) == 1
    return [
        _to_dbt_column(
            odcs, prop, supports_constraints, adapter_type,
            prop.name in primary_key_columns, is_single_pk,
            meta_key_style=meta_key_style,
        )
        for prop in properties
    ]


def _to_dbt_model(
    schema_name: str, schema_object: SchemaObject, odcs: OpenDataContractStandard,
    adapter_type: Optional[str], meta_key_style: DbtMetaKeyStyle,
) -> dict:
    model_type = _to_dbt_model_type(schema_object.physicalType)

    config: dict = {"meta": {"data_contract": odcs.id}}
    if model_type:
        config["materialized"] = model_type
    owner = _get_owner(odcs)
    if owner is not None:
        config["meta"]["owner"] = owner

    # Schema-object (table) level tags — dropped by the upstream models exporter —
    # land on the model's config.meta / config.tags.
    meta, tags = _governance(schema_object, meta_key_style)
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

    columns = _to_columns(
        odcs, schema_object.properties or [], _supports_constraints(model_type),
        adapter_type, primary_key_columns, meta_key_style=meta_key_style,
    )
    if columns:
        dbt_model["columns"] = columns
    return dbt_model


def _to_models_yaml(
    odcs: OpenDataContractStandard, server: Optional[str], meta_key_style: DbtMetaKeyStyle,
) -> str:
    adapter_type = _adapter_type(odcs, server)
    dbt = {"version": 2, "models": []}
    for schema_obj in odcs.schema_ or []:
        dbt["models"].append(
            _to_dbt_model(schema_obj.name, schema_obj, odcs, adapter_type, meta_key_style)
        )
    return yaml.safe_dump(dbt, indent=2, sort_keys=False, allow_unicode=True)


def _to_dbt_source_table(
    odcs: OpenDataContractStandard, model_key: str, model_value: SchemaObject,
    adapter_type: Optional[str], meta_key_style: DbtMetaKeyStyle,
) -> dict:
    table: dict = {"name": model_key}
    if model_value.description is not None:
        table["description"] = model_value.description.strip().replace("\n", " ")

    meta, tags = _governance(model_value, meta_key_style)
    config: dict = {}
    if meta:
        config["meta"] = meta
    if tags:
        config["tags"] = tags
    if config:
        table["config"] = config

    columns = _to_columns(
        odcs, model_value.properties or [], False, adapter_type, meta_key_style=meta_key_style,
    )
    if columns:
        table["columns"] = columns
    return table


def _to_sources_yaml(
    odcs: OpenDataContractStandard, server: Optional[str], meta_key_style: DbtMetaKeyStyle,
) -> str:
    source: dict = {"name": odcs.id}
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

    source["tables"] = [
        _to_dbt_source_table(odcs, schema_obj.name, schema_obj, adapter_type, meta_key_style)
        for schema_obj in (odcs.schema_ or [])
    ]
    return yaml.safe_dump(dbt, indent=2, sort_keys=False, allow_unicode=True)


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
) -> str:
    """Render the requested dbt artifact for a contract."""
    kind = DbtKind(kind)
    meta_key_style = DbtMetaKeyStyle(meta_key_style)
    if kind is DbtKind.models:
        return _to_models_yaml(contract, server, meta_key_style)
    if kind is DbtKind.sources:
        return _to_sources_yaml(contract, server, meta_key_style)
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
        )


# === Registration with upstream factory =====================================
exporter_factory.register_lazy_exporter(
    "dbt", "dcx.exporters.dbt", "DcxDbtExporter",
)
