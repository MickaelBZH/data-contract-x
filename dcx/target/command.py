"""`dcx target <server-type>` — bind a contract to a target platform.

Each server type is a Typer subcommand with its own type-specific required and
optional flags, so `dcx target snowflake --help` shows only Snowflake options.
The shared machinery (server-block upsert + physical-type resolution) lives in
`apply_target()`.

Physical-type resolution dispatches on (server.type, server.format):
- SQL dialects (snowflake, postgres, mysql, redshift, databricks, sqlserver,
  bigquery, trino, oracle, duckdb) → via `convert_to_sql_type`. Multi-dialect
  contracts write per-dialect customProperty keys.
- Non-SQL types with a resolvable format (kafka/avro, kafka/protobuf,
  kafka|s3|local/json, s3|local/parquet|delta) → via a format mapping.
  Always writes `physicalType` (no per-format customProperty system yet).
- Other (csv, xml, no format, unsupported types) → server block updated,
  physical types skipped with a warning.
"""

import contextvars
from enum import Enum
from pathlib import Path
from typing import Callable, Optional

import typer
from datacontract.export.sql_type_converter import convert_to_sql_type
from open_data_contract_standard.model import (
    CustomProperty,
    OpenDataContractStandard,
    SchemaProperty,
    Server,
)

# ODCS server type → converter dialect understood by `convert_to_sql_type`.
SQL_DIALECT_MAP: dict[str, str] = {
    "snowflake":  "snowflake",
    "postgres":   "postgres",
    "postgresql": "postgres",
    "redshift":   "postgres",
    "mysql":      "mysql",
    "databricks": "databricks",
    "sqlserver":  "sqlserver",
    "synapse":    "sqlserver",
    "bigquery":   "bigquery",
    "trino":      "trino",
    "oracle":     "oracle",
    "duckdb":     "local",
}

# Converter dialect → customProperty key the converter reads first in multi-dialect mode.
DIALECT_CUSTOM_PROPERTY_KEY: dict[str, str] = {
    "snowflake":  "snowflakeType",
    "postgres":   "postgresType",
    "mysql":      "mysqlType",
    "databricks": "databricksType",
    "sqlserver":  "sqlserverType",
    "bigquery":   "bigqueryType",
    "trino":      "trinoType",
}


class KafkaFormat(str, Enum):
    json = "json"
    avro = "avro"
    protobuf = "protobuf"
    xml = "xml"


class ObjectStoreFormat(str, Enum):
    parquet = "parquet"
    delta = "delta"
    json = "json"
    csv = "csv"


# Per-format mapping from ODCS logicalType (lowercased) → format-specific physical type string.
# Logical types come from ODCS v3: string, integer, number, boolean, date, timestamp, object, array.
# We also accept the common synonyms `bytes` and `binary` since some importers emit them.
_AVRO_TYPES: dict[str, str] = {
    "string":    "string",
    "integer":   "long",
    "number":    "double",
    "boolean":   "boolean",
    "date":      "date",                # avro logical type name
    "timestamp": "timestamp-micros",    # avro logical type name; modern default
    "object":    "record",
    "array":     "array",
    "bytes":     "bytes",
    "binary":    "bytes",
}

_PROTOBUF_TYPES: dict[str, str] = {
    "string":    "string",
    "integer":   "int64",
    "number":    "double",
    "boolean":   "bool",
    "date":      "google.type.Date",
    "timestamp": "google.protobuf.Timestamp",
    "object":    "message",
    "array":     "repeated",
    "bytes":     "bytes",
    "binary":    "bytes",
}

_JSONSCHEMA_TYPES: dict[str, str] = {
    "string":    "string",
    "integer":   "integer",
    "number":    "number",
    "boolean":   "boolean",
    "date":      "string",   # JSON Schema: format=date hint lives in logicalTypeOptions
    "timestamp": "string",   # format=date-time
    "object":    "object",
    "array":     "array",
    "bytes":     "string",   # typically base64-encoded
    "binary":    "string",
}

_PARQUET_TYPES: dict[str, str] = {
    "string":    "STRING",
    "integer":   "INT64",
    "number":    "DOUBLE",
    "boolean":   "BOOLEAN",
    "date":      "DATE",
    "timestamp": "TIMESTAMP_MICROS",
    "object":    "STRUCT",
    "array":     "LIST",
    "bytes":     "BINARY",
    "binary":    "BINARY",
}

# Format value (server.format) → per-format type map.
# `delta` lake stores data as parquet, so we reuse the parquet map.
# csv and xml are intentionally absent: csv is untyped, xml has no useful mapping.
_FORMAT_TYPE_MAPS: dict[str, dict[str, str]] = {
    "avro":     _AVRO_TYPES,
    "protobuf": _PROTOBUF_TYPES,
    "json":     _JSONSCHEMA_TYPES,
    "parquet":  _PARQUET_TYPES,
    "delta":    _PARQUET_TYPES,
}


PhysicalTypeResolver = Callable[[SchemaProperty], Optional[str]]


def _sql_resolver(dialect: str) -> PhysicalTypeResolver:
    """Resolver that maps a property's logicalType to a SQL dialect's type."""

    def resolve(prop: SchemaProperty) -> Optional[str]:
        # Always compute from logicalType; ignore any stale physicalType.
        scratch = prop.model_copy(update={"physicalType": None})
        return convert_to_sql_type(scratch, dialect)

    return resolve


def _format_resolver(fmt: str) -> Optional[PhysicalTypeResolver]:
    """Resolver factory for `format`-based mapping. Returns None if format is unsupported."""
    mapping = _FORMAT_TYPE_MAPS.get(fmt)
    if mapping is None:
        return None

    def resolve(prop: SchemaProperty) -> Optional[str]:
        logical = (prop.logicalType or "").lower()
        return mapping.get(logical) if logical else None

    return resolve


def _err(msg: str) -> None:
    typer.secho(msg, err=True, fg=typer.colors.RED)


def _warn(msg: str) -> None:
    typer.secho(msg, err=True, fg=typer.colors.YELLOW)


class TargetConflictError(Exception):
    """A server entry with the same name but a different type already exists.

    Raised by `transform_contract_for_target` when overwrite=False. The CLI wrapper
    catches this and exits non-zero; the API layer catches it and returns HTTP 409.
    """

    def __init__(self, server_name: str, existing_type: str, new_type: str) -> None:
        self.server_name = server_name
        self.existing_type = existing_type
        self.new_type = new_type
        super().__init__(
            f"Server '{server_name}' already exists with type '{existing_type}', "
            f"but you're targeting '{new_type}'."
        )


def transform_contract_for_target(
    *,
    server: Server,
    contract: OpenDataContractStandard,
    schema_name: str = "all",
    overwrite: bool = False,
) -> OpenDataContractStandard:
    """Apply a target operation to an in-memory contract and return the modified contract.

    Pure function — no file IO. Mutates and returns the same `contract` object for
    convenience; callers wanting a copy should pass `contract.model_copy(deep=True)`.

    Raises `TargetConflictError` when a same-named server entry exists with a
    different type and `overwrite=False`.
    """
    existing = next(
        (s for s in contract.servers or [] if s.server == server.server), None
    )
    if existing is not None and existing.type != server.type and not overwrite:
        raise TargetConflictError(server.server, existing.type, server.type)

    contract.servers = _upsert_server(contract.servers or [], server)

    resolver, custom_property_key = _select_resolver(server, contract.servers)
    if resolver is not None:
        for schema_obj in contract.schema_ or []:
            if schema_name != "all" and schema_obj.name != schema_name:
                continue
            _walk_and_apply(schema_obj.properties or [], resolver, custom_property_key)

    return contract


# When set (e.g. by the API mirror helper), apply_target stores its args here
# instead of doing file IO. The mirror runs each Typer command function and just
# wants the constructed Server; the actual transform runs in the API handler
# with the contract from the request body. Using a contextvar keeps this safe
# across concurrent async requests in FastAPI.
_target_capture_var: contextvars.ContextVar[Optional[dict]] = contextvars.ContextVar(
    "_target_capture_var", default=None,
)


def apply_target(
    *,
    server: Server,
    contract_path: Path,
    schema_name: str = "all",
    output: Optional[Path] = None,
    overwrite: bool = False,
) -> None:
    """CLI wrapper: read contract from file → transform → write to file or stdout.

    If the `_target_capture_var` contextvar is set (API mode), capture the args
    into it and return without touching the filesystem.
    """
    capture = _target_capture_var.get()
    if capture is not None:
        capture["server"] = server
        capture["schema_name"] = schema_name
        capture["overwrite"] = overwrite
        return

    if not contract_path.exists():
        _err(f"Contract file not found: {contract_path}")
        raise typer.Exit(1)
    contract = OpenDataContractStandard.from_file(str(contract_path))

    try:
        contract = transform_contract_for_target(
            server=server, contract=contract,
            schema_name=schema_name, overwrite=overwrite,
        )
    except TargetConflictError as exc:
        _err(
            f"{exc} Pass --overwrite to replace it, or use --server-name <other> "
            f"to add as a new entry."
        )
        raise typer.Exit(1)

    yaml_content = contract.to_yaml()
    if output is None:
        # Default: write the modified contract to stdout (matches `dcx import`/`dcx export`).
        # Pass --output <path> to write to a file (use --output <input> to overwrite in place).
        typer.echo(yaml_content, nl=False)
    else:
        output.write_text(yaml_content, encoding="utf-8")
        typer.echo(f"Wrote {output}", err=True)


def _upsert_server(servers: list[Server], new: Server) -> list[Server]:
    """Replace the entry whose `server` name matches, otherwise append."""
    result = list(servers)
    for i, existing in enumerate(result):
        if existing.server == new.server:
            result[i] = new
            return result
    result.append(new)
    return result


def _distinct_sql_dialects(servers: Optional[list[Server]]) -> set[str]:
    if not servers:
        return set()
    return {SQL_DIALECT_MAP[s.type] for s in servers if s.type in SQL_DIALECT_MAP}


def _select_resolver(
    server: Server, all_servers: Optional[list[Server]],
) -> tuple[Optional[PhysicalTypeResolver], Optional[str]]:
    """Pick the physical-type resolver and write target (None=physicalType, str=customProperty key).

    Emits a warning and returns (None, None) when no resolver applies.
    """
    sql_dialect = SQL_DIALECT_MAP.get(server.type)
    if sql_dialect is not None:
        all_dialects = _distinct_sql_dialects(all_servers)
        write_to_custom_property = len(all_dialects) > 1
        if write_to_custom_property and sql_dialect not in DIALECT_CUSTOM_PROPERTY_KEY:
            _warn(
                f"Contract has multiple SQL dialects ({sorted(all_dialects)}), but "
                f"'{sql_dialect}' has no dedicated customProperty key in the converter; "
                f"skipping physical-type resolution for '{server.type}'."
            )
            return (None, None)
        custom_key = (
            DIALECT_CUSTOM_PROPERTY_KEY[sql_dialect] if write_to_custom_property else None
        )
        return (_sql_resolver(sql_dialect), custom_key)

    if server.format:
        fmt_resolver = _format_resolver(server.format)
        if fmt_resolver is None:
            _warn(
                f"No physical-type resolver for format '{server.format}' on '{server.type}'. "
                f"Server block updated only."
            )
            return (None, None)
        return (fmt_resolver, None)

    _warn(
        f"Physical-type resolution for '{server.type}' (no format given) is not supported. "
        f"Server block updated only."
    )
    return (None, None)


def _walk_and_apply(
    props: list[SchemaProperty],
    resolver: PhysicalTypeResolver,
    custom_property_key: Optional[str],
) -> None:
    """Recursively apply a resolver to every property (and nested properties/items)."""
    for prop in props:
        physical = resolver(prop)
        if physical is not None:
            if custom_property_key is None:
                prop.physicalType = physical
            else:
                _set_custom_property(prop, custom_property_key, physical)
        if prop.properties:
            _walk_and_apply(prop.properties, resolver, custom_property_key)
        if prop.items:
            _walk_and_apply([prop.items], resolver, custom_property_key)


def _set_custom_property(prop: SchemaProperty, key: str, value: str) -> None:
    if prop.customProperties is None:
        prop.customProperties = []
    for cp in prop.customProperties:
        if cp.property == key:
            cp.value = value
            return
    prop.customProperties.append(CustomProperty(property=key, value=value))


def _build_server(
    server_type: str, server_name: str, *, schema: Optional[str] = None, **fields
) -> Server:
    """Construct a Server, dropping None-valued fields and setting the aliased `schema_` correctly.

    Pydantic v2 doesn't honor `schema_=...` at construction time (the model uses an `alias="schema"`),
    so we set it as an attribute after construction. See gotcha-odcs-aliased-fields memory.
    """
    server = Server(
        server=server_name,
        type=server_type,
        **{k: v for k, v in fields.items() if v is not None},
    )
    if schema is not None:
        server.schema_ = schema
    return server


# === Typer sub-app ===========================================================

target_app = typer.Typer(
    help="Bind a data contract to a target platform: set the server block and resolve physicalType.",
    no_args_is_help=True,
)


# Argument/option definitions reused by every subcommand.
_LOCATION_ARG = typer.Argument(
    Path("datacontract.yaml"), help="Path to the data contract file."
)
_SERVER_NAME_OPT = typer.Option(
    "production", "--server-name", help="Server entry to add/update (upsert key)."
)
_ID_OPT = typer.Option(None, "--id", help="Stable server identifier.")
_DESC_OPT = typer.Option(None, "--description", help="Server description.")
_ENV_OPT = typer.Option(
    None, "--environment", help="Environment label (prod, staging, dev, ...)."
)
_SCHEMA_NAME_OPT = typer.Option(
    "all", "--schema-name", help="Contract schema to process (default: all)."
)
_OUTPUT_OPT = typer.Option(
    None, "--output",
    help="Write modified contract here. Default: stdout. Pass the input path to overwrite in place.",
)
_OVERWRITE_OPT = typer.Option(
    False, "--overwrite",
    help="Allow replacing an existing server entry that has a different type.",
)


@target_app.command("snowflake")
def snowflake(
    location: Path = _LOCATION_ARG,
    account: str = typer.Option(..., "--account", help="Snowflake account identifier (e.g. xy12345.us-east-1)."),
    database: str = typer.Option(..., "--database", help="Database name."),
    schema: str = typer.Option(..., "--schema", help="Schema name."),
    warehouse: Optional[str] = typer.Option(None, "--warehouse", help="Warehouse name."),
    server_name: str = _SERVER_NAME_OPT,
    environment: Optional[str] = _ENV_OPT,
    description: Optional[str] = _DESC_OPT,
    server_id: Optional[str] = _ID_OPT,
    schema_name: str = _SCHEMA_NAME_OPT,
    output: Optional[Path] = _OUTPUT_OPT,
    overwrite: bool = _OVERWRITE_OPT,
) -> None:
    """Bind a data contract to Snowflake."""
    server = Server(
        server=server_name, type="snowflake",
        account=account, database=database, warehouse=warehouse,
        environment=environment, description=description, id=server_id,
    )
    server.schema_ = schema
    apply_target(server=server, contract_path=location, schema_name=schema_name, output=output, overwrite=overwrite)


@target_app.command("postgres")
def postgres(
    location: Path = _LOCATION_ARG,
    host: str = typer.Option(..., "--host", help="Hostname."),
    database: str = typer.Option(..., "--database", help="Database name."),
    schema: str = typer.Option(..., "--schema", help="Schema name."),
    port: int = typer.Option(5432, "--port", help="Port number."),
    server_name: str = _SERVER_NAME_OPT,
    environment: Optional[str] = _ENV_OPT,
    description: Optional[str] = _DESC_OPT,
    server_id: Optional[str] = _ID_OPT,
    schema_name: str = _SCHEMA_NAME_OPT,
    output: Optional[Path] = _OUTPUT_OPT,
    overwrite: bool = _OVERWRITE_OPT,
) -> None:
    """Bind a data contract to PostgreSQL."""
    server = Server(
        server=server_name, type="postgres",
        host=host, port=port, database=database,
        environment=environment, description=description, id=server_id,
    )
    server.schema_ = schema
    apply_target(server=server, contract_path=location, schema_name=schema_name, output=output, overwrite=overwrite)


@target_app.command("mysql")
def mysql(
    location: Path = _LOCATION_ARG,
    host: str = typer.Option(..., "--host", help="Hostname."),
    database: str = typer.Option(..., "--database", help="Database name."),
    port: int = typer.Option(3306, "--port", help="Port number."),
    server_name: str = _SERVER_NAME_OPT,
    environment: Optional[str] = _ENV_OPT,
    description: Optional[str] = _DESC_OPT,
    server_id: Optional[str] = _ID_OPT,
    schema_name: str = _SCHEMA_NAME_OPT,
    output: Optional[Path] = _OUTPUT_OPT,
    overwrite: bool = _OVERWRITE_OPT,
) -> None:
    """Bind a data contract to MySQL."""
    server = Server(
        server=server_name, type="mysql",
        host=host, port=port, database=database,
        environment=environment, description=description, id=server_id,
    )
    apply_target(server=server, contract_path=location, schema_name=schema_name, output=output, overwrite=overwrite)


@target_app.command("redshift")
def redshift(
    location: Path = _LOCATION_ARG,
    database: str = typer.Option(..., "--database", help="Database name."),
    schema: str = typer.Option(..., "--schema", help="Schema name."),
    host: Optional[str] = typer.Option(None, "--host", help="Cluster endpoint host."),
    region: Optional[str] = typer.Option(None, "--region", help="AWS region."),
    account: Optional[str] = typer.Option(None, "--account", help="AWS account ID."),
    server_name: str = _SERVER_NAME_OPT,
    environment: Optional[str] = _ENV_OPT,
    description: Optional[str] = _DESC_OPT,
    server_id: Optional[str] = _ID_OPT,
    schema_name: str = _SCHEMA_NAME_OPT,
    output: Optional[Path] = _OUTPUT_OPT,
    overwrite: bool = _OVERWRITE_OPT,
) -> None:
    """Bind a data contract to Amazon Redshift."""
    server = Server(
        server=server_name, type="redshift",
        database=database, host=host, region=region, account=account,
        environment=environment, description=description, id=server_id,
    )
    server.schema_ = schema
    apply_target(server=server, contract_path=location, schema_name=schema_name, output=output, overwrite=overwrite)


@target_app.command("databricks")
def databricks(
    location: Path = _LOCATION_ARG,
    catalog: str = typer.Option(..., "--catalog", help="Unity catalog name."),
    schema: str = typer.Option(..., "--schema", help="Schema name."),
    host: Optional[str] = typer.Option(None, "--host", help="Workspace hostname."),
    server_name: str = _SERVER_NAME_OPT,
    environment: Optional[str] = _ENV_OPT,
    description: Optional[str] = _DESC_OPT,
    server_id: Optional[str] = _ID_OPT,
    schema_name: str = _SCHEMA_NAME_OPT,
    output: Optional[Path] = _OUTPUT_OPT,
    overwrite: bool = _OVERWRITE_OPT,
) -> None:
    """Bind a data contract to Databricks."""
    server = Server(
        server=server_name, type="databricks",
        catalog=catalog, host=host,
        environment=environment, description=description, id=server_id,
    )
    server.schema_ = schema
    apply_target(server=server, contract_path=location, schema_name=schema_name, output=output, overwrite=overwrite)


@target_app.command("bigquery")
def bigquery(
    location: Path = _LOCATION_ARG,
    project: str = typer.Option(..., "--project", help="GCP project ID."),
    dataset: str = typer.Option(..., "--dataset", help="BigQuery dataset."),
    server_name: str = _SERVER_NAME_OPT,
    environment: Optional[str] = _ENV_OPT,
    description: Optional[str] = _DESC_OPT,
    server_id: Optional[str] = _ID_OPT,
    schema_name: str = _SCHEMA_NAME_OPT,
    output: Optional[Path] = _OUTPUT_OPT,
    overwrite: bool = _OVERWRITE_OPT,
) -> None:
    """Bind a data contract to BigQuery."""
    server = Server(
        server=server_name, type="bigquery",
        project=project, dataset=dataset,
        environment=environment, description=description, id=server_id,
    )
    apply_target(server=server, contract_path=location, schema_name=schema_name, output=output, overwrite=overwrite)


@target_app.command("sqlserver")
def sqlserver(
    location: Path = _LOCATION_ARG,
    host: str = typer.Option(..., "--host", help="Hostname."),
    database: str = typer.Option(..., "--database", help="Database name."),
    schema: Optional[str] = typer.Option(None, "--schema", help="Schema name."),
    port: int = typer.Option(1433, "--port", help="Port number."),
    server_name: str = _SERVER_NAME_OPT,
    environment: Optional[str] = _ENV_OPT,
    description: Optional[str] = _DESC_OPT,
    server_id: Optional[str] = _ID_OPT,
    schema_name: str = _SCHEMA_NAME_OPT,
    output: Optional[Path] = _OUTPUT_OPT,
    overwrite: bool = _OVERWRITE_OPT,
) -> None:
    """Bind a data contract to SQL Server."""
    server = Server(
        server=server_name, type="sqlserver",
        host=host, port=port, database=database,
        environment=environment, description=description, id=server_id,
    )
    if schema is not None:
        server.schema_ = schema
    apply_target(server=server, contract_path=location, schema_name=schema_name, output=output, overwrite=overwrite)


@target_app.command("kafka")
def kafka(
    location: Path = _LOCATION_ARG,
    host: str = typer.Option(..., "--host", help="Bootstrap servers (e.g. kafka:9092)."),
    format: KafkaFormat = typer.Option(KafkaFormat.json, "--format", help="Message format."),
    server_name: str = _SERVER_NAME_OPT,
    environment: Optional[str] = _ENV_OPT,
    description: Optional[str] = _DESC_OPT,
    server_id: Optional[str] = _ID_OPT,
    schema_name: str = _SCHEMA_NAME_OPT,
    output: Optional[Path] = _OUTPUT_OPT,
    overwrite: bool = _OVERWRITE_OPT,
) -> None:
    """Bind a data contract to a Kafka topic.

    Physical types are resolved from `--format` (avro, protobuf, or json). The
    `xml` format has no useful logical-to-physical mapping and is skipped with
    a warning.
    """
    server = Server(
        server=server_name, type="kafka",
        host=host, format=format.value,
        environment=environment, description=description, id=server_id,
    )
    apply_target(server=server, contract_path=location, schema_name=schema_name, output=output, overwrite=overwrite)


@target_app.command("s3")
def s3(
    location: Path = _LOCATION_ARG,
    location_uri: str = typer.Option(..., "--location", help="S3 URI (e.g. s3://bucket/prefix/)."),
    format: ObjectStoreFormat = typer.Option(..., "--format", help="Object format."),
    delimiter: Optional[str] = typer.Option(None, "--delimiter", help="Field delimiter (csv)."),
    endpoint_url: Optional[str] = typer.Option(None, "--endpoint-url", help="Custom S3 endpoint URL."),
    server_name: str = _SERVER_NAME_OPT,
    environment: Optional[str] = _ENV_OPT,
    description: Optional[str] = _DESC_OPT,
    server_id: Optional[str] = _ID_OPT,
    schema_name: str = _SCHEMA_NAME_OPT,
    output: Optional[Path] = _OUTPUT_OPT,
    overwrite: bool = _OVERWRITE_OPT,
) -> None:
    """Bind a data contract to S3 (object storage).

    Physical types are resolved from `--format` (parquet, delta, or json).
    The `csv` format is untyped and skipped with a warning.
    """
    server = Server(
        server=server_name, type="s3",
        location=location_uri, format=format.value,
        delimiter=delimiter, endpointUrl=endpoint_url,
        environment=environment, description=description, id=server_id,
    )
    apply_target(server=server, contract_path=location, schema_name=schema_name, output=output, overwrite=overwrite)


@target_app.command("local")
def local(
    location: Path = _LOCATION_ARG,
    path: str = typer.Option(..., "--path", help="Local file path."),
    format: ObjectStoreFormat = typer.Option(..., "--format", help="File format."),
    server_name: str = _SERVER_NAME_OPT,
    environment: Optional[str] = _ENV_OPT,
    description: Optional[str] = _DESC_OPT,
    server_id: Optional[str] = _ID_OPT,
    schema_name: str = _SCHEMA_NAME_OPT,
    output: Optional[Path] = _OUTPUT_OPT,
    overwrite: bool = _OVERWRITE_OPT,
) -> None:
    """Bind a data contract to a local file.

    Physical types are resolved from `--format` (parquet, delta, or json).
    The `csv` format is untyped and skipped with a warning.
    """
    server = Server(
        server=server_name, type="local",
        path=path, format=format.value,
        environment=environment, description=description, id=server_id,
    )
    apply_target(server=server, contract_path=location, schema_name=schema_name, output=output, overwrite=overwrite)


# === SQL aliases ============================================================
# `postgresql` and `synapse` are ODCS enum values that map onto existing converters.


@target_app.command("postgresql")
def postgresql(
    location: Path = _LOCATION_ARG,
    host: str = typer.Option(..., "--host", help="Hostname."),
    database: str = typer.Option(..., "--database", help="Database name."),
    schema: str = typer.Option(..., "--schema", help="Schema name."),
    port: int = typer.Option(5432, "--port", help="Port number."),
    server_name: str = _SERVER_NAME_OPT,
    environment: Optional[str] = _ENV_OPT,
    description: Optional[str] = _DESC_OPT,
    server_id: Optional[str] = _ID_OPT,
    schema_name: str = _SCHEMA_NAME_OPT,
    output: Optional[Path] = _OUTPUT_OPT,
    overwrite: bool = _OVERWRITE_OPT,
) -> None:
    """Bind a data contract to PostgreSQL (writes `type: postgresql`)."""
    server = _build_server(
        "postgresql", server_name, schema=schema,
        host=host, port=port, database=database,
        environment=environment, description=description, id=server_id,
    )
    apply_target(server=server, contract_path=location, schema_name=schema_name, output=output, overwrite=overwrite)


@target_app.command("synapse")
def synapse(
    location: Path = _LOCATION_ARG,
    host: str = typer.Option(..., "--host", help="Hostname."),
    database: str = typer.Option(..., "--database", help="Database name."),
    schema: Optional[str] = typer.Option(None, "--schema", help="Schema name."),
    port: int = typer.Option(1433, "--port", help="Port number."),
    server_name: str = _SERVER_NAME_OPT,
    environment: Optional[str] = _ENV_OPT,
    description: Optional[str] = _DESC_OPT,
    server_id: Optional[str] = _ID_OPT,
    schema_name: str = _SCHEMA_NAME_OPT,
    output: Optional[Path] = _OUTPUT_OPT,
    overwrite: bool = _OVERWRITE_OPT,
) -> None:
    """Bind a data contract to Azure Synapse (SQL Server-compatible dialect)."""
    server = _build_server(
        "synapse", server_name, schema=schema,
        host=host, port=port, database=database,
        environment=environment, description=description, id=server_id,
    )
    apply_target(server=server, contract_path=location, schema_name=schema_name, output=output, overwrite=overwrite)


# === Additional SQL dialects with type-converter support ====================


@target_app.command("oracle")
def oracle(
    location: Path = _LOCATION_ARG,
    host: str = typer.Option(..., "--host", help="Hostname."),
    service_name: str = typer.Option(..., "--service-name", help="Oracle service name."),
    port: int = typer.Option(1521, "--port", help="Port number."),
    server_name: str = _SERVER_NAME_OPT,
    environment: Optional[str] = _ENV_OPT,
    description: Optional[str] = _DESC_OPT,
    server_id: Optional[str] = _ID_OPT,
    schema_name: str = _SCHEMA_NAME_OPT,
    output: Optional[Path] = _OUTPUT_OPT,
    overwrite: bool = _OVERWRITE_OPT,
) -> None:
    """Bind a data contract to Oracle."""
    server = _build_server(
        "oracle", server_name,
        host=host, port=port, serviceName=service_name,
        environment=environment, description=description, id=server_id,
    )
    apply_target(server=server, contract_path=location, schema_name=schema_name, output=output, overwrite=overwrite)


@target_app.command("trino")
def trino(
    location: Path = _LOCATION_ARG,
    host: str = typer.Option(..., "--host", help="Coordinator hostname."),
    catalog: str = typer.Option(..., "--catalog", help="Catalog name."),
    schema: str = typer.Option(..., "--schema", help="Schema name."),
    port: int = typer.Option(8080, "--port", help="Port number."),
    server_name: str = _SERVER_NAME_OPT,
    environment: Optional[str] = _ENV_OPT,
    description: Optional[str] = _DESC_OPT,
    server_id: Optional[str] = _ID_OPT,
    schema_name: str = _SCHEMA_NAME_OPT,
    output: Optional[Path] = _OUTPUT_OPT,
    overwrite: bool = _OVERWRITE_OPT,
) -> None:
    """Bind a data contract to Trino."""
    server = _build_server(
        "trino", server_name, schema=schema,
        host=host, port=port, catalog=catalog,
        environment=environment, description=description, id=server_id,
    )
    apply_target(server=server, contract_path=location, schema_name=schema_name, output=output, overwrite=overwrite)


@target_app.command("duckdb")
def duckdb(
    location: Path = _LOCATION_ARG,
    database: str = typer.Option(..., "--database", help="DuckDB database file path or name."),
    schema: Optional[str] = typer.Option(None, "--schema", help="Schema name."),
    server_name: str = _SERVER_NAME_OPT,
    environment: Optional[str] = _ENV_OPT,
    description: Optional[str] = _DESC_OPT,
    server_id: Optional[str] = _ID_OPT,
    schema_name: str = _SCHEMA_NAME_OPT,
    output: Optional[Path] = _OUTPUT_OPT,
    overwrite: bool = _OVERWRITE_OPT,
) -> None:
    """Bind a data contract to DuckDB."""
    server = _build_server(
        "duckdb", server_name, schema=schema,
        database=database,
        environment=environment, description=description, id=server_id,
    )
    apply_target(server=server, contract_path=location, schema_name=schema_name, output=output, overwrite=overwrite)


# === Streaming and messaging ================================================


@target_app.command("kinesis")
def kinesis(
    location: Path = _LOCATION_ARG,
    stream: Optional[str] = typer.Option(None, "--stream", help="Kinesis stream name."),
    region: Optional[str] = typer.Option(None, "--region", help="AWS region."),
    format: Optional[str] = typer.Option(None, "--format", help="Record format (json, avro, protobuf, ...)."),
    server_name: str = _SERVER_NAME_OPT,
    environment: Optional[str] = _ENV_OPT,
    description: Optional[str] = _DESC_OPT,
    server_id: Optional[str] = _ID_OPT,
    schema_name: str = _SCHEMA_NAME_OPT,
    output: Optional[Path] = _OUTPUT_OPT,
    overwrite: bool = _OVERWRITE_OPT,
) -> None:
    """Bind a data contract to a Kinesis stream.

    Physical types are resolved when `--format` is avro, protobuf, or json. The
    ODCS schema leaves `format` as a free string for kinesis, so other values
    are accepted but skipped for resolution with a warning.
    """
    server = _build_server(
        "kinesis", server_name,
        stream=stream, region=region, format=format,
        environment=environment, description=description, id=server_id,
    )
    apply_target(server=server, contract_path=location, schema_name=schema_name, output=output, overwrite=overwrite)


@target_app.command("pubsub")
def pubsub(
    location: Path = _LOCATION_ARG,
    project: str = typer.Option(..., "--project", help="GCP project ID."),
    format: Optional[str] = typer.Option(None, "--format", help="Message format (json, avro, protobuf)."),
    server_name: str = _SERVER_NAME_OPT,
    environment: Optional[str] = _ENV_OPT,
    description: Optional[str] = _DESC_OPT,
    server_id: Optional[str] = _ID_OPT,
    schema_name: str = _SCHEMA_NAME_OPT,
    output: Optional[Path] = _OUTPUT_OPT,
    overwrite: bool = _OVERWRITE_OPT,
) -> None:
    """Bind a data contract to Google Cloud Pub/Sub.

    Physical types are resolved when `--format` is avro, protobuf, or json.
    """
    server = _build_server(
        "pubsub", server_name,
        project=project, format=format,
        environment=environment, description=description, id=server_id,
    )
    apply_target(server=server, contract_path=location, schema_name=schema_name, output=output, overwrite=overwrite)


# === Additional object stores ===============================================


@target_app.command("azure")
def azure(
    location: Path = _LOCATION_ARG,
    location_uri: str = typer.Option(..., "--location", help="Azure blob URI (e.g. abfss://...)."),
    format: ObjectStoreFormat = typer.Option(..., "--format", help="Object format."),
    delimiter: Optional[str] = typer.Option(None, "--delimiter", help="Field delimiter (csv)."),
    server_name: str = _SERVER_NAME_OPT,
    environment: Optional[str] = _ENV_OPT,
    description: Optional[str] = _DESC_OPT,
    server_id: Optional[str] = _ID_OPT,
    schema_name: str = _SCHEMA_NAME_OPT,
    output: Optional[Path] = _OUTPUT_OPT,
    overwrite: bool = _OVERWRITE_OPT,
) -> None:
    """Bind a data contract to Azure Blob/Data Lake Storage.

    Physical types are resolved from `--format` (parquet, delta, or json). The
    `csv` format is untyped and skipped with a warning.
    """
    server = _build_server(
        "azure", server_name,
        location=location_uri, format=format.value, delimiter=delimiter,
        environment=environment, description=description, id=server_id,
    )
    apply_target(server=server, contract_path=location, schema_name=schema_name, output=output, overwrite=overwrite)


@target_app.command("sftp")
def sftp(
    location: Path = _LOCATION_ARG,
    location_uri: str = typer.Option(..., "--location", help="SFTP URI (e.g. sftp://host/path/)."),
    format: ObjectStoreFormat = typer.Option(..., "--format", help="File format."),
    delimiter: Optional[str] = typer.Option(None, "--delimiter", help="Field delimiter (csv)."),
    server_name: str = _SERVER_NAME_OPT,
    environment: Optional[str] = _ENV_OPT,
    description: Optional[str] = _DESC_OPT,
    server_id: Optional[str] = _ID_OPT,
    schema_name: str = _SCHEMA_NAME_OPT,
    output: Optional[Path] = _OUTPUT_OPT,
    overwrite: bool = _OVERWRITE_OPT,
) -> None:
    """Bind a data contract to an SFTP location.

    Physical types are resolved from `--format` (parquet, delta, or json). The
    `csv` format is untyped and skipped with a warning.
    """
    server = _build_server(
        "sftp", server_name,
        location=location_uri, format=format.value, delimiter=delimiter,
        environment=environment, description=description, id=server_id,
    )
    apply_target(server=server, contract_path=location, schema_name=schema_name, output=output, overwrite=overwrite)


# === AWS-specific ============================================================


@target_app.command("athena")
def athena(
    location: Path = _LOCATION_ARG,
    staging_dir: str = typer.Option(..., "--staging-dir", help="S3 staging directory (e.g. s3://bucket/staging/)."),
    schema: str = typer.Option(..., "--schema", help="Database/schema name."),
    catalog: str = typer.Option("awsdatacatalog", "--catalog", help="Data catalog name."),
    region_name: Optional[str] = typer.Option(None, "--region-name", help="AWS region."),
    server_name: str = _SERVER_NAME_OPT,
    environment: Optional[str] = _ENV_OPT,
    description: Optional[str] = _DESC_OPT,
    server_id: Optional[str] = _ID_OPT,
    schema_name: str = _SCHEMA_NAME_OPT,
    output: Optional[Path] = _OUTPUT_OPT,
    overwrite: bool = _OVERWRITE_OPT,
) -> None:
    """Bind a data contract to Amazon Athena.

    No SQL converter is wired for athena, so physical types are skipped with a
    warning. Server block is always written.
    """
    server = _build_server(
        "athena", server_name, schema=schema,
        stagingDir=staging_dir, catalog=catalog, regionName=region_name,
        environment=environment, description=description, id=server_id,
    )
    apply_target(server=server, contract_path=location, schema_name=schema_name, output=output, overwrite=overwrite)


@target_app.command("glue")
def glue(
    location: Path = _LOCATION_ARG,
    account: str = typer.Option(..., "--account", help="AWS account ID."),
    database: str = typer.Option(..., "--database", help="Glue database."),
    location_uri: Optional[str] = typer.Option(None, "--location", help="S3 data location."),
    format: Optional[str] = typer.Option(None, "--format", help="Data format (e.g. parquet, json)."),
    server_name: str = _SERVER_NAME_OPT,
    environment: Optional[str] = _ENV_OPT,
    description: Optional[str] = _DESC_OPT,
    server_id: Optional[str] = _ID_OPT,
    schema_name: str = _SCHEMA_NAME_OPT,
    output: Optional[Path] = _OUTPUT_OPT,
    overwrite: bool = _OVERWRITE_OPT,
) -> None:
    """Bind a data contract to the AWS Glue Data Catalog.

    Physical types are resolved when `--format` is avro, protobuf, json, parquet,
    or delta.
    """
    server = _build_server(
        "glue", server_name,
        account=account, database=database, location=location_uri, format=format,
        environment=environment, description=description, id=server_id,
    )
    apply_target(server=server, contract_path=location, schema_name=schema_name, output=output, overwrite=overwrite)


# === Other SQL databases (no type converter — server block only) ============
# All follow the same shape: host (req), database (req), optional port + schema.


@target_app.command("clickhouse")
def clickhouse(
    location: Path = _LOCATION_ARG,
    host: str = typer.Option(..., "--host", help="Hostname."),
    database: str = typer.Option(..., "--database", help="Database name."),
    port: int = typer.Option(8123, "--port", help="Port number."),
    schema: Optional[str] = typer.Option(None, "--schema", help="Schema name."),
    server_name: str = _SERVER_NAME_OPT,
    environment: Optional[str] = _ENV_OPT,
    description: Optional[str] = _DESC_OPT,
    server_id: Optional[str] = _ID_OPT,
    schema_name: str = _SCHEMA_NAME_OPT,
    output: Optional[Path] = _OUTPUT_OPT,
    overwrite: bool = _OVERWRITE_OPT,
) -> None:
    """Bind a data contract to ClickHouse (server block only — no type converter)."""
    server = _build_server(
        "clickhouse", server_name, schema=schema,
        host=host, port=port, database=database,
        environment=environment, description=description, id=server_id,
    )
    apply_target(server=server, contract_path=location, schema_name=schema_name, output=output, overwrite=overwrite)


@target_app.command("db2")
def db2(
    location: Path = _LOCATION_ARG,
    host: str = typer.Option(..., "--host", help="Hostname."),
    database: str = typer.Option(..., "--database", help="Database name."),
    port: int = typer.Option(50000, "--port", help="Port number."),
    schema: Optional[str] = typer.Option(None, "--schema", help="Schema name."),
    server_name: str = _SERVER_NAME_OPT,
    environment: Optional[str] = _ENV_OPT,
    description: Optional[str] = _DESC_OPT,
    server_id: Optional[str] = _ID_OPT,
    schema_name: str = _SCHEMA_NAME_OPT,
    output: Optional[Path] = _OUTPUT_OPT,
    overwrite: bool = _OVERWRITE_OPT,
) -> None:
    """Bind a data contract to IBM Db2 (server block only — no type converter)."""
    server = _build_server(
        "db2", server_name, schema=schema,
        host=host, port=port, database=database,
        environment=environment, description=description, id=server_id,
    )
    apply_target(server=server, contract_path=location, schema_name=schema_name, output=output, overwrite=overwrite)


@target_app.command("denodo")
def denodo(
    location: Path = _LOCATION_ARG,
    host: str = typer.Option(..., "--host", help="Hostname."),
    port: int = typer.Option(..., "--port", help="Port number (required)."),
    database: Optional[str] = typer.Option(None, "--database", help="Database name."),
    server_name: str = _SERVER_NAME_OPT,
    environment: Optional[str] = _ENV_OPT,
    description: Optional[str] = _DESC_OPT,
    server_id: Optional[str] = _ID_OPT,
    schema_name: str = _SCHEMA_NAME_OPT,
    output: Optional[Path] = _OUTPUT_OPT,
    overwrite: bool = _OVERWRITE_OPT,
) -> None:
    """Bind a data contract to Denodo (server block only — no type converter)."""
    server = _build_server(
        "denodo", server_name,
        host=host, port=port, database=database,
        environment=environment, description=description, id=server_id,
    )
    apply_target(server=server, contract_path=location, schema_name=schema_name, output=output, overwrite=overwrite)


@target_app.command("dremio")
def dremio(
    location: Path = _LOCATION_ARG,
    host: str = typer.Option(..., "--host", help="Hostname."),
    database: str = typer.Option(..., "--database", help="Database / space name."),
    port: int = typer.Option(31010, "--port", help="Port number."),
    schema: Optional[str] = typer.Option(None, "--schema", help="Schema name."),
    server_name: str = _SERVER_NAME_OPT,
    environment: Optional[str] = _ENV_OPT,
    description: Optional[str] = _DESC_OPT,
    server_id: Optional[str] = _ID_OPT,
    schema_name: str = _SCHEMA_NAME_OPT,
    output: Optional[Path] = _OUTPUT_OPT,
    overwrite: bool = _OVERWRITE_OPT,
) -> None:
    """Bind a data contract to Dremio (server block only — no type converter)."""
    server = _build_server(
        "dremio", server_name, schema=schema,
        host=host, port=port, database=database,
        environment=environment, description=description, id=server_id,
    )
    apply_target(server=server, contract_path=location, schema_name=schema_name, output=output, overwrite=overwrite)


@target_app.command("hive")
def hive(
    location: Path = _LOCATION_ARG,
    host: str = typer.Option(..., "--host", help="Hostname."),
    database: str = typer.Option(..., "--database", help="Database name."),
    port: int = typer.Option(10000, "--port", help="Port number."),
    schema: Optional[str] = typer.Option(None, "--schema", help="Schema name."),
    server_name: str = _SERVER_NAME_OPT,
    environment: Optional[str] = _ENV_OPT,
    description: Optional[str] = _DESC_OPT,
    server_id: Optional[str] = _ID_OPT,
    schema_name: str = _SCHEMA_NAME_OPT,
    output: Optional[Path] = _OUTPUT_OPT,
    overwrite: bool = _OVERWRITE_OPT,
) -> None:
    """Bind a data contract to Apache Hive (server block only — no type converter)."""
    server = _build_server(
        "hive", server_name, schema=schema,
        host=host, port=port, database=database,
        environment=environment, description=description, id=server_id,
    )
    apply_target(server=server, contract_path=location, schema_name=schema_name, output=output, overwrite=overwrite)


@target_app.command("impala")
def impala(
    location: Path = _LOCATION_ARG,
    host: str = typer.Option(..., "--host", help="Hostname."),
    database: str = typer.Option(..., "--database", help="Database name."),
    port: int = typer.Option(21050, "--port", help="Port number."),
    schema: Optional[str] = typer.Option(None, "--schema", help="Schema name."),
    server_name: str = _SERVER_NAME_OPT,
    environment: Optional[str] = _ENV_OPT,
    description: Optional[str] = _DESC_OPT,
    server_id: Optional[str] = _ID_OPT,
    schema_name: str = _SCHEMA_NAME_OPT,
    output: Optional[Path] = _OUTPUT_OPT,
    overwrite: bool = _OVERWRITE_OPT,
) -> None:
    """Bind a data contract to Apache Impala (server block only — no type converter)."""
    server = _build_server(
        "impala", server_name, schema=schema,
        host=host, port=port, database=database,
        environment=environment, description=description, id=server_id,
    )
    apply_target(server=server, contract_path=location, schema_name=schema_name, output=output, overwrite=overwrite)


@target_app.command("informix")
def informix(
    location: Path = _LOCATION_ARG,
    host: str = typer.Option(..., "--host", help="Hostname."),
    database: str = typer.Option(..., "--database", help="Database name."),
    port: int = typer.Option(9088, "--port", help="Port number."),
    schema: Optional[str] = typer.Option(None, "--schema", help="Schema name."),
    server_name: str = _SERVER_NAME_OPT,
    environment: Optional[str] = _ENV_OPT,
    description: Optional[str] = _DESC_OPT,
    server_id: Optional[str] = _ID_OPT,
    schema_name: str = _SCHEMA_NAME_OPT,
    output: Optional[Path] = _OUTPUT_OPT,
    overwrite: bool = _OVERWRITE_OPT,
) -> None:
    """Bind a data contract to IBM Informix (server block only — no type converter)."""
    server = _build_server(
        "informix", server_name, schema=schema,
        host=host, port=port, database=database,
        environment=environment, description=description, id=server_id,
    )
    apply_target(server=server, contract_path=location, schema_name=schema_name, output=output, overwrite=overwrite)


@target_app.command("presto")
def presto(
    location: Path = _LOCATION_ARG,
    host: str = typer.Option(..., "--host", help="Coordinator hostname."),
    catalog: Optional[str] = typer.Option(None, "--catalog", help="Catalog name."),
    schema: Optional[str] = typer.Option(None, "--schema", help="Schema name."),
    port: int = typer.Option(8080, "--port", help="Port number."),
    server_name: str = _SERVER_NAME_OPT,
    environment: Optional[str] = _ENV_OPT,
    description: Optional[str] = _DESC_OPT,
    server_id: Optional[str] = _ID_OPT,
    schema_name: str = _SCHEMA_NAME_OPT,
    output: Optional[Path] = _OUTPUT_OPT,
    overwrite: bool = _OVERWRITE_OPT,
) -> None:
    """Bind a data contract to Presto (server block only — no type converter)."""
    server = _build_server(
        "presto", server_name, schema=schema,
        host=host, port=port, catalog=catalog,
        environment=environment, description=description, id=server_id,
    )
    apply_target(server=server, contract_path=location, schema_name=schema_name, output=output, overwrite=overwrite)


@target_app.command("vertica")
def vertica(
    location: Path = _LOCATION_ARG,
    host: str = typer.Option(..., "--host", help="Hostname."),
    database: str = typer.Option(..., "--database", help="Database name."),
    port: int = typer.Option(5433, "--port", help="Port number."),
    schema: Optional[str] = typer.Option(None, "--schema", help="Schema name."),
    server_name: str = _SERVER_NAME_OPT,
    environment: Optional[str] = _ENV_OPT,
    description: Optional[str] = _DESC_OPT,
    server_id: Optional[str] = _ID_OPT,
    schema_name: str = _SCHEMA_NAME_OPT,
    output: Optional[Path] = _OUTPUT_OPT,
    overwrite: bool = _OVERWRITE_OPT,
) -> None:
    """Bind a data contract to Vertica (server block only — no type converter)."""
    server = _build_server(
        "vertica", server_name, schema=schema,
        host=host, port=port, database=database,
        environment=environment, description=description, id=server_id,
    )
    apply_target(server=server, contract_path=location, schema_name=schema_name, output=output, overwrite=overwrite)


@target_app.command("cloudsql")
def cloudsql(
    location: Path = _LOCATION_ARG,
    host: str = typer.Option(..., "--host", help="Hostname or Cloud SQL connection name."),
    database: str = typer.Option(..., "--database", help="Database name."),
    port: Optional[int] = typer.Option(None, "--port", help="Port number."),
    schema: Optional[str] = typer.Option(None, "--schema", help="Schema name."),
    server_name: str = _SERVER_NAME_OPT,
    environment: Optional[str] = _ENV_OPT,
    description: Optional[str] = _DESC_OPT,
    server_id: Optional[str] = _ID_OPT,
    schema_name: str = _SCHEMA_NAME_OPT,
    output: Optional[Path] = _OUTPUT_OPT,
    overwrite: bool = _OVERWRITE_OPT,
) -> None:
    """Bind a data contract to Google Cloud SQL (server block only — no type converter)."""
    server = _build_server(
        "cloudsql", server_name, schema=schema,
        host=host, port=port, database=database,
        environment=environment, description=description, id=server_id,
    )
    apply_target(server=server, contract_path=location, schema_name=schema_name, output=output, overwrite=overwrite)


@target_app.command("zen")
def zen(
    location: Path = _LOCATION_ARG,
    host: str = typer.Option(..., "--host", help="Hostname."),
    database: str = typer.Option(..., "--database", help="Database name."),
    port: Optional[int] = typer.Option(None, "--port", help="Port number."),
    server_name: str = _SERVER_NAME_OPT,
    environment: Optional[str] = _ENV_OPT,
    description: Optional[str] = _DESC_OPT,
    server_id: Optional[str] = _ID_OPT,
    schema_name: str = _SCHEMA_NAME_OPT,
    output: Optional[Path] = _OUTPUT_OPT,
    overwrite: bool = _OVERWRITE_OPT,
) -> None:
    """Bind a data contract to Actian Zen (server block only — no type converter)."""
    server = _build_server(
        "zen", server_name,
        host=host, port=port, database=database,
        environment=environment, description=description, id=server_id,
    )
    apply_target(server=server, contract_path=location, schema_name=schema_name, output=output, overwrite=overwrite)


# === Other =================================================================


@target_app.command("api")
def api(
    location: Path = _LOCATION_ARG,
    location_uri: str = typer.Option(..., "--location", help="API endpoint URI."),
    server_name: str = _SERVER_NAME_OPT,
    environment: Optional[str] = _ENV_OPT,
    description: Optional[str] = _DESC_OPT,
    server_id: Optional[str] = _ID_OPT,
    schema_name: str = _SCHEMA_NAME_OPT,
    output: Optional[Path] = _OUTPUT_OPT,
    overwrite: bool = _OVERWRITE_OPT,
) -> None:
    """Bind a data contract to an HTTP API endpoint (server block only)."""
    server = _build_server(
        "api", server_name,
        location=location_uri,
        environment=environment, description=description, id=server_id,
    )
    apply_target(server=server, contract_path=location, schema_name=schema_name, output=output, overwrite=overwrite)
