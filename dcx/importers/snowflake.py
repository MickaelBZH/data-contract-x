"""`dcx import snowflake` — build an ODCS contract from a live Snowflake schema.

A *live* importer (named after the system, per the project convention): it
connects to Snowflake with `snowflake-connector-python`, reads
INFORMATION_SCHEMA + primary keys, and produces an `OpenDataContractStandard`
with one schema object per table.

**Auth mirrors `dcx apply snowflake`** (see [[design-dcx-apply-auth]]): secrets via
env vars only (no `--password` flag); non-secret context via CLI flags / env;
`--connection-name` reads `~/.snowflake/config.toml`. Reuses `apply._ENV_VARS`.

**Not exposed over the REST API** for the same reason `apply` isn't: the API
server would import using its own single set of credentials for every caller — a
multi-tenant data-access concern. Live importers are CLI-only for v1.
"""

import json
import logging
import os
import sys
from typing import Any, Optional

from datacontract.imports.importer import Importer
from open_data_contract_standard.model import (
    CustomProperty,
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
    Server,
)

from dcx.apply.snowflake import (
    _ENV_VARS,
    _first,
    quiet_aws_credential_noise,
    SNOWFLAKE_LOGIN_TIMEOUT,
    SNOWFLAKE_NETWORK_TIMEOUT,
)
from dcx.exporters.snowflake import _view_select_body

logger = logging.getLogger(__name__)


class SnowflakeImportError(Exception):
    """A live-import failure with a user-actionable message."""


def _warn(msg: str) -> None:
    print(f"Warning: {msg}", file=sys.stderr)


# Snowflake INFORMATION_SCHEMA DATA_TYPE → (ODCS logicalType, format). NUMBER is
# handled separately (scale 0 → integer). Anything unknown falls back to string.
_SF_TYPE_MAP: dict[str, tuple[str, Optional[str]]] = {
    "TEXT": ("string", None), "STRING": ("string", None), "VARCHAR": ("string", None),
    "CHAR": ("string", None), "CHARACTER": ("string", None),
    "FLOAT": ("number", None), "FLOAT4": ("number", None), "FLOAT8": ("number", None),
    "DOUBLE": ("number", None), "REAL": ("number", None),
    "BOOLEAN": ("boolean", None),
    "DATE": ("date", None), "TIME": ("time", None),
    "TIMESTAMP_NTZ": ("timestamp", None), "TIMESTAMP_LTZ": ("timestamp", None),
    "TIMESTAMP_TZ": ("timestamp", None), "DATETIME": ("timestamp", None),
    "BINARY": ("string", "binary"), "VARBINARY": ("string", "binary"),
    "VARIANT": ("object", None), "OBJECT": ("object", None), "ARRAY": ("array", None),
    "GEOGRAPHY": ("object", None), "GEOMETRY": ("object", None),
}


def _map_type(data_type: Optional[str], scale: Optional[int]) -> tuple[str, Optional[str]]:
    dt = (data_type or "").upper()
    if dt == "NUMBER":
        return ("integer" if (scale or 0) == 0 else "number", None)
    return _SF_TYPE_MAP.get(dt, ("string", None))


# Snowflake INFORMATION_SCHEMA.TABLES.TABLE_TYPE → ODCS `physicalType`. Preserves the
# real asset type; `view` is governed as a view by export/apply, others as tables.
_TABLE_TYPE_TO_PHYSICAL: dict[str, str] = {
    "BASE TABLE": "table",
    "TABLE": "table",
    "LOCAL TEMPORARY": "table",
    "TEMPORARY TABLE": "table",
    "VIEW": "view",
    "MATERIALIZED VIEW": "materialized view",
    "EXTERNAL TABLE": "external table",
}


def _physical_object_type(table_type: Optional[str]) -> str:
    """Map a Snowflake TABLE_TYPE to an ODCS `physicalType` (defaults to `table`)."""
    if not table_type:
        return "table"
    tt = table_type.strip().upper()
    return _TABLE_TYPE_TO_PHYSICAL.get(tt, tt.lower())


def _vector_type_from_show_columns(payload: Any) -> Optional[str]:
    """Reconstruct `VECTOR(<element>, <dim>)` from a `SHOW COLUMNS` data_type payload.

    `INFORMATION_SCHEMA.COLUMNS.DATA_TYPE` reports a bare `VECTOR` for vector columns —
    the element type and dimension appear nowhere in that view — and a bare `VECTOR` is
    not valid DDL. A contract imported from such a table therefore produced a
    `CREATE TABLE` Snowflake refuses to parse. `SHOW COLUMNS` carries the complete type
    as a JSON payload, so we recover it from there.

    Returns None for anything that is not a confidently-reconstructable VECTOR, leaving
    the INFORMATION_SCHEMA-derived type untouched — this only ever adds precision.
    """
    try:
        info = json.loads(payload)
    except (TypeError, ValueError):
        return None
    if not isinstance(info, dict) or str(info.get("type", "")).upper() != "VECTOR":
        return None
    element = info.get("elementType") or info.get("element_type")
    dimension = info.get("dimension")
    if not element or not isinstance(dimension, int) or isinstance(dimension, bool):
        return None
    return f"VECTOR({str(element).upper()}, {dimension})"


def _physical_type(data_type: Optional[str], char_len, prec, scale, full_type=None) -> str:
    """Reconstruct a canonical Snowflake type string for `physicalType`.

    `full_type` (from `SHOW COLUMNS`) wins when present: it is the only source for types
    INFORMATION_SCHEMA cannot express, such as `VECTOR(FLOAT, 256)`.
    """
    if full_type:
        return full_type
    dt = (data_type or "").upper()
    if dt in ("TEXT", "STRING", "VARCHAR", "CHAR", "CHARACTER"):
        return f"VARCHAR({char_len})" if char_len else "VARCHAR"
    if dt == "NUMBER":
        return f"NUMBER({prec},{scale or 0})" if prec is not None else "NUMBER"
    if dt in ("BINARY", "VARBINARY"):
        return f"BINARY({char_len})" if char_len else "BINARY"
    return dt


# ---------------------------------------------------------------------------
# Pure contract builder (no IO — easy to unit test)
# ---------------------------------------------------------------------------


def build_snowflake_contract(
    *,
    server_info: dict,
    columns: list[dict],
    primary_keys: dict[str, set],
    table_comments: dict[str, Optional[str]],
    column_tags: Optional[dict] = None,
    table_tags: Optional[dict] = None,
    table_types: Optional[dict] = None,
    view_definitions: Optional[dict] = None,
    full_types: Optional[dict] = None,
    server_name: str = "production",
) -> OpenDataContractStandard:
    """Build an ODCS contract from already-fetched Snowflake metadata.

    `columns`: flat list of dicts with keys table, name, data_type, nullable
    (bool), comment, char_len, precision, scale (in INFORMATION_SCHEMA order).
    `primary_keys`: table name → set of PK column names.
    `table_comments`: table name → comment.
    `column_tags`: (table, column) → list of `DB.SCHEMA.NAME=VALUE` tag strings.
    `table_tags`: table → list of `DB.SCHEMA.NAME=VALUE` tag strings.
    `table_types`: table → Snowflake TABLE_TYPE (e.g. `VIEW`) → sets `physicalType`.
    `view_definitions`: view → SELECT body → stored as a `viewDefinition` customProperty.
    `full_types`: (table, column) → complete Snowflake type, for types INFORMATION_SCHEMA
    cannot express (e.g. `VECTOR(FLOAT, 256)`); overrides the reconstructed type.
    `server_info`: account, database, schema, warehouse.
    """
    column_tags = column_tags or {}
    table_tags = table_tags or {}
    table_types = table_types or {}
    view_definitions = view_definitions or {}
    full_types = full_types or {}
    # Group columns by table, preserving first-seen order.
    tables: dict[str, list[dict]] = {}
    for col in columns:
        tables.setdefault(col["table"], []).append(col)

    schema_objects: list[SchemaObject] = []
    for table_name, cols in tables.items():
        pk_cols = primary_keys.get(table_name, set())
        props: list[SchemaProperty] = []
        for col in cols:
            logical, fmt = _map_type(col.get("data_type"), col.get("scale"))
            prop = SchemaProperty(
                name=col["name"],
                physicalType=_physical_type(
                    col.get("data_type"), col.get("char_len"),
                    col.get("precision"), col.get("scale"),
                    full_types.get((table_name, col["name"])),
                ),
                logicalType=logical,
            )
            if col.get("comment"):
                prop.description = col["comment"]
            if not col.get("nullable", True):
                prop.required = True

            opts: dict[str, Any] = {}
            if fmt:
                opts["format"] = fmt
            elif logical == "string" and col.get("char_len"):
                opts["maxLength"] = col["char_len"]
            if opts:
                prop.logicalTypeOptions = opts

            if col["name"] in pk_cols:
                prop.primaryKey = True
                prop.required = True
                if len(pk_cols) == 1:  # single-column PK ⇒ values are unique
                    prop.unique = True

            ctags = column_tags.get((table_name, col["name"]))
            if ctags:
                prop.tags = ctags

            props.append(prop)

        obj = SchemaObject(
            name=table_name,
            physicalType=_physical_object_type(table_types.get(table_name)),
            properties=props,
        )
        if table_comments.get(table_name):
            obj.description = table_comments[table_name]
        ttags = table_tags.get(table_name)
        if ttags:
            obj.tags = ttags
        vdef = _view_select_body(view_definitions.get(table_name))
        if vdef:
            obj.customProperties = (obj.customProperties or []) + [
                CustomProperty(property="viewDefinition", value=vdef)
            ]
        schema_objects.append(obj)

    database = server_info.get("database")
    schema = server_info.get("schema")

    server = Server(
        server=server_name,
        type="snowflake",
        account=server_info.get("account"),
        database=database,
        warehouse=server_info.get("warehouse"),
    )
    if schema is not None:
        server.schema_ = schema  # aliased field — set post-construction (see gotcha memory)

    contract = OpenDataContractStandard(
        apiVersion="v3.1.0",
        kind="DataContract",
        id=f"{database}.{schema}".lower() if database and schema else "snowflake-import",
        name=schema or "Snowflake import",
        version="1.0.0",
        status="draft",
    )
    contract.servers = [server]
    contract.schema_ = schema_objects
    return contract


# ---------------------------------------------------------------------------
# Live connection + metadata fetch
# ---------------------------------------------------------------------------


def _resolve_conn_params(import_args: dict) -> dict:
    """Connection kwargs from CLI args + env (CLI wins). Secrets env-only."""
    params: dict[str, Any] = {
        "account": _first(import_args.get("account"), os.environ.get(_ENV_VARS["account"])),
        "user": _first(import_args.get("user"), os.environ.get(_ENV_VARS["user"])),
        "role": _first(import_args.get("role"), os.environ.get(_ENV_VARS["role"])),
        "warehouse": _first(import_args.get("warehouse"), os.environ.get(_ENV_VARS["warehouse"])),
        "database": _first(import_args.get("database"), os.environ.get(_ENV_VARS["database"])),
        "schema": _first(import_args.get("schema"), os.environ.get(_ENV_VARS["schema"])),
        "authenticator": _first(import_args.get("authenticator"),
                                os.environ.get(_ENV_VARS["authenticator"])),
    }
    for kwarg in ("password", "private_key_file", "private_key_file_pwd", "token"):
        v = os.environ.get(_ENV_VARS[kwarg])
        if v:
            params[kwarg] = v

    if not params["account"]:
        raise SnowflakeImportError(
            "Cannot determine Snowflake account: pass --account or set SNOWFLAKE_ACCOUNT."
        )
    if not params["user"]:
        raise SnowflakeImportError(
            "Cannot determine Snowflake user: pass --user or set SNOWFLAKE_USER."
        )
    return {k: v for k, v in params.items() if v is not None}


def _connect(import_args: dict):
    try:
        import snowflake.connector
    except ImportError:
        raise SnowflakeImportError(
            "snowflake-connector-python is not installed. "
            "Install it via `pip install snowflake-connector-python`."
        )

    connection_name = import_args.get("connection_name")
    if connection_name:
        conn_kwargs: dict[str, Any] = {"connection_name": connection_name}
        for k in ("account", "user", "role", "warehouse", "database", "schema", "authenticator"):
            if import_args.get(k):
                conn_kwargs[k] = import_args[k]
    else:
        conn_kwargs = _resolve_conn_params(import_args)

    conn_kwargs.setdefault("login_timeout", SNOWFLAKE_LOGIN_TIMEOUT)
    conn_kwargs.setdefault("network_timeout", SNOWFLAKE_NETWORK_TIMEOUT)
    quiet_aws_credential_noise()
    try:
        return snowflake.connector.connect(**conn_kwargs)
    except Exception as exc:
        raise SnowflakeImportError(f"Snowflake connection failed: {exc}")


def _fetch_metadata(conn, database: str, schema: str, tables: Optional[list[str]]):
    """Read columns, primary keys, table comments, types and view definitions."""
    db = database.upper()
    sch = schema.upper()
    table_filter = [t.upper() for t in tables] if tables else None

    cur = conn.cursor()
    try:
        # --- columns ---
        col_sql = (
            f'SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COMMENT, '
            f'CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE '
            f'FROM "{db}".INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = %s '
            f'ORDER BY TABLE_NAME, ORDINAL_POSITION'
        )
        cur.execute(col_sql, (sch,))
        columns: list[dict] = []
        for row in cur.fetchall():
            (tname, cname, dtype, nullable, comment, char_len, prec, scale) = row
            if table_filter and tname.upper() not in table_filter:
                continue
            columns.append({
                "table": tname, "name": cname, "data_type": dtype,
                "nullable": str(nullable).upper() != "NO",
                "comment": comment, "char_len": char_len,
                "precision": prec, "scale": scale,
            })

        # --- table comments + types (covers tables and views) ---
        cur.execute(
            f'SELECT TABLE_NAME, COMMENT, TABLE_TYPE FROM "{db}".INFORMATION_SCHEMA.TABLES '
            f'WHERE TABLE_SCHEMA = %s',
            (sch,),
        )
        table_comments: dict = {}
        table_types: dict = {}
        for row in cur.fetchall():
            table_comments[row[0]] = row[1]
            table_types[row[0]] = row[2]

        # --- view definitions (the SELECT body, so views can be (re)created) ---
        cur.execute(
            f'SELECT TABLE_NAME, VIEW_DEFINITION FROM "{db}".INFORMATION_SCHEMA.VIEWS '
            f'WHERE TABLE_SCHEMA = %s',
            (sch,),
        )
        view_definitions = {row[0]: row[1] for row in cur.fetchall() if row[1]}

        # --- primary keys ---
        primary_keys: dict[str, set] = {}
        cur.execute(f'SHOW PRIMARY KEYS IN SCHEMA "{db}"."{sch}"')
        idx = {c[0].lower(): i for i, c in enumerate(cur.description)}
        for row in cur.fetchall():
            tname = row[idx["table_name"]]
            cname = row[idx["column_name"]]
            primary_keys.setdefault(tname, set()).add(cname)

        # --- full types INFORMATION_SCHEMA can't express (VECTOR's element/dimension) ---
        # Best-effort: SHOW COLUMNS needs its own privileges, and the contract is still
        # correct without it for every type INFORMATION_SCHEMA does describe.
        full_types: dict[tuple[str, str], str] = {}
        try:
            cur.execute(f'SHOW COLUMNS IN SCHEMA "{db}"."{sch}"')
            idx = {c[0].lower(): i for i, c in enumerate(cur.description)}
            for row in cur.fetchall():
                rendered = _vector_type_from_show_columns(row[idx["data_type"]])
                if rendered:
                    full_types[(row[idx["table_name"]], row[idx["column_name"]])] = rendered
        except Exception:
            logger.debug("SHOW COLUMNS unavailable; parameterised types may be incomplete",
                         exc_info=True)
    finally:
        cur.close()

    return columns, primary_keys, table_comments, table_types, view_definitions, full_types


def _fq_tag(row: tuple, idx: dict) -> str:
    """Build a fully-qualified `DB.SCHEMA.TAG_NAME=VALUE` tag string.

    Keeping the tag's namespace (database + schema) is required so `apply` /
    `export snowflake-full` can emit `SET TAG DB.SCHEMA.NAME = '...'` against the
    exact tag object — a bare name would resolve against the session's *current*
    schema and target the wrong tag (or none). Degrades to fewer qualifiers if the
    namespace columns are absent/empty.
    """
    parts = [
        str(row[idx[key]])
        for key in ("tag_database", "tag_schema")
        if key in idx and row[idx[key]]
    ]
    parts.append(str(row[idx["tag_name"]]))
    return f"{'.'.join(parts)}={row[idx['tag_value']]}"


def _fetch_tags(conn, database: str, schema: str, table_names: list[str]):
    """Read column- and table-level tags via INFORMATION_SCHEMA table functions.

    Returns (column_tags, table_tags) keyed by (table, column) / table, each a list
    of fully-qualified `DB.SCHEMA.NAME=VALUE` strings (the dcx tag convention).
    Object tagging is an Enterprise feature and tag visibility is role-dependent; on
    any query failure we warn once and return whatever we have (graceful degradation).
    """
    db = database.upper()
    sch = schema.upper()
    column_tags: dict[tuple, list] = {}
    table_tags: dict[str, list] = {}
    errors: list[str] = []

    cur = conn.cursor()
    try:
        for table in table_names:
            fq = f"{db}.{sch}.{table.upper()}"

            # Column-level tags (LEVEL=COLUMN filters out tags inherited from
            # the schema/database).
            try:
                cur.execute(
                    f'SELECT * FROM TABLE("{db}".INFORMATION_SCHEMA.'
                    f"TAG_REFERENCES_ALL_COLUMNS('{fq}', 'table'))"
                )
                idx = {c[0].lower(): i for i, c in enumerate(cur.description)}
                for row in cur.fetchall():
                    if "level" in idx and row[idx["level"]] and str(row[idx["level"]]).upper() != "COLUMN":
                        continue
                    col = row[idx["column_name"]]
                    column_tags.setdefault((table, col), []).append(_fq_tag(row, idx))
            except Exception as exc:  # noqa: BLE001 — graceful degradation
                errors.append(str(exc))

            # Table-level tags directly assigned to the table.
            try:
                cur.execute(
                    f'SELECT * FROM TABLE("{db}".INFORMATION_SCHEMA.'
                    f"TAG_REFERENCES('{fq}', 'TABLE'))"
                )
                idx = {c[0].lower(): i for i, c in enumerate(cur.description)}
                for row in cur.fetchall():
                    if "level" in idx and row[idx["level"]] and str(row[idx["level"]]).upper() != "TABLE":
                        continue
                    table_tags.setdefault(table, []).append(_fq_tag(row, idx))
            except Exception as exc:  # noqa: BLE001
                errors.append(str(exc))
    finally:
        cur.close()

    if errors and not column_tags and not table_tags:
        _warn(
            "Could not read Snowflake tags (none visible to this role, or object "
            f"tagging not in use): {errors[0]}"
        )
    return column_tags, table_tags


def _contract_from_connection(
    conn,
    *,
    database: str,
    schema: str,
    tables: Optional[list[str]],
    fetch_tags: bool,
    server_info: dict,
    server_name: str,
) -> OpenDataContractStandard:
    """Read metadata over an open connection and build the contract (caller closes conn)."""
    columns, primary_keys, table_comments, table_types, view_definitions, full_types = _fetch_metadata(
        conn, database, schema, tables,
    )
    if not columns:
        raise SnowflakeImportError(
            f"No columns found in {database}.{schema}"
            + (f" for tables {tables}." if tables else ".")
        )

    column_tags: dict = {}
    table_tags: dict = {}
    if fetch_tags:
        table_names = list(dict.fromkeys(c["table"] for c in columns))
        column_tags, table_tags = _fetch_tags(conn, database, schema, table_names)

    return build_snowflake_contract(
        server_info=server_info,
        columns=columns,
        primary_keys=primary_keys,
        table_comments=table_comments,
        column_tags=column_tags,
        table_tags=table_tags,
        table_types=table_types,
        view_definitions=view_definitions,
        full_types=full_types,
        server_name=server_name,
    )


def import_snowflake(import_args: dict) -> OpenDataContractStandard:
    """Connect to Snowflake (CLI path: CLI flags + env) and build an ODCS contract."""
    database = _first(import_args.get("database"), os.environ.get(_ENV_VARS["database"]))
    schema = _first(import_args.get("schema"), os.environ.get(_ENV_VARS["schema"]))
    if not database or not schema:
        raise SnowflakeImportError(
            "Both --database and --schema are required to import from Snowflake."
        )

    conn = _connect(import_args)
    try:
        return _contract_from_connection(
            conn,
            database=database,
            schema=schema,
            tables=import_args.get("tables"),
            fetch_tags=import_args.get("tags", True),
            server_info={
                "account": _first(import_args.get("account"), os.environ.get(_ENV_VARS["account"])),
                "database": database,
                "schema": schema,
                "warehouse": _first(import_args.get("warehouse"), os.environ.get(_ENV_VARS["warehouse"])),
            },
            server_name=import_args.get("server_name") or "production",
        )
    finally:
        conn.close()


def import_snowflake_oauth(
    *,
    token: str,
    account: str,
    database: str,
    schema: str,
    tables: Optional[list[str]] = None,
    role: Optional[str] = None,
    warehouse: Optional[str] = None,
    tags: bool = True,
    server_name: str = "production",
) -> OpenDataContractStandard:
    """Import using a caller-supplied Snowflake **OAuth token** — no env, no ambient creds.

    This is the API path: each caller brings their own bearer token, so the
    server acts on behalf of the caller rather than with shared credentials.
    """
    if not token:
        raise SnowflakeImportError("An OAuth token is required.")
    if not (account and database and schema):
        raise SnowflakeImportError("account, database and schema are required.")

    try:
        import snowflake.connector
    except ImportError:
        raise SnowflakeImportError(
            "snowflake-connector-python is not installed. "
            "Install it via `pip install snowflake-connector-python`."
        )

    conn_kwargs: dict[str, Any] = {
        "account": account,
        "authenticator": "oauth",
        "token": token,
        "database": database,
        "schema": schema,
    }
    if role:
        conn_kwargs["role"] = role
    if warehouse:
        conn_kwargs["warehouse"] = warehouse

    conn_kwargs.setdefault("login_timeout", SNOWFLAKE_LOGIN_TIMEOUT)
    conn_kwargs.setdefault("network_timeout", SNOWFLAKE_NETWORK_TIMEOUT)
    quiet_aws_credential_noise()
    try:
        conn = snowflake.connector.connect(**conn_kwargs)
    except Exception as exc:
        raise SnowflakeImportError(f"Snowflake connection failed: {exc}")

    try:
        return _contract_from_connection(
            conn,
            database=database,
            schema=schema,
            tables=tables,
            fetch_tags=tags,
            server_info={"account": account, "database": database, "schema": schema, "warehouse": warehouse},
            server_name=server_name,
        )
    finally:
        conn.close()


class SnowflakeImporter(Importer):
    """Registered into the upstream importer_factory as `snowflake`."""

    def import_source(self, source: str, import_args: dict) -> OpenDataContractStandard:
        return import_snowflake(import_args)
