"""`dcx apply <platform>` — execute generated SQL against a live target system.

Phase 7 first cut: Snowflake only. Reuses `to_snowflake_full_sql` from Phase 5
to build the script, then runs it via `snowflake-connector-python`.

**Authentication strategy** (see also: design-dcx-api memory and project notes):

- **No `--password` CLI flag.** Secrets must come from environment variables
  (`SNOWFLAKE_PASSWORD`, `SNOWFLAKE_PRIVATE_KEY_PATH`, `SNOWFLAKE_TOKEN`, ...) or
  Snowflake's `~/.snowflake/config.toml` connection profile.
- Precedence for connection parameters: **CLI flag > env var > contract server
  block**. CLI flags exist for non-secret context (`--user`, `--role`,
  `--warehouse`, `--account`, `--authenticator`); env vars carry the secrets.
- Supports all auth methods the official connector supports (password,
  key-pair, externalbrowser SSO, OAuth) — we just pass the values through.
"""

import logging
import os
import re
from typing import Any, List, Optional

import typer
from open_data_contract_standard.model import OpenDataContractStandard, Server
from typing_extensions import Annotated

# DdlMode lives with the SQL generator (it maps to to_snowflake_full_sql flags) and
# is re-exported here so `dcx.apply.snowflake.DdlMode` keeps working.
from dcx.exporters.snowflake import DdlMode, to_snowflake_full_sql


def quiet_aws_credential_noise() -> None:
    """Drop botocore's credential-refresh logger to ERROR before a Snowflake connect.

    `snowflake-connector-python` hard-depends on boto3 and probes the local AWS
    credential chain on connect (its WIF util builds a `boto3.session.Session()`),
    even for Azure/GCP accounts that never touch AWS. An expired/absent AWS SSO
    profile then logs a scary but harmless WARNING + traceback. Reaching Snowflake
    never legitimately needs AWS, so we silence that one logger at every connect
    site — CLI *and* API, import *and* apply. Real connection failures still raise
    and are surfaced as ApplyError/SnowflakeImportError, so nothing is hidden.
    """
    logging.getLogger("botocore.credentials").setLevel(logging.ERROR)


class ApplyError(Exception):
    """An apply-time failure with a user-actionable message."""


# Snowflake connector kwarg → env var name. The connector itself doesn't
# auto-read these, so we resolve them here.
_ENV_VARS: dict[str, str] = {
    "user":                  "SNOWFLAKE_USER",
    "password":              "SNOWFLAKE_PASSWORD",
    "account":               "SNOWFLAKE_ACCOUNT",
    "role":                  "SNOWFLAKE_ROLE",
    "warehouse":             "SNOWFLAKE_WAREHOUSE",
    "database":              "SNOWFLAKE_DATABASE",
    "schema":                "SNOWFLAKE_SCHEMA",
    "authenticator":         "SNOWFLAKE_AUTHENTICATOR",
    "private_key_file":      "SNOWFLAKE_PRIVATE_KEY_PATH",
    "private_key_file_pwd":  "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE",
    "token":                 "SNOWFLAKE_TOKEN",
}

# Connection timeouts (seconds) shared by `apply` and the live importers, so a
# dead/slow Snowflake endpoint can't hang a request forever (and, via the API,
# exhaust the threadpool). `login` bounds the auth handshake; `network` bounds
# each query/DDL wait. Applied with setdefault, so callers may still override.
SNOWFLAKE_LOGIN_TIMEOUT = 30
SNOWFLAKE_NETWORK_TIMEOUT = 120


def _first(*candidates: Optional[str]) -> Optional[str]:
    """Return the first non-empty value from candidates."""
    for c in candidates:
        if c is not None and c != "":
            return c
    return None


# Head of our generated `ALTER ... ADD DATA METRIC FUNCTION <assoc> EXPECTATION <exp>`.
# `rest` deliberately captures everything after the keyword: the association can be
# `ON ()`, `ON (col)`, or ACCEPTED_VALUES' lambda form
# `ON (col, col -> col IN ('A', 'B'))`, which no fixed paren pattern matches safely.
_ADD_DMF_HEAD_RE = re.compile(
    r"^(?P<head>ALTER\s+(?:TABLE|VIEW)\s+\S+)\s+"
    r"ADD\s+DATA\s+METRIC\s+FUNCTION\s+(?P<rest>.+)$",
    re.IGNORECASE | re.DOTALL,
)

# Separates the association from the trailing `EXPECTATION <name> (<expr>)` clause.
_EXPECTATION_SEP_RE = re.compile(r"\s+EXPECTATION\s+", re.IGNORECASE)


def _has_expectation(stmt: str) -> bool:
    """True if the statement carries an `EXPECTATION` clause."""
    return bool(_EXPECTATION_SEP_RE.search(stmt))


def _add_dmf_to_modify_expectation(stmt: str) -> Optional[str]:
    """Rewrite one of our `ADD DATA METRIC FUNCTION ... EXPECTATION ...` statements as
    `MODIFY DATA METRIC FUNCTION ... ADD EXPECTATION ...`, or None if it isn't one of
    ours or carries no expectation clause.

    Splitting on the LAST ` EXPECTATION ` is what makes the ACCEPTED_VALUES lambda form
    work: the association may embed arbitrary string literals, but the expectation
    expression we generate (`VALUE = 0`, `VALUE >= 10`, ...) never contains the keyword,
    so the final occurrence is always the real clause boundary.
    """
    m = _ADD_DMF_HEAD_RE.match(stmt.strip())
    if not m:
        return None
    rest = m.group("rest")
    separators = list(_EXPECTATION_SEP_RE.finditer(rest))
    if not separators:
        return None
    last = separators[-1]
    assoc = rest[: last.start()].strip()
    expectation = rest[last.end():].strip()
    if not assoc or not expectation:
        return None
    return (
        f"{m.group('head')} MODIFY DATA METRIC FUNCTION "
        f"{assoc} ADD EXPECTATION {expectation}"
    )


# Snowflake signals these two idempotent "already there" conditions only in the message
# text — the connector exposes no distinct errno for either — so they are matched on
# wording. If a future Snowflake release rewords them, re-applying an unchanged contract
# starts failing loudly rather than silently skipping work, which is the safe direction.
_DMF_ALREADY_ATTACHED = "already has the data metric function"
_EXPECTATION_ALREADY_EXISTS = "already has an expectation"


def _split_sql_statements(sql: str) -> list[str]:
    """Split a generated script into individual statements, quote-aware.

    Uses the connector's own parser — the one `execute_string` calls internally — so a
    semicolon inside a string literal never splits a statement in half. That is not
    hypothetical: a column comment (`'Lifecycle state; one of NEW, DONE'`) or a value in
    an ACCEPTED_VALUES list will contain one. `remove_comments` drops the `-- TODO` lines
    the exporter emits for unmappable rules, so they never reach Snowflake.

    We iterate and execute the statements ourselves instead of calling `execute_string`
    so each one can carry its own error handling (see `_execute_statement`).
    """
    from io import StringIO

    from snowflake.connector.util_text import split_statements

    statements: list[str] = []
    for stmt, _is_put_or_get in split_statements(StringIO(sql), remove_comments=True):
        cleaned = (stmt or "").strip().rstrip(";").strip()
        if cleaned:
            statements.append(cleaned)
    return statements


def _execute_statement(conn, stmt: str) -> None:
    """Execute one statement, absorbing only the genuinely idempotent conflicts."""
    cur = conn.cursor()
    try:
        try:
            cur.execute(stmt)
        except Exception as exc:
            if _DMF_ALREADY_ATTACHED not in str(exc).lower():
                raise
            # Snowflake permits one instance of a given DMF per column, so re-applying
            # a contract fails here. Re-issue as `MODIFY ... ADD EXPECTATION` so a
            # changed threshold is added alongside the existing one (Snowsight style).
            modify = _add_dmf_to_modify_expectation(stmt)
            if modify is None:
                # A bare DMF with no expectation is simply already attached — nothing
                # more to apply. But if the statement DID carry an expectation we could
                # not rewrite, staying silent would drop a governance change on the
                # floor, so surface the original error instead.
                if _has_expectation(stmt):
                    raise
                return
            try:
                cur.execute(modify)
            except Exception as exc2:
                # The same expectation name is already present (re-applying an unchanged
                # rule). Snowflake has no `ADD EXPECTATION IF NOT EXISTS`, so a duplicate
                # is treated as the no-op it is.
                if _EXPECTATION_ALREADY_EXISTS not in str(exc2).lower():
                    raise
    finally:
        cur.close()



def _find_snowflake_server(
    contract: OpenDataContractStandard, server_name: Optional[str],
) -> Optional[Server]:
    for srv in contract.servers or []:
        if srv.type != "snowflake":
            continue
        if server_name and srv.server != server_name:
            continue
        return srv
    return None


def _resolve_connection_params(
    contract: OpenDataContractStandard,
    *,
    server_name: Optional[str] = None,
    user: Optional[str] = None,
    role: Optional[str] = None,
    warehouse: Optional[str] = None,
    account: Optional[str] = None,
    database: Optional[str] = None,
    schema: Optional[str] = None,
    authenticator: Optional[str] = None,
) -> dict[str, Any]:
    """Build the `snowflake.connector.connect()` kwargs from contract + env + CLI.

    Precedence (highest wins): CLI flag → env var → contract server block.
    Secrets (password / private key / token) are read from env vars only.
    """
    srv = _find_snowflake_server(contract, server_name)

    params: dict[str, Any] = {
        "account":       _first(account, os.environ.get(_ENV_VARS["account"]),
                                srv.account if srv else None),
        "user":          _first(user, os.environ.get(_ENV_VARS["user"])),
        "role":          _first(role, os.environ.get(_ENV_VARS["role"])),
        "warehouse":     _first(warehouse, os.environ.get(_ENV_VARS["warehouse"]),
                                srv.warehouse if srv else None),
        "database":      _first(database, os.environ.get(_ENV_VARS["database"]),
                                srv.database if srv else None),
        "schema":        _first(schema, os.environ.get(_ENV_VARS["schema"]),
                                srv.schema_ if srv else None),
        "authenticator": _first(authenticator, os.environ.get(_ENV_VARS["authenticator"])),
    }

    # Secrets — env var only, never CLI
    for kwarg in ("password", "private_key_file", "private_key_file_pwd", "token"):
        v = os.environ.get(_ENV_VARS[kwarg])
        if v:
            params[kwarg] = v

    if not params["account"]:
        raise ApplyError(
            "Cannot determine Snowflake account: not in contract server block, "
            "SNOWFLAKE_ACCOUNT env var, or --account flag."
        )
    if not params["user"]:
        raise ApplyError(
            "Cannot determine Snowflake user: set SNOWFLAKE_USER env var or pass --user."
        )

    # Drop None values; connector errors on them
    return {k: v for k, v in params.items() if v is not None}


def apply_snowflake(
    contract: OpenDataContractStandard,
    *,
    server_name: Optional[str] = None,
    user: Optional[str] = None,
    role: Optional[str] = None,
    warehouse: Optional[str] = None,
    account: Optional[str] = None,
    database: Optional[str] = None,
    schema: Optional[str] = None,
    authenticator: Optional[str] = None,
    connection_name: Optional[str] = None,
    dry_run: bool = False,
    # SQL-generation options (forwarded to to_snowflake_full_sql). Defaults suit the
    # "I don't know if the table exists" flow: `auto` creates missing tables and
    # governs existing ones; comments + tags on, data quality on demand.
    ddl_mode: DdlMode = DdlMode.auto,
    structured_types: bool = False,
    include_comments: bool = True,
    include_tags: bool = True,
    include_quality: bool = False,
    create_tags: bool = False,
    tag_namespace: Optional[str] = None,
    tag_namespace_filter: Optional[list[str]] = None,
    metric_schedule: str = "USING CRON 0 0 * * * UTC",
    strict: bool = False,
) -> dict[str, Any]:
    """Generate Snowflake SQL from the contract and execute it.

    Returns `{dry_run, sql, statements_executed, account, warnings}`. `warnings`
    holds any schema-drift notes from comparing the contract to live tables; with
    `strict=True` drift raises `ApplyError` and nothing is executed. Raises
    `ApplyError` on connection/configuration problems.
    """
    sql = to_snowflake_full_sql(
        contract,
        **ddl_mode.to_sql_kwargs(),
        structured_types=structured_types,
        include_comments=include_comments,
        include_tags=include_tags,
        include_quality=include_quality,
        create_tags=create_tags,
        tag_namespace=tag_namespace,
        tag_namespace_filter=tag_namespace_filter,
        metric_schedule=metric_schedule,
        server=server_name,
    )

    if dry_run:
        return {
            "dry_run": True,
            "sql": sql,
            "statements_executed": 0,
            "warnings": [],
        }

    # Build connector kwargs
    if connection_name:
        # Snowflake's `connection_name` reads ~/.snowflake/config.toml; ignore
        # other resolution and let the connector do the work. CLI overrides still
        # layer on top.
        conn_kwargs: dict[str, Any] = {"connection_name": connection_name}
        for k, v in {
            "user": user, "role": role, "warehouse": warehouse,
            "account": account, "database": database, "schema": schema,
            "authenticator": authenticator,
        }.items():
            if v is not None:
                conn_kwargs[k] = v
    else:
        conn_kwargs = _resolve_connection_params(
            contract,
            server_name=server_name, user=user, role=role,
            warehouse=warehouse, account=account, database=database,
            schema=schema, authenticator=authenticator,
        )

    statements_executed, warnings = _connect_apply(
        sql, conn_kwargs, contract=contract, strict=strict,
    )
    return {
        "dry_run": False,
        "sql": sql,
        "statements_executed": statements_executed,
        "warnings": warnings,
        "account": conn_kwargs.get("account"),
    }


def apply_snowflake_oauth(
    contract: OpenDataContractStandard,
    *,
    token: str,
    server_name: Optional[str] = None,
    account: Optional[str] = None,
    role: Optional[str] = None,
    warehouse: Optional[str] = None,
    dry_run: bool = False,
    # SQL-generation options. `auto` (default) creates missing tables and governs
    # existing ones in a single script (CREATE TABLE IF NOT EXISTS + comments/tags/DQ).
    ddl_mode: DdlMode = DdlMode.auto,
    structured_types: bool = False,
    include_comments: bool = True,
    include_tags: bool = True,
    include_quality: bool = True,
    create_tags: bool = False,
    tag_namespace: Optional[str] = None,
    tag_namespace_filter: Optional[list[str]] = None,
    metric_schedule: str = "USING CRON 0 0 * * * UTC",
    strict: bool = False,
) -> dict[str, Any]:
    """Apply a contract to Snowflake using a caller-supplied **OAuth token**.

    The API path: no env / ambient credentials. Connection context (account,
    warehouse, database, schema) is taken from the contract's Snowflake server
    block, with optional overrides. Defaults to `auto` DDL — creates the table if
    missing, otherwise governs the existing one. Returns schema-drift `warnings`;
    with `strict=True` drift raises instead of applying.
    """
    srv = _find_snowflake_server(contract, server_name)
    account = account or (srv.account if srv else None)
    if not account:
        raise ApplyError(
            "Cannot determine Snowflake account: not in the contract server block "
            "or the request."
        )
    warehouse = warehouse or (srv.warehouse if srv else None)

    sql = to_snowflake_full_sql(
        contract,
        **ddl_mode.to_sql_kwargs(),
        structured_types=structured_types,
        include_comments=include_comments,
        include_tags=include_tags,
        include_quality=include_quality,
        create_tags=create_tags,
        tag_namespace=tag_namespace,
        tag_namespace_filter=tag_namespace_filter,
        metric_schedule=metric_schedule,
        server=server_name,
    )

    if dry_run:  # preview the SQL without connecting — no token needed
        return {
            "dry_run": True, "sql": sql, "statements_executed": 0,
            "warnings": [], "account": account,
        }

    if not token:
        raise ApplyError("An OAuth token is required.")

    conn_kwargs: dict[str, Any] = {
        "account": account,
        "authenticator": "oauth",
        "token": token,
    }
    if role:
        conn_kwargs["role"] = role
    if warehouse:
        conn_kwargs["warehouse"] = warehouse
    if srv and srv.database:
        conn_kwargs["database"] = srv.database
    if srv and srv.schema_:
        conn_kwargs["schema"] = srv.schema_

    statements_executed, warnings = _connect_apply(
        sql, conn_kwargs, contract=contract, strict=strict,
    )
    return {
        "dry_run": False,
        "sql": sql,
        "statements_executed": statements_executed,
        "warnings": warnings,
        "account": account,
    }


# Coarse Snowflake type families for drift comparison — strip precision/length and
# fold synonyms so e.g. contract VARCHAR(255) matches the live INFORMATION_SCHEMA
# `TEXT`, and NUMBER(38,0) matches `NUMBER`. Only clear family differences are flagged.
_TYPE_FAMILY: dict[str, str] = {
    "NUMBER": "NUMBER", "DECIMAL": "NUMBER", "NUMERIC": "NUMBER", "INT": "NUMBER",
    "INTEGER": "NUMBER", "BIGINT": "NUMBER", "SMALLINT": "NUMBER", "TINYINT": "NUMBER",
    "BYTEINT": "NUMBER",
    "FLOAT": "FLOAT", "FLOAT4": "FLOAT", "FLOAT8": "FLOAT", "DOUBLE": "FLOAT",
    "DOUBLE PRECISION": "FLOAT", "REAL": "FLOAT",
    "VARCHAR": "TEXT", "CHAR": "TEXT", "CHARACTER": "TEXT", "STRING": "TEXT", "TEXT": "TEXT",
    "BOOLEAN": "BOOLEAN",
    "DATE": "DATE",
    "TIMESTAMP": "TIMESTAMP", "DATETIME": "TIMESTAMP", "TIMESTAMP_NTZ": "TIMESTAMP",
    "TIMESTAMP_LTZ": "TIMESTAMP", "TIMESTAMP_TZ": "TIMESTAMP",
    "VARIANT": "VARIANT", "OBJECT": "OBJECT", "ARRAY": "ARRAY",
}


def _type_family(t: Optional[str]) -> str:
    base = (t or "").upper().split("(")[0].strip()
    return _TYPE_FAMILY.get(base, base)


def _detect_drift(
    conn, contract: OpenDataContractStandard, database: Optional[str], schema: Optional[str],
) -> list[str]:
    """Compare each contract table's top-level columns to the live Snowflake table.

    Uses `DESCRIBE TABLE` — a metadata command that needs **no active warehouse**,
    so drift detection works in exactly the sessions where the apply itself works
    (unlike INFORMATION_SCHEMA queries, which need compute).

    Returns human-readable warnings: columns in the contract but missing from the
    table, columns in the table but not the contract, and clear type-family
    mismatches. A table that doesn't exist yet (DESCRIBE errors) is skipped — it
    will be created. Only top-level columns are compared; nested struct fields live
    inside an OBJECT/VARIANT/ARRAY column, which Snowflake exposes as one column.
    """
    db = (database or "").upper()
    sch = (schema or "").upper()
    if not db or not sch:
        return []  # need a known database + schema to qualify the table

    warnings: list[str] = []
    cur = conn.cursor()
    try:
        for schema_obj in contract.schema_ or []:
            table = schema_obj.physicalName or schema_obj.name
            if not table:
                continue
            try:
                cur.execute(f'DESCRIBE TABLE "{db}"."{sch}"."{table.upper()}"')
                rows = cur.fetchall()
            except Exception:
                continue  # table absent (or not visible) — it will be created

            # DESCRIBE TABLE's first columns are `name` and `type`; locate by header.
            idx = {c[0].lower(): i for i, c in enumerate(cur.description)}
            name_i, type_i = idx.get("name", 0), idx.get("type", 1)
            live_cols = {row[name_i].upper(): row[type_i] for row in rows}

            contract_cols = {
                (p.physicalName or p.name).upper(): p.physicalType
                for p in (schema_obj.properties or [])
                if (p.physicalName or p.name)
            }
            for col in contract_cols:
                if col not in live_cols:
                    warnings.append(
                        f"{table}: column '{col}' is in the contract but not in the Snowflake table."
                    )
            for col in live_cols:
                if col not in contract_cols:
                    warnings.append(
                        f"{table}: column '{col}' exists in Snowflake but not in the contract."
                    )
            for col, ctype in contract_cols.items():
                if col in live_cols and ctype and _type_family(ctype) != _type_family(live_cols[col]):
                    warnings.append(
                        f"{table}.{col}: contract type {ctype} differs from Snowflake {live_cols[col]}."
                    )
    finally:
        cur.close()
    return warnings


def _connect_apply(
    sql: str,
    conn_kwargs: dict[str, Any],
    *,
    contract: Optional[OpenDataContractStandard] = None,
    check_drift: bool = True,
    strict: bool = False,
) -> tuple[int, list[str]]:
    """Connect, optionally check schema drift, then execute the multi-statement SQL.

    Returns `(statements_executed, warnings)`. With `strict`, any drift becomes an
    `ApplyError` and nothing is executed. Drift introspection is best-effort: if it
    fails (e.g. no INFORMATION_SCHEMA access) the apply still proceeds with a note.
    """
    try:
        import snowflake.connector
    except ImportError:
        raise ApplyError(
            "snowflake-connector-python is not installed. "
            "Install it via `pip install snowflake-connector-python`."
        )

    conn_kwargs.setdefault("login_timeout", SNOWFLAKE_LOGIN_TIMEOUT)
    conn_kwargs.setdefault("network_timeout", SNOWFLAKE_NETWORK_TIMEOUT)
    quiet_aws_credential_noise()
    try:
        conn = snowflake.connector.connect(**conn_kwargs)
    except Exception as exc:
        raise ApplyError(f"Snowflake connection failed: {exc}")

    try:
        warnings: list[str] = []
        if check_drift and contract is not None:
            try:
                warnings = _detect_drift(
                    conn, contract, conn_kwargs.get("database"), conn_kwargs.get("schema"),
                )
            except Exception as exc:
                warnings = [f"(drift check skipped: {exc})"]

        real_drift = [w for w in warnings if not w.startswith("(drift check skipped")]
        if strict and real_drift:
            raise ApplyError(
                "Schema drift detected (use --no-strict to apply anyway):\n- "
                + "\n- ".join(real_drift)
            )

        # Statements are split quote-aware and run one at a time, rather than handed to
        # `execute_string`, so each can absorb its own idempotent DMF/expectation
        # conflicts (see `_execute_statement`).
        try:
            executed = 0
            for stmt in _split_sql_statements(sql):
                _execute_statement(conn, stmt)
                executed += 1
        except Exception as exc:
            raise ApplyError(f"Snowflake execution failed: {exc}")
        return executed, warnings
    finally:
        conn.close()


# === Typer sub-app ==========================================================

apply_app = typer.Typer(
    help="Apply a data contract to a live target system (executes DDL/ALTER).",
    no_args_is_help=True,
)


@apply_app.command("snowflake")
def apply_snowflake_command(
    location: Annotated[
        str, typer.Argument(help="Path to the data contract."),
    ] = "datacontract.yaml",
    server: Annotated[
        Optional[str],
        typer.Option(help="Use this named server from the contract."),
    ] = None,
    user: Annotated[
        Optional[str],
        typer.Option(help="Snowflake username (or set SNOWFLAKE_USER env var)."),
    ] = None,
    role: Annotated[
        Optional[str], typer.Option(help="Snowflake role to assume."),
    ] = None,
    warehouse: Annotated[
        Optional[str], typer.Option(help="Override warehouse from contract."),
    ] = None,
    account: Annotated[
        Optional[str], typer.Option(help="Override account from contract."),
    ] = None,
    database: Annotated[
        Optional[str], typer.Option(help="Override database from contract."),
    ] = None,
    schema: Annotated[
        Optional[str], typer.Option(help="Override schema from contract."),
    ] = None,
    authenticator: Annotated[
        Optional[str],
        typer.Option(
            help=(
                "Auth method: `snowflake` (password, default), `externalbrowser` "
                "(SSO), `oauth`, `snowflake_jwt` (key-pair)."
            ),
        ),
    ] = None,
    connection_name: Annotated[
        Optional[str],
        typer.Option(
            help="Use a named connection profile from ~/.snowflake/config.toml.",
        ),
    ] = None,
    dry_run: Annotated[
        bool, typer.Option(help="Print the SQL without connecting or executing."),
    ] = False,
    ddl_mode: Annotated[
        DdlMode,
        typer.Option(
            "--ddl-mode",
            help="Table handling: auto = create if missing else govern (default); "
            "always = CREATE TABLE (errors if it exists); never = govern existing only.",
        ),
    ] = DdlMode.auto,
    strict: Annotated[
        bool,
        typer.Option(
            "--strict/--no-strict",
            help="Fail instead of warn when an existing table's schema differs from the contract.",
        ),
    ] = False,
    structured_types: Annotated[
        bool,
        typer.Option(
            "--structured-types/--no-structured-types",
            help="Render nested columns as Snowflake structured types "
            "(OBJECT(field type, ...) / ARRAY(type)) instead of bare OBJECT/ARRAY.",
        ),
    ] = False,
    comments: Annotated[
        bool,
        typer.Option(
            "--comments/--no-comments",
            help="Emit COMMENT ON TABLE/COLUMN for descriptions (applies to existing tables).",
        ),
    ] = True,
    include_tags: Annotated[
        bool, typer.Option(help="Emit ALTER TABLE / MODIFY COLUMN SET TAG statements."),
    ] = True,
    include_quality: Annotated[
        bool,
        typer.Option(
            help=(
                "Emit Snowflake Data Metric Function statements (Enterprise feature)."
            ),
        ),
    ] = False,
    create_tags: Annotated[
        bool, typer.Option(help="Also emit `CREATE TAG IF NOT EXISTS` for each tag used."),
    ] = False,
    tag_namespace: Annotated[
        Optional[str],
        typer.Option(help="Database.schema prefix for tag references."),
    ] = None,
    tag_namespace_filter: Annotated[
        Optional[List[str]],
        typer.Option(
            "--tag-namespace-filter",
            help="Only emit tags whose namespace (DB.SCHEMA) is given. Repeatable; "
            "un-namespaced tags are skipped. Useful to apply only the tags you own.",
        ),
    ] = None,
    metric_schedule: Annotated[
        str, typer.Option(help="DATA_METRIC_SCHEDULE clause to set on tables with DMFs."),
    ] = "USING CRON 0 0 * * * UTC",
) -> None:
    """Apply a data contract to Snowflake.

    With the default `--ddl-mode auto` you don't need to know whether the table
    exists: missing tables are created (CREATE TABLE IF NOT EXISTS) and existing
    ones are governed — column/table comments, tags, and (with --include-quality)
    data-quality metrics. For existing tables, columns that differ from the
    contract are reported as warnings (or, with --strict, an error that aborts
    before any change). Use `--dry-run` to preview the SQL first.

    Secrets must be in environment variables (SNOWFLAKE_PASSWORD,
    SNOWFLAKE_PRIVATE_KEY_PATH, SNOWFLAKE_TOKEN). The CLI does not accept a
    --password flag by design.
    """
    contract = OpenDataContractStandard.from_file(location)
    try:
        result = apply_snowflake(
            contract,
            server_name=server,
            user=user, role=role,
            warehouse=warehouse, account=account, database=database, schema=schema,
            authenticator=authenticator,
            connection_name=connection_name,
            dry_run=dry_run,
            ddl_mode=ddl_mode,
            structured_types=structured_types,
            include_comments=comments,
            include_tags=include_tags,
            include_quality=include_quality,
            create_tags=create_tags,
            tag_namespace=tag_namespace,
            tag_namespace_filter=tag_namespace_filter,
            metric_schedule=metric_schedule,
            strict=strict,
        )
    except ApplyError as exc:
        typer.secho(f"Error: {exc}", err=True, fg=typer.colors.RED)
        raise typer.Exit(1)

    for warning in result.get("warnings") or []:
        typer.secho(f"Warning: {warning}", err=True, fg=typer.colors.YELLOW)

    if result["dry_run"]:
        typer.echo(result["sql"], nl=False)
    else:
        typer.echo(
            f"Applied {result['statements_executed']} statements to "
            f"Snowflake account '{result.get('account')}'.",
            err=True,
        )
