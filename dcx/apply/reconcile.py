"""Reconcile a contract's desired DMF state against live Snowflake.

`to_snowflake_full_sql` is deliberately *additive*: it emits `ADD DATA METRIC
FUNCTION` / `ADD EXPECTATION`, and (via `apply`'s idempotent rewrites) never removes
anything. That is safe for "govern what exists", but it means a rule the user
*deleted*, *edited*, or *disabled* leaves the old Data Metric Function — or the old
threshold expectation — attached in Snowflake.

This module supplies the missing side: it reads the live DMF references and their
expectations for the contract's tables, diffs them against what the contract wants,
and returns the minimal `DROP DATA METRIC FUNCTION` (rule deleted) and
`MODIFY ... DROP EXPECTATION` (threshold changed / rule disabled) statements needed
to bring Snowflake in line.

Blast radius is bounded on purpose so a reconcile can never touch objects the
platform did not author:

  * only associations whose DMF maps to a supported ODCS rule (`SNOWFLAKE.CORE`, in
    the importer's ``_DMF_TO_QUALITY``) are ever dropped — a user-defined DMF is left
    alone;
  * only expectations dcx itself wrote (the ``EXP__DCX__`` name prefix) are ever
    dropped — an expectation a user created directly in Snowsight is left alone;
  * only tables present in the contract are considered.
"""

from __future__ import annotations

from typing import Any, Optional

from open_data_contract_standard.model import OpenDataContractStandard

from dcx.exporters.snowflake import (
    _EXP_PREFIX,
    _dmf_binding,
    _expectation_name_and_expr,
    _object_kind,
    _quality_iter,
    _rule_disabled,
    _sla_by_object,
    _sla_seconds,
    _snowflake_table_prefix,
    _value_token,
)
from dcx.importers.snowflake import (
    _DMF_TO_QUALITY,
    _dmf_identity,
    _dmf_ref_columns,
    _dmf_ref_condition,
)

# (dmf_fully_qualified_name_upper, columns_upper) — identifies one DMF association,
# matching how both the exporter emits it and the live reference reports it.
AssocKey = tuple[str, tuple[str, ...]]

# Fully-qualified table-scope freshness DMF, mirroring the exporter's SLA emission.
_FRESHNESS_DMF = "SNOWFLAKE.CORE.FRESHNESS"

# customProperties key the frontend stamps on a rule it read back from Snowflake.
# Such a rule's live threshold is not recoverable (INFORMATION_SCHEMA does not expose
# expectation expressions), so reconcile must preserve — never drop — its expectations.
_SNOWFLAKE_SOURCE_PROPERTY = "snowflakeSource"

# Desired-state marker for an association whose expectations must be left intact: an
# imported rule. Distinct from an empty set (which means "drop every expectation",
# i.e. a disabled rule). `is`-compared, never equality-compared.
_PRESERVE = object()


def _rule_imported(q: Any) -> bool:
    """True if the rule carries the frontend's `snowflakeSource: import` tag."""
    for cp in (getattr(q, "customProperties", None) or []):
        if getattr(cp, "property", None) == _SNOWFLAKE_SOURCE_PROPERTY:
            return str(getattr(cp, "value", "")).strip().lower() == "import"
    return False


def _assoc_key(add_dmf: str, column: Optional[str]) -> tuple[AssocKey, Optional[str]]:
    """`((dmf_fqn_upper, columns_upper), effective_column)` for an exporter binding.

    `add_dmf` is the `<dmf> ON (...)` string from `_dmf_binding`. A table-scope metric
    ends in `ON ()` and binds to no column even when it was authored under one (mirrors
    the exporter's `effective_column` rule), so its key carries an empty column tuple.
    """
    fqn = add_dmf.split(" ON (")[0].strip()
    if add_dmf.rstrip().endswith("ON ()"):
        return (fqn.upper(), ()), None
    col = (column or "").upper()
    cols = (col,) if col else ()
    return (fqn.upper(), cols), column


def _on_args(columns: list[str], condition: Optional[str]) -> str:
    """The `ON (...)` argument body for a live reference's DROP/MODIFY statement.

    Rebuilds the exact association signature Snowflake needs to detach a metric:
    `` `` (table scope), ``<col>`` (column scope), or the ACCEPTED_VALUES lambda
    ``<col>, <col> -> <condition>`` when the reference carries a predicate (mirrors
    the exporter's `ON ({col}, {col} -> {col} IN (...))` form).
    """
    if not columns:
        return ""
    col = columns[0]
    if condition:
        return f"{col}, {col} -> {condition}"
    return col


def desired_state(
    contract: OpenDataContractStandard, *, server: Optional[str] = None,
) -> dict[str, dict]:
    """Desired DMF state per table, as ``qualified_upper -> {kind, statement_table, assocs}``.

    ``assocs`` maps each desired association (`AssocKey`) to EITHER the set of dcx
    expectation NAMES the contract wants on it (empty ⇒ `DROP EXPECTATION`, i.e. a
    disabled rule) OR the `_PRESERVE` sentinel for an imported rule whose live
    threshold is unknown and must be left untouched.

    The desired set is the *complete* thing the user wants kept: it spans the
    contract's quality rules AND its `slaProperties` freshness (a table-scope
    `SNOWFLAKE.CORE.FRESHNESS`), so a reconcile never removes a metric the contract
    still declares just because it lives outside the `quality` array.
    """
    prefix = _snowflake_table_prefix(contract, server)
    slas_by_object = _sla_by_object(contract)
    out: dict[str, dict] = {}
    for schema_obj in contract.schema_ or []:
        name = schema_obj.name
        if not name:
            continue
        statement_table = f"{prefix}{name}"
        table = out.setdefault(
            statement_table.upper(),
            {"kind": _object_kind(schema_obj), "statement_table": statement_table, "assocs": {}},
        )
        assocs = table["assocs"]
        for column, q in _quality_iter(schema_obj):
            add_dmf = _dmf_binding(q, column=column)
            if not add_dmf:
                continue
            key, effective_column = _assoc_key(add_dmf, column)
            if _rule_imported(q):
                assocs[key] = _PRESERVE
                continue
            if assocs.get(key) is _PRESERVE:
                continue  # an imported sibling already locked this association
            exps: set[str] = assocs.setdefault(key, set())
            if _rule_disabled(q):
                continue
            parts = _expectation_name_and_expr(q, add_dmf, effective_column)
            if parts:
                exps.add(parts[0].upper())
        for sla in slas_by_object.get(name, []):
            seconds = _sla_seconds(sla)
            if seconds is None:
                continue
            key = (_FRESHNESS_DMF, ())
            if assocs.get(key) is _PRESERVE:
                continue
            exps = assocs.setdefault(key, set())
            exps.add(f"{_EXP_PREFIX}FRESHNESS__LESSTHANOREQUALTO{_value_token(seconds)}".upper())
    return out


def fetch_live_state(
    conn,
    contract: OpenDataContractStandard,
    *,
    database: Optional[str],
    schema: Optional[str],
    server: Optional[str] = None,
) -> dict[str, list[dict]]:
    """Read live DMF associations + expectation names for each contract table.

    Returns ``qualified_upper -> [assoc, ...]`` where each assoc is
    ``{key, qualified_dmf, on_args, supported, exp_names}``. Best-effort per table:
    DMFs are Enterprise-only and role-gated, so a table whose functions aren't visible
    simply contributes nothing (no reconcile drops for it) rather than failing.
    """
    prefix = _snowflake_table_prefix(contract, server)
    db = (database or "").upper()
    sch = (schema or "").upper()
    live: dict[str, list[dict]] = {}
    if not db or not sch:
        return live

    cur = conn.cursor()
    try:
        for schema_obj in contract.schema_ or []:
            name = schema_obj.name
            if not name:
                continue
            qualified_upper = f"{prefix}{name}".upper()
            domain = "VIEW" if _object_kind(schema_obj) == "VIEW" else "TABLE"
            entity = f"{db}.{sch}.{name.upper()}"

            refs_by_id: dict[Any, dict] = {}
            try:
                cur.execute(
                    f'SELECT * FROM TABLE("{db}".INFORMATION_SCHEMA.'
                    f"DATA_METRIC_FUNCTION_REFERENCES("
                    f"REF_ENTITY_NAME => '{entity}', REF_ENTITY_DOMAIN => '{domain}'))"
                )
                idx = {c[0].lower(): i for i, c in enumerate(cur.description)}

                def _col(row, *names, _idx=idx):
                    for n in names:
                        if n in _idx:
                            return row[_idx[n]]
                    return None

                for row in cur.fetchall():
                    ref_arguments = _col(row, "ref_arguments")
                    short, qualified = _dmf_identity(
                        _col(row, "metric_database_name"),
                        _col(row, "metric_schema_name"),
                        _col(row, "metric_name"),
                    )
                    refs_by_id[_col(row, "ref_id")] = {
                        "dmf": short,
                        "qualified": qualified,
                        "columns": _dmf_ref_columns(ref_arguments),
                        "condition": _dmf_ref_condition(ref_arguments),
                    }
            except Exception:
                continue  # functions not visible to this role, or none attached

            exp_by_id: dict[Any, set[str]] = {}
            try:
                cur.execute(
                    f'SELECT * FROM TABLE("{db}".INFORMATION_SCHEMA.'
                    f"DATA_METRIC_FUNCTION_EXPECTATIONS("
                    f"REF_ENTITY_NAME => '{entity}', REF_ENTITY_DOMAIN => '{domain}'))"
                )
                idx = {c[0].lower(): i for i, c in enumerate(cur.description)}
                for row in cur.fetchall():
                    rid = row[idx["ref_id"]] if "ref_id" in idx else None
                    ename = row[idx["expectation_name"]] if "expectation_name" in idx else None
                    if rid is not None and ename:
                        exp_by_id.setdefault(rid, set()).add(str(ename).upper())
            except Exception:
                pass  # no expectations readable — only association drops remain possible

            assocs: list[dict] = []
            for ref_id, ref in refs_by_id.items():
                cols = tuple(str(c).upper() for c in (ref["columns"] or []))
                assocs.append({
                    "key": (str(ref["qualified"]).upper(), cols),
                    "qualified_dmf": ref["qualified"],
                    "on_args": _on_args(ref["columns"] or [], ref["condition"]),
                    "supported": (ref["dmf"] or "") in _DMF_TO_QUALITY,
                    "exp_names": exp_by_id.get(ref_id, set()),
                })
            if assocs:
                live[qualified_upper] = assocs
    finally:
        cur.close()
    return live


def plan_from_states(desired: dict[str, dict], live: dict[str, list[dict]]) -> list[str]:
    """Diff desired vs live and return the ordered DROP/MODIFY statements.

    Expectation drops are ordered before association drops so that, paired with the
    additive `ADD ... EXPECTATION` the exporter already emits, an edited threshold is
    replaced with never a moment where the metric has no pass condition.
    """
    expectation_drops: list[str] = []
    association_drops: list[str] = []
    prefix_upper = _EXP_PREFIX.upper()

    for qualified_upper, assocs in live.items():
        d_table = desired.get(qualified_upper)
        if not d_table:
            continue  # table not in the contract — not managed here
        kind = d_table["kind"]
        stmt_table = d_table["statement_table"]
        # Values are `set[str]` (managed expectations) or the `_PRESERVE` sentinel.
        d_assocs: dict[AssocKey, Any] = d_table["assocs"]

        for a in assocs:
            if not a["supported"]:
                continue  # user-defined DMF — never auto-dropped
            dmf = a["qualified_dmf"]
            on = a["on_args"]
            if a["key"] not in d_assocs:
                association_drops.append(
                    f"ALTER {kind} {stmt_table} DROP DATA METRIC FUNCTION {dmf} ON ({on});"
                )
                continue
            desired_exps = d_assocs[a["key"]]
            if desired_exps is _PRESERVE:
                continue  # imported association — leave the metric and its expectations
            for ename in sorted(a["exp_names"]):
                if ename.startswith(prefix_upper) and ename not in desired_exps:
                    expectation_drops.append(
                        f"ALTER {kind} {stmt_table} MODIFY DATA METRIC FUNCTION {dmf} "
                        f"ON ({on}) DROP EXPECTATION {ename};"
                    )
    return expectation_drops + association_drops


def plan_reconcile(
    conn,
    contract: OpenDataContractStandard,
    *,
    database: Optional[str],
    schema: Optional[str],
    server: Optional[str] = None,
) -> list[str]:
    """Read live state over `conn` and return the reconcile DROP/MODIFY statements."""
    desired = desired_state(contract, server=server)
    live = fetch_live_state(conn, contract, database=database, schema=schema, server=server)
    return plan_from_states(desired, live)
