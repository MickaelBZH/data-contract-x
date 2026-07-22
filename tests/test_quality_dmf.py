"""Data-quality → Snowflake DMF mapping: expectations, ACCEPTED_VALUES, SLA, schedule.

Covers the behaviour added on top of the bare `ADD DATA METRIC FUNCTION` emission:
ODCS operators becoming enforced `EXPECTATION` clauses, the `check` custom-property
escape hatch, freshness sourced from `slaProperties`, and the apply-side statement
handling that makes re-applying a contract idempotent.
"""

import textwrap

import pytest
from open_data_contract_standard.model import OpenDataContractStandard

from dcx.apply.snowflake import (
    _add_dmf_to_modify_expectation,
    _execute_statement,
    _has_expectation,
    _split_sql_statements,
)
from dcx.exporters.snowflake import _render_sql_literal, to_snowflake_full_sql


def _contract(body: str) -> OpenDataContractStandard:
    return OpenDataContractStandard.from_string(
        textwrap.dedent(
            """\
            apiVersion: v3.1.0
            kind: DataContract
            id: db.sch
            name: SCH
            version: 1.0.0
            status: draft
            servers:
              - server: production
                type: snowflake
                account: ACME
                database: DB
                schema: SCH
            """
        )
        + textwrap.dedent(body)
    )


def _quality_sql(body: str, **kwargs) -> str:
    return to_snowflake_full_sql(_contract(body), include_quality=True, include_ddl=False, **kwargs)


# === Operators become expectations ==========================================


def test_operator_becomes_enforced_expectation():
    """A `mustBeGreaterThan` is what makes the DMF fail — without it the metric is
    merely computed and nothing ever breaks."""
    sql = _quality_sql(
        """
        schema:
          - name: T
            physicalType: table
            quality:
              - type: library
                metric: rowCount
                mustBeGreaterThan: 0
        """
    )
    assert (
        "ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ()\n"
        "  EXPECTATION EXP__DCX__ROW_COUNT__GREATERTHAN0 (VALUE > 0);"
    ) in sql


def test_zero_count_column_check_uses_friendly_alias():
    sql = _quality_sql(
        """
        schema:
          - name: T
            physicalType: table
            properties:
              - name: c
                physicalType: STRING
                quality:
                  - type: library
                    metric: nullValues
                    mustBe: 0
        """
    )
    assert "EXPECTATION EXP__DCX__C__NONULLS (VALUE = 0);" in sql


def test_range_operator_renders_between():
    sql = _quality_sql(
        """
        schema:
          - name: T
            physicalType: table
            quality:
              - type: library
                metric: rowCount
                mustBeBetween: [10, 20]
        """
    )
    assert "EXPECTATION EXP__DCX__ROW_COUNT__BETWEEN10_20 (10 <= VALUE AND VALUE <= 20);" in sql


def test_rule_without_operator_emits_bare_dmf():
    """No operator means no pass condition to enforce — the DMF is still attached."""
    sql = _quality_sql(
        """
        schema:
          - name: T
            physicalType: table
            quality:
              - type: library
                metric: rowCount
        """
    )
    assert "ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();" in sql
    assert "EXPECTATION" not in sql


# === invalidValues → ACCEPTED_VALUES ========================================


def test_invalid_values_renders_accepted_values_lambda():
    sql = _quality_sql(
        """
        schema:
          - name: T
            physicalType: table
            properties:
              - name: status
                physicalType: STRING
                quality:
                  - type: library
                    metric: invalidValues
                    arguments:
                      validValues: [PENDING, "O'BRIEN"]
                    mustBe: 0
        """
    )
    assert (
        "ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ACCEPTED_VALUES "
        "ON (status, status -> status IN ('PENDING', 'O''BRIEN'))"
    ) in sql


def test_invalid_values_without_allowed_set_is_not_emitted():
    """With no allowed set there is nothing to check, so it must not silently emit a
    DMF that would accept everything."""
    sql = _quality_sql(
        """
        schema:
          - name: T
            physicalType: table
            properties:
              - name: status
                physicalType: STRING
                quality:
                  - type: library
                    metric: invalidValues
                    mustBe: 0
        """
    )
    assert "ACCEPTED_VALUES" not in sql
    assert "-- TODO: unmappable quality rule" in sql


@pytest.mark.parametrize(
    "value,expected",
    [("O'Brien", "'O''Brien'"), ("back\\slash", "'back\\\\slash'"), (5.0, "5"), (True, "TRUE")],
)
def test_sql_literal_escaping(value, expected):
    assert _render_sql_literal(value) == expected


# === `check` custom property ================================================


def test_check_tag_upgrades_sql_rule_to_native_dmf():
    sql = _quality_sql(
        """
        schema:
          - name: T
            physicalType: table
            properties:
              - name: c
                physicalType: STRING
                quality:
                  - type: sql
                    query: SELECT COUNT(*) FROM ${table}
                    mustBe: 0
                    customProperties:
                      - property: check
                        value: blankCount
        """
    )
    assert "ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.BLANK_COUNT ON (c)" in sql
    assert "EXPECTATION EXP__DCX__C__NOBLANKS (VALUE = 0);" in sql


def test_unknown_check_tag_stays_a_todo():
    sql = _quality_sql(
        """
        schema:
          - name: T
            physicalType: table
            properties:
              - name: c
                physicalType: STRING
                quality:
                  - type: sql
                    query: SELECT 1
                    mustBe: 0
                    customProperties:
                      - property: check
                        value: somethingElse
        """
    )
    assert "DATA METRIC FUNCTION" not in sql
    assert "-- TODO: unmappable quality rule" in sql


# === Freshness comes from slaProperties, not quality ========================

_SLA_BODY = """
slaProperties:
  - property: latency
    value: 4
    unit: h
    element: DB.SCH.T
  - property: retention
    value: 7
    unit: y
    element: DB.SCH.T
schema:
  - name: T
    physicalType: table
    quality:
      - type: library
        metric: rowCount
        mustBeGreaterThan: 0
"""


def test_sla_latency_becomes_freshness_dmf_in_seconds():
    sql = _quality_sql(_SLA_BODY)
    assert (
        "ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ()\n"
        "  EXPECTATION EXP__DCX__FRESHNESS__LESSTHANOREQUALTO14400 (VALUE <= 14400);"
    ) in sql


def test_sla_without_a_dmf_is_left_as_documentation():
    """`retention` has no Snowflake DMF; it must not appear, and must not warn."""
    sql = _quality_sql(_SLA_BODY)
    assert "retention" not in sql.lower()


def test_sla_with_unknown_unit_is_reported_not_guessed():
    sql = _quality_sql(
        """
        slaProperties:
          - property: latency
            value: 4
            unit: fortnights
            element: DB.SCH.T
        schema:
          - name: T
            physicalType: table
        """
    )
    assert "FRESHNESS" not in sql
    assert "-- TODO: SLA 'latency'" in sql


def test_sla_alone_still_emits_a_quality_section():
    """A table with an SLA but no quality rules must still be governed."""
    sql = _quality_sql(
        """
        slaProperties:
          - property: latency
            value: 30
            unit: m
            element: T
        schema:
          - name: T
            physicalType: table
        """
    )
    assert "SET DATA_METRIC_SCHEDULE" in sql
    assert "EXPECTATION EXP__DCX__FRESHNESS__LESSTHANOREQUALTO1800 (VALUE <= 1800);" in sql


# === Schedule ===============================================================


def test_bare_cron_is_wrapped_and_conflicts_are_reported():
    sql = _quality_sql(
        """
        schema:
          - name: T
            physicalType: table
            quality:
              - type: library
                metric: rowCount
                mustBeGreaterThan: 0
                schedule: 0 6 * * *
            properties:
              - name: c
                physicalType: STRING
                quality:
                  - type: library
                    metric: nullValues
                    mustBe: 0
                    schedule: 0 20 * * *
        """
    )
    assert "SET DATA_METRIC_SCHEDULE = 'USING CRON 0 6 * * * UTC';" in sql
    # DATA_METRIC_SCHEDULE is per-table, so the second cadence cannot be honoured —
    # it must be reported rather than silently dropped.
    assert "-- WARNING:" in sql
    assert "0 20 * * *" in sql


def test_snowflake_native_schedule_passes_through():
    sql = _quality_sql(
        """
        schema:
          - name: T
            physicalType: table
            quality:
              - type: library
                metric: rowCount
                mustBeGreaterThan: 0
                schedule: TRIGGER_ON_CHANGES
        """
    )
    assert "SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';" in sql


# === Views ==================================================================


def test_view_quality_uses_alter_view():
    sql = _quality_sql(
        """
        schema:
          - name: V
            physicalType: view
            quality:
              - type: library
                metric: rowCount
                mustBeGreaterThan: 0
        """
    )
    assert "ALTER VIEW DB.SCH.V ADD DATA METRIC FUNCTION" in sql
    assert "ALTER TABLE DB.SCH.V" not in sql


# === Apply-side statement handling ==========================================


def test_split_is_quote_aware():
    """A semicolon inside a string literal — an ordinary column comment — must not
    split the statement."""
    sql = (
        "COMMENT ON COLUMN DB.S.T.c IS 'Lifecycle state; one of NEW, DONE';\n"
        "ALTER TABLE DB.S.T SET TAG x = 'y';\n"
    )
    stmts = _split_sql_statements(sql)
    assert len(stmts) == 2
    assert stmts[0].endswith("'Lifecycle state; one of NEW, DONE'")


def test_split_drops_comment_lines():
    stmts = _split_sql_statements("-- TODO: unmappable rule\nALTER TABLE T SET TAG a = 'b';\n")
    assert len(stmts) == 1
    assert stmts[0].startswith("ALTER TABLE")


@pytest.mark.parametrize(
    "assoc",
    [
        "SNOWFLAKE.CORE.NULL_COUNT ON (email)",
        "SNOWFLAKE.CORE.ROW_COUNT ON ()",
        # The lambda form is the one a first-`)` pattern silently fails to match,
        # which previously made an ACCEPTED_VALUES threshold change a no-op.
        "SNOWFLAKE.CORE.ACCEPTED_VALUES ON (s, s -> s IN ('A', 'B'))",
    ],
)
def test_add_dmf_rewrites_to_modify_expectation(assoc):
    stmt = (
        f"ALTER TABLE DB.S.T ADD DATA METRIC FUNCTION {assoc}\n"
        "  EXPECTATION EXP__DCX__X (VALUE = 0)"
    )
    rewritten = _add_dmf_to_modify_expectation(stmt)
    assert rewritten is not None
    assert rewritten.startswith("ALTER TABLE DB.S.T MODIFY DATA METRIC FUNCTION ")
    assert assoc in rewritten
    assert rewritten.endswith("ADD EXPECTATION EXP__DCX__X (VALUE = 0)")


def test_bare_dmf_has_no_expectation_to_rewrite():
    stmt = "ALTER TABLE DB.S.T ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ()"
    assert _add_dmf_to_modify_expectation(stmt) is None
    assert _has_expectation(stmt) is False


def test_non_dmf_statement_is_not_rewritten():
    assert _add_dmf_to_modify_expectation("ALTER TABLE DB.S.T SET TAG a = 'b'") is None


# === Idempotent re-apply ====================================================


class _Conn:
    """Connection that fails statements matching a needle, once each."""

    def __init__(self, failures=()):
        self.failures = list(failures)
        self.executed: list[str] = []
        self.open_cursors = 0

    def cursor(self):
        conn = self

        class _Cur:
            def execute(self, sql):
                for needle, message in list(conn.failures):
                    if needle.lower() in sql.lower():
                        conn.failures.remove((needle, message))
                        raise Exception(message)
                conn.executed.append(sql)

            def close(self):
                conn.open_cursors -= 1

        self.open_cursors += 1
        return _Cur()


_ADD = (
    "ALTER TABLE DB.S.T ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (c)\n"
    "  EXPECTATION EXP__DCX__C__NONULLS (VALUE = 0)"
)
_DMF_EXISTS = "Table already has the data metric function attached"
_EXP_EXISTS = "Table already has an expectation with that name"


def test_reapply_adds_expectation_when_dmf_already_attached():
    conn = _Conn(failures=[("ADD DATA METRIC FUNCTION", _DMF_EXISTS)])
    _execute_statement(conn, _ADD)
    assert len(conn.executed) == 1
    assert conn.executed[0].startswith("ALTER TABLE DB.S.T MODIFY DATA METRIC FUNCTION ")
    assert "ADD EXPECTATION EXP__DCX__C__NONULLS (VALUE = 0)" in conn.executed[0]


def test_reapply_of_unchanged_rule_is_a_no_op():
    conn = _Conn(failures=[
        ("ADD DATA METRIC FUNCTION", _DMF_EXISTS),
        ("MODIFY DATA METRIC FUNCTION", _EXP_EXISTS),
    ])
    _execute_statement(conn, _ADD)  # must not raise
    assert conn.executed == []


def test_bare_dmf_already_attached_is_a_no_op():
    stmt = "ALTER TABLE DB.S.T ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ()"
    conn = _Conn(failures=[("ADD DATA METRIC FUNCTION", _DMF_EXISTS)])
    _execute_statement(conn, stmt)
    assert conn.executed == []


def test_unrewritable_expectation_raises_instead_of_being_dropped():
    """The regression guard: silently swallowing this loses a governance change."""
    stmt = "ALTER TABLE DB.S.T ADD DATA METRIC FUNCTION EXPECTATION EXP__X (VALUE = 0)"
    conn = _Conn(failures=[("ADD DATA METRIC FUNCTION", _DMF_EXISTS)])
    with pytest.raises(Exception, match="already has the data metric function"):
        _execute_statement(conn, stmt)


def test_unrelated_error_is_never_swallowed():
    conn = _Conn(failures=[("ADD DATA METRIC FUNCTION", "Insufficient privileges")])
    with pytest.raises(Exception, match="Insufficient privileges"):
        _execute_statement(conn, _ADD)


@pytest.mark.parametrize("failures", [(), [("ADD DATA METRIC", _DMF_EXISTS)]])
def test_cursors_are_always_closed(failures):
    conn = _Conn(failures=list(failures))
    _execute_statement(conn, _ADD)
    assert conn.open_cursors == 0


# === Table-scope metrics authored under a column ============================


_COLUMN_LEVEL_ROWCOUNT = """
schema:
  - name: T
    physicalType: table
    properties:
      - name: a
        physicalType: STRING
        quality:
          - type: library
            metric: rowCount
            mustBeGreaterThan: 0
      - name: b
        physicalType: STRING
        quality:
          - type: library
            metric: rowCount
            mustBeGreaterThan: 0
"""


def test_table_scope_metric_is_not_named_after_a_column():
    """`rowCount` binds to the table (`ON ()`) even when authored under a column, so
    naming its expectation after that column would misdescribe what is enforced."""
    sql = _quality_sql(_COLUMN_LEVEL_ROWCOUNT)
    assert "EXPECTATION EXP__DCX__ROW_COUNT__GREATERTHAN0 (VALUE > 0);" in sql
    assert "EXP__DCX__A__ROW_COUNT" not in sql


def test_repeated_table_scope_rules_collapse_to_one_statement():
    """Two columns carrying the same rowCount rule produce one association, not two —
    Snowflake would treat the second as already-present anyway."""
    sql = _quality_sql(_COLUMN_LEVEL_ROWCOUNT)
    assert sql.count("ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ()") == 1


def test_distinct_thresholds_on_one_metric_are_both_emitted():
    """Deduplication must key on the whole statement: different thresholds are
    genuinely different expectations."""
    sql = _quality_sql(
        """
        schema:
          - name: T
            physicalType: table
            quality:
              - type: library
                metric: rowCount
                mustBeGreaterThan: 0
              - type: library
                metric: rowCount
                mustBeLessThan: 1000
        """
    )
    assert "EXP__DCX__ROW_COUNT__GREATERTHAN0 (VALUE > 0)" in sql
    assert "EXP__DCX__ROW_COUNT__LESSTHAN1000 (VALUE < 1000)" in sql
