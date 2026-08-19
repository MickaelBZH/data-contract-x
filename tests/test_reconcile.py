"""Reconcile: diffing desired contract DMF state against live Snowflake.

Covers the DROP/MODIFY side that `to_snowflake_full_sql` (additive only) never emits —
deleting a rule detaches its metric, editing a threshold replaces the expectation, and
disabling a rule drops the expectation while leaving the metric attached — plus the
guardrails that keep a reconcile from touching user-authored DMFs/expectations.
"""

import textwrap

import json

from open_data_contract_standard.model import OpenDataContractStandard

from dcx.apply.reconcile import desired_state, plan_from_states
from dcx.apply.snowflake import apply_snowflake_oauth

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


def _table(assocs: dict) -> dict:
    return {"kind": "TABLE", "statement_table": "DB.SCH.T", "assocs": assocs}


# === plan_from_states: delete ===============================================


def test_removed_association_is_dropped():
    desired = {"DB.SCH.T": _table({("SNOWFLAKE.CORE.NULL_COUNT", ("EMAIL",)): {"EXP__DCX__EMAIL__NONULLS"}})}
    live = {
        "DB.SCH.T": [
            {
                "key": ("SNOWFLAKE.CORE.NULL_COUNT", ("EMAIL",)),
                "qualified_dmf": "SNOWFLAKE.CORE.NULL_COUNT",
                "on_args": "EMAIL",
                "supported": True,
                "exp_names": {"EXP__DCX__EMAIL__NONULLS"},
            },
            {
                "key": ("SNOWFLAKE.CORE.ROW_COUNT", ()),
                "qualified_dmf": "SNOWFLAKE.CORE.ROW_COUNT",
                "on_args": "",
                "supported": True,
                "exp_names": set(),
            },
        ]
    }
    assert plan_from_states(desired, live) == [
        "ALTER TABLE DB.SCH.T DROP DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();"
    ]


# === plan_from_states: edit ================================================


def test_edited_threshold_drops_stale_expectation():
    """The new expectation is added by the additive exporter; reconcile removes the old."""
    desired = {
        "DB.SCH.T": _table(
            {("SNOWFLAKE.CORE.NULL_COUNT", ("EMAIL",)): {"EXP__DCX__EMAIL__NULL_COUNT__LESSTHAN25"}}
        )
    }
    live = {
        "DB.SCH.T": [
            {
                "key": ("SNOWFLAKE.CORE.NULL_COUNT", ("EMAIL",)),
                "qualified_dmf": "SNOWFLAKE.CORE.NULL_COUNT",
                "on_args": "EMAIL",
                "supported": True,
                "exp_names": {"EXP__DCX__EMAIL__NULL_COUNT__LESSTHAN10"},
            }
        ]
    }
    assert plan_from_states(desired, live) == [
        "ALTER TABLE DB.SCH.T MODIFY DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT "
        "ON (EMAIL) DROP EXPECTATION EXP__DCX__EMAIL__NULL_COUNT__LESSTHAN10;"
    ]


# === plan_from_states: disable =============================================


def test_disabled_rule_drops_expectation_but_keeps_association():
    """Disabled ⇒ desired keeps the association with no expectations; the metric stays
    attached (computing) and only its pass condition is removed."""
    desired = {"DB.SCH.T": _table({("SNOWFLAKE.CORE.NULL_COUNT", ("EMAIL",)): set()})}
    live = {
        "DB.SCH.T": [
            {
                "key": ("SNOWFLAKE.CORE.NULL_COUNT", ("EMAIL",)),
                "qualified_dmf": "SNOWFLAKE.CORE.NULL_COUNT",
                "on_args": "EMAIL",
                "supported": True,
                "exp_names": {"EXP__DCX__EMAIL__NONULLS"},
            }
        ]
    }
    plan = plan_from_states(desired, live)
    assert plan == [
        "ALTER TABLE DB.SCH.T MODIFY DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT "
        "ON (EMAIL) DROP EXPECTATION EXP__DCX__EMAIL__NONULLS;"
    ]
    assert not any("DROP DATA METRIC FUNCTION" in s for s in plan)


# === guardrails ============================================================


def test_user_defined_dmf_is_never_dropped():
    desired = {"DB.SCH.T": _table({})}
    live = {
        "DB.SCH.T": [
            {
                "key": ("MYDB.MYSCHEMA.MY_METRIC", ("EMAIL",)),
                "qualified_dmf": "MYDB.MYSCHEMA.MY_METRIC",
                "on_args": "EMAIL",
                "supported": False,
                "exp_names": {"EXP__DCX__EMAIL__NONULLS"},
            }
        ]
    }
    assert plan_from_states(desired, live) == []


def test_user_authored_expectation_is_never_dropped():
    desired = {"DB.SCH.T": _table({("SNOWFLAKE.CORE.NULL_COUNT", ("EMAIL",)): set()})}
    live = {
        "DB.SCH.T": [
            {
                "key": ("SNOWFLAKE.CORE.NULL_COUNT", ("EMAIL",)),
                "qualified_dmf": "SNOWFLAKE.CORE.NULL_COUNT",
                "on_args": "EMAIL",
                "supported": True,
                "exp_names": {"EXP_MY_SNOWSIGHT_RULE"},
            }
        ]
    }
    assert plan_from_states(desired, live) == []


def test_table_absent_from_contract_is_untouched():
    live = {
        "DB.SCH.OTHER": [
            {
                "key": ("SNOWFLAKE.CORE.ROW_COUNT", ()),
                "qualified_dmf": "SNOWFLAKE.CORE.ROW_COUNT",
                "on_args": "",
                "supported": True,
                "exp_names": set(),
            }
        ]
    }
    assert plan_from_states({}, live) == []


def test_expectation_drops_precede_association_drops():
    desired = {
        "DB.SCH.T": _table({("SNOWFLAKE.CORE.NULL_COUNT", ("EMAIL",)): set()})
    }
    live = {
        "DB.SCH.T": [
            {
                "key": ("SNOWFLAKE.CORE.ROW_COUNT", ()),
                "qualified_dmf": "SNOWFLAKE.CORE.ROW_COUNT",
                "on_args": "",
                "supported": True,
                "exp_names": set(),
            },
            {
                "key": ("SNOWFLAKE.CORE.NULL_COUNT", ("EMAIL",)),
                "qualified_dmf": "SNOWFLAKE.CORE.NULL_COUNT",
                "on_args": "EMAIL",
                "supported": True,
                "exp_names": {"EXP__DCX__EMAIL__NONULLS"},
            },
        ]
    }
    plan = plan_from_states(desired, live)
    assert "DROP EXPECTATION" in plan[0]
    assert "DROP DATA METRIC FUNCTION" in plan[1]


# === desired_state from a contract =========================================


def test_desired_state_captures_association_and_expectation():
    contract = _contract(
        """
        schema:
          - name: T
            physicalType: table
            quality:
              - type: library
                metric: rowCount
                mustBeGreaterThan: 0
            properties:
              - name: EMAIL
                physicalType: STRING
                quality:
                  - type: library
                    metric: nullValues
                    mustBe: 0
        """
    )
    state = desired_state(contract)
    table = state["DB.SCH.T"]
    assert table["kind"] == "TABLE"
    assert table["statement_table"] == "DB.SCH.T"
    assert table["assocs"][("SNOWFLAKE.CORE.ROW_COUNT", ())] == {"EXP__DCX__ROW_COUNT__GREATERTHAN0"}
    assert table["assocs"][("SNOWFLAKE.CORE.NULL_COUNT", ("EMAIL",))] == {"EXP__DCX__EMAIL__NONULLS"}


def test_desired_state_disabled_rule_has_no_expectation():
    contract = _contract(
        """
        schema:
          - name: T
            physicalType: table
            quality:
              - type: library
                metric: rowCount
                mustBeGreaterThan: 0
                customProperties:
                  - property: enabled
                    value: false
        """
    )
    state = desired_state(contract)
    assert state["DB.SCH.T"]["assocs"][("SNOWFLAKE.CORE.ROW_COUNT", ())] == set()


# === end-to-end apply → reconcile (fake connector serving live DMF state) ====


class _FakeCursor:
    """Serves live DMF references/expectations for reconcile and records DDL executed."""

    def __init__(self, conn):
        self._conn = conn
        self._rows: list = []
        self.description: list = []

    def execute(self, sql):
        up = sql.strip().upper()
        if "DATA_METRIC_FUNCTION_REFERENCES" in up:
            self.description = [
                ("metric_database_name",), ("metric_schema_name",), ("metric_name",),
                ("ref_arguments",), ("ref_id",),
            ]
            self._rows = self._conn.refs
        elif "DATA_METRIC_FUNCTION_EXPECTATIONS" in up:
            self.description = [("ref_id",), ("expectation_name",), ("expectation_expression",)]
            self._rows = self._conn.exps
        elif up.startswith("DESCRIBE TABLE"):
            raise Exception("skip drift in test")
        else:
            self.description = []
            self._rows = []
            self._conn.executed.append(sql.strip())

    def fetchall(self):
        return self._rows

    def close(self):
        pass


class _FakeConn:
    def __init__(self, refs, exps):
        self.refs = refs
        self.exps = exps
        self.executed: list = []

    def cursor(self):
        return _FakeCursor(self)

    def close(self):
        pass


def _install_fake_connector(monkeypatch, refs, exps):
    conn = _FakeConn(refs, exps)
    import snowflake.connector as connector

    monkeypatch.setattr(connector, "connect", lambda **_kw: conn)
    return conn


def test_reconcile_delete_detaches_removed_metric(monkeypatch):
    """A NULL_COUNT is attached in Snowflake but the contract no longer carries it, so
    reconcile detaches it."""
    refs = [("SNOWFLAKE", "CORE", "NULL_COUNT",
             json.dumps([{"domain": "COLUMN", "name": "EMAIL"}]), "ref1")]
    conn = _install_fake_connector(monkeypatch, refs, exps=[])
    contract = _contract(
        """
        schema:
          - name: T
            physicalType: table
            properties:
              - name: EMAIL
                physicalType: STRING
        """
    )
    result = apply_snowflake_oauth(contract, token="tok", reconcile=True, include_quality=True)
    drop = "ALTER TABLE DB.SCH.T DROP DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (EMAIL);"
    assert drop in result["reconcile_sql"]
    assert drop.rstrip(";") in "\n".join(conn.executed)


def test_reconcile_disable_drops_expectation_keeps_metric(monkeypatch):
    """A disabled rule keeps the metric attached (association stays) but its dcx
    expectation is dropped."""
    refs = [("SNOWFLAKE", "CORE", "NULL_COUNT",
             json.dumps([{"domain": "COLUMN", "name": "EMAIL"}]), "ref1")]
    exps = [("ref1", "EXP__DCX__EMAIL__NONULLS", "VALUE = 0")]
    conn = _install_fake_connector(monkeypatch, refs, exps)
    contract = _contract(
        """
        schema:
          - name: T
            physicalType: table
            properties:
              - name: EMAIL
                physicalType: STRING
                quality:
                  - type: library
                    metric: nullValues
                    mustBe: 0
                    customProperties:
                      - property: enabled
                        value: false
        """
    )
    result = apply_snowflake_oauth(contract, token="tok", reconcile=True, include_quality=True)
    assert (
        "MODIFY DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (EMAIL) "
        "DROP EXPECTATION EXP__DCX__EMAIL__NONULLS" in result["reconcile_sql"]
    )
    assert "DROP DATA METRIC FUNCTION" not in result["reconcile_sql"]

