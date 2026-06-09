"""Exporters — `dcx export <target>` beyond the upstream set.

One file per export target (`exporters/snowflake.py` today). `command.py` wires
the `dcx export snowflake-full` CLI command and is imported for its side effects
by `dcx.cli`. To add a target, drop a new module here plus its command wiring.
"""
