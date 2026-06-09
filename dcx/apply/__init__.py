"""`dcx apply <platform>` — execute generated SQL against a live target system.

One file per target platform (`apply/snowflake.py` today). This package exposes
the Typer app for the CLI assembly layer; platform internals are imported from
their module, e.g. `from dcx.apply.snowflake import apply_snowflake`.
"""

from dcx.apply.snowflake import apply_app

__all__ = ["apply_app"]
