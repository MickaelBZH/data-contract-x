"""`dcx target` — bind an ODCS contract to a platform.

The implementation lives in `dcx.target.command`; this package exposes the
Typer app for the CLI assembly layer (`dcx.cli`). Code that needs the internals
(transform helpers, capture var) imports `dcx.target.command` directly.
"""

from dcx.target.command import target_app

__all__ = ["target_app"]
