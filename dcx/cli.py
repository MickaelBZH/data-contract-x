from importlib import metadata
from typing import Optional

import typer
from click import Context
from datacontract.cli import OrderedCommandsWithMigrationHints, app
from datacontract.command_import import import_app
from typer.core import TyperGroup
from typing_extensions import Annotated

from dcx.apply import apply_app
from dcx.enrich import enrich_app
from dcx.exporters import command  # noqa: F401  registers `export snowflake-full` + its exporter
from dcx.importers import registry  # noqa: F401  registers live importers + their CLI commands
from dcx.target import target_app

# Commands defined by dcx (not by upstream datacontract-cli). The migration
# shim that rewrites flags like `--format` and `--schema` was written for
# upstream's `import`/`export` subcommands and must not run for ours. `import`
# is included so the shim doesn't rewrite our live importers' flags at the top
# level; the `import` group still applies the shim to upstream subcommands
# (see `_ImportBypassDcxShim`).
DCX_COMMANDS: set[str] = {"info", "target", "api", "apply", "enrich", "import"}

# Live import subcommands added by dcx (named after the system). Their flags
# (`--schema`, ...) must reach the command untouched by the migration shim.
LIVE_IMPORT_COMMANDS: set[str] = {"snowflake", "kafka"}


class _OrderedCommandsBypassDcxShim(OrderedCommandsWithMigrationHints):
    """Skip the v0.12.0 flag-rename shim for dcx subcommands."""

    def parse_args(self, ctx: Context, args):
        subcommand = next(
            (a for a in args if isinstance(a, str) and not a.startswith("-")), None
        )
        if subcommand in DCX_COMMANDS:
            return TyperGroup.parse_args(self, ctx, args)
        return super().parse_args(ctx, args)


class _ImportBypassDcxShim(OrderedCommandsWithMigrationHints):
    """Within the `import` group: skip the shim for dcx live importers, but keep
    it for upstream subcommands (so `--schema`→`--json-schema` still works there)."""

    def parse_args(self, ctx: Context, args):
        subcommand = next(
            (a for a in args if isinstance(a, str) and not a.startswith("-")), None
        )
        if subcommand in LIVE_IMPORT_COMMANDS:
            return TyperGroup.parse_args(self, ctx, args)
        return super().parse_args(ctx, args)


app.info.cls = _OrderedCommandsBypassDcxShim
import_app.info.cls = _ImportBypassDcxShim


def _version_callback(value: bool) -> None:
    if value:
        import dcx

        typer.echo(dcx.__version__)
        raise typer.Exit()


# Re-register the root callback so `dcx --version` reports dcx's version instead
# of upstream's, which prints the datacontract-cli version. Re-applying
# `@app.callback()` replaces upstream's. Use `dcx info` for both versions.
@app.callback()
def common(
    ctx: Context,
    version: bool = typer.Option(
        None,
        "--version",
        help="Prints the dcx version.",
        callback=_version_callback,
        is_eager=True,
    ),
) -> None:
    """Data Contract eXtended — AI-native, platform-extensible data contracts."""


@app.command("info")
def info() -> None:
    """Show dcx and underlying datacontract-cli versions."""
    import dcx

    typer.echo(f"dcx              {dcx.__version__}")
    typer.echo(f"datacontract-cli {metadata.version('datacontract-cli')}")


app.add_typer(target_app, name="target")
app.add_typer(apply_app, name="apply")
app.add_typer(enrich_app, name="enrich")


# Drop upstream's `api` command so the dcx `api` command below replaces it.
app.registered_commands = [c for c in app.registered_commands if c.name != "api"]


@app.command(
    "api",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
    epilog="Example: dcx api --port 4242 --host 0.0.0.0",
)
def api(
    ctx: typer.Context,
    port: Annotated[int, typer.Option(help="Bind socket to this port.")] = 4242,
    host: Annotated[
        str, typer.Option(help="Bind socket to this host. For docker, use 0.0.0.0."),
    ] = "127.0.0.1",
    reload: Annotated[
        bool, typer.Option(help="Auto-reload on source changes (development only; single worker)."),
    ] = False,
    workers: Annotated[
        Optional[int],
        typer.Option(help="Worker processes to run (production). Ignored when --reload is set."),
    ] = None,
) -> None:
    """Start the dcx REST API server.

    Serves both upstream datacontract-cli routes (lint, test, changelog, export)
    and the dcx routes (target, ...). Swagger UI at http://{host}:{port}/docs.

    Defaults to a single process; pass --workers N for production concurrency, or
    --reload for live-reloading during development.
    """
    import uvicorn
    from uvicorn.config import LOGGING_CONFIG

    log_config = LOGGING_CONFIG
    log_config["root"] = {"level": "INFO"}

    extra_pairs = list(zip(ctx.args[::2], ctx.args[1::2]))
    extra_kwargs = {k.lstrip("-").replace("-", "_"): v for k, v in extra_pairs}

    uvicorn.run(
        app="dcx.serve:app",
        host=host,
        port=port,
        reload=reload,
        workers=workers,
        log_config=log_config,
        **extra_kwargs,
    )


def main() -> None:
    app()


if __name__ == "__main__":
    main()
