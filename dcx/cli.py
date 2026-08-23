import os
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
from dcx import yaml_style  # noqa: F401  multi-line strings dump as block scalars

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


def _report_snowflake_config() -> None:
    """Log which Snowflake config the server will read, and the profiles in it.

    Only called when `--allow-local-credentials` is on, because that is the only
    case where the server reads a config at all. Worth printing even without
    `--snowflake-config`: the connector's default location is `$SNOWFLAKE_HOME`
    (default `~/.snowflake/`) *if that directory exists*, and otherwise the
    platform config dir — so "which file is actually in play" is not obvious.

    Profile names only; never the values inside them.
    """
    try:
        from snowflake.connector.config_manager import CONFIG_MANAGER
        from snowflake.connector.constants import CONFIG_FILE

        profiles = list(CONFIG_MANAGER["connections"])
        default = CONFIG_MANAGER["default_connection_name"]
    except Exception as exc:  # no connector, unreadable file, malformed TOML
        typer.secho(
            f"Warning: server-side profiles are enabled but the Snowflake config "
            f"could not be read ({exc}).",
            err=True, fg=typer.colors.YELLOW,
        )
        return

    if not profiles:
        typer.secho(
            f"Warning: server-side profiles are enabled but {CONFIG_FILE} defines none — "
            "`auth.type: config` requests will fail.",
            err=True, fg=typer.colors.YELLOW,
        )
        return

    suffix = f"; default: {default}" if default in profiles else ""
    typer.secho(
        f"Snowflake config: {CONFIG_FILE} "
        f"({len(profiles)} profile{'s' if len(profiles) != 1 else ''}: "
        f"{', '.join(profiles)}{suffix})",
        err=True, fg=typer.colors.CYAN,
    )
    typer.secho(
        "Server-side profiles enabled: any caller may authenticate as this host via "
        "`auth.type: config`.",
        err=True, fg=typer.colors.YELLOW,
    )


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
    allow_local_credentials: Annotated[
        bool,
        typer.Option(
            "--allow-local-credentials",
            help="Let requests authenticate to Snowflake with a named profile from this "
                 "host's own Snowflake connection config. Off by default — only safe when the "
                 "server is yours alone, since it has no auth of its own.",
        ),
    ] = False,
    snowflake_config: Annotated[
        Optional[str],
        typer.Option(
            "--snowflake-config",
            help="Directory holding the server's config.toml / connections.toml (or the "
                 "file itself). Defaults to the connector's own location. Only meaningful "
                 "with --allow-local-credentials.",
        ),
    ] = None,
) -> None:
    """Start the dcx REST API server.

    Serves both upstream datacontract-cli routes (lint, test, changelog, export)
    and the dcx routes (target, ...). Swagger UI at http://{host}:{port}/docs.

    Defaults to a single process; pass --workers N for production concurrency, or
    --reload for live-reloading during development.

    The live Snowflake endpoints take their credentials from each request, so the
    server holds none. `--allow-local-credentials` opts out of that: it lets a
    request name a connection profile from *this host's* config instead. Do not
    use it on a shared instance — every caller who can reach the port would then
    be able to connect as whoever runs the server.
    """
    import uvicorn
    from uvicorn.config import LOGGING_CONFIG

    from dcx.snowflake_auth import (
        ALLOW_LOCAL_CREDENTIALS_ENV,
        SnowflakeAuthError,
        apply_snowflake_home,
        resolve_snowflake_home,
    )

    if snowflake_config and not allow_local_credentials:
        typer.secho(
            "Error: --snowflake-config only has an effect with --allow-local-credentials; "
            "without it the server never reads a Snowflake config at all.",
            err=True, fg=typer.colors.RED,
        )
        raise typer.Exit(2)

    if allow_local_credentials:
        # Via the environment rather than app state: --workers/--reload run the
        # app in child processes, which inherit env but not in-process globals.
        os.environ[ALLOW_LOCAL_CREDENTIALS_ENV] = "1"

    if snowflake_config:
        try:
            # SNOWFLAKE_HOME is the connector's own knob; `apply_snowflake_home`
            # also re-points an already-imported connector, whose config paths are
            # fixed at import time.
            apply_snowflake_home(resolve_snowflake_home(snowflake_config))
        except SnowflakeAuthError as exc:
            typer.secho(f"Error: {exc}", err=True, fg=typer.colors.RED)
            raise typer.Exit(2)

    if allow_local_credentials:
        # After SNOWFLAKE_HOME is set: the connector resolves its paths on import.
        _report_snowflake_config()

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
