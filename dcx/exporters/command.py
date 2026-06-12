"""`dcx export snowflake-full` — Snowflake setup script (DDL + tags + DQ).

Adds a subcommand to the upstream `dcx export` sub-app. Bypasses the upstream
`_export` helper because that helper only forwards a fixed set of kwargs to the
exporter, and we need to pass custom ones (include_tags, include_quality,
create_tags, tag_namespace, metric_schedule). Instead we call
DataContract.export() directly with the kwargs, and replicate the upstream
capture-or-write branch so the API mirror still works via the
`_export_capture_var` contextvar.

Imported for its side effects by `dcx.cli`.
"""

from pathlib import Path
from typing import Optional

import typer
from datacontract.command_export import export_app
from typing_extensions import Annotated

from dcx.exporters import dbt as _dbt  # noqa: F401  registers the dbt exporter
from dcx.exporters import snowflake  # noqa: F401  registers the snowflake-full exporter
from dcx.exporters.dbt import DbtKind
from dcx.exporters.snowflake import DdlMode

# Replace upstream's `export dbt` removed-shim (renamed to `dbt-models` in v0.12.0)
# with the dcx unified `dbt` command defined below. The upstream `dbt-models`,
# `dbt-sources`, and `dbt-staging-sql` commands are left untouched.
export_app.registered_commands = [
    c for c in export_app.registered_commands if c.name != "dbt"
]


@export_app.command(
    name="snowflake-full",
    epilog="Example: dcx export snowflake-full datacontract.yaml --include-quality --output setup.sql",
)
def _export_snowflake_full(
    location: Annotated[str, typer.Argument(help="Path to the data contract.")] = "datacontract.yaml",
    output: Annotated[
        Optional[Path], typer.Option(help="Write the SQL to this path. Default: stdout."),
    ] = None,
    server: Annotated[
        Optional[str], typer.Option(help="Use this server name from the contract."),
    ] = None,
    schema_name: Annotated[
        str, typer.Option(help="Contract schema to export (default: all)."),
    ] = "all",
    json_schema: Annotated[
        Optional[str], typer.Option(help="Validate the contract against this JSON Schema URL."),
    ] = None,
    ddl_mode: Annotated[
        DdlMode,
        typer.Option(
            "--ddl-mode",
            help="Table handling (matches `apply`): auto = CREATE TABLE IF NOT EXISTS + govern "
            "(default); always = plain CREATE TABLE; never = ALTER/govern existing only.",
        ),
    ] = DdlMode.auto,
    comments: Annotated[
        bool,
        typer.Option(
            "--comments/--no-comments",
            help="Emit COMMENT ON TABLE/COLUMN for descriptions (in auto / never modes).",
        ),
    ] = True,
    structured_types: Annotated[
        bool,
        typer.Option(
            help="Render nested columns as Snowflake structured types "
            "(OBJECT(field type, ...) / ARRAY(type)) instead of bare OBJECT/ARRAY.",
        ),
    ] = False,
    include_tags: Annotated[
        bool, typer.Option(help="Emit ALTER TABLE / MODIFY COLUMN SET TAG statements."),
    ] = True,
    include_quality: Annotated[
        bool,
        typer.Option(
            help=(
                "Emit Snowflake Data Metric Function statements. NOTE: DMFs are a "
                "Snowflake Enterprise-tier feature."
            ),
        ),
    ] = False,
    create_tags: Annotated[
        bool, typer.Option(help="Also emit `CREATE TAG IF NOT EXISTS` for each tag used."),
    ] = False,
    tag_namespace: Annotated[
        Optional[str],
        typer.Option(
            help="Database.schema prefix for tag references (e.g. GOVERNANCE_DB.TAGS).",
        ),
    ] = None,
    metric_schedule: Annotated[
        str, typer.Option(help="DATA_METRIC_SCHEDULE clause to set on each table with DMFs."),
    ] = "USING CRON 0 0 * * * UTC",
    inline_references: Annotated[
        bool, typer.Option(help="Resolve $ref in the contract before exporting."),
    ] = True,
) -> None:
    """Export a Snowflake setup script: DDL + tags + (optional) data quality rules."""
    from datacontract.data_contract import DataContract
    from dcx.api import _export_capture_var

    result = DataContract(
        data_contract_file=location,
        schema_location=json_schema,
        server=server,
        inline_references=inline_references,
    ).export(
        export_format="snowflake-full",
        schema_name=schema_name,
        sql_server_type="snowflake",
        **ddl_mode.to_sql_kwargs(),
        include_comments=comments,
        structured_types=structured_types,
        include_tags=include_tags,
        include_quality=include_quality,
        create_tags=create_tags,
        tag_namespace=tag_namespace,
        metric_schedule=metric_schedule,
    )

    capture = _export_capture_var.get()
    if capture is not None:
        capture["result"] = result
        capture["format"] = "snowflake-full"
        return

    if output is None:
        typer.echo(result, nl=False)
    else:
        if isinstance(result, bytes):
            output.write_bytes(result)
        else:
            output.write_text(result, encoding="utf-8")
        typer.echo(f"Wrote {output}", err=True)


@export_app.command(
    name="dbt",
    epilog="Example: dcx export dbt datacontract.yaml --kind models --output schema.yml",
)
def _export_dbt(
    location: Annotated[str, typer.Argument(help="Path to the data contract.")] = "datacontract.yaml",
    output: Annotated[
        Optional[Path], typer.Option(help="Write the output to this path. Default: stdout."),
    ] = None,
    kind: Annotated[
        DbtKind,
        typer.Option(
            "--kind",
            help="Which dbt artifact to emit: models (schema.yml, default), sources "
            "(sources.yml), or staging (a staging SELECT).",
        ),
    ] = DbtKind.models,
    server: Annotated[
        Optional[str], typer.Option(help="Use this server name from the contract (sets the dbt adapter)."),
    ] = None,
    schema_name: Annotated[
        str, typer.Option(help="Contract schema to export (default: all). Required for --kind staging."),
    ] = "all",
    json_schema: Annotated[
        Optional[str], typer.Option(help="Validate the contract against this JSON Schema URL."),
    ] = None,
    inline_references: Annotated[
        bool, typer.Option(help="Resolve $ref in the contract before exporting."),
    ] = True,
) -> None:
    """Export a contract to dbt: model schema, sources, or a staging SQL file.

    ODCS governance is mapped to idiomatic dbt: `NAME=VALUE` tags and `classification`
    /`businessName`/`criticalDataElement` go to `config.meta`, while bare tags become
    `config.tags`. Schema-level tags land on the model's `config`.
    """
    from datacontract.data_contract import DataContract
    from dcx.api import _export_capture_var

    result = DataContract(
        data_contract_file=location,
        schema_location=json_schema,
        server=server,
        inline_references=inline_references,
    ).export(
        export_format="dbt",
        schema_name=schema_name,
        kind=kind.value,
    )

    capture = _export_capture_var.get()
    if capture is not None:
        capture["result"] = result
        capture["format"] = "dbt"
        return

    if output is None:
        typer.echo(result, nl=False)
    else:
        if isinstance(result, bytes):
            output.write_bytes(result)
        else:
            output.write_text(result, encoding="utf-8")
        typer.echo(f"Wrote {output}", err=True)
