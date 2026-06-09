"""Live-import CLI commands (`dcx import snowflake`, ... `kafka` next).

Registers the live importers into the upstream `importer_factory` and adds the
matching subcommands to the upstream `import_app`. Imported for side effects by
`dcx.cli`. The migration-shim bypass for these subcommands is configured in
`dcx.cli` (so `--schema` etc. reach the command untouched).
"""

import logging
from typing import List, Optional

import typer
from datacontract.cli import debug_option, enable_debug_logging
from datacontract.command_import import (
    _write_result,
    id_option,
    import_app,
    output_option,
    owner_option,
)
from datacontract.data_contract import DataContract
from datacontract.imports.importer_factory import importer_factory
from typing_extensions import Annotated

from dcx.importers.kafka import KafkaImporter
from dcx.importers.snowflake import SnowflakeImporter

importer_factory.register_importer("snowflake", SnowflakeImporter)
importer_factory.register_importer("kafka", KafkaImporter)


def _quiet_aws_credential_noise(debug) -> None:
    """Silence botocore's non-fatal credential/SSO refresh tracebacks.

    `snowflake-connector-python` hard-depends on boto3, so botocore probes the
    local AWS credential chain on connect even when the account is on Azure/GCP
    and AWS is never used. An expired/missing AWS SSO profile then logs a scary
    (but harmless) WARNING+traceback. The live importers never need AWS, so we
    drop that logger to ERROR. Skipped under --debug; real AWS errors in the
    glue/athena importers raise exceptions and are unaffected.
    """
    if not debug:
        logging.getLogger("botocore.credentials").setLevel(logging.ERROR)


@import_app.command(
    name="snowflake",
    epilog="Example: dcx import snowflake --database MY_DB --schema LOAD --output datacontract.yaml",
)
def import_snowflake(
    database: Annotated[str, typer.Option(help="Snowflake database to import from.")],
    schema: Annotated[str, typer.Option("--schema", help="Schema within the database.")],
    table: Annotated[
        Optional[List[str]],
        typer.Option(help="Limit to these tables (repeatable). Default: all tables in the schema."),
    ] = None,
    server_name: Annotated[
        str, typer.Option("--server-name", help="Name for the server entry in the contract.")
    ] = "production",
    account: Annotated[
        Optional[str], typer.Option(help="Account identifier (or SNOWFLAKE_ACCOUNT env var).")
    ] = None,
    user: Annotated[
        Optional[str], typer.Option(help="Username (or SNOWFLAKE_USER env var).")
    ] = None,
    role: Annotated[Optional[str], typer.Option(help="Role to assume.")] = None,
    warehouse: Annotated[Optional[str], typer.Option(help="Warehouse to use for the queries.")] = None,
    authenticator: Annotated[
        Optional[str],
        typer.Option(help="Auth method: externalbrowser (SSO), oauth, snowflake_jwt, ..."),
    ] = None,
    connection_name: Annotated[
        Optional[str],
        typer.Option(help="Named connection profile from ~/.snowflake/config.toml."),
    ] = None,
    tags: Annotated[
        bool,
        typer.Option(
            "--tags/--no-tags",
            help="Import object tags as NAME=VALUE (Enterprise; needs tag read access).",
        ),
    ] = True,
    output: output_option = None,
    owner: owner_option = None,
    id: id_option = None,
    debug: debug_option = None,
):
    """Import a data contract from a live Snowflake schema.

    Reads INFORMATION_SCHEMA, primary keys, and (with --tags) object tags as
    NAME=VALUE. Secrets come from environment variables (SNOWFLAKE_PASSWORD,
    SNOWFLAKE_PRIVATE_KEY_PATH, SNOWFLAKE_TOKEN) — there is no --password flag.
    """
    enable_debug_logging(debug)
    _quiet_aws_credential_noise(debug)
    result = DataContract.import_from_source(
        format="snowflake",
        source=None,
        database=database,
        schema=schema,
        tables=table,
        account=account,
        user=user,
        role=role,
        warehouse=warehouse,
        authenticator=authenticator,
        connection_name=connection_name,
        tags=tags,
        server_name=server_name,
        owner=owner,
        id=id,
    )
    _write_result(result, output)


@import_app.command(
    name="kafka",
    epilog="Example: dcx import kafka --schema-registry https://sr:8081 --topic customers --output datacontract.yaml",
)
def import_kafka(
    topic: Annotated[
        Optional[str],
        typer.Option(help="Kafka topic; subject defaults to <topic>-value."),
    ] = None,
    subject: Annotated[
        Optional[str],
        typer.Option(help="Schema Registry subject (overrides the --topic default)."),
    ] = None,
    schema_registry: Annotated[
        Optional[str],
        typer.Option("--schema-registry", help="Schema Registry base URL (or SCHEMA_REGISTRY_URL env var)."),
    ] = None,
    bootstrap_servers: Annotated[
        Optional[str],
        typer.Option(help="Bootstrap servers to record in the server block (e.g. broker:9092)."),
    ] = None,
    server_name: Annotated[
        str, typer.Option("--server-name", help="Name for the server entry in the contract.")
    ] = "production",
    output: output_option = None,
    owner: owner_option = None,
    id: id_option = None,
    debug: debug_option = None,
):
    """Import a data contract from a Kafka topic's schema (via the Schema Registry).

    Registry basic-auth is read from SCHEMA_REGISTRY_API_KEY /
    SCHEMA_REGISTRY_API_SECRET — there are no secret CLI flags.
    """
    enable_debug_logging(debug)
    _quiet_aws_credential_noise(debug)
    result = DataContract.import_from_source(
        format="kafka",
        source=None,
        topic=topic,
        subject=subject,
        schema_registry=schema_registry,
        bootstrap_servers=bootstrap_servers,
        server_name=server_name,
        owner=owner,
        id=id,
    )
    _write_result(result, output)
