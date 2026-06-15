"""HTTP API for dcx — mirrors Typer commands as FastAPI routes.

Each `dcx target <subcommand>` becomes a `POST /target/<subcommand>` endpoint.
The helper introspects the Typer command's parameters, builds a Pydantic
request model from them, and registers a route. The handler reuses the Typer
command's body (which constructs the Server) — `apply_target` is captured via
a contextvar so no file IO happens — then runs the pure transform with the
contract from the request body.

Format conventions (Phase 2.2):
- Request body: JSON object. The `contract` field accepts either a JSON object
  (parsed ODCS) or a YAML string. All other fields mirror the Typer command's
  CLI flags.
- Response body: JSON by default. Pass `?format=yaml` or set
  `Accept: text/yaml` to get the YAML serialization instead.
- Errors: JSON `{detail: "..."}` (FastAPI default) with appropriate HTTP status.

Concurrency: the handlers are deliberately **sync** (`def`, not `async def`).
Their bodies do blocking work — LLM calls (enrich), Snowflake connections
(import/apply), and CPU/file IO (export/import) — so FastAPI runs each in its
threadpool, keeping one slow request from stalling the event loop. The capture
contextvars are set and read within a single handler invocation (and each
threadpool run gets its own copied context), so they stay request-isolated. Do
not make these `async` unless their bodies become truly non-blocking.
"""

import contextvars
import inspect
import tempfile
import typing
import warnings
from contextlib import contextmanager
from importlib import metadata
from pathlib import Path
from typing import Any, Dict, Optional, Union

import typer
from fastapi import FastAPI, Header, HTTPException, Query, Request, Response
from open_data_contract_standard.model import OpenDataContractStandard
from pydantic import BaseModel, ConfigDict, Field, create_model

from dcx import enrich as enrich_module
from dcx.apply.snowflake import DdlMode
from dcx.target import command as target_module

# Many CLI flags (`--schema`, etc.) intentionally shadow Pydantic BaseModel attributes
# of the same name (e.g. the deprecated v1 `schema()` method). Suppress the noise.
warnings.filterwarnings(
    "ignore",
    message='Field name "schema" in .* shadows an attribute in parent "BaseModel"',
    category=UserWarning,
)


# Pydantic field spec for the `contract` body field shared by /target and /export
# endpoints. Accepts either a JSON object (the parsed ODCS contract) or a YAML
# string. The Union renders both options in the OpenAPI schema so Swagger users
# can see what to send.
_CONTRACT_FIELD = (
    Union[Dict[str, Any], str],
    Field(
        ...,
        description=(
            "The data contract. Send either:\n"
            "- a JSON object (parsed ODCS), or\n"
            "- a YAML string containing the contract as it appears on disk."
        ),
        examples=[
            {
                "apiVersion": "v3.1.0",
                "kind": "DataContract",
                "id": "orders",
                "name": "Orders",
                "version": "1.0.0",
                "status": "draft",
                "schema": [
                    {
                        "name": "orders",
                        "physicalType": "table",
                        "properties": [
                            {"name": "id", "logicalType": "integer", "primaryKey": True},
                            {"name": "amount", "logicalType": "number"},
                        ],
                    }
                ],
            }
        ],
    ),
)

# Typer parameters that are CLI-only and have no API equivalent.
# `location` (the contract file path) is replaced by the `contract` body field.
# `output` (where to write the result) is replaced by the HTTP response.
_CLI_ONLY_PARAMS = {"location", "output"}


# === Response models & shared response docs =================================
# Endpoints return either a contract envelope, the apply outcome, or version
# info. Declaring these as models (rather than bare dicts) gives Swagger a real
# response schema + example, and documenting the error codes completes the spec.

# A representative ODCS contract, reused as the success example for responses.
_CONTRACT_EXAMPLE = {
    "apiVersion": "v3.1.0",
    "kind": "DataContract",
    "id": "orders",
    "name": "Orders",
    "version": "1.0.0",
    "status": "draft",
    "schema": [
        {
            "name": "orders",
            "physicalType": "table",
            "properties": [
                {
                    "name": "id", "logicalType": "integer", "primaryKey": True,
                    "required": True, "unique": True,
                    "description": "Surrogate key uniquely identifying each order.",
                },
                {
                    "name": "amount", "logicalType": "number",
                    "description": "Order total in the account's settlement currency.",
                    "logicalTypeOptions": {"minimum": 0},
                },
            ],
        }
    ],
}


class ContractResponse(BaseModel):
    """A data contract returned as a JSON object (default `application/json`).

    Request `?format=yaml` or send `Accept: text/yaml` to receive the contract as
    a YAML document (media type `text/yaml`) instead.
    """

    contract: Dict[str, Any] = Field(..., description="The resulting ODCS data contract.")

    model_config = ConfigDict(json_schema_extra={"example": {"contract": _CONTRACT_EXAMPLE}})


class ApplySnowflakeResponse(BaseModel):
    """Outcome of an `apply snowflake` run. The SQL is always returned for audit."""

    dry_run: bool = Field(..., description="True when the SQL was returned without executing.")
    statements_executed: int = Field(..., description="Number of statements run (0 when dry_run).")
    account: Optional[str] = Field(None, description="The Snowflake account the SQL targeted.")
    warnings: list[str] = Field(
        default_factory=list,
        description="Schema-drift notes: columns that differ between the contract and existing tables.",
    )
    sql: str = Field(..., description="The full SQL script generated (and executed unless dry_run).")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "dry_run": False,
                "statements_executed": 4,
                "account": "ab12345.eu-west-1",
                "warnings": ["ORDERS: column 'LEGACY_FLAG' exists in Snowflake but not in the contract."],
                "sql": "CREATE TABLE IF NOT EXISTS ORDERS (...); COMMENT ON TABLE ORDERS IS '...';",
            }
        }
    )


class InfoResponse(BaseModel):
    """Installed component versions."""

    dcx: str = Field(..., description="Installed dcx version.")
    datacontract_cli: str = Field(..., description="Underlying datacontract-cli version.")

    model_config = ConfigDict(json_schema_extra={"example": {"dcx": "0.1.0", "datacontract_cli": "0.12.5"}})


class ErrorResponse(BaseModel):
    """FastAPI's standard error envelope."""

    detail: str = Field(..., description="Human-readable error message.")

    model_config = ConfigDict(json_schema_extra={"example": {"detail": "Invalid contract: ..."}})


_ERROR_DESCRIPTIONS = {
    400: "Invalid request — e.g. a malformed contract, tag catalog, or source document.",
    401: "Missing or invalid authentication token.",
    409: "The requested change conflicts with the existing contract.",
    500: "Internal error while capturing the command result.",
    502: "Upstream failure — the LLM provider or Snowflake returned an error.",
}


def _error_responses(*codes: int) -> dict:
    """Build a `responses=` fragment documenting the given error status codes."""
    return {c: {"model": ErrorResponse, "description": _ERROR_DESCRIPTIONS[c]} for c in codes}


# Advertised on the 200 of contract-returning endpoints alongside the JSON model:
# the same contract is also available as a YAML document via content negotiation.
_YAML_ALT_200 = {
    200: {
        "description": "The resulting contract (JSON by default; YAML via `?format=yaml`).",
        "content": {
            "text/yaml": {
                "schema": {"type": "string"},
                "example": "apiVersion: v3.1.0\nkind: DataContract\nid: orders\nname: Orders\n",
            }
        },
    }
}


def _contract_responses(*error_codes: int) -> dict:
    """Responses for an endpoint that returns a contract: YAML alt + error codes."""
    return {**_YAML_ALT_200, **_error_responses(*error_codes)}


# `/export` output is genuinely polymorphic (format-dependent media type), so it
# gets a hand-written 200 rather than a single response model.
_EXPORT_RESPONSES = {
    200: {
        "description": (
            "The exported artifact. The media type depends on the format: "
            "`application/json` (jsonschema/json, or odcs with `?format=json`), "
            "`text/yaml` (odcs), `text/plain` (sql/html/markdown/dbml/…), or "
            "`application/octet-stream` (binary formats such as excel)."
        ),
        "content": {
            "application/json": {"schema": {"type": "object"}},
            "text/yaml": {"schema": {"type": "string"}},
            "text/plain": {"schema": {"type": "string"}},
            "application/octet-stream": {"schema": {"type": "string", "format": "binary"}},
        },
    },
    **_error_responses(400, 500),
}


# === Content negotiation ====================================================


def _wants_yaml(request: Request, format_query: Optional[str]) -> bool:
    if format_query and format_query.lower() == "yaml":
        return True
    accept = request.headers.get("accept", "")
    return "text/yaml" in accept or "application/x-yaml" in accept


def _parse_contract_input(value: Any) -> OpenDataContractStandard:
    """Accept a JSON-decoded dict or a YAML string."""
    if isinstance(value, str):
        try:
            return OpenDataContractStandard.from_string(value)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Invalid YAML contract: {exc}")
    if isinstance(value, dict):
        try:
            return OpenDataContractStandard.model_validate(value)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Invalid contract: {exc}")
    raise HTTPException(
        status_code=400,
        detail=f"`contract` must be a JSON object or YAML string; got {type(value).__name__}",
    )


def _serialize_response(contract: OpenDataContractStandard, as_yaml: bool):
    if as_yaml:
        return Response(content=contract.to_yaml(), media_type="text/yaml")
    return {"contract": contract.model_dump(by_alias=True, exclude_none=True)}


# === Typer introspection ====================================================


def _extract_typer_params(func) -> list[dict]:
    """Return one dict per Typer-decorated parameter: name, annotation, default, required.

    Handles both Typer parameter styles:
      old:     `port: int = typer.Option(5432, "--port")`
      new:     `port: Annotated[int, typer.Option("--port")] = 5432`
    """
    sig = inspect.signature(func)
    params = []
    for name, param in sig.parameters.items():
        default = param.default
        annotation = param.annotation

        # Old-style: typer.Option/Argument is the default value
        if isinstance(default, (typer.models.OptionInfo, typer.models.ArgumentInfo)):
            actual_default = default.default
            required = actual_default is ...  # Typer convention: ... means required
            actual_anno = annotation

        # New-style (Annotated): typer.Option/Argument is in the annotation's metadata
        elif hasattr(annotation, "__metadata__"):
            typer_info = next(
                (m for m in annotation.__metadata__
                 if isinstance(m, (typer.models.OptionInfo, typer.models.ArgumentInfo))),
                None,
            )
            if typer_info is None:
                continue
            actual_anno = typing.get_args(annotation)[0]
            if default is inspect.Parameter.empty:
                actual_default = None
                required = True
            else:
                actual_default = default
                required = False
        else:
            continue

        params.append({
            "name": name,
            "annotation": actual_anno,
            "default": None if required else actual_default,
            "required": required,
        })
    return params


def _build_options_model(name: str, params: list[dict], skip: set[str]) -> type[BaseModel]:
    """Build a nested Pydantic model holding the CLI-derived options."""
    fields: dict[str, tuple] = {}
    for p in params:
        if p["name"] in skip:
            continue
        annotation = p["annotation"]
        if p["required"]:
            fields[p["name"]] = (annotation, ...)
        else:
            fields[p["name"]] = (annotation, p["default"])
    # `protected_namespaces=()` so option fields like `model` (from `dcx enrich`)
    # don't collide with Pydantic's reserved `model_` namespace.
    return create_model(
        name,
        __config__=ConfigDict(extra="forbid", protected_namespaces=()),
        **fields,
    )


def _build_request_model(name: str, params: list[dict]) -> type[BaseModel]:
    """Build a Pydantic request model: {contract, options}."""
    options_model = _build_options_model(f"{name}Options", params, _CLI_ONLY_PARAMS)
    return create_model(
        name,
        __config__=ConfigDict(extra="forbid"),
        contract=_CONTRACT_FIELD,
        options=(options_model, ...),
    )


# === Target endpoint registration ===========================================


@contextmanager
def _capture_target_call():
    """Tell `apply_target` to capture its args into the contextvar instead of doing IO."""
    captured: dict = {}
    token = target_module._target_capture_var.set(captured)
    try:
        yield captured
    finally:
        target_module._target_capture_var.reset(token)


def _run_typer_command_for_api(cmd_func, params: list[dict], body: BaseModel) -> dict:
    """Invoke the Typer command with options-body values, capturing the Server it builds.

    Returns {server, schema_name, overwrite} from the captured apply_target call.
    """
    kwargs: dict[str, Any] = {}
    for p in params:
        if p["name"] == "location":
            kwargs["location"] = Path("__api_dummy__.yaml")
        elif p["name"] == "output":
            kwargs["output"] = None
        else:
            kwargs[p["name"]] = getattr(body.options, p["name"])

    with _capture_target_call() as captured:
        cmd_func(**kwargs)

    if "server" not in captured:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to capture server from {cmd_func.__name__}; did the command not call apply_target?",
        )
    return captured


def mirror_target_to_fastapi(api_app: FastAPI, prefix: str = "/target") -> None:
    """Register one `POST {prefix}/{type}` route per `dcx target <type>` subcommand."""
    for cmd_info in target_module.target_app.registered_commands:
        cmd_name = cmd_info.name
        cmd_func = cmd_info.callback
        params = _extract_typer_params(cmd_func)
        request_model = _build_request_model(
            f"Target{cmd_name.title().replace('_', '')}Request", params,
        )
        summary = (cmd_func.__doc__ or "").strip().split("\n")[0] or None

        # Closure factory binds the per-iteration values
        def _make_handler(cmd_name=cmd_name, cmd_func=cmd_func, params=params, RequestModel=request_model):
            def handler(
                body: RequestModel,
                request: Request,
                format: Optional[str] = Query(None, description="Response format: `json` (default) or `yaml`."),
            ):
                contract = _parse_contract_input(body.contract)
                captured = _run_typer_command_for_api(cmd_func, params, body)
                try:
                    result = target_module.transform_contract_for_target(
                        server=captured["server"],
                        contract=contract,
                        schema_name=captured["schema_name"],
                        overwrite=captured["overwrite"],
                    )
                except target_module.TargetConflictError as exc:
                    raise HTTPException(status_code=409, detail=str(exc))
                return _serialize_response(result, _wants_yaml(request, format))

            return handler

        api_app.post(
            f"{prefix}/{cmd_name}",
            summary=summary,
            tags=["target"],
            response_model=ContractResponse,
            responses=_contract_responses(400, 409),
        )(_make_handler())


# === Import endpoint registration ===========================================

# Contextvar used to capture the result of an import operation. When set, our
# patched `_write_result` (below) stores the ODCS result into the dict instead
# of writing YAML to stdout or a file.
_import_capture_var: contextvars.ContextVar[Optional[dict]] = contextvars.ContextVar(
    "_import_capture_var", default=None,
)


def _install_import_capture() -> None:
    """Monkey-patch upstream `_write_result` once so CLI behavior is unchanged
    but API mode (contextvar set) captures the result instead of writing."""
    # Import datacontract.cli first to fully initialize the upstream module
    # graph — without this, importing command_import standalone hits a
    # circular import because cli.py references command_import.import_app.
    import datacontract.cli  # noqa: F401
    import datacontract.command_import as upstream_import

    if getattr(upstream_import._write_result, "_dcx_patched", False):
        return  # already patched

    original = upstream_import._write_result

    def patched(result, output):
        capture = _import_capture_var.get()
        if capture is not None:
            capture["result"] = result
            return
        return original(result, output)

    patched._dcx_patched = True  # idempotency marker
    upstream_import._write_result = patched


_install_import_capture()


@contextmanager
def _capture_import_call():
    captured: dict = {}
    token = _import_capture_var.set(captured)
    try:
        yield captured
    finally:
        _import_capture_var.reset(token)


# When `source_content` is provided in the request body, we write it to a
# tempfile with a format-appropriate suffix and pass that path as `source`.
# Binary formats (parquet, excel) cannot be sent as JSON strings — users must
# provide a server-accessible URL or path in `source` for those.
_IMPORT_SOURCE_SUFFIX: dict[str, str] = {
    "json":       ".json",
    "jsonschema": ".json",
    "sql":        ".sql",
    "avro":       ".avsc",
    "dbml":       ".dbml",
    "protobuf":   ".proto",
    "csv":        ".csv",
    "odcs":       ".yaml",
    "dbt":        ".json",
}


def _build_import_request_model(name: str, params: list[dict]) -> type[BaseModel]:
    """Build a Pydantic model for an import endpoint: {source_content, options}."""
    options_model = _build_options_model(
        f"{name}Options", params, {"output", "debug"},
    )
    return create_model(
        name,
        __config__=ConfigDict(extra="forbid"),
        source_content=(Optional[str], None),
        options=(options_model, ...),
    )


# Live importers (named after the system) connect to a customer's database using
# the server's credentials — same multi-tenant concern as `apply`. They are
# CLI-only and deliberately not exposed over the API for v1.
_LIVE_IMPORT_FORMATS = {"snowflake", "kafka"}


def mirror_import_to_fastapi(api_app: FastAPI, prefix: str = "/import") -> None:
    """Register one `POST {prefix}/{format}` route per file-based `dcx import` subcommand.

    Live importers (`snowflake`, `kafka`) are skipped — see `_LIVE_IMPORT_FORMATS`.
    """
    from datacontract.command_import import import_app

    for cmd_info in import_app.registered_commands:
        cmd_name = cmd_info.name
        if cmd_name in _LIVE_IMPORT_FORMATS:
            continue
        cmd_func = cmd_info.callback
        params = _extract_typer_params(cmd_func)
        request_model = _build_import_request_model(
            f"Import{cmd_name.title()}Request", params,
        )
        summary = (cmd_func.__doc__ or "").strip().split("\n")[0] or None

        def _make_handler(cmd_name=cmd_name, cmd_func=cmd_func, params=params, RequestModel=request_model):
            def handler(
                body: RequestModel,
                request: Request,
                format: Optional[str] = Query(None, description="Response format: `json` (default) or `yaml`."),
            ):
                tempfile_path: Optional[str] = None
                try:
                    kwargs: dict[str, Any] = {}
                    for p in params:
                        if p["name"] in {"output", "debug"}:
                            kwargs[p["name"]] = None
                        else:
                            kwargs[p["name"]] = getattr(body.options, p["name"], None)

                    if body.source_content is not None:
                        suffix = _IMPORT_SOURCE_SUFFIX.get(cmd_name, ".txt")
                        with tempfile.NamedTemporaryFile(
                            mode="w", suffix=suffix, delete=False, encoding="utf-8",
                        ) as f:
                            f.write(body.source_content)
                            tempfile_path = f.name
                        kwargs["source"] = tempfile_path

                    with _capture_import_call() as captured:
                        try:
                            cmd_func(**kwargs)
                        except typer.Exit as exc:
                            raise HTTPException(
                                status_code=400,
                                detail=f"Import failed (exit code {exc.exit_code})",
                            )
                        except Exception as exc:
                            raise HTTPException(
                                status_code=400, detail=f"Import error: {exc}",
                            )

                    if "result" not in captured:
                        raise HTTPException(
                            status_code=500,
                            detail=f"Failed to capture import result from {cmd_name}",
                        )

                    return _serialize_response(captured["result"], _wants_yaml(request, format))

                finally:
                    if tempfile_path and Path(tempfile_path).exists():
                        Path(tempfile_path).unlink()

            return handler

        api_app.post(
            f"{prefix}/{cmd_name}", summary=summary, tags=["import"],
            response_model=ContractResponse,
            responses=_contract_responses(400, 500),
        )(_make_handler())


# === Info endpoint ===========================================================


# === Export endpoint registration ===========================================

_export_capture_var: contextvars.ContextVar[Optional[dict]] = contextvars.ContextVar(
    "_export_capture_var", default=None,
)


def _install_export_capture() -> None:
    """Monkey-patch upstream `_export` once so API mode captures the result."""
    import datacontract.cli  # noqa: F401  ensure module graph is loaded
    import datacontract.command_export as upstream_export

    if getattr(upstream_export._export, "_dcx_patched", False):
        return

    original = upstream_export._export

    def patched(
        export_format, location, output, server, schema_name, schema,
        sql_server_type="auto", rdf_base=None, engine=None, template=None,
        inline_references=True,
    ):
        capture = _export_capture_var.get()
        if capture is None:
            return original(
                export_format=export_format, location=location, output=output,
                server=server, schema_name=schema_name, schema=schema,
                sql_server_type=sql_server_type, rdf_base=rdf_base, engine=engine,
                template=template, inline_references=inline_references,
            )
        # API mode: compute the result but capture instead of writing.
        from datacontract.data_contract import DataContract
        result = DataContract(
            data_contract_file=location, schema_location=schema, server=server,
            inline_references=inline_references,
        ).export(
            export_format=export_format, schema_name=schema_name, server=server,
            rdf_base=rdf_base, sql_server_type=sql_server_type, engine=engine,
            template=template,
        )
        capture["result"] = result
        capture["format"] = (
            export_format.value if hasattr(export_format, "value") else str(export_format)
        )

    patched._dcx_patched = True
    upstream_export._export = patched


_install_export_capture()


@contextmanager
def _capture_export_call():
    captured: dict = {}
    token = _export_capture_var.set(captured)
    try:
        yield captured
    finally:
        _export_capture_var.reset(token)


def _build_export_request_model(name: str, params: list[dict]) -> type[BaseModel]:
    """Build a Pydantic model for an export endpoint: {contract, options}."""
    options_model = _build_options_model(
        f"{name}Options", params, {"location", "output", "debug", "ctx"},
    )
    return create_model(
        name,
        __config__=ConfigDict(extra="forbid"),
        contract=_CONTRACT_FIELD,
        options=(options_model, ...),
    )


def _serialize_export_response(
    captured: dict, request: Request, format_query: Optional[str],
):
    """Serialize an export result based on its native type and any format preference."""
    import yaml as yamllib

    result = captured["result"]
    export_format = captured.get("format", "")
    fmt = (format_query or "").lower()
    accept = request.headers.get("accept", "")
    wants_yaml = fmt == "yaml" or "text/yaml" in accept or "application/x-yaml" in accept
    wants_json = fmt == "json" or "application/json" in accept

    if isinstance(result, bytes):
        return Response(content=result, media_type="application/octet-stream")

    if isinstance(result, dict):
        if wants_yaml:
            return Response(content=yamllib.safe_dump(result), media_type="text/yaml")
        return result

    if isinstance(result, str):
        # ODCS export is YAML text — parse to JSON object on request.
        if export_format == "odcs" and wants_json:
            return {"contract": yamllib.safe_load(result)}
        # JSON Schema export is already a JSON document.
        if export_format in {"jsonschema", "json"}:
            return Response(content=result, media_type="application/json")
        # Everything else is its own text format.
        return Response(content=result, media_type="text/plain")

    raise HTTPException(
        status_code=500,
        detail=f"Unexpected export result type: {type(result).__name__}",
    )


def mirror_export_to_fastapi(api_app: FastAPI, prefix: str = "/export") -> None:
    """Register one `POST {prefix}/{format}` route per `dcx export <format>` subcommand."""
    from datacontract.command_export import export_app
    from dcx.exporters import command  # noqa: F401  registers `export snowflake-full` on export_app

    for cmd_info in export_app.registered_commands:
        cmd_name = cmd_info.name
        cmd_func = cmd_info.callback
        params = _extract_typer_params(cmd_func)
        # Skip the dbt removal shim (just takes ctx, no real export logic)
        if not any(p["name"] == "location" for p in params):
            continue

        request_model = _build_export_request_model(
            f"Export{cmd_name.title().replace('-', '')}Request", params,
        )
        summary = (cmd_func.__doc__ or "").strip().split("\n")[0] or None

        def _make_handler(cmd_name=cmd_name, cmd_func=cmd_func, params=params, RequestModel=request_model):
            def handler(
                body: RequestModel,
                request: Request,
                format: Optional[str] = Query(
                    None,
                    description="Response format override (e.g. `json` to parse ODCS YAML into JSON).",
                ),
            ):
                contract_yaml = _contract_to_yaml_string(body.contract)
                tempfile_path: Optional[str] = None
                try:
                    with tempfile.NamedTemporaryFile(
                        mode="w", suffix=".yaml", delete=False, encoding="utf-8",
                    ) as f:
                        f.write(contract_yaml)
                        tempfile_path = f.name

                    kwargs: dict[str, Any] = {}
                    for p in params:
                        if p["name"] == "location":
                            kwargs["location"] = tempfile_path
                        elif p["name"] in {"output", "debug"}:
                            kwargs[p["name"]] = None
                        else:
                            kwargs[p["name"]] = getattr(body.options, p["name"], None)

                    with _capture_export_call() as captured:
                        try:
                            cmd_func(**kwargs)
                        except typer.Exit as exc:
                            raise HTTPException(
                                status_code=400,
                                detail=f"Export failed (exit code {exc.exit_code})",
                            )
                        except Exception as exc:
                            raise HTTPException(status_code=400, detail=f"Export error: {exc}")

                    if "result" not in captured:
                        raise HTTPException(
                            status_code=500,
                            detail=f"Failed to capture export result from {cmd_name}",
                        )
                    return _serialize_export_response(captured, request, format)

                finally:
                    if tempfile_path and Path(tempfile_path).exists():
                        Path(tempfile_path).unlink()

            return handler

        api_app.post(
            f"{prefix}/{cmd_name}", summary=summary, tags=["export"],
            responses=_EXPORT_RESPONSES,
        )(_make_handler())


def _contract_to_yaml_string(value: Any) -> str:
    """Accept dict or YAML string; produce a YAML string for upstream readers."""
    import yaml as yamllib

    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        return yamllib.safe_dump(value)
    raise HTTPException(
        status_code=400,
        detail=f"`contract` must be a JSON object or YAML string; got {type(value).__name__}",
    )


# === Live Snowflake import (caller-supplied OAuth) ==========================
# Unlike the file-based importers, this is exposed with per-caller auth: the
# caller sends their own Snowflake OAuth bearer token in the `Authorization`
# header, so the server connects on their behalf and never uses ambient/server
# credentials. (The CLI `import snowflake` supports more auth methods, including
# interactive externalbrowser, which make no sense server-side.)


def _bearer_token(authorization: Optional[str]) -> Optional[str]:
    if not authorization:
        return None
    parts = authorization.split(None, 1)
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1].strip()
    return None


class SnowflakeImportRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    account: str = Field(..., description="Snowflake account identifier.")
    schema_: str = Field(..., alias="schema", description="Schema to import.")
    database: str = Field(..., description="Database to import from.")
    tables: Optional[list[str]] = Field(None, description="Limit to these tables. Default: all.")
    role: Optional[str] = Field(None, description="Role to assume.")
    warehouse: Optional[str] = Field(None, description="Warehouse for the queries.")
    tags: bool = Field(True, description="Import object tags as NAME=VALUE.")
    server_name: str = Field("production", description="Name for the server entry in the contract.")


def mirror_snowflake_import_to_fastapi(api_app: FastAPI, prefix: str = "/import") -> None:
    """Register `POST {prefix}/snowflake` authenticated by a caller OAuth token."""

    @api_app.post(
        f"{prefix}/snowflake",
        tags=["import"],
        summary="Import a contract from a live Snowflake schema (OAuth bearer token).",
        response_model=ContractResponse,
        responses=_contract_responses(401, 502),
    )
    def import_snowflake_endpoint(
        body: SnowflakeImportRequest,
        request: Request,
        authorization: Optional[str] = Header(
            None, description="Snowflake OAuth token: `Authorization: Bearer <token>`."
        ),
        format: Optional[str] = Query(None, description="Response format: `json` (default) or `yaml`."),
    ):
        from dcx.importers import snowflake as snowflake_import

        token = _bearer_token(authorization)
        if not token:
            raise HTTPException(
                status_code=401,
                detail="Provide a Snowflake OAuth token via 'Authorization: Bearer <token>'.",
            )
        try:
            contract = snowflake_import.import_snowflake_oauth(
                token=token,
                account=body.account,
                database=body.database,
                schema=body.schema_,
                tables=body.tables,
                role=body.role,
                warehouse=body.warehouse,
                tags=body.tags,
                server_name=body.server_name,
            )
        except snowflake_import.SnowflakeImportError as exc:
            raise HTTPException(status_code=502, detail=str(exc))

        return _serialize_response(contract, _wants_yaml(request, format))


# === Live Snowflake apply (caller-supplied OAuth) ==========================
# Closes the loop over HTTP: write tags + data-quality back to the caller's
# Snowflake using their own OAuth token. Defaults to alter-only (no CREATE
# TABLE), so it is safe against existing tables. `dry_run` returns the SQL for
# review and needs no token.


class ApplySnowflakeRequestOptions(BaseModel):
    """Apply options — the body's `options` object, mirroring the CLI flags. Matches
    the `{contract, options}` shape produced for the auto-generated endpoints."""

    model_config = ConfigDict(extra="forbid")

    server_name: Optional[str] = Field(None, description="Named server from the contract.")
    account: Optional[str] = Field(None, description="Override account (else from server block).")
    role: Optional[str] = Field(None, description="Role to assume (needs APPLY TAG / table ownership).")
    warehouse: Optional[str] = Field(None, description="Override warehouse (else from server block).")
    dry_run: bool = Field(False, description="Return the SQL without executing (no token needed).")
    ddl_mode: DdlMode = Field(
        DdlMode.auto,
        description=(
            "Table handling: `auto` (default) creates the table if missing else governs "
            "the existing one; `always` emits CREATE TABLE (errors if it exists); `never` "
            "governs existing tables only."
        ),
    )
    strict: bool = Field(
        False,
        description="Fail (502) instead of warning when an existing table's schema differs from the contract.",
    )
    structured_types: bool = Field(
        False,
        description=(
            "Render nested columns as Snowflake structured types "
            "(OBJECT(field type, ...) / ARRAY(type)) instead of bare OBJECT/ARRAY."
        ),
    )
    include_comments: bool = Field(
        True, description="Emit COMMENT ON TABLE/COLUMN for descriptions (applies to existing tables)."
    )
    include_tags: bool = Field(True, description="Emit SET TAG statements.")
    include_quality: bool = Field(True, description="Emit Data Metric Function statements (Enterprise).")
    create_tags: bool = Field(False, description="Also emit CREATE TAG IF NOT EXISTS.")
    tag_namespace: Optional[str] = Field(None, description="Database.schema prefix for tag references.")
    tag_namespace_filter: Optional[list[str]] = Field(
        None,
        description="Only emit tags whose namespace (DB.SCHEMA) is in this list; "
        "un-namespaced tags are skipped. Omit to emit all tags.",
    )
    metric_schedule: str = Field(
        "USING CRON 0 0 * * * UTC", description="DATA_METRIC_SCHEDULE clause for DMF tables."
    )


# Built with `create_model` (like the auto-generated endpoints) so it reuses the
# shared `_CONTRACT_FIELD` and lands on the same `{contract, options}` body shape.
ApplySnowflakeRequest = create_model(
    "ApplySnowflakeRequest",
    __config__=ConfigDict(extra="forbid"),
    contract=_CONTRACT_FIELD,
    options=(ApplySnowflakeRequestOptions, ...),
)


def mirror_apply_snowflake_to_fastapi(api_app: FastAPI, prefix: str = "/apply") -> None:
    """Register `POST {prefix}/snowflake` authenticated by a caller OAuth token."""

    @api_app.post(
        f"{prefix}/snowflake",
        tags=["apply"],
        summary="Apply tags + data quality to live Snowflake (OAuth bearer token).",
        response_model=ApplySnowflakeResponse,
        responses=_error_responses(401, 502),
    )
    def apply_snowflake_endpoint(
        body: ApplySnowflakeRequest,
        authorization: Optional[str] = Header(
            None, description="Snowflake OAuth token: `Authorization: Bearer <token>`."
        ),
    ):
        from dcx.apply import snowflake as apply_module

        opts = body.options
        token = _bearer_token(authorization)
        if not opts.dry_run and not token:
            raise HTTPException(
                status_code=401,
                detail="Provide a Snowflake OAuth token via 'Authorization: Bearer <token>'.",
            )
        contract = _parse_contract_input(body.contract)
        try:
            result = apply_module.apply_snowflake_oauth(
                contract,
                token=token or "",
                server_name=opts.server_name,
                account=opts.account,
                role=opts.role,
                warehouse=opts.warehouse,
                dry_run=opts.dry_run,
                ddl_mode=opts.ddl_mode,
                strict=opts.strict,
                structured_types=opts.structured_types,
                include_comments=opts.include_comments,
                include_tags=opts.include_tags,
                include_quality=opts.include_quality,
                create_tags=opts.create_tags,
                tag_namespace=opts.tag_namespace,
                tag_namespace_filter=opts.tag_namespace_filter,
                metric_schedule=opts.metric_schedule,
            )
        except apply_module.ApplyError as exc:
            raise HTTPException(status_code=502, detail=str(exc))

        # Always include the SQL so callers can review/audit what ran.
        return {
            "dry_run": result["dry_run"],
            "statements_executed": result["statements_executed"],
            "account": result.get("account"),
            "warnings": result.get("warnings") or [],
            "sql": result["sql"],
        }


# === Enrich endpoint registration ===========================================


@contextmanager
def _capture_enrich_call():
    """Tell `apply_enrich` to capture its settings into the contextvar (no IO/LLM)."""
    captured: dict = {}
    token = enrich_module._enrich_capture_var.set(captured)
    try:
        yield captured
    finally:
        enrich_module._enrich_capture_var.reset(token)


def mirror_enrich_to_fastapi(api_app: FastAPI, prefix: str = "/enrich") -> None:
    """Register one `POST {prefix}/{subcommand}` route per `dcx enrich <sub>` command.

    The provider API key comes from the *server's* environment (litellm reads the
    standard provider env var); callers never send credentials.
    """
    # Contract+options enrich subcommands share the same body shape and capture
    # var; only the core transform differs, so we dispatch by name. `tags` is
    # handled separately (its catalog must be inline in the request body).
    cores = {
        "columns": enrich_module.enrich_columns_contract,
        "quality": enrich_module.enrich_quality_contract,
    }
    for cmd_info in enrich_module.enrich_app.registered_commands:
        cmd_name = cmd_info.name
        if cmd_name not in cores:
            continue
        cmd_func = cmd_info.callback
        params = _extract_typer_params(cmd_func)
        request_model = _build_request_model(
            f"Enrich{cmd_name.title()}Request", params,
        )
        summary = (cmd_func.__doc__ or "").strip().split("\n")[0] or None

        def _make_handler(cmd_name=cmd_name, cmd_func=cmd_func, params=params, RequestModel=request_model, core=cores[cmd_name]):
            def handler(
                body: RequestModel,
                request: Request,
                format: Optional[str] = Query(None, description="Response format: `json` (default) or `yaml`."),
            ):
                contract = _parse_contract_input(body.contract)

                kwargs: dict[str, Any] = {}
                for p in params:
                    if p["name"] == "location":
                        kwargs["location"] = "__api_dummy__.yaml"
                    elif p["name"] == "output":
                        kwargs["output"] = None
                    else:
                        kwargs[p["name"]] = getattr(body.options, p["name"])

                with _capture_enrich_call() as captured:
                    cmd_func(**kwargs)

                settings = captured.get("settings")
                if settings is None:
                    raise HTTPException(
                        status_code=500,
                        detail=f"Failed to capture settings from {cmd_name}.",
                    )
                try:
                    result = core(contract, settings)
                except enrich_module.EnrichError as exc:
                    # Upstream LLM/config problem — 502 Bad Gateway is the closest fit.
                    raise HTTPException(status_code=502, detail=str(exc))

                return _serialize_response(result, _wants_yaml(request, format))

            return handler

        api_app.post(
            f"{prefix}/{cmd_name}", summary=summary, tags=["enrich"],
            response_model=ContractResponse,
            responses=_contract_responses(400, 500, 502),
        )(_make_handler())


# === Enrich-tags endpoint (catalog inline) ==================================

_TAG_CATALOG_FIELD = (
    Union[Dict[str, Any], str],
    Field(
        ...,
        description=(
            "The controlled tag catalog. Send a JSON object (or YAML string) with a "
            "`tags` list; each tag has a `name`, optional `multiple`, and `values` "
            "(each with `value`, optional `description`/`examples`, and optional "
            "`default: true` — assigned when the model picks no value for that tag)."
        ),
        examples=[
            {
                "tags": [
                    {
                        "name": "DATA_CLASSIFICATION",
                        "description": "Sensitivity of the column.",
                        "multiple": False,
                        "values": [
                            {"value": "CONFIDENTIAL", "description": "Personal or sensitive business data.",
                             "examples": ["email", "phone", "full_name"]},
                            {"value": "PUBLIC", "description": "Non-sensitive, shareable data."},
                        ],
                    }
                ]
            }
        ],
    ),
)


def mirror_enrich_tags_to_fastapi(api_app: FastAPI, prefix: str = "/enrich") -> None:
    """Register `POST {prefix}/tags`. Body: {contract, catalog, options}.

    The tag catalog is sent inline (the CLI's `--catalog` is a file path, which
    has no API equivalent). The provider API key comes from the server's env.
    """
    cmd_info = next(
        (c for c in enrich_module.enrich_app.registered_commands if c.name == "tags"), None,
    )
    if cmd_info is None:
        return
    cmd_func = cmd_info.callback
    params = _extract_typer_params(cmd_func)
    options_model = _build_options_model(
        "EnrichTagsRequestOptions", params, {"location", "output", "catalog"},
    )
    request_model = create_model(
        "EnrichTagsRequest",
        __config__=ConfigDict(extra="forbid", protected_namespaces=()),
        contract=_CONTRACT_FIELD,
        catalog=_TAG_CATALOG_FIELD,
        options=(options_model, ...),
    )
    summary = (cmd_func.__doc__ or "").strip().split("\n")[0] or None

    def handler(
        body: request_model,  # type: ignore[valid-type]
        request: Request,
        format: Optional[str] = Query(None, description="Response format: `json` (default) or `yaml`."),
    ):
        contract = _parse_contract_input(body.contract)
        try:
            catalog = enrich_module.parse_tag_catalog(body.catalog)
        except enrich_module.EnrichError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

        opts = body.options
        settings = enrich_module.EnrichSettings(
            model=opts.model,
            base_url=opts.base_url,
            schema_name=opts.schema_name,
            overwrite=opts.overwrite,
            instructions=opts.instructions,
            debug=opts.debug,
        )
        try:
            result = enrich_module.enrich_tags_contract(contract, settings, catalog)
        except enrich_module.EnrichError as exc:
            raise HTTPException(status_code=502, detail=str(exc))

        return _serialize_response(result, _wants_yaml(request, format))

    api_app.post(
        f"{prefix}/tags", summary=summary, tags=["enrich"],
        response_model=ContractResponse,
        responses=_contract_responses(400, 502),
    )(handler)


def mirror_enrich_all_to_fastapi(api_app: FastAPI, prefix: str = "/enrich") -> None:
    """Register `POST {prefix}/all`. Body: {contract, catalog?, options}.

    Runs columns → tags (if `catalog` given) → quality. The catalog is optional
    and inline; provider API key comes from the server's env.
    """
    cmd_info = next(
        (c for c in enrich_module.enrich_app.registered_commands if c.name == "all"), None,
    )
    if cmd_info is None:
        return
    cmd_func = cmd_info.callback
    params = _extract_typer_params(cmd_func)
    options_model = _build_options_model(
        "EnrichAllRequestOptions", params, {"location", "output", "catalog"},
    )
    request_model = create_model(
        "EnrichAllRequest",
        __config__=ConfigDict(extra="forbid", protected_namespaces=()),
        contract=_CONTRACT_FIELD,
        catalog=(Optional[Union[Dict[str, Any], str]], Field(
            None,
            description="Optional tag catalog (same shape as /enrich/tags). Omit to skip tagging.",
        )),
        options=(options_model, ...),
    )
    summary = (cmd_func.__doc__ or "").strip().split("\n")[0] or None

    def handler(
        body: request_model,  # type: ignore[valid-type]
        request: Request,
        format: Optional[str] = Query(None, description="Response format: `json` (default) or `yaml`."),
    ):
        contract = _parse_contract_input(body.contract)

        catalog = None
        if body.catalog is not None:
            try:
                catalog = enrich_module.parse_tag_catalog(body.catalog)
            except enrich_module.EnrichError as exc:
                raise HTTPException(status_code=400, detail=str(exc))

        opts = body.options
        settings = enrich_module.EnrichSettings(
            model=opts.model,
            base_url=opts.base_url,
            schema_name=opts.schema_name,
            overwrite=opts.overwrite,
            enrich_descriptions=opts.descriptions,
            enrich_type_options=opts.type_options,
            instructions=opts.instructions,
            debug=opts.debug,
        )
        try:
            result = enrich_module.enrich_all_contract(contract, settings, catalog)
        except enrich_module.EnrichError as exc:
            raise HTTPException(status_code=502, detail=str(exc))

        return _serialize_response(result, _wants_yaml(request, format))

    api_app.post(
        f"{prefix}/all", summary=summary, tags=["enrich"],
        response_model=ContractResponse,
        responses=_contract_responses(400, 502),
    )(handler)


# === Info endpoint ===========================================================


def mount_info_endpoint(api_app: FastAPI) -> None:
    """Register `GET /info` returning the dcx + datacontract-cli versions."""

    @api_app.get(
        "/info", tags=["info"], summary="Get dcx and datacontract-cli versions.",
        response_model=InfoResponse,
    )
    def info() -> dict:
        import dcx

        return {
            "dcx": dcx.__version__,
            "datacontract_cli": metadata.version("datacontract-cli"),
        }


_OPENAPI_TAGS = [
    {"name": "target", "description": "Bind a contract to a platform by setting its server block."},
    {"name": "import", "description": "Create a contract from a source — a file/document or a live system."},
    {"name": "export", "description": "Convert a contract to a target format (SQL, JSON Schema, HTML, …)."},
    {"name": "apply", "description": "Apply tags + data quality to a live system (Snowflake)."},
    {"name": "enrich", "description": "LLM enrichment: descriptions, type options, tags, and data quality."},
    {"name": "info", "description": "Component version information."},
]

_API_DESCRIPTION = (
    "AI-native data contracts over HTTP. Each endpoint mirrors a `dcx` CLI command.\n\n"
    "**Request body:** a JSON object. The `contract` field accepts either a parsed "
    "ODCS JSON object or a YAML string.\n\n"
    "**Response:** JSON by default; request `?format=yaml` or send `Accept: text/yaml` "
    "to receive YAML. Errors use `{\"detail\": \"...\"}`.\n\n"
    "**Auth:** `/apply/*` and `/import/snowflake` require a caller Snowflake OAuth token "
    "via `Authorization: Bearer <token>`. `/enrich/*` use the server's provider API key."
)


def build_dcx_api_app() -> FastAPI:
    """Build a standalone FastAPI app with dcx routes mounted (for testing)."""
    import dcx

    app = FastAPI(
        title="dcx API",
        version=dcx.__version__,
        description=_API_DESCRIPTION,
        openapi_tags=_OPENAPI_TAGS,
    )
    mirror_target_to_fastapi(app)
    mirror_import_to_fastapi(app)
    mirror_snowflake_import_to_fastapi(app)
    mirror_export_to_fastapi(app)
    mirror_apply_snowflake_to_fastapi(app)
    mirror_enrich_to_fastapi(app)
    mirror_enrich_tags_to_fastapi(app)
    mirror_enrich_all_to_fastapi(app)
    mount_info_endpoint(app)
    return app
