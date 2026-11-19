"""Application logging configuration and safe Snowflake diagnostics."""

import logging
import re
from copy import deepcopy
from time import perf_counter
from contextlib import contextmanager


_ERRORS = {
    "SF_AUTH_FAILED": "Snowflake authentication failed. Check your credentials.",
    "SF_ACCESS_DENIED": "Insufficient permissions to complete the operation.",
    "SF_WAREHOUSE_UNAVAILABLE": "Snowflake compute resources are unavailable.",
    "SF_OBJECT_UNAVAILABLE": "The target resource does not exist or is not accessible.",
    "SF_CONNECTION_FAILED": "Unable to connect to Snowflake.",
    "SF_TIMEOUT": "The operation timed out. Verify the outcome before retrying writes.",
    "SF_SCHEMA_DRIFT": "The live schema differs from the contract.",
    "SF_OPERATION_FAILED": "The Snowflake operation could not be completed.",
}

_STAGES = {"operation", "connect", "session", "metadata", "drift", "execute", "tags", "quality"}


class SnowflakeLogger(logging.LoggerAdapter):
    def __init__(self, logger: logging.Logger, operation: str):
        super().__init__(logger)
        self.prefix = f"platform=snowflake operation={operation}"

    def process(self, msg, kwargs):
        return f"{self.prefix} {msg}", kwargs


def _stage(exc: Exception, default: str) -> str:
    value = getattr(exc, "safe_stage", default)
    return value if value in _STAGES else "operation"


def api_log_config(base: dict, level: str = "INFO") -> dict:
    level = level.upper()
    if level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
        raise ValueError("Application log level must be DEBUG, INFO, WARNING, ERROR or CRITICAL.")
    config = deepcopy(base)
    config["formatters"]["dcx"] = {
        "format": "%(asctime)s %(levelname)s %(name)s %(message)s",
    }
    config["handlers"]["dcx"] = {
        "class": "logging.StreamHandler",
        "stream": "ext://sys.stderr",
        "formatter": "dcx",
    }
    config["root"] = {"handlers": ["dcx"], "level": "INFO"}
    config["loggers"]["dcx"] = {"handlers": [], "level": level, "propagate": True}
    return config


def _exception_chain(exc: Exception):
    seen = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        yield exc
        exc = exc.__cause__ if exc.__cause__ is not None else exc.__context__


def snowflake_error(exc: Exception, operation: str, stage: str = "operation") -> dict:
    """Classify by structured attributes, never by potentially sensitive messages."""
    code = "SF_OPERATION_FAILED"
    for error in _exception_chain(exc):
        explicit = getattr(error, "safe_code", None)
        raw_errno = str(getattr(error, "errno", ""))
        errno = str(int(raw_errno)) if re.fullmatch(r"[0-9]{1,6}", raw_errno) else ""
        state = str(getattr(error, "sqlstate", ""))
        if not re.fullmatch(r"[0-9A-Z]{5}", state):
            state = ""
        if explicit in _ERRORS:
            code = explicit
        elif isinstance(error, TimeoutError) or state == "HYT00" or errno == "604":
            code = "SF_TIMEOUT"
        elif state.startswith("28") or errno in {"390100", "390114", "390112"}:
            code = "SF_AUTH_FAILED"
        elif state == "42501" or errno == "3001":
            code = "SF_ACCESS_DENIED"
        elif errno == "2003":
            code = "SF_OBJECT_UNAVAILABLE"
        elif errno == "606":
            code = "SF_WAREHOUSE_UNAVAILABLE"
        elif isinstance(error, ConnectionError) or state.startswith("08"):
            code = "SF_CONNECTION_FAILED"
        else:
            continue
        break
    return {
        "code": code,
        "message": _ERRORS[code],
        "operation": operation if operation in {"import", "apply"} else "operation",
        "stage": _stage(exc, stage),
        "retryable": code in {"SF_CONNECTION_FAILED", "SF_TIMEOUT"} and operation == "import",
    }


def log_snowflake_error(logger: logging.Logger, exc: Exception, operation: str) -> dict:
    detail = snowflake_error(exc, operation)
    frames = []
    error_code = "unknown"
    sqlstate = "unknown"
    error_type = "Exception"
    for error in _exception_chain(exc):
        name = type(error).__name__
        if name in {"ProgrammingError", "OperationalError", "DatabaseError", "InterfaceError",
                    "TimeoutError", "ConnectionError", "ValueError", "RuntimeError"}:
            error_type = name
        raw_code = str(getattr(error, "errno", ""))
        raw_state = str(getattr(error, "sqlstate", ""))
        if re.fullmatch(r"[0-9]{1,6}", raw_code):
            error_code = raw_code
        if re.fullmatch(r"[0-9A-Z]{5}", raw_state):
            sqlstate = raw_state
        traceback = error.__traceback__
        while traceback is not None:
            frame = traceback.tb_frame
            module = frame.f_globals.get("__name__", "")
            if module.startswith("dcx."):
                frames.append(f"{module}.{frame.f_code.co_name}:{traceback.tb_lineno}")
            traceback = traceback.tb_next
    action = _snowflake_action(detail["stage"], getattr(exc, "statement_index", None))
    SnowflakeLogger(logger, detail["operation"]).error(
        "%s event=failed code=%s error_type=%s error_code=%s sqlstate=%s stack=%s",
        action, detail["code"], error_type, error_code, sqlstate,
        " > ".join(frames) or "unavailable",
    )
    return detail


def _snowflake_action(stage: str, statement_index: int | None) -> str:
    action = f"stage={stage}"
    if type(statement_index) is int and statement_index > 0:
        action += f" statement_index={statement_index}"
    return action


@contextmanager
def snowflake_stage(
    logger: SnowflakeLogger, stage: str, *, statement_index: int | None = None,
):
    action = _snowflake_action(stage, statement_index)
    started = perf_counter()
    logger.debug("%s event=started", action)
    try:
        yield
    except Exception as exc:
        if not hasattr(exc, "safe_stage"):
            exc.safe_stage = stage
        raise
    else:
        level = logging.INFO if stage == "connect" else logging.DEBUG
        logger.log(level, "%s event=completed duration_ms=%.1f", action, (perf_counter() - started) * 1000)