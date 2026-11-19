import io
import logging
import unittest
import subprocess
import sys

from dcx.logging_config import api_log_config, log_snowflake_error, snowflake_error, snowflake_stage


class LoggingTests(unittest.TestCase):
    def test_real_configuration_filters_and_emits_once(self):
        code = "\n".join([
            "import logging",
            "from logging.config import dictConfig",
            "from uvicorn.config import LOGGING_CONFIG",
            "from dcx.logging_config import api_log_config",
            "dictConfig(api_log_config(LOGGING_CONFIG))",
            "logger = logging.getLogger('dcx.importers.snowflake')",
            "logger.debug('hidden_debug_event')",
            "logger.info('visible_info_event')",
            "dictConfig(api_log_config(LOGGING_CONFIG, 'DEBUG'))",
            "logger.debug('visible_debug_event')",
        ])
        result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "")
        self.assertNotIn("hidden_debug_event", result.stderr)
        self.assertEqual(result.stderr.count("visible_info_event"), 1)
        self.assertEqual(result.stderr.count("visible_debug_event"), 1)
        self.assertIn("INFO dcx.importers.snowflake", result.stderr)

    def test_config_copies_base(self):
        base = {"formatters": {}, "handlers": {}, "loggers": {}}
        config = api_log_config(base, "debug")
        self.assertEqual(config["loggers"]["dcx"]["level"], "DEBUG")
        self.assertEqual(config["root"]["handlers"], ["dcx"])
        self.assertEqual(base, {"formatters": {}, "handlers": {}, "loggers": {}})
        with self.assertRaises(ValueError):
            api_log_config(base, "not-a-level")

    def test_structured_mapping_and_safe_logs(self):
        output = io.StringIO()
        logger = logging.Logger("dcx.test")
        logger.addHandler(logging.StreamHandler(output))
        secret = "password=SECRET SELECT * FROM PRIVATE_DB.PRIVATE_TABLE"
        try:
            try:
                cause = RuntimeError(secret)
                cause.errno = 3001
                cause.sqlstate = "42501"
                raise cause
            except RuntimeError as cause:
                raise ValueError(secret) from cause
        except ValueError as exc:
            detail = log_snowflake_error(logger, exc, "apply")
        self.assertEqual(detail["code"], "SF_ACCESS_DENIED")
        self.assertFalse(detail["retryable"])
        self.assertNotIn(secret, output.getvalue())
        self.assertNotIn("PRIVATE", str(detail))
        self.assertIn("error_code=3001", output.getvalue())

    def test_unknown_and_timeout(self):
        self.assertEqual(snowflake_error(ValueError("secret"), "apply")["code"], "SF_OPERATION_FAILED")
        self.assertFalse(snowflake_error(TimeoutError(), "apply")["retryable"])
        self.assertTrue(snowflake_error(TimeoutError(), "import")["retryable"])

    def test_mapping_uses_structured_fields(self):
        cases = [
            ({"errno": "003001"}, "SF_ACCESS_DENIED"),
            ({"errno": 2003}, "SF_OBJECT_UNAVAILABLE"),
            ({"errno": 606}, "SF_WAREHOUSE_UNAVAILABLE"),
            ({"errno": 604}, "SF_TIMEOUT"),
            ({"errno": 390114}, "SF_AUTH_FAILED"),
            ({"sqlstate": "08001"}, "SF_CONNECTION_FAILED"),
            ({"sqlstate": "28000"}, "SF_AUTH_FAILED"),
            ({"safe_code": "SF_SCHEMA_DRIFT"}, "SF_SCHEMA_DRIFT"),
            ({"sqlstate": "08001 SECRET"}, "SF_OPERATION_FAILED"),
        ]
        for attributes, expected in cases:
            with self.subTest(attributes=attributes):
                error = RuntimeError("password=SECRET")
                for key, value in attributes.items():
                    setattr(error, key, value)
                self.assertEqual(snowflake_error(error, "apply")["code"], expected)

    def test_suppressed_chain_cycles_and_untrusted_attributes(self):
        cause = ValueError("password=SECRET")
        cause.errno = 3001
        wrapped = RuntimeError("PRIVATE_DB")
        wrapped.__cause__ = cause
        wrapped.__suppress_context__ = True
        cause.__context__ = wrapped
        wrapped.safe_stage = "PRIVATE_WAREHOUSE"
        detail = snowflake_error(wrapped, "apply")
        self.assertEqual(detail["code"], "SF_ACCESS_DENIED")
        self.assertEqual(detail["stage"], "operation")
        self.assertNotIn("PRIVATE", str(detail))

    def test_stage_preserves_exception(self):
        from dcx.logging_config import SnowflakeLogger

        error = ValueError("secret")
        with self.assertRaises(ValueError) as caught:
            with snowflake_stage(SnowflakeLogger(logging.getLogger("dcx.test"), "import"), "connect"):
                raise error
        self.assertIs(caught.exception, error)
        self.assertEqual(error.safe_stage, "connect")