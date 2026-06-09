import json

import pytest
from open_data_contract_standard.model import OpenDataContractStandard
from typer.testing import CliRunner

from dcx.cli import app
from dcx.importers.kafka import (
    KafkaImportError,
    build_kafka_contract,
    import_kafka,
)

runner = CliRunner()


AVRO_SCHEMA = json.dumps({
    "type": "record",
    "name": "Customer",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "email", "type": ["null", "string"], "default": None},
    ],
})

JSON_SCHEMA = json.dumps({
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "Customer",
    "type": "object",
    "properties": {"id": {"type": "integer"}, "email": {"type": "string"}},
    "required": ["id"],
})


# === build_kafka_contract (real upstream conversion) =========================


def test_build_from_avro_sets_kafka_server():
    c = build_kafka_contract(
        "AVRO", AVRO_SCHEMA, topic="customers", bootstrap_servers="broker:9092",
    )
    assert isinstance(c, OpenDataContractStandard)
    srv = c.servers[0]
    assert srv.type == "kafka"
    assert srv.format == "avro"
    assert srv.host == "broker:9092"
    assert c.id == "customers"
    # the avro schema actually got converted into a schema object with fields
    assert c.schema_ and c.schema_[0].properties


def test_build_from_json_schema():
    c = build_kafka_contract("JSON", JSON_SCHEMA, topic="customers")
    assert c.servers[0].format == "json"
    assert c.schema_ and c.schema_[0].properties


def test_build_unsupported_type_errors():
    with pytest.raises(KafkaImportError, match="Unsupported schema type"):
        build_kafka_contract("YAML", "x", topic="t")


def test_missing_schemaType_defaults_to_avro():
    c = build_kafka_contract("", AVRO_SCHEMA, topic="t")
    assert c.servers[0].format == "avro"


def test_build_default_and_custom_server_name():
    assert build_kafka_contract("AVRO", AVRO_SCHEMA, topic="t").servers[0].server == "production"
    c = build_kafka_contract("AVRO", AVRO_SCHEMA, topic="t", server_name="prod_eu")
    assert c.servers[0].server == "prod_eu"


# === import_kafka (fetch injected) ==========================================


def test_import_kafka_defaults_subject_from_topic(monkeypatch):
    import dcx.importers.kafka as ki
    captured = {}

    def fake_fetch(registry_url, subject, *, auth=None, timeout=30):
        captured["registry"] = registry_url
        captured["subject"] = subject
        return "AVRO", AVRO_SCHEMA

    monkeypatch.setattr(ki, "_fetch_subject_schema", fake_fetch)
    c = import_kafka({"schema_registry": "https://sr:8081", "topic": "customers"})
    assert captured["subject"] == "customers-value"   # TopicNameStrategy default
    assert captured["registry"] == "https://sr:8081"
    assert c.servers[0].type == "kafka"


def test_import_kafka_explicit_subject(monkeypatch):
    import dcx.importers.kafka as ki
    captured = {}
    monkeypatch.setattr(ki, "_fetch_subject_schema",
                        lambda url, subj, **kw: captured.update(subject=subj) or ("AVRO", AVRO_SCHEMA))
    import_kafka({"schema_registry": "https://sr:8081", "subject": "my-subject"})
    assert captured["subject"] == "my-subject"


def test_import_kafka_requires_registry(monkeypatch):
    monkeypatch.delenv("SCHEMA_REGISTRY_URL", raising=False)
    with pytest.raises(KafkaImportError, match="Schema Registry URL required"):
        import_kafka({"topic": "customers"})


def test_import_kafka_requires_topic_or_subject(monkeypatch):
    with pytest.raises(KafkaImportError, match="--topic .* or --subject"):
        import_kafka({"schema_registry": "https://sr:8081"})


# === CLI / API ==============================================================


def test_cli_kafka(monkeypatch):
    from datacontract.data_contract import DataContract
    captured = {}

    def fake(format, source=None, **kw):
        captured["format"] = format
        captured.update(kw)
        return OpenDataContractStandard(
            apiVersion="v3.1.0", kind="DataContract", id="x", name="X", version="1.0.0",
        )

    monkeypatch.setattr(DataContract, "import_from_source", staticmethod(fake))
    result = runner.invoke(app, [
        "import", "kafka",
        "--schema-registry", "https://sr:8081", "--topic", "customers",
        "--bootstrap-servers", "broker:9092",
    ])
    assert result.exit_code == 0, result.output
    assert captured["format"] == "kafka"
    assert captured["schema_registry"] == "https://sr:8081"
    assert captured["topic"] == "customers"
    assert captured["bootstrap_servers"] == "broker:9092"
    assert captured["server_name"] == "production"


def test_kafka_not_in_api():
    from dcx.api import build_dcx_api_app
    paths = {getattr(r, "path", "") for r in build_dcx_api_app().routes}
    assert "/import/kafka" not in paths
