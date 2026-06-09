"""`dcx import kafka` — build an ODCS contract from a Kafka topic's registered schema.

Raw Kafka messages carry no schema, so the meaningful "live" Kafka import reads
the topic's schema from a **Confluent Schema Registry** (REST API) and converts
it to ODCS by reusing the upstream avro / jsonschema / protobuf importers.

- `--topic` resolves to the subject `<topic>-value` (Confluent TopicNameStrategy);
  override with `--subject`.
- `--schema-registry` (or `SCHEMA_REGISTRY_URL`) is the registry base URL.
- Registry basic-auth via env only: `SCHEMA_REGISTRY_API_KEY` /
  `SCHEMA_REGISTRY_API_SECRET` (Confluent Cloud style). No secret CLI flags.

CLI-only (not exposed over the API) — same multi-tenant credential rationale as
the other live importers.
"""

import base64
import json
import os
import tempfile
import urllib.error
import urllib.request
from pathlib import Path
from typing import Optional

from datacontract.data_contract import DataContract
from datacontract.imports.importer import Importer
from open_data_contract_standard.model import OpenDataContractStandard, Server


class KafkaImportError(Exception):
    """A Kafka/Schema-Registry import failure with a user-actionable message."""


# Schema Registry `schemaType` → (upstream import format, tempfile suffix, ODCS server format).
# The registry omits `schemaType` for Avro (its default).
_SCHEMA_TYPE_MAP: dict[str, tuple[str, str, str]] = {
    "AVRO": ("avro", ".avsc", "avro"),
    "JSON": ("jsonschema", ".json", "json"),
    "PROTOBUF": ("protobuf", ".proto", "protobuf"),
}


def _auth_from_env() -> Optional[str]:
    key = os.environ.get("SCHEMA_REGISTRY_API_KEY")
    secret = os.environ.get("SCHEMA_REGISTRY_API_SECRET")
    return f"{key}:{secret}" if key and secret else None


def _fetch_subject_schema(
    registry_url: str, subject: str, *, auth: Optional[str] = None, timeout: int = 30,
) -> tuple[str, str]:
    """Return (schemaType, schema_string) for a subject's latest version."""
    url = f"{registry_url.rstrip('/')}/subjects/{subject}/versions/latest"
    req = urllib.request.Request(url, headers={"Accept": "application/vnd.schemaregistry.v1+json"})
    if auth:
        token = base64.b64encode(auth.encode()).decode()
        req.add_header("Authorization", f"Basic {token}")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        raise KafkaImportError(
            f"Schema Registry returned {exc.code} for subject '{subject}': {exc.reason}"
        )
    except Exception as exc:
        raise KafkaImportError(f"Could not reach Schema Registry at {registry_url}: {exc}")

    schema = data.get("schema")
    if not schema:
        raise KafkaImportError(f"Schema Registry response for '{subject}' has no schema.")
    # Avro registrations omit schemaType.
    return (data.get("schemaType") or "AVRO"), schema


def build_kafka_contract(
    schema_type: str,
    schema_str: str,
    *,
    topic: Optional[str] = None,
    bootstrap_servers: Optional[str] = None,
    subject: Optional[str] = None,
    server_name: str = "production",
) -> OpenDataContractStandard:
    """Convert a registry schema string to ODCS and attach a kafka server block."""
    mapping = _SCHEMA_TYPE_MAP.get((schema_type or "AVRO").upper())
    if mapping is None:
        raise KafkaImportError(
            f"Unsupported schema type '{schema_type}'. Expected AVRO, JSON or PROTOBUF."
        )
    import_format, suffix, server_format = mapping

    tmp_path: Optional[str] = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=suffix, delete=False, encoding="utf-8") as f:
            f.write(schema_str)
            tmp_path = f.name
        try:
            contract = DataContract.import_from_source(format=import_format, source=tmp_path)
        except Exception as exc:
            raise KafkaImportError(f"Failed to convert {import_format} schema: {exc}")
    finally:
        if tmp_path and Path(tmp_path).exists():
            Path(tmp_path).unlink()

    server = Server(server=server_name, type="kafka", format=server_format)
    if bootstrap_servers:
        server.host = bootstrap_servers
    contract.servers = [server]

    if topic:
        contract.id = topic
        if not contract.name:
            contract.name = topic
    return contract


def import_kafka(import_args: dict) -> OpenDataContractStandard:
    """Fetch a topic's schema from the Schema Registry and build an ODCS contract."""
    registry = import_args.get("schema_registry") or os.environ.get("SCHEMA_REGISTRY_URL")
    if not registry:
        raise KafkaImportError(
            "Schema Registry URL required: pass --schema-registry or set SCHEMA_REGISTRY_URL."
        )

    topic = import_args.get("topic")
    subject = import_args.get("subject") or (f"{topic}-value" if topic else None)
    if not subject:
        raise KafkaImportError("Provide --topic (subject defaults to <topic>-value) or --subject.")

    schema_type, schema_str = _fetch_subject_schema(registry, subject, auth=_auth_from_env())
    return build_kafka_contract(
        schema_type, schema_str,
        topic=topic, bootstrap_servers=import_args.get("bootstrap_servers"), subject=subject,
        server_name=import_args.get("server_name") or "production",
    )


class KafkaImporter(Importer):
    """Registered into the upstream importer_factory as `kafka`."""

    def import_source(self, source: str, import_args: dict) -> OpenDataContractStandard:
        return import_kafka(import_args)
