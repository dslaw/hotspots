from io import BytesIO
from pathlib import Path
from typing import Any, Callable, Iterable, Protocol

import fastavro
from kafka import KafkaProducer  # type: ignore


def load_schema(schemas_dir: Path, schema_name: str) -> dict[str, Any]:
    schema_file = (schemas_dir / schema_name).with_suffix(".avsc")
    schema = fastavro.schema.load_schema(str(schema_file))
    assert isinstance(schema, dict)  # Type assertion for mypy.
    return schema


class Writer(Protocol):
    def write_batch(self, records: Iterable[dict]) -> int: ...


def make_serializer(schema: dict) -> Callable[[dict], bytes]:
    def serialize(v: dict) -> bytes:
        fh = BytesIO()
        fastavro.schemaless_writer(fh, schema, v)
        return fh.getvalue()

    return serialize


class KafkaWriter(Writer):
    def __init__(
        self, producer: KafkaProducer, topic: str, schema_name: str, schema: dict
    ):
        self.producer = producer
        self.topic = topic
        self.schema_name = schema_name
        self.schema = schema
        self.schema_header_name = "schema_name"

    @classmethod
    def from_url(
        cls, url: str, topic: str, schema_name: str, schema: dict
    ) -> "KafkaWriter":
        producer = KafkaProducer(
            bootstrap_servers=url, value_serializer=make_serializer(schema)
        )
        return cls(producer, topic, schema_name, schema)

    def write_batch(self, records: Iterable[dict]) -> int:
        headers = [(self.schema_header_name, self.schema_name.encode("utf-8"))]
        sent_records = 0
        for record in records:
            self.producer.send(self.topic, record, headers=headers)
            sent_records += 1

        return sent_records
