import tempfile
from pathlib import Path
from unittest.mock import Mock

from src.writer import KafkaWriter, LocalFileWriter


class TestLocalFileWriter:
    def test_write_batch(self):
        schema = {
            "type": "record",
            "name": "test",
            "fields": [{"name": "key", "type": "int"}],
        }
        records = [{"key": 1}, {"key": 2}]

        tmp_dir = tempfile.gettempdir()
        writer = LocalFileWriter(Path(tmp_dir), schema)
        actual = writer.write_batch(records)
        assert actual == len(records)


class TestKafkaWriter:
    def test_write_batch(self):
        records = [{"key": 1}, {"key": 2}]
        mock_kafka_producer = Mock()
        mock_kafka_producer.send = Mock(return_value=None)

        writer = KafkaWriter(mock_kafka_producer, "topic", "schema_name", {})
        writer.write_batch(records)

        assert mock_kafka_producer.send.call_count == 2
        mock_kafka_producer.send.assert_called_with(
            "topic", {"key": 2}, headers=[("schema_name", b"schema_name")]
        )
