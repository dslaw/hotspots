from unittest.mock import Mock

from src.writer import KafkaWriter


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
