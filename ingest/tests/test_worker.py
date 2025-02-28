from unittest.mock import Mock, _Call

from src.config import ResourceConfig
from src.worker import IngestWorker


class TestIngestWorker:
    def test_fetch(self):
        records = [
            {"key": 1},
            {"key": 3},
            {"key": 3},
        ]

        mock_source_client = Mock()
        mock_source_client.get_page.side_effect = [records, records, records, []]

        mock_validator = Mock()
        mock_validator.validate.side_effect = [records, records, records]

        mock_writer = Mock()
        mock_writer.write_batch.side_effect = [len(records), len(records), len(records)]

        worker = IngestWorker(mock_source_client, mock_validator, mock_writer)
        worker._backoff = Mock(return_value=None)

        resource_config = ResourceConfig(
            resource_name="test",
            resource_id="test-id",
            order_by="test-order-by",
            cadence="hourly",
            schema_name="test-schema",
        )
        actual = worker.fetch(resource_config)
        assert actual == (9, 9)

        mock_source_client.get_page.assert_has_calls(
            [
                _Call((("test-id", "test-order-by", 0),)),
                _Call((("test-id", "test-order-by", 3),)),
                _Call((("test-id", "test-order-by", 6),)),
                _Call((("test-id", "test-order-by", 9),)),
            ]
        )
        mock_validator.validate.assert_called_with(records)
        assert mock_validator.validate.call_count == 3  # Once per page with data.
        mock_writer.write_batch.assert_called_with(records)
        assert mock_writer.write_batch.call_count == 3  # Once per page with data.
        assert worker._backoff.call_count == 3  # Once per page with data.
