from unittest.mock import Mock

import httpx

from src.client import Client
from src.config import ResourceConfig
from src.worker import IngestWorker


class TestIngestWorker:
    def test_fetch(self):
        records = [
            {"key": 1},
            {"key": 3},
            {"key": 3},
        ]
        max_requests = 3

        def handler(request):
            data = [] if handler.n_requests >= max_requests else records
            handler.n_requests += 1
            return httpx.Response(200, json=data)

        handler.n_requests = 0

        http_client = httpx.Client(transport=httpx.MockTransport(handler))

        source_client = Client(
            base_url="localhost:8000", token=None, retries=2, backoff=1
        )
        source_client._make_client = Mock(return_value=http_client)

        mock_validator = Mock()
        mock_validator.validate.side_effect = [records, records, records]

        mock_writer = Mock()
        mock_writer.write_batch.side_effect = [len(records), len(records), len(records)]

        worker = IngestWorker(source_client, mock_validator, mock_writer)
        worker._backoff = Mock(return_value=None)

        resource_config = ResourceConfig(
            resource_name="test",
            resource_id="test-id",
            order_by="test-order-by",
            cadence="hourly",
            schema_name="test-schema",
        )
        actual = worker.fetch(resource_config, page_size=10)
        assert actual == (9, 9)

        mock_validator.validate.assert_called_with(records)
        assert mock_validator.validate.call_count == 3  # Once per page with data.

        mock_writer.write_batch.assert_called_with(records)
        assert mock_writer.write_batch.call_count == 3  # Once per page with data.

        assert worker._backoff.call_count == 3  # Once per page with data.
