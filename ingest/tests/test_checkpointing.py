import datetime
from unittest.mock import Mock, patch

from src.checkpointing import Checkpoint, CheckpointingClient


class TestCheckpoint:
    def test_wait_until(self):
        last_request_at = datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)
        checkpoint = Checkpoint(last_request_at=last_request_at, n_ingested=321)
        expected = datetime.datetime(2025, 1, 1, hour=1, minute=3, tzinfo=datetime.UTC)
        actual = checkpoint.wait_until(63)
        assert actual == expected

    def test_wait_until_when_empty(self, monkeypatch):
        with patch("src.checkpointing.datetime.datetime") as mock_datetime:
            mock_datetime.now.return_value = datetime.datetime(
                2025, 1, 1, tzinfo=datetime.UTC
            )

            checkpoint = Checkpoint()
            expected = datetime.datetime(
                2025, 1, 1, hour=1, minute=3, tzinfo=datetime.UTC
            )
            actual = checkpoint.wait_until(63)
            assert actual == expected

    def test_serialize_last_request_at(self):
        dt = datetime.datetime(2025, 1, 1, hour=1, minute=3, tzinfo=datetime.UTC)
        expected = "2025-01-01T01:03:00+00:00"
        checkpoint = Checkpoint(last_request_at=dt)
        actual = checkpoint.serialize_last_request_at(dt)
        assert actual == expected

    def test_serialize_last_request_at_when_none(self):
        checkpoint = Checkpoint(last_request_at=None)
        actual = checkpoint.serialize_last_request_at(None)
        assert actual is None


class TestCheckpointingClient:
    def test_make_key(self):
        mock_redis_client = {}
        client = CheckpointingClient(mock_redis_client)
        assert client.make_key("resource_name") == "ingest-checkpoint:resource_name"

    def test_get_checkpoint(self):
        mock_redis_client = Mock()
        mock_redis_client.get.return_value = (
            '{"last_request_at":"2025-01-01T00:00:00+00:00","n_ingested":321}'
        )

        client = CheckpointingClient(mock_redis_client)
        checkpoint = client.get_checkpoint("resource_name")

        assert checkpoint.last_request_at == datetime.datetime(
            2025, 1, 1, tzinfo=datetime.UTC
        )
        assert checkpoint.n_ingested == 321
        mock_redis_client.get.assert_called_once_with("ingest-checkpoint:resource_name")

    def test_get_checkpoint_when_no_checkpoint(self):
        mock_redis_client = Mock()
        mock_redis_client.get.return_value = None

        client = CheckpointingClient(mock_redis_client)
        checkpoint = client.get_checkpoint("resource_name")

        assert checkpoint.last_request_at is None
        assert checkpoint.n_ingested == 0
        mock_redis_client.get.assert_called_once_with("ingest-checkpoint:resource_name")

    def test_set_checkpoint(self):
        mock_redis_client = Mock()
        mock_redis_client.set.return_value = None

        client = CheckpointingClient(mock_redis_client)
        last_request_at = datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)
        n_ingested = 321
        client.set_checkpoint("resource_name", last_request_at, n_ingested)

        mock_redis_client.set.assert_called_once_with(
            "ingest-checkpoint:resource_name",
            '{"last_request_at":"2025-01-01T00:00:00+00:00","n_ingested":321}',
        )
