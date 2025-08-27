import datetime
from typing import cast

import redis
from pydantic import BaseModel, field_serializer
from redis.backoff import ExponentialBackoff
from redis.exceptions import BusyLoadingError, ConnectionError, TimeoutError
from redis.retry import Retry


class Checkpoint(BaseModel):
    last_request_at: datetime.datetime | None = None
    n_ingested: int = 0

    def wait_until(self, cadence: int) -> datetime.datetime:
        if self.last_request_at is None:
            return datetime.datetime.now(datetime.UTC)
        return self.last_request_at + datetime.timedelta(minutes=cadence)

    @field_serializer("last_request_at")
    def serialize_last_request_at(
        self, last_request_at: datetime.datetime
    ) -> str | None:
        if last_request_at is None:
            return None
        return last_request_at.isoformat()

    def to_json(self) -> str:
        return self.model_dump_json()

    @classmethod
    def from_json(cls, s: str) -> "Checkpoint":
        return cls.model_validate_json(s)


class CheckpointingClient:
    def __init__(self, redis_client: redis.Redis):
        self._redis_client = redis_client

    @classmethod
    def from_redis_url(
        cls,
        redis_url: str,
        retries: int,
        timeout: int = 5,
        connect_timeout: int = 5,
    ) -> "CheckpointingClient":
        redis_client = redis.from_url(
            redis_url,
            retry=Retry(ExponentialBackoff(), retries),
            retry_on_error=[BusyLoadingError, ConnectionError, TimeoutError],
            socket_timeout=timeout,
            socket_connect_timeout=connect_timeout,
            decode_responses=True,
        )
        return cls(redis_client)

    @staticmethod
    def make_key(resource_name: str) -> str:
        return f"ingest-checkpoint:{resource_name}"

    def get_checkpoint(self, resource_name: str) -> Checkpoint:
        key = self.make_key(resource_name)
        data = self._redis_client.get(key)
        if data is None:
            return Checkpoint()

        data = cast(str, data)
        return Checkpoint.from_json(data)

    def set_checkpoint(
        self, resource_name: str, last_request_at: datetime.datetime, n_ingested: int
    ) -> None:
        key = self.make_key(resource_name)
        checkpoint = Checkpoint(last_request_at=last_request_at, n_ingested=n_ingested)
        data = checkpoint.to_json()
        self._redis_client.set(key, data)
        return
