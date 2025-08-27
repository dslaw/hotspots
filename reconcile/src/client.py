import datetime
from typing import Any, Iterable

import httpx


def serialize_datetime(d: datetime.datetime) -> str:
    return d.astimezone(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def to_aggregate_item(
    occurred_at: datetime.datetime, geohash: str, count: int
) -> dict[str, Any]:
    return {
        "occurred_at": serialize_datetime(occurred_at),
        "geohash": geohash,
        "count": count,
    }


def write(
    url: str, records: Iterable[Iterable[dict]], connect_retries: int = 3
) -> None:
    with httpx.Client(
        transport=httpx.HTTPTransport(retries=connect_retries)
    ) as http_client:
        for records_batch in records:
            response = http_client.put(url, json=records_batch)
            response.raise_for_status()

    return
