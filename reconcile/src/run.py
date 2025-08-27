import argparse
import datetime
import os
from contextlib import closing
from itertools import batched

import clickhouse_connect as clickhouse

from src.client import to_aggregate_item, write


def read(src_client, start_time, end_time):
    return src_client.query_row_block_stream(
        """
        with new_incidents as (
            select bucket_timestamp, bucket_geohash,  incident_type, unique_id
            from warehouse.incidents
            where
                loaded_at >= %(start_timestamp)s
                and loaded_at < %(end_timestamp)s
        ), processable_incidents as (
            -- Fully recompute counts for every affected bucket, so that all matching
            -- records in the target table can be purged and replaced.
            select bucket_timestamp, bucket_geohash
            from warehouse.incidents
            inner join new_incidents on
                incidents.incident_type = new_incidents.incident_type
                and incidents.unique_id = new_incidents.unique_id
        )
        select
            bucket_timestamp,
            bucket_geohash,
            count(*) as incident_count
        from processable_incidents
        where
            processable_incidents.bucket_timestamp is not null
            and processable_incidents.bucket_geohash is not null
        group by
            bucket_timestamp,
            bucket_geohash
        """,
        {"start_timestamp": start_time, "end_timestamp": end_time},
    )


def process(stream):
    for block in stream:
        for row in block:
            yield to_aggregate_item(*row)

    return


def make_time(s: str) -> datetime.datetime:
    dt = datetime.datetime.fromisoformat(s)
    return dt.replace(second=0, microsecond=0)


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-time", type=str, required=True)
    parser.add_argument("--end-time", type=str, required=True)
    return parser


def main(start_time: datetime.datetime, end_time: datetime.datetime) -> None:
    if start_time >= end_time:
        raise ValueError("Start time must be before end time")

    src_url = os.environ["WAREHOUSE_URL"]
    dst_url = os.environ["APP_URL"]
    batch_size = int(os.environ["RECONCILE_BATCH_SIZE"])

    with closing(clickhouse.get_client(dsn=src_url)) as src_client:
        with read(src_client, start_time, end_time) as stream:
            records = process(stream)
            records_batched = batched(records, batch_size)
            write(dst_url, records_batched)

    return


if __name__ == "__main__":
    args = make_parser().parse_args()
    main(make_time(args.start_time), make_time(args.end_time))
