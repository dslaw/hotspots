import argparse
import datetime
import os

import clickhouse_connect as clickhouse
import psycopg


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


def write(dst_conn, blocks, *, page_size=10_000):
    with dst_conn, dst_conn.cursor() as cursor:
        cursor.execute(
            """
            create temp table reconciled_aggregate_buckets on commit drop as
            select occurred_at, geo_id, incident_count from aggregate_buckets
            where 0 = 1
            """
        )

        for row_block in blocks:
            cursor.executemany(
                """
                insert into reconciled_aggregate_buckets (occurred_at, geo_id, incident_count)
                values (%s, %s, %s)
                """,
                row_block,
            )

        cursor.execute(
            """
            with deleted as (
                delete from aggregate_buckets as dst
                where exists (
                    select 1
                    from reconciled_aggregate_buckets as src
                    where
                        dst.occurred_at = src.occurred_at
                        and dst.geo_id = src.geo_id
                )
                returning dst.id
            ), inserted as (
                insert into aggregate_buckets (occurred_at, geo_id, incident_count)
                select * from reconciled_aggregate_buckets
                returning id
            )
            select count(*), 'staged' as op from reconciled_aggregate_buckets
            union all
            select count(*), 'deleted' as op from deleted
            union all
            select count(*), 'inserted' as op from inserted
            """
        )

        result = {op: count for count, op in cursor}

    return result


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

    dst_url = os.environ["AGGREGATES_DB_URL"]
    src_url = os.environ["WAREHOUSE_URL"]

    src_client = clickhouse.get_client(dsn=src_url)
    dst_conn = psycopg.connect(dst_url)

    with read(src_client, start_time, end_time) as stream:
        result = write(dst_conn, stream)

    dst_conn.close()
    src_client.close()

    print(result)
    return


if __name__ == "__main__":
    args = make_parser().parse_args()
    main(make_time(args.start_time), make_time(args.end_time))
