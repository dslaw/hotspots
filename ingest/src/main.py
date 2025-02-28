import argparse
import datetime
import logging
from pathlib import Path
from time import sleep

import dotenv

from src.checkpointing import CheckpointingClient
from src.client import Client
from src.config import load_config, load_resource_configs
from src.validation import Validator
from src.worker import NO_MAX_PAGES, IngestWorker
from src.writer import KafkaWriter, LocalFileWriter, load_schema

LOCAL_DATA_DIR = Path("data")
RESOURCE_CONFIGS_FILE = Path("src") / "resource_configs.json"
SCHEMAS_DIR = Path("schemas") / "raw"


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Ingest data from an external resource"
    )
    parser.add_argument(
        "--resource-name", type=str, help="Name of the resource to ingest data from"
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=NO_MAX_PAGES,
        help="Maximum number of pages to ingest",
    )
    parser.add_argument(
        "--write-to-file",
        action="store_true",
        help="Write ingested data to a local file. If omitted, data is ingested to Kafka (default).",
    )
    parser.add_argument(
        "--skip-wait",
        action="store_true",
        help="Do not wait before ingesting data. If omitted, ingestion will wait until the next scheduled run, as determined by the last checkpoint and the resource's cadence (default).",
    )
    return parser


def main(
    resource_name: str,
    max_pages: int,
    *,
    write_to_file: bool = False,
    skip_wait: bool = False,
) -> None:
    logging.info(
        f"Running ingest worker with args "
        f"resource_name={resource_name} "
        f"max_pages={max_pages} "
        f"write_to_file={write_to_file}"
    )

    dotenv.load_dotenv()
    config = load_config()
    resource_configs = load_resource_configs(RESOURCE_CONFIGS_FILE)

    resource_config = resource_configs[resource_name]
    schema_name = resource_config.schema_name
    schema = load_schema(SCHEMAS_DIR, schema_name)

    client = Client(
        config.base_url,
        config.api_token,
        config.page_size,
        config.max_retries,
        config.backoff,
        config.timeout,
    )
    checkpointing_client = CheckpointingClient.from_redis_url(
        config.redis_url,
        timeout=config.timeout,
        connect_timeout=config.timeout,
        max_retries=config.max_retries,
    )
    validator = Validator(schema_name)
    writer = (
        KafkaWriter.from_url(
            config.kafka_url, config.kafka_topic, schema_name=schema_name, schema=schema
        )
        if not write_to_file
        else LocalFileWriter(LOCAL_DATA_DIR / args.resource_name, schema=schema)
    )
    ingest_worker = IngestWorker(client, validator, writer)

    logging.info(f"Getting checkpoint for {resource_name}")
    checkpoint = checkpointing_client.get_checkpoint(resource_name)
    logging.info(f"Finished getting checkpoint for {resource_name}")
    logging.info(checkpoint.to_json())

    now = datetime.datetime.now(datetime.UTC)
    ingest_at = checkpoint.wait_until(resource_config.cadence.minutes())
    wait_for = (ingest_at - now).seconds
    logging.info(f"Next scheduled run in {wait_for} seconds")
    if skip_wait:
        logging.info("Skipping wait time")
    else:
        logging.info(f"Waiting for {wait_for} seconds...")
        sleep(wait_for)

    logging.info("Ingesting")
    n_fetched, n_ingested = ingest_worker.fetch(resource_config, max_pages=max_pages)
    finished_at = datetime.datetime.now(datetime.UTC)
    logging.info("Finished ingesting")

    n_dropped = n_fetched - n_ingested
    if n_dropped > 0:
        logging.error(f"Dropped {n_dropped} records during run")

    logging.info("Updating checkpoint")
    checkpointing_client.set_checkpoint(
        resource_name,
        finished_at,
        checkpoint.n_ingested + n_ingested,
    )
    logging.info("Finished updating checkpoint")
    return


if __name__ == "__main__":
    args = make_parser().parse_args()
    main(
        args.resource_name,
        args.max_pages,
        write_to_file=args.write_to_file,
        skip_wait=args.skip_wait,
    )
