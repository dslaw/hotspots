"""Publish records to Kafka from a local file. For local development purposes."""

import argparse
import json
from pathlib import Path

from src.config import RESOURCE_CONFIGS_FILE, load_config, load_resource_configs
from src.validation import Validator
from src.writer import KafkaWriter, load_schema


def make_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--resource-name", type=str)
    parser.add_argument("--input-file", type=Path)
    return parser


def main(resource_name: str, input_file: Path):
    config = load_config()
    resource_configs = load_resource_configs(RESOURCE_CONFIGS_FILE)

    resource_config = resource_configs[resource_name]
    schema_name = resource_config.schema_name
    schema = load_schema(config.schemas_dir, schema_name)

    writer = KafkaWriter.from_url(
        config.kafka_url, config.kafka_topic, schema_name=schema_name, schema=schema
    )
    validator = Validator(schema_name)

    with input_file.open("r") as fh:
        records = json.load(fh)

    validated_records = validator.validate(records)
    sent_records = writer.write_batch(validated_records)

    writer.producer.flush()
    print(f"{resource_name}: Read {len(records)}, published {sent_records}")
    return


if __name__ == "__main__":
    args = make_parser().parse_args()
    main(args.resource_name, args.input_file)
