import json
import os
from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel

RESOURCE_CONFIGS_FILE = Path("src") / "resource_configs.json"


class Cadence(StrEnum):
    Hourly = "hourly"
    Daily = "daily"
    Quarterly = "quarterly"

    def minutes(self) -> int:
        one_hour = 60
        if self.value == "hourly":
            return one_hour
        if self.value == "daily":
            return 24 * one_hour
        if self.value == "quarterly":
            return 3 * 30 * 24 * one_hour
        raise RuntimeError


class ResourceConfig(BaseModel):
    resource_name: str
    resource_id: str
    cadence: Cadence
    order_by: str
    schema_name: str


class Config(BaseModel):
    base_url: str
    api_token: str | None
    page_size: int
    retries: int
    backoff: int
    redis_url: str
    kafka_url: str
    kafka_topic: str
    schemas_dir: Path


def load_resource_configs(file: Path) -> dict[str, ResourceConfig]:
    with file.open("r") as fh:
        raw_resource_configs = json.load(fh)

    return {
        resource_name: ResourceConfig(
            resource_name=resource_name,
            resource_id=resource_config["resource_id"],
            cadence=Cadence(resource_config["cadence"]),
            order_by=resource_config["order_by"],
            schema_name=resource_config["schema_name"],
        )
        for resource_name, resource_config in raw_resource_configs.items()
    }


def load_config() -> Config:
    base_url = os.environ["API_BASE_URL"]
    api_token = os.environ.get("API_TOKEN", None)
    page_size = int(os.environ["PAGE_SIZE"])
    retries = int(os.environ["RETRIES"])
    backoff = int(os.environ["BACKOFF"])
    redis_url = os.environ["REDIS_URL"]
    kafka_url = os.environ["KAFKA_URL"]
    kafka_topic = os.environ["KAFKA_TOPIC"]
    schemas_dir = Path(os.environ["SCHEMAS_DIR"])

    return Config(
        base_url=base_url,
        api_token=api_token,
        page_size=page_size,
        retries=retries,
        backoff=backoff,
        redis_url=redis_url,
        kafka_url=kafka_url,
        kafka_topic=kafka_topic,
        schemas_dir=schemas_dir,
    )
