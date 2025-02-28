from random import random
from time import sleep

from src.client import Client
from src.config import ResourceConfig
from src.validation import Validator
from src.writer import Writer

NO_MAX_PAGES = -1
PAGING_BACKOFF_PROB = 0.25


class IngestWorker:
    def __init__(self, source_client: Client, validator: Validator, writer: Writer):
        self.source_client = source_client
        self.validator = validator
        self.writer = writer

    def _backoff(self) -> None:
        if random() < PAGING_BACKOFF_PROB:
            sleep(self.source_client.backoff)
        return

    def fetch(
        self,
        resource_config: ResourceConfig,
        start_offset: int = 0,
        max_pages: int = NO_MAX_PAGES,
    ) -> tuple[int, int]:
        if start_offset < 0:
            raise ValueError
        if max_pages <= 0 and max_pages != NO_MAX_PAGES:
            raise ValueError

        consumed_records = 0
        sent_records = 0
        page = 0
        while True:
            if max_pages != NO_MAX_PAGES and page >= max_pages:
                break

            offset = start_offset + consumed_records
            records = self.source_client.get_page(
                resource_config.resource_id,
                resource_config.order_by,
                offset,
            )
            if not records:
                break

            validated_records = self.validator.validate(records)
            batch_sent_records = self.writer.write_batch(validated_records)

            consumed_records += len(records)
            sent_records += batch_sent_records
            page += 1

            self._backoff()

        return consumed_records, sent_records
