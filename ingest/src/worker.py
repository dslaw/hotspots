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
        page_size: int,
        start_offset: int = 0,
        max_pages: int = NO_MAX_PAGES,
    ) -> tuple[int, int]:
        if page_size <= 0:
            raise ValueError
        if start_offset < 0:
            raise ValueError
        if max_pages <= 0 and max_pages != NO_MAX_PAGES:
            raise ValueError

        consumed_records = 0
        sent_records = 0
        consumed_pages = 0
        for records in self.source_client.paginate(
            resource_config.resource_id,
            page_size,
            resource_config.order_by,
            start_offset,
        ):
            validated_records = self.validator.validate(records)
            batch_sent_records = self.writer.write_batch(validated_records)

            consumed_pages += 1
            consumed_records += len(records)
            sent_records += batch_sent_records

            if consumed_pages >= max_pages and max_pages != NO_MAX_PAGES:
                break

            self._backoff()

        return consumed_records, sent_records
