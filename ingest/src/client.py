import random
from time import sleep
from typing import Generator

import httpx


class Client:
    def __init__(
        self,
        base_url: str,
        token: str | None,
        retries: int,
        backoff: int,
        connect_retries: int = 3,
    ):
        if retries < 0:
            raise ValueError
        if backoff <= 0:
            raise ValueError

        self.base_url = base_url
        self.token = token
        self.retries = retries
        self.backoff = backoff
        self.connect_retries = connect_retries

    def _make_client(self) -> httpx.Client:
        headers: dict[str, str] = {"Accepts": "application/json"}
        if self.token is not None:
            headers["X-App-Token"] = self.token

        return httpx.Client(
            transport=httpx.HTTPTransport(retries=self.connect_retries), headers=headers
        )

    @staticmethod
    def _retryable(response: httpx.Response) -> bool:
        if response.status_code == 429:
            return True
        return response.is_server_error

    def _backoff(self, failures: int) -> None:
        if failures <= 0:
            raise ValueError

        backoff_seconds = (failures * self.backoff) + random.random()
        sleep(backoff_seconds)
        return

    def _get_page(
        self, client: httpx.Client, url: str, params: dict[str, str | int]
    ) -> list[dict]:
        for attempt_number in range(self.retries + 1):
            response = client.get(url, params=params)
            if response.is_success:
                break

            if not self._retryable(response):
                break

            self._backoff(attempt_number + 1)

        response.raise_for_status()
        return response.json()

    def paginate(
        self, resource_id: str, page_size: int, order_by: str, start_offset: int
    ) -> Generator[list[dict], None, None]:
        if page_size <= 0:
            raise ValueError
        if start_offset < 0:
            raise ValueError

        url = f"https://{self.base_url}/resource/{resource_id}.json"

        with self._make_client() as http_client:
            offset = start_offset
            while True:
                params: dict[str, str | int] = {
                    "$order": order_by,
                    "$limit": page_size,
                    "$offset": offset,
                }
                records = self._get_page(http_client, url, params)
                if not records:
                    break

                yield records

                offset += len(records)

        return
