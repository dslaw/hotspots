import random
from time import sleep

import httpx


class Client:
    def __init__(
        self,
        base_url: str,
        token: str | None,
        page_size: int,
        max_retries: int,
        backoff: int,
        timeout: int,
    ):
        if page_size <= 0:
            raise ValueError
        if max_retries < 0:
            raise ValueError
        if backoff <= 0:
            raise ValueError
        if timeout < 0:
            raise ValueError

        self.base_url = base_url
        self.token = token
        self.page_size = page_size
        self.max_retries = max_retries
        self.backoff = backoff
        self.timeout = timeout

    @property
    def headers(self) -> dict[str, str]:
        headers: dict[str, str] = {"Accepts": "application/json"}
        if self.token is not None:
            headers["X-App-Token"] = self.token
        return headers

    @staticmethod
    def _retryable(response: httpx.Response) -> bool:
        if response.status_code in (409, 429):
            return True
        return response.is_server_error

    def _backoff(self, failures: int) -> None:
        if failures <= 0:
            raise ValueError

        backoff_seconds = (failures * self.backoff) + random.random()
        sleep(backoff_seconds)
        return

    def get_page(self, resource_id: str, order_by: str, offset: int) -> list[dict]:
        if offset < 0:
            raise ValueError

        url = f"https://{self.base_url}/resource/{resource_id}.json"
        params: dict[str, str | int] = {"$order": order_by, "$limit": self.page_size}
        if offset > 0:
            params["offset"] = offset

        for attempt_number in range(self.max_retries + 1):
            response = httpx.get(url, params=params, headers=self.headers)
            if response.is_success:
                break

            if not self._retryable(response):
                break

            self._backoff(attempt_number + 1)

        response.raise_for_status()
        return response.json()
