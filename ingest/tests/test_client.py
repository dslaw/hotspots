from unittest.mock import patch

import httpx
import pytest

from src.client import Client


@pytest.fixture
def patch_sleep():
    with patch("src.client.sleep"):
        yield
    return


@pytest.mark.usefixtures("patch_sleep")
class TestClient:
    def test_make_client(self):
        client = Client(base_url="localhost:8000", token="token", retries=3, backoff=1)
        http_client = client._make_client()

        http_client.headers.get("Accepts") == "application/json"
        http_client.headers.get("X-App-Token") == "token"

    @pytest.mark.parametrize(
        "status_code, expected", [(400, False), (429, True), (500, True)]
    )
    def test_retryable(self, status_code, expected):
        response = httpx.Response(status_code=status_code)
        actual = Client._retryable(response)
        assert actual is expected

    def test_get_page_retries(self):
        def handler(request):
            status_code = 200 if handler.n_requests >= 1 else 500
            handler.n_requests += 1
            return httpx.Response(status_code, json=[{"key": 1}])

        handler.n_requests = 0

        http_client = httpx.Client(transport=httpx.MockTransport(handler))

        client = Client(base_url="localhost:8000", token="token", retries=1, backoff=1)
        actual = client._get_page(
            http_client, "http://localhost:8000/path/to/resource.json", params={}
        )
        assert actual == [{"key": 1}]

    def test_get_page_fails_after_max_retries(self):
        def handler(request):
            handler.n_requests += 1
            return httpx.Response(500, json=[{"key": 1}])

        handler.n_requests = 0

        http_client = httpx.Client(transport=httpx.MockTransport(handler))

        client = Client(base_url="localhost:8000", token="token", retries=1, backoff=1)

        with pytest.raises(httpx.HTTPStatusError):
            client._get_page(
                http_client, "http://localhost:8000/path/to/resource.json", params={}
            )

        assert handler.n_requests == 2

    def test_paginate(self):
        def handler(request):
            data = [{"key": 1}] if handler.n_requests <= 2 else []
            handler.n_requests += 1
            return httpx.Response(200, json=data)

        handler.n_requests = 0

        http_client = httpx.Client(transport=httpx.MockTransport(handler))

        client = Client(base_url="localhost:8000", token="token", retries=1, backoff=1)
        with patch.object(client, "_make_client", return_value=http_client) as mock:
            it = client.paginate("resource_id", 10, "order_by", start_offset=0)
            records = []
            for page in it:
                records += page

        assert records == [{"key": 1}, {"key": 1}, {"key": 1}]
        mock.assert_called_once()
