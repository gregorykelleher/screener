# _utils/test_client.py

import pytest
from httpx import AsyncClient

from equity_aggregator.adapters.data_sources._utils import make_client

pytestmark = pytest.mark.unit


async def test_make_client_returns_asyncclient() -> None:
    """
    ARRANGE: call make_client with a base_url
    ACT:     create client
    ASSERT:  object returned is an AsyncClient
    """
    client = make_client(base_url="https://api.test")

    assert isinstance(client, AsyncClient)

    await client.aclose()


async def test_make_client_applies_custom_timeout() -> None:
    """
    ARRANGE: custom connect-timeout passed in
    ACT:     create client
    ASSERT:  client's connect timeout equals expected value
    """
    expected_timeout = 2.5

    client = make_client(timeout=expected_timeout)

    assert client.timeout.connect == expected_timeout

    await client.aclose()


async def test_make_client_sets_base_url() -> None:
    """
    ARRANGE: base_url parameter supplied
    ACT:     create client
    ASSERT:  client.base_url matches input
    """
    url = "https://override.test"

    client = make_client(base_url=url)

    assert str(client.base_url) == url

    await client.aclose()


async def test_make_client_merges_default_and_custom_headers() -> None:
    """
    ARRANGE: override headers with a new custom key
    ACT:     create client
    ASSERT:  custom header present and default header preserved
    """
    custom_header = {"X-Custom": "42"}

    client = make_client(headers=custom_header)
    headers = client.headers

    assert headers["X-Custom"] == "42" and "User-Agent" in headers

    await client.aclose()


async def test_make_client_default_connect_timeout_is_three_seconds() -> None:
    """
    ARRANGE: call make_client without timeout override
    ACT:     inspect client.timeout
    ASSERT:  connect timeout equals 3.0 seconds
    """
    expected_timeout = 3.0
    client = make_client()

    assert client.timeout.connect == expected_timeout

    await client.aclose()
