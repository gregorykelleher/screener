import asyncio
import os
from decimal import Decimal

import httpx
import pytest

from equity_aggregator.adapters.data_sources._cache._cache import save_cache
from equity_aggregator.adapters.data_sources.reference_lookup.exchange_rate_api import (
    _assert_success,
    _build_url,
    _convert_rate,
    _fetch_and_validate,
    _get_api_key,
    retrieve_conversion_rates,
)

pytestmark = pytest.mark.unit


def test_build_url_embeds_key_once() -> None:
    """
    ARRANGE: api_key = 'ABC'
    ACT:     call _build_url('ABC')
    ASSERT:  returned URL contains exactly one occurrence of 'ABC'
    """
    api_key = "ABC"

    url = _build_url(api_key)

    assert url.count(api_key) == 1


def test_convert_rate_produces_decimal() -> None:
    """
    ARRANGE: key='EUR', rate=1.23
    ACT:     call _convert_rate
    ASSERT:  second element is Decimal('1.23')
    """
    key, rate = "EUR", 1.23

    _, actual_rate = _convert_rate(key, rate)

    assert actual_rate == Decimal("1.23")


def test_assert_success_no_raise_on_success() -> None:
    """
    ARRANGE: payload with result='success'
    ACT:     call _assert_success
    ASSERT:  returns None without exception
    """
    payload = {"result": "success"}

    assert _assert_success(payload) is None


def test_assert_success_raises_when_error() -> None:
    """
    ARRANGE: payload with result!='success'
    ACT:     call _assert_success
    ASSERT:  raises generic Exception containing error message
    """
    payload = {"result": "error", "error-type": "bad-key"}

    with pytest.raises(Exception, match="bad-key"):
        _assert_success(payload)


def test_get_api_key_missing_env_raises() -> None:
    """
    ARRANGE: unset EXCHANGE_RATE_API_KEY
    ACT:     call _get_api_key
    ASSERT:  OSError is raised
    """
    os.environ.pop("EXCHANGE_RATE_API_KEY", None)

    with pytest.raises(OSError):
        _get_api_key()


def test_get_api_key_returns_value_when_set() -> None:
    """
    ARRANGE: set EXCHANGE_RATE_API_KEY='TOKEN'
    ACT:     call _get_api_key
    ASSERT:  returns 'TOKEN'
    """
    os.environ["EXCHANGE_RATE_API_KEY"] = "TOKEN"

    assert _get_api_key() == "TOKEN"


def test_retrieve_conversion_rates_uses_cache() -> None:
    """
    ARRANGE: cache seeded with two known rates
    ACT:     retrieve_conversion_rates()
    ASSERT:  function returns cached mapping unchanged
    """
    payload = {"USD": Decimal("1"), "EUR": Decimal("0.85")}
    save_cache("exchange_rate_api", payload)

    async def run() -> dict[str, Decimal]:
        return await retrieve_conversion_rates()

    actual = asyncio.run(run())

    assert actual == payload


def test_retrieve_conversion_rates_exits_on_http_error() -> None:
    """
    ARRANGE: mock transport always returns HTTP 500
    ACT:     call retrieve_conversion_rates()
    ASSERT:  SystemExit is raised (fatal exit path taken)
    """
    os.environ["EXCHANGE_RATE_API_KEY"] = "KEY"

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    with pytest.raises(SystemExit):
        asyncio.run(retrieve_conversion_rates(client))


def test_fetch_and_validate_http_error_propagates() -> None:
    """
    ARRANGE: _fetch_and_validate receives HTTP 500 response
    ACT:     execute _fetch_and_validate
    ASSERT:  httpx.HTTPStatusError is raised
    """

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    url = "https://test.invalid"

    async def run() -> object:
        await _fetch_and_validate(client, url)

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(run())


def test_fetch_and_validate_json_error_propagates() -> None:
    """
    ARRANGE: _fetch_and_validate receives 200 with invalid JSON
    ACT:     execute _fetch_and_validate
    ASSERT:  generic Exception bubbles up
    """

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"not-json")

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    url = "https://test.invalid"

    async def run() -> object:
        await _fetch_and_validate(client, url)

    with pytest.raises(ValueError):
        asyncio.run(run())
