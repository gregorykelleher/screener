import asyncio
import os
from decimal import Decimal

import httpx
import pytest

from equity_aggregator.adapters.data_sources.reference_lookup.exchange_rate_api import (
    _assert_success,
    _build_url,
    _convert_rate,
    _fetch_and_validate,
    _get_api_key,
    _get_rates,
    retrieve_conversion_rates,
)
from equity_aggregator.storage import load_cache, save_cache

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


def test_fetch_and_validate_success_returns_payload() -> None:
    """
    ARRANGE: 200 OK with valid JSON {'result':'success', 'conversion_rates':{}}.
    ACT:     call _fetch_and_validate().
    ASSERT:  payload['result'] == 'success'.
    """

    async def handler(_: httpx.Request) -> httpx.Response:
        body = {"result": "success", "conversion_rates": {"USD": 1}}
        return httpx.Response(200, json=body)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    url = "https://test.invalid"

    async def run() -> dict:
        return await _fetch_and_validate(client, url)

    payload = asyncio.run(run())

    assert payload["result"] == "success"


def test_get_rates_converts_numbers_to_decimal() -> None:
    """
    ARRANGE: HTTP transport returns 200 with numeric rates.
    ACT:     call _get_rates().
    ASSERT:  returned mapping contains Decimal values.
    """

    async def handler(_: httpx.Request) -> httpx.Response:
        body = {
            "result": "success",
            "conversion_rates": {"JPY": 110.0},
        }
        return httpx.Response(200, json=body)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    url = "https://example.invalid"

    async def run() -> dict[str, Decimal]:
        return await _get_rates(client, url)

    rates = asyncio.run(run())

    assert rates["JPY"] == Decimal("110.0")


def test_retrieve_conversion_rates_fetches_and_caches() -> None:
    """
    ARRANGE: empty cache + mocked HTTP transport that succeeds.
    ACT:     call retrieve_conversion_rates().
    ASSERT:  data is persisted to cache (second load_cache yields same mapping).
    """
    cache_key = "exchange_rate_api_test"
    os.environ["EXCHANGE_RATE_API_KEY"] = "DUMMY_KEY"

    body = {
        "result": "success",
        "conversion_rates": {"GBP": 0.7, "USD": 1},
    }

    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=body)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    async def run() -> dict[str, Decimal]:
        return await retrieve_conversion_rates(client, cache_key=cache_key)

    first_call = asyncio.run(run())
    cached_after_call = load_cache(cache_key)

    assert cached_after_call == first_call
