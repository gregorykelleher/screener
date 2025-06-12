# authoritative_feeds/test_xetra.py

import asyncio
import json

import httpx
import pytest
from httpx import AsyncClient, MockTransport

from equity_aggregator.adapters.data_sources.authoritative_feeds.xetra import (
    _build_search_payload,
    _extract_equity_records,
    _fetch_page,
    _get_total_records,
    fetch_equity_records,
)

pytestmark = pytest.mark.unit


def test_build_search_payload_contains_expected_keys() -> None:
    """
    ARRANGE: offset is set to 5
    ACT:     call _build_search_payload(5)
    ASSERT:  keys exactly match expected set
    """
    offset = 5

    actual = _build_search_payload(offset)

    assert set(actual.keys()) == {"stockExchanges", "lang", "offset", "limit"}


def test_get_total_records_parses_integer() -> None:
    """
    ARRANGE: page_json with recordsTotal as string
    ACT:     call _get_total_records
    ASSERT:  returns integer 7
    """
    expected_records_total = 7
    page_json = {"recordsTotal": "7"}

    actual = _get_total_records(page_json)

    assert actual == expected_records_total


def test_extract_equity_records_maps_name() -> None:
    """
    ARRANGE: page_json with one record having originalValue "Foo Corp"
    ACT:     call _extract_equity_records
    ASSERT:  first record's name == "Foo Corp"
    """
    record = {
        "name": {"originalValue": "Foo Corp"},
        "wkn": "WKN123",
        "isin": "ISIN123",
        "slug": "foo-corp",
        "overview": {"desc": "desc"},
        "performance": {"p": 1},
        "keyData": {"k": 2},
        "sustainability": {"s": 3},
    }
    page_json = {"data": [record]}

    actual = _extract_equity_records(page_json)

    assert actual[0]["name"] == "Foo Corp"


def test_fetch_page_returns_json_response() -> None:
    """
    ARRANGE: AsyncClient with MockTransport returning JSON {"a":1}
    ACT:     call _fetch_page(client, offset=10)
    ASSERT:  result equals {"a":1}
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"a": 1})

    client = AsyncClient(transport=MockTransport(handler))

    actual = asyncio.run(_fetch_page(client, offset=10))

    assert actual == {"a": 1}


def test_fetch_equity_records_streams_two_records() -> None:
    """
    ARRANGE: two-page MockTransport, each with a single record
    ACT:     collect via fetch_equity_records with dummy factory
    ASSERT:  two records are returned
    """
    expected_records_total = 2

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        offset = body.get("offset", 0)
        if offset == 0:
            response_json = {
                "recordsTotal": 2,
                "data": [
                    {
                        "name": {"originalValue": "First"},
                        "wkn": "",
                        "isin": "ISIN_FIRST",
                        "slug": "",
                        "overview": {},
                        "performance": {},
                        "keyData": {},
                        "sustainability": {},
                    },
                ],
            }
        else:
            response_json = {
                "recordsTotal": 2,
                "data": [
                    {
                        "name": {"originalValue": "Second"},
                        "wkn": "",
                        "isin": "ISIN_SECOND",
                        "slug": "",
                        "overview": {},
                        "performance": {},
                        "keyData": {},
                        "sustainability": {},
                    },
                ],
            }
        return httpx.Response(200, json=response_json)

    def factory() -> AsyncClient:
        return AsyncClient(
            transport=MockTransport(handler),
        )

    async def collect() -> list[dict[str, object]]:
        return [record async for record in fetch_equity_records(client_factory=factory)]

    actual = asyncio.run(collect())

    assert len(actual) == expected_records_total


def test_fetch_page_400_bad_request_raises_http_status_error() -> None:
    """
    ARRANGE: MockTransport returns 400 Bad Request
    ACT:     call _fetch_page
    ASSERT:  HTTPStatusError is raised
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400)

    client = AsyncClient(transport=MockTransport(handler))

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(_fetch_page(client, offset=0))


def test_fetch_page_404_not_found_raises_http_status_error() -> None:
    """
    ARRANGE: MockTransport returns 404 Not Found
    ACT:     call _fetch_page
    ASSERT:  HTTPStatusError is raised
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404)

    client = AsyncClient(transport=MockTransport(handler))

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(_fetch_page(client, offset=0))


def test_fetch_page_429_too_many_requests_raises_http_status_error() -> None:
    """
    ARRANGE: MockTransport returns 429 Too Many Requests
    ACT:     call _fetch_page
    ASSERT:  HTTPStatusError is raised
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429)

    client = AsyncClient(transport=MockTransport(handler))

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(_fetch_page(client, offset=0))


def test_fetch_page_500_internal_server_error_raises_http_status_error() -> None:
    """
    ARRANGE: MockTransport returns 500 Internal Server Error
    ACT:     call _fetch_page
    ASSERT:  HTTPStatusError is raised
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    client = AsyncClient(transport=MockTransport(handler))

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(_fetch_page(client, offset=0))


def test_fetch_equity_records_propagates_500_error() -> None:
    """
    ARRANGE: first-page fetch returns 500
    ACT:     iterate fetch_equity_records
    ASSERT:  HTTPStatusError bubbles up
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    def factory() -> AsyncClient:
        return AsyncClient(transport=MockTransport(handler))

    with pytest.raises(httpx.HTTPStatusError):

        async def collect() -> list[dict[str, object]]:
            return [
                record async for record in fetch_equity_records(client_factory=factory)
            ]

        asyncio.run(collect())


def test_fetch_page_malformed_json_raises_value_error() -> None:
    """
    ARRANGE: MockTransport returns non-JSON body
    ACT:     call _fetch_page
    ASSERT:  raises ValueError
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"not json")

    client = AsyncClient(transport=MockTransport(handler))

    with pytest.raises(ValueError):
        asyncio.run(_fetch_page(client, offset=0))


def test_fetch_page_read_timeout() -> None:
    """
    ARRANGE: handler raises ReadTimeout
    ACT:     call _fetch_page
    ASSERT:  ReadTimeout is raised
    """

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("timeout")

    client = AsyncClient(transport=MockTransport(handler))

    with pytest.raises(httpx.ReadTimeout):
        asyncio.run(_fetch_page(client, offset=0))


def test_fetch_equity_records_partial_first_page() -> None:
    """
    ARRANGE: first page len=5, total=10
    ACT:     call fetch_equity_records
    ASSERT:  yields 10 records
    """

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        offset = body.get("offset", 0)
        if offset == 0:
            data = [
                {
                    "name": {"originalValue": str(i)},
                    "wkn": "",
                    "isin": str(i),
                    "slug": "",
                    "overview": {},
                    "performance": {},
                    "keyData": {},
                    "sustainability": {},
                }
                for i in range(5)
            ]
            total = 10
        else:
            data = [
                {
                    "name": {"originalValue": str(i)},
                    "wkn": "",
                    "isin": str(i),
                    "slug": "",
                    "overview": {},
                    "performance": {},
                    "keyData": {},
                    "sustainability": {},
                }
                for i in range(5, 10)
            ]
            total = 10
        return httpx.Response(200, json={"recordsTotal": total, "data": data})

    expected_records_total = 10

    def factory() -> AsyncClient:
        return AsyncClient(transport=MockTransport(handler))

    async def collect() -> list[dict[str, object]]:
        return [record async for record in fetch_equity_records(client_factory=factory)]

    actual = asyncio.run(collect())

    assert len(actual) == expected_records_total


def test_extract_equity_records_missing_original_value_raises_key_error() -> None:
    """
    ARRANGE: data item missing originalValue
    ACT:     call _extract_equity_records
    ASSERT:  KeyError is raised
    """
    with pytest.raises(KeyError):
        _extract_equity_records({"data": [{"name": {}}]})


def test_request_payload_offset_in_body() -> None:
    """
    ARRANGE: capture request content
    ACT:     call _fetch_page
    ASSERT:  payload offset equals 7
    """
    seen = {}
    expected_records_total = 7

    def handler(request: httpx.Request) -> httpx.Response:
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={"recordsTotal": 0, "data": []})

    client = AsyncClient(transport=MockTransport(handler))

    asyncio.run(_fetch_page(client, offset=7))
    assert seen["body"]["offset"] == expected_records_total
