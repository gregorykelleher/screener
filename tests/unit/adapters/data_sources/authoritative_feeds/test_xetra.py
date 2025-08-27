# authoritative_feeds/test_xetra.py

import asyncio
import json
from collections.abc import AsyncIterator

import httpx
import pytest
from httpx import AsyncClient, MockTransport

from equity_aggregator.adapters.data_sources.authoritative_feeds.xetra import (
    _build_payload,
    _consume_queue,
    _deduplicate_records,
    _extract_records,
    _fetch_page,
    _get_total_records,
    _produce_page,
    fetch_equity_records,
)
from equity_aggregator.storage import save_cache

pytestmark = pytest.mark.unit


def test_build_payload_contains_expected_keys() -> None:
    """
    ARRANGE: offset is 5
    ACT:     call _build_payload(5)
    ASSERT:  keys exactly match expected set
    """
    payload = _build_payload(5)

    assert set(payload.keys()) == {"stockExchanges", "lang", "offset", "limit"}


def test_request_payload_offset_in_body() -> None:
    """
    ARRANGE: capture request content
    ACT:     _fetch_page with offset 7
    ASSERT:  payload offset equals 7
    """
    captured: dict[str, object] = {}
    expected_offset = 7

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"recordsTotal": 0, "data": []})

    client = AsyncClient(transport=MockTransport(handler))

    asyncio.run(_fetch_page(client, offset=expected_offset))

    assert captured["body"]["offset"] == expected_offset


def test_get_total_records_parses_integer() -> None:
    """
    ARRANGE: page_json with recordsTotal "7"
    ACT:     call _get_total_records
    ASSERT:  returns integer 7
    """
    page_json = {"recordsTotal": "7"}
    expected_records_total = 7

    actual = _get_total_records(page_json)

    assert actual == expected_records_total


def test_get_total_records_returns_zero_when_missing() -> None:
    """
    ARRANGE: page_json without recordsTotal
    ACT:     call _get_total_records
    ASSERT:  returns 0
    """
    actual = _get_total_records({})

    assert actual == 0


def test_extract_records_maps_name() -> None:
    """
    ARRANGE: page_json with one record named "Foo Corp"
    ACT:     call _extract_records
    ASSERT:  first record's name == "Foo Corp"
    """
    page_json = {
        "data": [
            {
                "name": {"originalValue": "Foo Corp"},
                "wkn": "",
                "isin": "ISIN1",
                "slug": "",
                "overview": {},
                "performance": {},
                "keyData": {},
                "sustainability": {},
            },
        ],
    }

    records = _extract_records(page_json)

    assert records[0]["name"] == "Foo Corp"


def test_extract_records_empty_data_returns_empty_list() -> None:
    """
    ARRANGE: page_json with empty data list
    ACT:     call _extract_records
    ASSERT:  returns empty list
    """
    records = _extract_records({"data": []})

    assert records == []


def test_extract_records_missing_original_value_raises_key_error() -> None:
    """
    ARRANGE: data item missing originalValue
    ACT:     call _extract_records
    ASSERT:  KeyError is raised
    """
    page_json = {"data": [{"name": {}}]}

    with pytest.raises(KeyError):
        _extract_records(page_json)


def test_fetch_page_returns_json_response() -> None:
    """
    ARRANGE: MockTransport returns JSON {"a":1}
    ACT:     call _fetch_page(offset=10)
    ASSERT:  actual equals {"a":1}
    """

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"a": 1})

    client = AsyncClient(transport=MockTransport(handler))

    actual = asyncio.run(_fetch_page(client, offset=10))

    assert actual == {"a": 1}


def test_fetch_page_400_bad_request_raises_http_status_error() -> None:
    """
    ARRANGE: MockTransport returns 400
    ACT:     call _fetch_page
    ASSERT:  HTTPStatusError is raised
    """

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(400)

    client = AsyncClient(transport=MockTransport(handler))

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(_fetch_page(client, offset=0))


def test_fetch_page_404_not_found_raises_http_status_error() -> None:
    """
    ARRANGE: MockTransport returns 404
    ACT:     call _fetch_page
    ASSERT:  HTTPStatusError is raised
    """

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(404)

    client = AsyncClient(transport=MockTransport(handler))

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(_fetch_page(client, offset=0))


def test_fetch_page_429_too_many_requests_raises_http_status_error() -> None:
    """
    ARRANGE: MockTransport returns 429
    ACT:     call _fetch_page
    ASSERT:  HTTPStatusError is raised
    """

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(429)

    client = AsyncClient(transport=MockTransport(handler))

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(_fetch_page(client, offset=0))


def test_fetch_page_500_internal_server_error_raises_http_status_error() -> None:
    """
    ARRANGE: MockTransport returns 500
    ACT:     call _fetch_page
    ASSERT:  HTTPStatusError is raised
    """

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    client = AsyncClient(transport=MockTransport(handler))

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(_fetch_page(client, offset=0))


def test_fetch_page_malformed_json_raises_value_error() -> None:
    """
    ARRANGE: MockTransport returns non-JSON body
    ACT:     call _fetch_page
    ASSERT:  ValueError is raised
    """

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"not json")

    client = AsyncClient(transport=MockTransport(handler))

    with pytest.raises(ValueError):
        asyncio.run(_fetch_page(client, offset=0))


def test_fetch_page_read_timeout_propagates() -> None:
    """
    ARRANGE: handler raises ReadTimeout
    ACT:     call _fetch_page
    ASSERT:  ReadTimeout is raised
    """

    def handler(_: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("timeout")

    client = AsyncClient(transport=MockTransport(handler))

    with pytest.raises(httpx.ReadTimeout):
        asyncio.run(_fetch_page(client, offset=0))


def test_deduplicate_records_single_output_for_duplicate_isin() -> None:
    """
    ARRANGE: two dicts share same ISIN
    ACT:     run through _deduplicate_records
    ASSERT:  only first dict yielded
    """

    async def source() -> AsyncIterator[dict[str, str]]:
        for record in [{"isin": "DUP"}, {"isin": "DUP"}]:
            yield record

    async def collect() -> list[dict[str, str]]:
        dedup = _deduplicate_records(lambda record: record["isin"])
        return [record async for record in dedup(source())]

    actual = asyncio.run(collect())

    assert len(actual) == 1


def test_consume_queue_yields_until_sentinel() -> None:
    """
    ARRANGE: queue with two items and sentinel
    ACT:     _consume_queue(expected_sentinels=1)
    ASSERT:  items [1,2] yielded in order
    """

    async def collect() -> list[int]:
        queue: asyncio.Queue[int | None] = asyncio.Queue()
        await queue.put(1)
        await queue.put(2)
        await queue.put(None)
        return [record async for record in _consume_queue(queue, expected_sentinels=1)]

    actual = asyncio.run(collect())

    assert actual == [1, 2]


def test_produce_page_places_sentinel_on_success() -> None:
    """
    ARRANGE: MockTransport returns single-row page
    ACT:     _produce_page
    ASSERT:  record followed by sentinel enqueued
    """
    row = {
        "name": {"originalValue": "One"},
        "wkn": "",
        "isin": "ISIN1",
        "slug": "",
        "overview": {},
        "performance": {},
        "keyData": {},
        "sustainability": {},
    }
    payload = {"recordsTotal": 1, "data": [row]}

    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    client = AsyncClient(transport=MockTransport(handler))

    async def run_test() -> tuple[dict | None, dict | None]:
        queue: asyncio.Queue[dict | None] = asyncio.Queue()
        await _produce_page(client, offset=0, queue=queue)
        return await queue.get(), await queue.get()

    first, second = asyncio.run(run_test())

    assert (first, second) == (_extract_records(payload)[0], None)


def test_fetch_equity_records_streams_two_records() -> None:
    """
    ARRANGE: first page has 100 rows, total=101 so a second page is required
    ACT:     iterate fetch_equity_records with MockTransport
    ASSERT:  101 records are returned
    """

    def make_row(index: int) -> dict[str, object]:
        return {
            "name": {"originalValue": f"R{index}"},
            "wkn": "",
            "isin": f"ISIN_{index}",
            "slug": "",
            "overview": {},
            "performance": {},
            "keyData": {},
            "sustainability": {},
        }

    def handler(request: httpx.Request) -> httpx.Response:
        offset = json.loads(request.content)["offset"]
        data = (
            [make_row(i) for i in range(100)] if offset == 0 else [make_row(100)]
        )  # first page (100 rows), final page (1 row)
        return httpx.Response(200, json={"recordsTotal": 101, "data": data})

    expected_total_records = 101

    client = AsyncClient(transport=MockTransport(handler))

    async def collect() -> list[dict[str, object]]:
        return [record async for record in fetch_equity_records(client)]

    records = asyncio.run(collect())

    assert len(records) == expected_total_records


def test_fetch_equity_records_exits_on_first_page_500() -> None:
    """
    ARRANGE: first page returns 500
    ACT:     iterate fetch_equity_records
    ASSERT:  httpx.HTTPStatusError is raised
    """

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    client = AsyncClient(transport=MockTransport(handler))

    async def iterate() -> None:
        async for _ in fetch_equity_records(client):
            pass

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(iterate())


def test_fetch_equity_records_deduplicates_across_pages() -> None:
    """
    ARRANGE: two pages share same ISIN
    ACT:     iterate fetch_equity_records
    ASSERT:  only one unique record yielded
    """

    def handler(request: httpx.Request) -> httpx.Response:
        offset = json.loads(request.content)["offset"]
        data = [
            {
                "name": {"originalValue": f"Name-{offset}"},
                "wkn": "",
                "isin": "DUPLICATE",
                "slug": "",
                "overview": {},
                "performance": {},
                "keyData": {},
                "sustainability": {},
            },
        ]
        return httpx.Response(200, json={"recordsTotal": 2, "data": data})

    client = AsyncClient(transport=MockTransport(handler))

    async def collect() -> list[dict[str, object]]:
        return [record async for record in fetch_equity_records(client)]

    records = asyncio.run(collect())

    assert len(records) == 1


async def test_produce_page_raises_and_still_sends_sentinel() -> None:
    """
    ARRANGE: MockTransport whose handler raises ReadTimeout
    ACT:     call _produce_page
    ASSERT:  ReadTimeout propagates *and* exactly one sentinel enqueued
    """

    async def handler(_: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("timeout")

    client = AsyncClient(transport=MockTransport(handler))
    queue: asyncio.Queue[dict | None] = asyncio.Queue()

    with pytest.raises(httpx.ReadTimeout):
        await _produce_page(client, offset=0, queue=queue)

    sentinel = await queue.get()
    assert sentinel is None


def test_fetch_equity_records_uses_cache() -> None:
    """
    ARRANGE: cache primed with two known records
    ACT:     collect via fetch_equity_records (no HTTP invoked)
    ASSERT:  yielded records equal the cached payload
    """
    payload = [
        {"isin": "XCACHED1", "mics": ["XETR"]},
        {"isin": "XCACHED2", "mics": ["XETR"]},
    ]

    save_cache("xetra_records", payload)

    async def collect() -> list[dict]:
        return [record async for record in fetch_equity_records()]

    actual = asyncio.run(collect())

    assert actual == payload
