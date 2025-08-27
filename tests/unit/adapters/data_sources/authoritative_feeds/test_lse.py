# authoritative_feeds/test_lse.py

import asyncio
import json
from collections.abc import AsyncIterator

import httpx
import pytest
from httpx import AsyncClient, MockTransport

from equity_aggregator.adapters.data_sources.authoritative_feeds.lse import (
    _build_payload,
    _deduplicate_records,
    _fetch_page,
    _parse_equities,
    _produce_page,
    _stream_all_pages,
    fetch_equity_records,
)
from equity_aggregator.storage import save_cache

pytestmark = pytest.mark.unit


def _page_from_request(request: httpx.Request) -> int:
    """
    Extract the 'page=' query-argument from the JSON payload sent to the LSE API.
    """
    body = json.loads(request.content)
    params = body["components"][0]["parameters"]
    return int(params.split("&page=")[1].split("&")[0])


def test_build_payload_contains_expected_keys() -> None:
    """
    ARRANGE: page set to 3
    ACT:     call _build_payload(3)
    ASSERT:  dict has keys {'path','parameters','components'}
    """
    actual = _build_payload(3)

    assert set(actual.keys()) == {"path", "parameters", "components"}


def test_build_payload_encodes_page_and_size() -> None:
    """
    ARRANGE: page set to 5
    ACT:     call _build_payload(5)
    ASSERT:  parameters contain '&page=5' and '&size=100'
    """
    params = _build_payload(5)["components"][0]["parameters"]

    assert "&page=5" in params and "&size=100" in params


def test_parse_equities_returns_records_list() -> None:
    """
    ARRANGE: one record inside a priceexplorersearch block
    ACT:     call _parse_equities
    ASSERT:  records list equals input
    """
    expected_record = {"foo": "bar"}
    payload = {
        "content": [
            {
                "name": "priceexplorersearch",
                "value": {"content": [expected_record], "totalPages": 5},
            },
        ],
    }

    records, _ = _parse_equities(payload)

    assert records == [expected_record]


def test_parse_equities_returns_total_pages() -> None:
    """
    ARRANGE: totalPages set to 7
    ACT:     call _parse_equities
    ASSERT:  returned total_pages == 7
    """
    payload = {
        "content": [
            {
                "name": "priceexplorersearch",
                "value": {"content": [], "totalPages": 7},
            },
        ],
    }

    expected_total_pages = 7

    _, total_pages = _parse_equities(payload)

    assert total_pages == expected_total_pages


def test_parse_equities_handles_missing_search_key() -> None:
    """
    ARRANGE: payload without priceexplorersearch block
    ACT:     call _parse_equities
    ASSERT:  records list is empty
    """
    records, _ = _parse_equities({"content": [{"name": "other", "value": {}}]})

    assert records == []


def test_parse_equities_handles_missing_content_key() -> None:
    """
    ARRANGE: payload missing 'content'
    ACT:     call _parse_equities
    ASSERT:  records list is empty
    """
    records, _ = _parse_equities({})

    assert records == []


def test_parse_equities_handles_missing_content_key_total_pages() -> None:
    """
    ARRANGE: payload missing 'content'
    ACT:     call _parse_equities
    ASSERT:  total_pages is None
    """
    _, total_pages = _parse_equities({})

    assert total_pages is None


def test_fetch_page_returns_first_json_element() -> None:
    """
    ARRANGE: transport returns JSON list [{'a':1}]
    ACT:     call _fetch_page(client, page=1)
    ASSERT:  actual == {'a':1}
    """

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[{"a": 1}])

    client = AsyncClient(transport=MockTransport(handler))

    actual = asyncio.run(_fetch_page(client, 1))

    assert actual == {"a": 1}


def test_fetch_page_raises_index_error_on_empty_list() -> None:
    """
    ARRANGE: transport returns []
    ACT:     call _fetch_page
    ASSERT:  IndexError is raised
    """

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[])

    client = AsyncClient(transport=MockTransport(handler))

    with pytest.raises(IndexError):
        asyncio.run(_fetch_page(client, 1))


def test_stream_all_pages_yields_records_from_two_pages() -> None:
    """
    ARRANGE: first page totalPages=2, pages contain distinct values
    ACT:     collect via _stream_all_pages
    ASSERT:  records [{'val':1}, {'val':2}] returned
    """

    def handler(request: httpx.Request) -> httpx.Response:
        page = _page_from_request(request)
        content_item = {
            "name": "priceexplorersearch",
            "value": {"content": [{"val": page}], "totalPages": 2},
        }
        return httpx.Response(200, json=[{"content": [content_item]}])

    client = AsyncClient(transport=MockTransport(handler))

    async def collect() -> list[dict]:
        return [rec async for rec in _stream_all_pages(client)]

    actual = asyncio.run(collect())

    assert actual == [
        {"val": 1, "mics": ["XLON"]},
        {"val": 2, "mics": ["XLON"]},
    ]


def test_stream_all_pages_single_page() -> None:
    """
    ARRANGE: API reports only one page
    ACT:     collect via _stream_all_pages
    ASSERT:  exactly the records from the single page are yielded
    """

    def handler(_: httpx.Request) -> httpx.Response:
        content_item = {
            "name": "priceexplorersearch",
            "value": {"content": [{"foo": "bar"}], "totalPages": 1},
        }
        return httpx.Response(200, json=[{"content": [content_item]}])

    client = AsyncClient(transport=MockTransport(handler))

    async def collect() -> list[dict]:
        return [rec async for rec in _stream_all_pages(client)]

    actual = asyncio.run(collect())

    assert actual == [{"foo": "bar", "mics": ["XLON"]}]


def test_fetch_equity_records_flattens_pages() -> None:
    """
    ARRANGE: two pages with one record each
    ACT:     collect via fetch_equity_records
    ASSERT:  two records returned
    """

    def handler(request: httpx.Request) -> httpx.Response:
        page = _page_from_request(request)
        content_item = {
            "name": "priceexplorersearch",
            "value": {
                "content": [
                    {"isin": str(page)},
                ],
                "totalPages": 2,
            },
        }
        return httpx.Response(200, json=[{"content": [content_item]}])

    client = AsyncClient(transport=MockTransport(handler))

    async def collect_records() -> list[dict]:
        return [record async for record in fetch_equity_records(client)]

    actual = asyncio.run(collect_records())

    assert actual == [
        {"isin": "1", "mics": ["XLON"]},
        {"isin": "2", "mics": ["XLON"]},
    ]


def test_fetch_equity_records_exits_on_first_page_error() -> None:
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


def test_fetch_equity_records_deduplicates_isin_across_pages() -> None:
    """
    ARRANGE: two pages share the same ISIN
    ACT:     collect via fetch_equity_records
    ASSERT:  only one unique record returned
    """

    def handler(request: httpx.Request) -> httpx.Response:
        _ = _page_from_request(request)
        record = {"isin": "DUP"}
        content_item = {
            "name": "priceexplorersearch",
            "value": {"content": [record], "totalPages": 2},
        }
        return httpx.Response(200, json=[{"content": [content_item]}])

    client = AsyncClient(transport=MockTransport(handler))

    async def collect() -> list[dict]:
        return [r async for r in fetch_equity_records(client)]

    actual = asyncio.run(collect())

    assert len(actual) == 1


def test_deduplicate_records_filters_duplicates() -> None:
    """
    ARRANGE: two records share the same ISIN
    ACT:     run through _deduplicate_records
    ASSERT:  only first record yielded
    """

    async def source() -> AsyncIterator[dict]:
        for record in [{"isin": "X"}, {"isin": "X"}]:
            yield record

    async def collect() -> list[dict]:
        dedup = _deduplicate_records(lambda rec: rec["isin"])
        return [record async for record in dedup(source())]

    actual = asyncio.run(collect())

    assert actual == [{"isin": "X"}]


def test_deduplicate_records_preserves_all_none_keys() -> None:
    """
    ARRANGE: mix of None and non-None keys
    ACT:     call _deduplicate_records
    ASSERT:  all None-key records kept, non-None deduped
    """

    async def src() -> AsyncIterator[dict]:
        yield {"isin": None, "val": 1}
        yield {"isin": None, "val": 2}
        yield {"isin": "A", "val": 3}
        yield {"isin": None, "val": 4}
        yield {"isin": "A", "val": 5}

    async def collect() -> list[dict]:
        dedup = _deduplicate_records(lambda record: record["isin"])
        return [record async for record in dedup(src())]

    actual = asyncio.run(collect())

    assert actual == [
        {"isin": None, "val": 1},
        {"isin": "A", "val": 3},
    ]


def test_produce_page_on_error_always_puts_sentinel() -> None:
    """
    ARRANGE: a client whose .post(...) raises
    ACT:     run _produce_page and then consume its queue
    ASSERT:  we get exactly one None sentinel, even though it raised
    """

    class BadClient:
        async def post(self, *args: object, **kwargs: object) -> None:
            raise RuntimeError("boom!")

    queue: asyncio.Queue[dict | None] = asyncio.Queue()

    async def run_and_collect() -> list[None]:
        import contextlib

        with contextlib.suppress(RuntimeError):
            await _produce_page(BadClient(), page=5, queue=queue)

        # Ensure that a sentinel is in the queue
        return [await queue.get()]

    actual = asyncio.run(run_and_collect())
    assert actual == [None]


def test_fetch_equity_records_uses_cache() -> None:
    """
    ARRANGE: cache primed with two known records
    ACT:     collect via fetch_equity_records
    ASSERT:  yielded records equal the cached payload
    """
    payload = [
        {"isin": "CACHED1", "mics": ["XLON"]},
        {"isin": "CACHED2", "mics": ["XLON"]},
    ]

    save_cache("lse_records", payload)

    async def collect() -> list[dict]:
        return [record async for record in fetch_equity_records()]

    actual = asyncio.run(collect())

    assert actual == payload
