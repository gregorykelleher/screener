import asyncio
import json
from collections.abc import AsyncIterator

import httpx
import pytest
from httpx import AsyncClient, MockTransport

from equity_aggregator.adapters.data_sources.authoritative_feeds.lse import (
    _build_payload,
    _equity_pages,
    _fetch_page,
    _parse_equities,
    _try_fetch_page,
    _unique_by_key,
    fetch_equity_records,
)

pytestmark = pytest.mark.unit


def test_build_payload_contains_expected_keys() -> None:
    """
    ARRANGE: page is set to 3
    ACT:     call _build_payload(3)
    ASSERT:  the returned dict has exactly the expected keys
    """
    page = 3

    actual = _build_payload(page)

    assert set(actual.keys()) == {"path", "parameters", "components"}


def test_parse_equities_returns_records_list() -> None:
    """
    ARRANGE: well-formed data with one content item named priceexplorersearch
    ACT:     call _parse_equities
    ASSERT:  the returned records list matches input
    """
    expected_record = {"foo": "bar"}

    data = {
        "content": [
            {
                "name": "priceexplorersearch",
                "value": {"content": [expected_record], "totalPages": 5},
            },
        ],
    }

    records, _ = _parse_equities(data)

    assert records == [expected_record]


def test_parse_equities_returns_total_pages() -> None:
    """
    ARRANGE: well-formed data with totalPages = 7
    ACT:     call _parse_equities
    ASSERT:  the returned total_pages equals 7
    """
    expected_page_total = 7
    raw_data = {
        "content": [
            {
                "name": "priceexplorersearch",
                "value": {"content": [], "totalPages": 7},
            },
        ],
    }

    _, total = _parse_equities(raw_data)

    assert total == expected_page_total


def test_parse_equities_handles_missing_search_key() -> None:
    """
    ARRANGE: data without a priceexplorersearch item
    ACT:     call _parse_equities
    ASSERT:  records list is empty
    """
    raw_data = {"content": [{"name": "other", "value": {}}]}

    records, _ = _parse_equities(raw_data)

    assert records == []


def test_parse_equities_handles_none_input() -> None:
    """
    ARRANGE: input is None
    ACT:     call _parse_equities
    ASSERT:  total_pages is None
    """
    _, total = _parse_equities(None)

    assert total is None


def test_fetch_page_returns_first_json_element() -> None:
    """
    ARRANGE: AsyncClient with MockTransport returning JSON list [{"a": 1}]
    ACT:     call _fetch_page with payload for page 1
    ASSERT:  result equals {"a": 1}
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[{"a": 1}])

    client = AsyncClient(transport=MockTransport(handler))
    payload = _build_payload(1)

    actual = asyncio.run(_fetch_page(client, payload))

    assert actual == {"a": 1}


def test_try_fetch_page_returns_none_on_http_error() -> None:
    """
    ARRANGE: transport returns 500
    ACT:     call _try_fetch_page for page 1
    ASSERT:  result is None
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    client = AsyncClient(transport=MockTransport(handler))

    actual = asyncio.run(_try_fetch_page(client, 1))

    assert actual is None


def test_try_fetch_page_returns_data_on_success() -> None:
    """
    ARRANGE: transport returns JSON list [{"b": 2}]
    ACT:     call _try_fetch_page for page 2
    ASSERT:  result equals {"b": 2}
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[{"b": 2}])

    client = AsyncClient(transport=MockTransport(handler))

    actual = asyncio.run(_try_fetch_page(client, 2))

    assert actual == {"b": 2}


def test_equity_pages_stream_two_pages() -> None:
    """
    ARRANGE: two-page MockTransport, each with a distinct record
    ACT:     collect pages via _equity_pages
    ASSERT:  two lists of records are returned
    """

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        # extract page number from parameters string in the first component
        params = body["components"][0]["parameters"]
        page_str = params.split("&page=")[1].split("&")[0]
        page = int(page_str)
        content_item = {
            "name": "priceexplorersearch",
            "value": {"content": [{"val": page}], "totalPages": 2},
        }
        return httpx.Response(200, json=[{"content": [content_item]}])

    client = AsyncClient(transport=MockTransport(handler))

    async def collect_pages() -> list[list[dict]]:
        return [page async for page in _equity_pages(client)]

    actual = asyncio.run(collect_pages())

    assert actual == [[{"val": 1}], [{"val": 2}]]


def test_stream_equity_records_flattens_pages() -> None:
    """
    ARRANGE: two-page MockTransport, each with one record
    ACT:     collect records via fetch_equity_records
    ASSERT:  two records are returned
    """

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        params = body["components"][0]["parameters"]
        page_str = params.split("&page=")[1].split("&")[0]
        page = int(page_str)
        content_item = {
            "name": "priceexplorersearch",
            "value": {"content": [{"idx": page}], "totalPages": 2},
        }
        return httpx.Response(200, json=[{"content": [content_item]}])

    def factory() -> AsyncClient:
        return AsyncClient(transport=MockTransport(handler))

    async def collect_records() -> list[dict]:
        return [rec async for rec in fetch_equity_records(client_factory=factory)]

    actual = asyncio.run(collect_records())

    assert actual == [{"idx": 1}, {"idx": 2}]


def test_fetch_equity_records_skips_failed_first_page() -> None:
    """
    ARRANGE: first page always returns 500
    ACT:     collect records via fetch_equity_records
    ASSERT:  no records returned
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    def factory() -> AsyncClient:
        return AsyncClient(transport=MockTransport(handler))

    async def collect_records() -> list[dict]:
        return [rec async for rec in fetch_equity_records(client_factory=factory)]

    actual = asyncio.run(collect_records())

    assert len(actual) == 0


def test_fetch_equity_records_deduplicates_isin_across_pages() -> None:
    """
    ARRANGE: two pages both contain the same ISIN
    ACT:     collect records via fetch_equity_records
    ASSERT:  only one unique record yielded
    """

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        params = body["components"][0]["parameters"]
        _ = int(params.split("&page=")[1].split("&")[0])
        record = {"isin": "DUP"}
        content_item = {
            "name": "priceexplorersearch",
            "value": {"content": [record], "totalPages": 2},
        }
        return httpx.Response(200, json=[{"content": [content_item]}])

    def factory() -> AsyncClient:
        return AsyncClient(transport=MockTransport(handler))

    async def collect_records() -> list[dict]:
        return [rec async for rec in fetch_equity_records(client_factory=factory)]

    actual = asyncio.run(collect_records())

    assert len(actual) == 1


def test_unique_by_key_emits_single_record_for_duplicate_isin() -> None:
    """
    ARRANGE: two dicts share the same ISIN
    ACT:     run through _unique_by_key
    ASSERT:  only the first dict is yielded
    """

    async def source() -> AsyncIterator[dict]:
        for record in [{"isin": "X"}, {"isin": "X"}]:
            yield record

    async def collect_unique() -> list[dict]:
        return [
            record async for record in _unique_by_key(source(), lambda x: x["isin"])
        ]

    actual = asyncio.run(collect_unique())

    assert len(actual) == 1


def test_build_payload_encodes_page_and_size() -> None:
    """
    ARRANGE: page is set to 5
    ACT:     call _build_payload(5)
    ASSERT:  parameters include both page and size
    """
    page = 5

    payload = _build_payload(page)
    params = payload["components"][0]["parameters"]

    assert f"&page={page}" in params and "&size=100" in params


def test_parse_equities_handles_missing_content_key_records() -> None:
    """
    ARRANGE: data missing 'content' key
    ACT:     call _parse_equities({})
    ASSERT:  records list is empty
    """
    records, _ = _parse_equities({})

    assert records == []


def test_parse_equities_handles_missing_content_key_total_pages() -> None:
    """
    ARRANGE: data missing 'content' key
    ACT:     call _parse_equities({})
    ASSERT:  total_pages is None
    """
    _, total = _parse_equities({})

    assert total is None


def test_try_fetch_page_returns_none_on_read_error() -> None:
    """
    ARRANGE: transport raises ReadError
    ACT:     call _try_fetch_page for page 1
    ASSERT:  result is None
    """

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadError("network glitch")

    client = AsyncClient(transport=MockTransport(handler))

    actual = asyncio.run(_try_fetch_page(client, 1))

    assert actual is None


def test_fetch_page_raises_index_error_on_empty_list() -> None:
    """
    ARRANGE: MockTransport returns empty JSON list
    ACT:     call _fetch_page with payload for page 1
    ASSERT:  IndexError is raised
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[])

    client = AsyncClient(transport=MockTransport(handler))

    with pytest.raises(IndexError):
        asyncio.run(_fetch_page(client, _build_payload(1)))


def test_equity_pages_single_page() -> None:
    """
    ARRANGE: API reports only one page
    ACT:     collect pages via _equity_pages
    ASSERT:  yields exactly one page with expected record
    """

    def handler(request: httpx.Request) -> httpx.Response:
        content_item = {
            "name": "priceexplorersearch",
            "value": {"content": [{"foo": "bar"}], "totalPages": 1},
        }
        return httpx.Response(200, json=[{"content": [content_item]}])

    client = AsyncClient(transport=MockTransport(handler))

    async def collect_pages() -> list[list[dict]]:
        return [page async for page in _equity_pages(client)]

    actual = asyncio.run(collect_pages())

    assert actual == [[{"foo": "bar"}]]


def test_unique_by_key_preserves_all_none_keys() -> None:
    """
    ARRANGE: mix of None and non-None isin keys
    ACT:     call _unique_by_key
    ASSERT:  preserves all None-key records and first non-None unique record
    """

    async def src() -> AsyncIterator[dict[str, object]]:
        yield {"isin": None, "foo": 1}
        yield {"isin": None, "foo": 2}
        yield {"isin": "A", "foo": 3}
        yield {"isin": None, "foo": 4}
        yield {"isin": "A", "foo": 5}

    async def collect() -> list[dict[str, object]]:
        return [
            record
            async for record in _unique_by_key(src(), lambda record: record["isin"])
        ]

    actual = asyncio.run(collect())

    assert actual == [
        {"isin": None, "foo": 1},
        {"isin": None, "foo": 2},
        {"isin": "A", "foo": 3},
        {"isin": None, "foo": 4},
    ]
