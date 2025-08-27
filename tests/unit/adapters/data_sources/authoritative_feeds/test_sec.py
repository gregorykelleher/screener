# authoritative_feeds/test_sec.py

import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from equity_aggregator.adapters.data_sources.authoritative_feeds.sec import (
    _deduplicate_records,
    _parse_row,
    _stream_and_cache,
    _stream_sec,
    fetch_equity_records,
)
from equity_aggregator.storage import save_cache

pytestmark = pytest.mark.unit


def test_parse_row_maps_mic() -> None:
    """
    ARRANGE: row with Nasdaq exchange
    ACT:     call _parse_row
    ASSERT:  maps Nasdaq → XNAS
    """
    row = [1, "Foo Inc", "FOO", "Nasdaq"]

    record = _parse_row(row)

    assert record["mics"] == ["XNAS"]


def test_parse_row_unknown_exchange() -> None:
    """
    ARRANGE: row with unmapped exchange
    ACT:     call _parse_row
    ASSERT:  returns empty mics list
    """
    row = [1, "Foo Inc", "FOO", "UNKNOWN"]

    record = _parse_row(row)

    assert record["mics"] == []


async def test_deduplicate_records_filters_duplicates() -> None:
    """
    ARRANGE: two records share identical CIK
    ACT:     run deduplicator
    ASSERT:  yields a single unique record
    """

    async def _gen() -> AsyncGenerator[dict, None]:
        yield {"cik": 1}
        yield {"cik": 1}

    deduplicator = _deduplicate_records(lambda r: r["cik"])

    uniques = [record async for record in deduplicator(_gen())]

    assert len(uniques) == 1


async def test_deduplicate_records_preserves_first_occurrence() -> None:
    """
    ARRANGE: first record differs from duplicate that follows
    ACT:     run deduplicator
    ASSERT:  first record is preserved
    """

    async def _gen() -> AsyncGenerator[dict, None]:
        yield {"cik": 1, "name": "FIRST"}
        yield {"cik": 1, "name": "SECOND"}

    deduplicator = _deduplicate_records(lambda record: record["cik"])

    uniques = [record async for record in deduplicator(_gen())]

    assert uniques[0]["name"] == "FIRST"


async def test_stream_sec_yields_records() -> None:
    """
    ARRANGE: mock HTTP response carrying one row
    ACT:     iterate _stream_sec
    ASSERT:  yielded record correctly maps MIC
    """
    payload = {"data": [[1, "Foo Inc", "FOO", "Nasdaq"]]}

    def _respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(_respond)

    async with httpx.AsyncClient(transport=transport) as client:
        records = [record async for record in _stream_sec(client)]

    assert records[0]["mics"] == ["XNAS"]


async def test_stream_and_cache_deduplicates_and_caches() -> None:
    """
    ARRANGE: duplicate rows in mocked transport
    ACT:     run _stream_and_cache
    ASSERT:  only one record yielded (deduplicated)
    """
    payload = {
        "data": [
            [1, "Foo Inc", "FOO", "Nasdaq"],
            [1, "Foo Inc", "FOO", "Nasdaq"],
        ],
    }

    def _respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(_respond)
    cache_key = f"sec_test_cache_{uuid.uuid4()}"

    async with httpx.AsyncClient(transport=transport) as client:
        records = [
            record async for record in _stream_and_cache(client, cache_key=cache_key)
        ]

    assert len(records) == 1


async def test_fetch_equity_records_uses_existing_cache() -> None:
    """
    ARRANGE: pre-populate cache with a single record
    ACT:     call fetch_equity_records
    ASSERT:  record comes straight from cache (no HTTP required)
    """
    cache_key = f"sec_cached_{uuid.uuid4()}"
    expected = [
        {
            "cik": 42,
            "name": "Cached Co",
            "symbol": "CCC",
            "exchange": "Nasdaq",
            "mics": ["XNAS"],
        },
    ]
    save_cache(cache_key, expected)

    records = [record async for record in fetch_equity_records(cache_key=cache_key)]

    assert records == expected


async def test_fetch_equity_records_streams_with_supplied_client() -> None:
    """
    ARRANGE: AsyncClient with MockTransport serves two identical SEC rows
    ACT:     call fetch_equity_records with that client and an empty cache
    ASSERT:  only one unique record is yielded (deduplicated)
    """
    payload = {
        "data": [
            [1, "Foo Inc", "FOO", "Nasdaq"],
            [1, "Foo Inc", "FOO", "Nasdaq"],
        ],
    }

    def _respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code=200, json=payload)

    transport = httpx.MockTransport(_respond)
    client = httpx.AsyncClient(transport=transport)

    cache_key = f"sec_branch_cov_{uuid.uuid4()}"

    records = [
        record
        async for record in fetch_equity_records(
            client=client,
            cache_key=cache_key,
        )
    ]

    assert len(records) == 1


def test_parse_row_returns_none_for_invalid_row() -> None:
    """
    ARRANGE: row is missing mandatory fields
    ACT:     call _parse_row
    ASSERT:  returns None
    """
    row = [None, None]

    record = _parse_row(row)

    assert record is None


async def test_stream_sec_skips_invalid_rows() -> None:
    """
    ARRANGE: payload contains one invalid and one valid SEC row
    ACT:     iterate _stream_sec
    ASSERT:  only the valid record is yielded
    """
    payload = {
        "data": [
            [None, None, None, None],
            [1, "Foo Inc", "FOO", "Nasdaq"],
        ],
    }

    def _respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(_respond)

    async with httpx.AsyncClient(transport=transport) as client:
        records = [record async for record in _stream_sec(client)]

    assert len(records) == 1
