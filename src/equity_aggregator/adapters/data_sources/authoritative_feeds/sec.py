# authoritative_feeds/sec.py

import logging

from httpx import AsyncClient

from equity_aggregator.adapters.data_sources._utils import make_client
from equity_aggregator.storage import load_cache, save_cache

from ._record_types import (
    EquityRecord,
    RecordStream,
    RecordUniqueKeyExtractor,
    UniqueRecordStream,
)

logger = logging.getLogger(__name__)

_SEC_SEARCH_URL = "https://www.sec.gov/files/company_tickers_exchange.json"

_HEADERS = {
    "User-Agent": "EquityAggregator gregory@gregorykelleher.com",
}

EXCHANGE_TO_MIC = {
    "Nasdaq": "XNAS",
    "NYSE": "XNYS",
    "CBOE": "XCBO",
    "OTC": "XOTC",
}


async def fetch_equity_records(
    client: AsyncClient | None = None,
    *,
    cache_key: str = "sec_records",
) -> RecordStream:
    """
    Yield each SEC equity record exactly once, using cache if available.

    If a cache is present, loads and yields records from cache. Otherwise, streams
    all MICs concurrently, yields records as they arrive, and caches the results.

    Args:
        client (AsyncClient | None): Optional HTTP client to use for requests.
        cache_key (str): The key under which to cache the records.

    Yields:
        EquityRecord: Parsed SEC equity record.
    """
    cached = load_cache(cache_key)

    if cached:
        logger.info("Loaded %d SEC records from cache.", len(cached))
        for record in cached:
            yield record
        return

    # use provided client or create a bespoke sec client
    client = client or make_client(headers=_HEADERS)

    async with client:
        async for record in _stream_and_cache(client, cache_key=cache_key):
            yield record


async def _stream_and_cache(
    client: AsyncClient,
    *,
    cache_key: str,
) -> RecordStream:
    """
    Stream SEC equity records, deduplicate by CIK, cache them, and yield each record.

    Args:
        client (AsyncClient): HTTP client for SEC requests.
        cache_key (str): Key under which to store cached records.

    Returns:
        RecordStream: Async iterator yielding unique EquityRecord objects.
    """
    buffer: list[EquityRecord] = []

    async for record in _deduplicate_records(lambda record: record["cik"])(
        _stream_sec(client),
    ):
        buffer.append(record)
        yield record

    save_cache(cache_key, buffer)
    logger.info("Saved %d SEC records to cache.", len(buffer))


async def _stream_sec(client: AsyncClient) -> RecordStream:
    """
    Fetch and stream SEC equity records from the authoritative JSON endpoint.

    Args:
        client (AsyncClient): HTTP client for making requests.

    Yields:
        EquityRecord: Each valid, normalised SEC equity record.
    """
    response = await client.get(_SEC_SEARCH_URL)
    response.raise_for_status()

    payload = response.json()
    rows = payload.get("data", [])

    for row in rows:
        record = _parse_row(row)
        if record:
            yield record


def _parse_row(
    row: list[object] | None,
) -> EquityRecord | None:
    """
    Parse a SEC data row into an EquityRecord if valid.

    Args:
        row (list[object] | None): List containing CIK, company name, ticker,
            and exchange. Example: [cik, name, symbol, exchange]. Must have at
            least 4 elements.

    Returns:
        EquityRecord | None: Dictionary with parsed record fields if valid,
            otherwise None. Includes keys: "cik", "name", "symbol", "exchange",
            and "mics" (list of MICs).
    """
    number_of_fields = 4

    if not row or len(row) < number_of_fields:
        return None

    cik, name, symbol, exchange = row[:4]

    if not cik or not name or not symbol or not exchange:
        return None

    mic = EXCHANGE_TO_MIC.get(exchange)

    return {
        "cik": cik,
        "name": name,
        "symbol": symbol,
        "exchange": exchange,
        "mics": [mic] if mic else [],
    }


def _deduplicate_records(extract_key: RecordUniqueKeyExtractor) -> UniqueRecordStream:
    """
    Creates a deduplication coroutine for async iterators of dictionaries, yielding only
    unique records based on a key extracted from each record.
    Args:
        extract_key (RecordUniqueKeyExtractor): A function that takes a
            dictionary record and returns a value used to determine uniqueness.
    Returns:
        UniqueRecordStream: A coroutine that accepts an async iterator of dictionaries,
            yields only unique records, as determined by the extracted key.
    """

    async def deduplicator(records: RecordStream) -> RecordStream:
        """
        Deduplicate async iterator of dicts by a key extracted from each record.

        Args:
            records (RecordStream): Async iterator of records to deduplicate.

        Yields:
            EquityRecord: Unique records, as determined by the extracted key.
        """
        seen: set[object] = set()
        async for record in records:
            key = extract_key(record)
            if key in seen:
                continue
            seen.add(key)
            yield record

    return deduplicator
