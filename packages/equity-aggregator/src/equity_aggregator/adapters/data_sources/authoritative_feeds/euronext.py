# authoritative_feeds/euronext.py

import asyncio
import logging
import re
from collections.abc import AsyncIterator, Callable
from typing import Final

from httpx import AsyncClient

from equity_aggregator.adapters.data_sources._cache import load_cache, save_cache
from equity_aggregator.adapters.data_sources._utils import make_client

logger = logging.getLogger(__name__)


_BASE_URL: Final = "https://live.euronext.com/en/pd_es/data/stocks"

_HEADERS: Final = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "User-Agent": "Mozilla/5.0",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://live.euronext.com",
    "Referer": "https://live.euronext.com/en/markets",
    "Accept-Encoding": "gzip, deflate",
}

_COUNTRY_TO_MIC: Final = {
    "France": "XPAR",
    "Netherlands": "XAMS",
    "Belgium": "XBRU",
    "Ireland": "XMSM",
    "Portugal": "XLIS",
    "Italy": "MTAA",
    "Norway": "XOSL",
}

# a single equity record
EquityRecord = dict[str, object]

# an async stream of records
RecordStream = AsyncIterator[EquityRecord]

# a function that extracts a unique key from an EquityRecord
RecordUniqueKeyExtractor = Callable[[EquityRecord], object]

# a function that takes a RecordStream and returns a deduplicated RecordStream
UniqueRecordStream = Callable[[RecordStream], RecordStream]


async def fetch_equity_records(
    client: AsyncClient | None = None,
    page_size: int = 100,
) -> AsyncIterator[EquityRecord]:
    """
    Yield each Euronext equity record exactly once, using cache if available.

    If a cache is present, loads and yields records from cache. Otherwise, streams
    all MICs concurrently, yields records as they arrive, and caches the results.

    Args:
        client (AsyncClient | None): Optional HTTP client to use for requests.
        page_size (int): Number of records to fetch per page (default: 100).

    Yields:
        EquityRecord: Parsed Euronext equity record.
    """
    cached = load_cache("euronext_records")

    if cached:
        logger.info("Loaded %d Euronext records from cache.", len(cached))
        for record in cached:
            yield record
        return

    # use provided client or create a bespoke euronext client
    client = client or make_client(headers=_HEADERS)

    async with client:
        # stream all MICs concurrently and deduplicate by ISIN
        stream = _deduplicate_records(lambda record: record["isin"])(
            _stream_all_mics(client, page_size),
        )

        # collect all records in a buffer to cache them later
        buffer: list[EquityRecord] = []

        # stream each record as it arrives, yielding immediately
        async for record in stream:
            buffer.append(record)
            yield record

    save_cache("euronext_records", buffer)
    logger.info("Saved %d Euronext records to cache.", len(buffer))


def _deduplicate_records(
    extract_key: RecordUniqueKeyExtractor,
) -> UniqueRecordStream:
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

    async def deduplicator(
        records: RecordStream,
    ) -> RecordStream:
        """
        Deduplicate async iterator of dicts by a key extracted from each record.

        Args:
            records (RecordStream): Async iterator of records to
                deduplicate.

        Yields:
            EquityRecord: Unique records, as determined by the extracted key.
        """
        seen_keys: set[object] = set()
        async for record in records:
            record_id = extract_key(record)
            if record_id in seen_keys:
                continue
            seen_keys.add(record_id)
            yield record

    return deduplicator


async def _stream_all_mics(
    client: AsyncClient,
    page_size: int,
) -> AsyncIterator[EquityRecord]:
    """
    Asynchronously fetches and yields equity records for all available MICs.

    This function launches a separate asynchronous task for each Market Identifier
    Code (MIC). Each task fetches equity records for its respective MIC using the
    provided async client and page size. Records are yielded as soon as each task
    completes, allowing for efficient streaming of results as they become available.
    If fetching data for a MIC fails, a warning is logged and processing continues
    for the remaining MICs.

    Args:
        client (AsyncClient): The asynchronous HTTP client used to fetch data.
        page_size (int): The number of records to fetch per request.

    Yields:
        EquityRecord: Parsed equity records from each MIC as they are retrieved.
    """

    async def _task(mic: str) -> list[EquityRecord]:
        try:
            return await _fetch_mic_records(client, mic, page_size)
        except Exception as exc:
            logger.warning("Euronext MIC %s failed: %s", mic, exc)
            return []

    # create a list of tasks for each MIC, fetching concurrently
    coroutines = [_task(mic) for mic in _COUNTRY_TO_MIC.values()]

    for future in asyncio.as_completed(coroutines):
        for record in await future:
            yield record


async def _fetch_mic_records(
    client: AsyncClient,
    mic: str,
    page_size: int,
) -> list[EquityRecord]:
    """
    Fetch and parse all paginated equity records for a single Euronext MIC.

    Args:
        client (AsyncClient): HTTP client to perform POST requests.
        mic (str): Market Identifier Code (e.g., "XPAR").
        page_size (int): Number of records to request per page.

    Returns:
        list[EquityRecord]: List of all parsed equity records for the given MIC.
    """
    mic_request_url = f"{_BASE_URL}?mics={mic}"

    # DataTables uses start (offset) and draw (request counter) for paging
    offset, draw_count = 0, 1

    # accumulate every parsed row across all pages
    all_records: list[EquityRecord] = []

    while True:
        payload = _payload(offset, draw_count, page_size)

        # perform request, fail fast on non-2xx, and deserialise JSON payload
        response = await client.post(mic_request_url, data=payload)
        response.raise_for_status()
        result = response.json()

        # combine parse + accumulate in a single logical statement
        all_records.extend(map(_parse_row, result.get("aaData", [])))

        # stop when final page has been consumed or no more records are available
        total_records = int(result.get("iTotalRecords", 0))
        if offset + page_size >= total_records:
            break

        # advance paging cursors for the next iteration
        offset, draw_count = offset + page_size, draw_count + 1

    return all_records


def _payload(start: int, draw: int, size: int) -> dict[str, int]:
    """
    Constructs the form-data payload required by Euronext's DataTables back-end API.

    Args:
        start (int): The starting index of the data to fetch (pagination offset).
        draw (int): Draw counter for DataTables to ensure correct sequence of requests.
        size (int): Number of records to retrieve per page.

    Returns:
        dict[str, int]: Dictionary containing the payload parameters for the request.
    """
    return {
        "draw": draw,
        "start": start,
        "length": size,
        "iDisplayLength": size,
        "iDisplayStart": start,
    }


def _parse_row(row: list[str]) -> EquityRecord:
    """
    Parses a single HTML table row and extracts structured equity data fields.

    Args:
        row (list[str]): A list of HTML strings representing columns of a table row,
            where each element contains HTML markup for a specific equity attribute.

    Returns:
        EquityRecord: A dictionary containing the parsed equity fields:
            - name (str): The extracted equity name.
            - symbol (str): The equity symbol.
            - isin (str): The ISIN code.
            - mics (list[str]): List of MIC codes.
            - currency (str): The currency code.
            - last_price (str): The last traded price as a string.
    """
    name_match = re.search(r">(.*?)<", row[1])
    mic_match = re.search(r">(.*?)<", row[4])
    price_match = re.search(r">([A-Z]{3})\s*<span[^>]*>([\d\.,]+)</span>", row[5])

    mics = [code.strip() for code in mic_match.group(1).split(",")] if mic_match else []
    currency, last_price = price_match.groups() if price_match else ("", "")

    return {
        "name": name_match.group(1).strip() if name_match else row[1].strip(),
        "symbol": row[3].strip(),
        "isin": row[2].strip(),
        "mics": mics,
        "currency": currency,
        "last_price": last_price,
    }
