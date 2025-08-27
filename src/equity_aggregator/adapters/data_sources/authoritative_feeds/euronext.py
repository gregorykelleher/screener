# authoritative_feeds/euronext.py

import asyncio
import logging
import re
from collections.abc import Sequence

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

_PAGE_SIZE = 100

_EURONEXT_SEARCH_URL = "https://live.euronext.com/en/pd_es/data/stocks"

_HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "User-Agent": "Mozilla/5.0",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://live.euronext.com",
    "Referer": "https://live.euronext.com/en/markets",
    "Accept-Encoding": "gzip, deflate",
}

_COUNTRY_TO_MIC = {
    "France": "XPAR",
    "Netherlands": "XAMS",
    "Belgium": "XBRU",
    "Ireland": "XMSM",
    "Portugal": "XLIS",
    "Italy": "MTAA",
    "Norway": "XOSL",
}


async def fetch_equity_records(
    client: AsyncClient | None = None,
    *,
    cache_key: str = "euronext_records",
) -> RecordStream:
    """
    Yield each Euronext equity record exactly once, using cache if available.

    If a cache is present, loads and yields records from cache. Otherwise, streams
    all MICs concurrently, yields records as they arrive, and caches the results.

    Args:
        client (AsyncClient | None): Optional HTTP client to use for requests.
        cache_key (str): The key under which to cache the records.

    Yields:
        EquityRecord: Parsed Euronext equity record.
    """
    cached = load_cache(cache_key)

    if cached:
        logger.info("Loaded %d Euronext records from cache.", len(cached))
        for record in cached:
            yield record
        return

    # use provided client or create a bespoke euronext client
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
    Asynchronously stream unique Euronext equity records, cache them, and yield each.

    Args:
        client (AsyncClient): The asynchronous HTTP client used for requests.
        cache_key (str): The key under which to cache the records.

    Yields:
        EquityRecord: Each unique Euronext equity record as it is retrieved.

    Side Effects:
        Saves all streamed records to cache after streaming completes.
    """
    # collect all records in a buffer to cache them later
    buffer: list[EquityRecord] = []

    # stream all records concurrently and deduplicate by ISIN
    async for record in _deduplicate_records(lambda record: record["isin"])(
        _stream_all_mics(client),
    ):
        buffer.append(record)
        yield record

    save_cache(cache_key, buffer)
    logger.info("Saved %d Euronext records to cache.", len(buffer))


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
            records (RecordStream): Async iterator of records to
                deduplicate.

        Yields:
            EquityRecord: Unique records, as determined by the extracted key.
        """
        seen_keys: set[object] = set()
        async for record in records:
            key = extract_key(record)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            yield record

    return deduplicator


async def _stream_all_mics(client: AsyncClient) -> RecordStream:
    """
    Concurrently fetch and yield equity records for all MICs.

    For each MIC, a producer coroutine fetches and enqueues parsed records into a
    shared asyncio.Queue. This function consumes from the queue and yields each record
    as soon as it is available. Each producer sends a None sentinel when completed; once
    all sentinels are received, streaming is complete. Any producer exception is
    propagated and causes a fatal exit.

    Args:
        client (AsyncClient): Shared HTTP client for all MIC requests.

    Returns:
        RecordStream: Yields parsed records from all MICs.
    """
    # shared queue for all producers to enqueue records
    queue: asyncio.Queue[EquityRecord | None] = asyncio.Queue()

    # spawn one producer task per MIC
    producers = [
        asyncio.create_task(_produce_mic(client, mic, queue))
        for mic in _COUNTRY_TO_MIC.values()
    ]

    # consume queue until every producer sends its sentinel.
    async for record in _consume_queue(queue, len(producers)):
        yield record

    # ensure exceptions (if any) propagate after consumption finishes
    await asyncio.gather(*producers)


async def _produce_mic(
    client: AsyncClient,
    mic: str,
    queue: asyncio.Queue[EquityRecord | None],
) -> None:
    """
    Asynchronously streams and enqueues all equity records for a given MIC.

    This function fetches records for the specified Market Identifier Code (MIC) using
    the provided asynchronous client, and pushes each parsed record into given queue.
    After all records have been processed, a sentinel value (None) is added to the queue
    to signal completion. If an error occurs in processing, it's logged and re-raised.

    Args:
        client (AsyncClient): The asynchronous HTTP client used to fetch records.
        mic (str): The Market Identifier Code to fetch records for.
        page_size (int): The number of records to fetch per page from the data source.
        queue (asyncio.Queue[EquityRecord | None]): The queue to which records and the
            sentinel value are pushed.

    Returns:
        None
    """
    # track the number of records processed for this MIC
    row_count = 0

    try:
        # stream records for the specified MIC and enqueue them
        async for record in _stream_mic_records(client, mic):
            row_count += 1
            await queue.put(record)

        logger.debug("MIC %s completed with %d rows", mic, row_count)

    except Exception as error:
        logger.fatal("Euronext MIC %s failed: %s", mic, error)
        raise

    finally:
        await queue.put(None)


async def _consume_queue(
    queue: asyncio.Queue[EquityRecord | None],
    expected_sentinels: int,
) -> RecordStream:
    """
    Yield records from the queue until the expected number of sentinel values (None)
    have been received, indicating all producers are completed.

    Args:
        queue (asyncio.Queue[EquityRecord | None]): The queue from which to consume
            equity records or sentinel values.
        expected_sentinels (int): The number of sentinel (None) values to wait for
            before stopping iteration.

    Yields:
        EquityRecord: Each equity record retrieved from the queue, as they arrive.
    """
    completed = 0
    while completed < expected_sentinels:
        record = await queue.get()
        if record is None:
            completed += 1
        else:
            yield record


async def _stream_mic_records(
    client: AsyncClient,
    mic: str,
) -> RecordStream:
    """
    Asynchronously streams equity records for a given MIC (Market Identifier Code) from
    Euronext, yielding each record as soon as its page is parsed.

    Args:
        client (AsyncClient): An asynchronous HTTP client used to make requests.
        mic (str): The Market Identifier Code to fetch records for.
        page_size (int): The number of records to fetch per page.

    Yields:
        EquityRecord: An equity record parsed from the Euronext feed for specified MIC.

    Raises:
        HTTPStatusError: If the HTTP request to the Euronext feed fails.
    """
    mic_request_url = f"{_EURONEXT_SEARCH_URL}?mics={mic}"

    # pagination cursors for DataTables API
    offset, draw_count = 0, 1

    # fetch all pages until exhausted
    while True:
        payload = _build_payload(offset, draw_count)
        response = await client.post(mic_request_url, data=payload)
        response.raise_for_status()

        # deserialise JSON payload
        result = response.json()

        # parse each row in the response and yield valid records
        for record in filter(None, map(_parse_row, result.get("aaData", []))):
            yield record

        # total rows on the server
        total_records = int(result.get("iTotalRecords", 0))

        # determine if final page reached
        if offset + _PAGE_SIZE >= total_records:
            break

        # advance offset to next page and increment draw counter
        offset, draw_count = offset + _PAGE_SIZE, draw_count + 1


def _build_payload(start: int, draw: int) -> dict[str, int]:
    """
    Constructs the form-data payload required by Euronext's DataTables back-end API.

    Args:
        start (int): The starting index of the data to fetch (pagination offset).
        draw (int): Draw counter for DataTables to ensure correct sequence of requests.

    Returns:
        dict[str, int]: Dictionary containing the payload parameters for the request.
    """
    return {
        "draw": draw,
        "start": start,
        "length": _PAGE_SIZE,
        "iDisplayLength": _PAGE_SIZE,
        "iDisplayStart": start,
    }


def _parse_row(row: list[str] | None) -> EquityRecord | None:
    """
    Parse a single Euronext HTML table row into a structured equity record.

    Args:
        row (list[str] | None): List of HTML strings representing columns of a table
            row. Each element contains HTML markup for a specific equity attribute.
            The expected order is: [unused, name, isin, symbol, mics, price/currency].

    Returns:
        EquityRecord | None: Dictionary with parsed equity fields, or None if parsing
            fails due to missing required fields.
    """
    # Ensure row has exactly 6 elements
    cells = (row or [])[:6]

    # Pad missing cells if less than 6 cells
    cells += [""] * (6 - len(cells))

    name = _extract_text(_safe_cell(cells, 1))
    isin = _safe_cell(cells, 2)
    symbol = _safe_cell(cells, 3)
    mics = _extract_mics(_safe_cell(cells, 4))

    # Extract price and currency from the price cell HTML
    currency, last_price = extract_currency_and_last_price(_safe_cell(cells, 5))

    if not name or not symbol:
        logger.warning("Skipping invalid Euronext record: missing name or symbol")
        return None

    return {
        "name": name,
        "symbol": symbol,
        "isin": isin,
        "mics": mics,
        "currency": currency,
        "last_price": last_price,
    }


def _safe_cell(cells: Sequence[str], index: int) -> str:
    """
    Safely retrieve and strip a string from a list of cells at the given index.

    Args:
        cells (Sequence[str]): List or sequence of cell strings.
        index (int): Index of the cell to retrieve.

    Returns:
        str: The stripped cell string, or an empty string if out of range or not a
            string.
    """
    if 0 <= index < len(cells) and isinstance(cells[index], str):
        return cells[index].strip()
    return ""


def _extract_text(html: str) -> str:
    """
    Extract the inner text from an HTML tag string.

    Args:
        html (str): HTML string, e.g. "<tag>Text</tag>".

    Returns:
        str: The extracted inner text, or the original string if no match is found.
    """
    match = re.search(r">(.*?)<", html)
    return match.group(1).strip() if match else html


def _extract_mics(html: str) -> list[str]:
    """
    Extract a list of MIC codes from an HTML string.

    Args:
        html (str): HTML string containing comma-separated MIC codes, e.g.
            "<div>MIC1, MIC2</div>".

    Returns:
        list[str]: List of MIC codes, stripped of whitespace.
    """
    match = re.search(r">(.*?)<", html)
    raw = match.group(1) if match else html
    return [mic.strip() for mic in raw.split(",") if mic.strip()]


def extract_currency_and_last_price(html: str) -> tuple[str, str]:
    """
    Extract currency code and last price value from HTML string containing price info.

    Args:
        html (str): HTML string, e.g. "...>USD<span>123.45</span>...".

    Returns:
        tuple[str, str]: (currency_code, last_price) or ("", "") if not found.
    """
    match = re.search(
        r">([A-Z]{3})\s*<span[^>]*>([\d\.,]+)</span>",
        html,
    )
    if match:
        return match.group(1), match.group(2)
    return "", ""
