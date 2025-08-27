# authoritative_feeds/xetra.py

import asyncio
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

_PAGE_SIZE = 100

_XETRA_SEARCH_URL = "https://api.boerse-frankfurt.de/v1/search/equity_search"

_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "User-Agent": "Mozilla/5.0",
    "Content-Type": "application/json; charset=UTF-8",
    "Referer": "https://www.boerse-frankfurt.de/",
    "Origin": "https://www.boerse-frankfurt.de",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}


async def fetch_equity_records(
    client: AsyncClient | None = None,
    *,
    cache_key: str = "xetra_records",
) -> RecordStream:
    """
    Yield each Xetra equity record exactly once, using cache if available.

    If a cache is present, loads and yields records from cache. Otherwise, streams
    all pages concurrently, yields records as they arrive, and caches the results.

    Args:
        client (AsyncClient | None): Optional HTTP client to use for requests.
        cache_key (str): The key under which to cache the records.

    Yields:
        EquityRecord: Parsed Xetra equity record.
    """
    cached = load_cache(cache_key)

    if cached:
        logger.info("Loaded %d Xetra records from cache.", len(cached))
        for record in cached:
            yield record
        return

    # use provided client or create a bespoke xetra client
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
    Asynchronously stream unique Xetra equity records, cache them, and yield each.

    Args:
        client (AsyncClient): The asynchronous HTTP client used for requests.
        cache_key (str): The key under which to cache the records.

    Yields:
        EquityRecord: Each unique Xetra equity record as it is retrieved.

    Side Effects:
        Saves all streamed records to cache after streaming completes.
    """
    # collect all records in a buffer to cache them later
    buffer: list[EquityRecord] = []

    # stream all records concurrently and deduplicate by ISIN
    async for record in _deduplicate_records(lambda record: record["isin"])(
        _stream_all_pages(client),
    ):
        buffer.append(record)
        yield record

    save_cache(cache_key, buffer)
    logger.info("Saved %d Xetra records to cache.", len(buffer))


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
            key = extract_key(record)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            yield record

    return deduplicator


async def _stream_all_pages(client: AsyncClient) -> RecordStream:
    """
    Stream all Xetra equity records across all pages.

    Fetches the first page to determine the total number of records, then launches
    concurrent producer tasks for each remaining page. Producers enqueue parsed
    records into a shared queue, which this coroutine consumes and yields lazily.

    Args:
        client (AsyncClient): The asynchronous HTTP client used for requests.

    Yields:
        EquityRecord: Each equity record from all pages, as soon as it is available.
    """

    # shared queue for all producers to enqueue records
    queue: asyncio.Queue[EquityRecord | None] = asyncio.Queue()

    first_page = await _fetch_page(client, offset=0)
    first_page_records = _extract_records(first_page)
    total_records = _get_total_records(first_page)

    # yield first-page records immediately
    for record in first_page_records:
        yield record

    # if there is only a single page, just return early
    if total_records <= _PAGE_SIZE:
        return

    # offsets for remaining pages (skipping the first page already fetched)
    remaining_pages = range(_PAGE_SIZE, total_records, _PAGE_SIZE)

    # spawn one producer task per remaining page
    producers = [
        asyncio.create_task(_produce_page(client, offset, queue))
        for offset in remaining_pages
    ]

    # consume queue until every producer sends its sentinel
    async for record in _consume_queue(queue, expected_sentinels=len(producers)):
        yield record

    # ensure exceptions (if any) propagate after consumption finishes
    await asyncio.gather(*producers)


async def _produce_page(
    client: AsyncClient,
    offset: int,
    queue: asyncio.Queue[EquityRecord | None],
) -> None:
    """
    Fetch a single Xetra page, enqueue each record, and signal completion.

    Args:
        client (AsyncClient): The asynchronous HTTP client for requests.
        offset (int): The offset for the page to fetch.
        queue (asyncio.Queue[EquityRecord | None]): Queue to push records and sentinel.

    Returns:
        None

    Side Effects:
        Enqueues each parsed record from the page into the queue. After all records
        are enqueued, pushes a `None` sentinel to signal completion. Logs fatal and
        raises on any error.
    """
    try:
        # stream records from the page and enqueue them
        page = await _fetch_page(client, offset)
        for record in _extract_records(page):
            await queue.put(record)

        logger.debug("Xetra page at offset %s completed", offset)

    except Exception as error:
        logger.fatal("Xetra page at offset %s failed: %s", offset, error, exc_info=True)
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
        item = await queue.get()
        if item is None:
            completed += 1
        else:
            yield item


async def _fetch_page(client: AsyncClient, offset: int) -> dict[str, object]:
    """
    Fetch a single page of results from the Xetra feed.

    Sends a POST request to the Xetra search endpoint with the specified offset and
    returns the parsed JSON response. HTTP and JSON errors are propagated to the caller.

    Args:
        client (AsyncClient): The HTTP client used to send the request.
        offset (int): The pagination offset for the results.

    Returns:
        dict[str, object]: The parsed JSON response from the Xetra feed.

        httpx.HTTPStatusError: If the response status is not successful.
        httpx.ReadError: If there is a network or connection error.
        ValueError: If the response body cannot be parsed as JSON.
    """
    response = await client.post(_XETRA_SEARCH_URL, json=_build_payload(offset))
    response.raise_for_status()

    try:
        return response.json()

    except ValueError as error:
        logger.fatal(
            "Xetra JSON decode error at offset %s: %s",
            offset,
            error,
            exc_info=True,
        )
        raise


def _extract_records(page_response_json: dict[str, object]) -> list[EquityRecord]:
    """
    Normalise raw Xetra JSON page data into a list of EquityRecord dictionaries.

    Args:
        page_response_json (dict[str, object]): Parsed JSON response from a Xetra page,
            expected to contain a "data" key with a list of equity rows.

    Returns:
        list[EquityRecord]: A list of normalised equity records, each as a dictionary
            with standardised keys matching the eurONext schema.
    """
    rows = page_response_json.get("data", [])
    return [
        {
            "name": row["name"]["originalValue"],
            "wkn": row.get("wkn", ""),
            "isin": row.get("isin", ""),
            "slug": row.get("slug", ""),
            "mics": ["XETR"],
            "currency": "EUR",
            "overview": row.get("overview", {}),
            "performance": row.get("performance", {}),
            "key_data": row.get("keyData", {}),
            "sustainability": row.get("sustainability", {}),
        }
        for row in rows
    ]


def _get_total_records(page_json: dict[str, object]) -> int:
    """
    Extract the total number of equity records from the first page of Xetra results.

    Args:
        page_json (dict[str, object]): The parsed JSON response from a Xetra page,
            expected to contain a "recordsTotal" key indicating the total count.

    Returns:
        int: The total number of equity records available in the feed.
    """
    return int(page_json.get("recordsTotal", 0))


def _build_payload(offset: int) -> dict[str, object]:
    """
    Construct the JSON payload for a Xetra search POST request.

    Args:
        offset (int): The pagination offset for the results.

    Returns:
        dict[str, object]: The payload dictionary for the POST request.
    """
    return {
        "stockExchanges": ["XETR"],
        "lang": "en",
        "offset": offset,
        "limit": _PAGE_SIZE,
    }
