# authoritative_feeds/lse.py

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

_LSE_SEARCH_URL = "https://api.londonstockexchange.com/api/v1/components/refresh"

_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "User-Agent": "Mozilla/5.0",
    "Content-Type": "application/json; charset=UTF-8",
    "Referer": "https://www.londonstockexchange.com/",
    "Origin": "https://www.londonstockexchange.com",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}


async def fetch_equity_records(
    client: AsyncClient | None = None,
    *,
    cache_key: str = "lse_records",
) -> RecordStream:
    """
    Yield each LSE equity record exactly once, using cache if available.

    If a cache is present, loads and yields records from cache. Otherwise, streams
    all MICs concurrently, yields records as they arrive, and caches the results.

    Args:
        client (AsyncClient | None): Optional HTTP client to use for requests.
        cache_key (str): The key under which to cache the records.

    Yields:
        EquityRecord: Parsed LSE equity record.
    """
    cached = load_cache(cache_key)

    if cached:
        logger.info("Loaded %d LSE records from cache.", len(cached))
        for record in cached:
            yield record
        return

    # use provided client or create a bespoke lse client
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
    Asynchronously stream unique LSE equity records, cache them, and yield each.

    Args:
        client (AsyncClient): The asynchronous HTTP client used for requests.
        cache_key (str): The key under which to cache the records.

    Yields:
        EquityRecord: Each unique LSE equity record as it is retrieved.

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
    logger.info("Saved %d LSE records to cache.", len(buffer))


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
        seen: set[object] = set()
        async for record in records:
            key = extract_key(record)
            if key in seen:
                continue
            seen.add(key)
            yield record

    return deduplicator


async def _stream_all_pages(client: AsyncClient) -> RecordStream:
    """
    Stream all LSE equity records across all pages.

    Args:
        client (AsyncClient): The asynchronous HTTP client used for requests.

    Yields:
        EquityRecord: Each equity record from all pages, as soon as it is available.
    """
    # shared queue for all producers to enqueue records
    queue: asyncio.Queue[EquityRecord | None] = asyncio.Queue()

    first_page = await _fetch_page(client, page=1)
    first_page_records = _extract_records(first_page)

    total_pages = _get_total_pages(first_page)

    # yield first-page records immediately
    for record in first_page_records:
        yield record

    logger.debug("LSE page 1 completed")

    # if there is only a single page, just return early
    if total_pages <= 1:
        return

    # spawn one producer task per remaining page
    producers = [
        asyncio.create_task(_produce_page(client, page, queue))
        for page in range(2, total_pages + 1)
    ]

    # consume queue until every producer sends its sentinel
    async for record in _consume_queue(queue, expected_sentinels=len(producers)):
        yield record

    # ensure exceptions (if any) propagate after consumption finishes
    await asyncio.gather(*producers)


async def _produce_page(
    client: AsyncClient,
    page: int,
    queue: asyncio.Queue[EquityRecord | None],
) -> None:
    """
    Fetch a single LSE page, enqueue its records, and signal completion.

    Args:
        client (AsyncClient): The HTTP client for making requests.
        page (int): The 1-based page number to fetch.
        queue (asyncio.Queue[EquityRecord | None]): Queue to put records and sentinel.

    Side Effects:
        - Puts each EquityRecord from the page into the queue.
        - Puts None into the queue after all records (even on error) to signal done.

    Returns:
        None
    """
    try:
        # stream records from the page and enqueue them
        page_json = await _fetch_page(client, page)
        for record in _extract_records(page_json):
            await queue.put(record)

        logger.debug("LSE page %s completed", page)

    except Exception as error:
        logger.fatal("LSE page %s failed: %s", page, error, exc_info=True)
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


async def _fetch_page(client: AsyncClient, page: int) -> dict[str, object]:
    """
    Fetch a single page of results from the LSE feed.

    Sends a POST request to the LSE search endpoint with the specified page and
    returns the parsed JSON response. HTTP and JSON errors are propagated to the caller.

    Args:
        client (AsyncClient): The HTTP client used to send the request.
        page (int): The 1-based page number to fetch.

    Returns:
        dict[str, object]: The parsed JSON response from the LSE feed.

        httpx.HTTPStatusError: If the response status is not successful.
        httpx.ReadError: If there is a network or connection error.
        ValueError: If the response body cannot be parsed as JSON.
    """
    response = await client.post(_LSE_SEARCH_URL, json=_build_payload(page))
    response.raise_for_status()

    try:
        return response.json()[0]

    except (ValueError, IndexError) as error:
        logger.fatal(
            "LSE JSON decode error at page %s: %s",
            page,
            error,
            exc_info=True,
        )
        raise


def _extract_records(page_response_json: dict[str, object]) -> list[EquityRecord]:
    """
    Normalise raw LSE JSON page data into a list of EquityRecord dictionaries.

    Args:
        page_response_json (dict[str, object]): Parsed JSON response from a LSE page.

    Returns:
        list[EquityRecord]: A list of normalised equity records, each as a dictionary
            with standardised keys matching the eurONext schema.
    """
    rows, _ = _parse_equities(page_response_json)
    records: list[EquityRecord] = []

    for row in rows:
        record = dict(row)
        record.setdefault("mics", ["XLON"])
        records.append(record)

    return records


def _get_total_pages(page_json: dict[str, object]) -> int:
    """
    Extract the total number of pages from the first page of LSE results.

    Args:
        page_json (dict[str, object]): Parsed JSON response from a LSE page.

    Returns:
        int: The total number of result pages. Returns 1 if not found.
    """
    _, total_pages = _parse_equities(page_json)
    return int(total_pages or 1)


def _build_payload(page: int, page_size: int = 100) -> dict[str, object]:
    """
    Construct the JSON payload for a LSE search POST request.

    Args:
        page (int): The 1-based page number to request.
        page_size (int, optional): Number of records per page. Defaults to 100.

    Returns:
        dict[str, object]: The payload dictionary to send in the POST request.
    """
    return {
        "path": "live-markets/market-data-dashboard/price-explorer",
        "parameters": (
            "markets%3DMAINMARKET%26categories%3DEQUITY%26indices%3DASX"
            f"%26showonlylse%3Dtrue&page%3D{page}"
        ),
        "components": [
            {
                "componentId": "block_content%3A9524a5dd-7053-4f7a-ac75-71d12db796b4",
                "parameters": (
                    "markets=MAINMARKET&categories=EQUITY&indices=ASX"
                    f"&showonlylse=true&page={page}&size={page_size}"
                ),
            },
        ],
    }


def _parse_equities(page_json: dict[str, object]) -> tuple[list[dict], int | None]:
    """
    Extracts equity data rows and total page count from a LSE price explorer JSON block.

    Args:
        page_json (dict[str, object]): The JSON dictionary representing a page of
            LSE data, expected to contain a "content" key with blocks.

    Returns:
        tuple[list[dict], int | None]: A tuple containing:
            - A list of dictionaries, each representing an equity row from the
              price explorer block (empty if not found).
            - The total number of pages as an integer, or None if unavailable.
    """
    price_explorer_block = next(
        (
            item
            for item in page_json.get("content", [])
            if item.get("name") == "priceexplorersearch"
        ),
        None,
    )
    if not price_explorer_block:
        return [], None

    value_section = price_explorer_block.get("value", {})
    return value_section.get("content", []), value_section.get("totalPages")
