# authoritative_feeds/euronext.py

import asyncio
import logging
import re
import sys
from collections.abc import AsyncIterator, Callable

from httpx import AsyncClient

from equity_aggregator.adapters.data_sources._cache import load_cache, save_cache
from equity_aggregator.adapters.data_sources._utils import make_client

logger = logging.getLogger(__name__)


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
) -> RecordStream:
    """
    Yield each Euronext equity record exactly once, using cache if available.

    If a cache is present, loads and yields records from cache. Otherwise, streams
    all MICs concurrently, yields records as they arrive, and caches the results.

    Args:
        client (AsyncClient | None): Optional HTTP client to use for requests.

    Yields:
        EquityRecord: Parsed Euronext equity record.
    """
    cached = load_cache("euronext_records")

    if cached:
        logger.info("Loaded %d Euronext records from cache.", len(cached))
        for record in cached:
            yield record
        return

    try:
        # use provided client or create a bespoke euronext client
        client = client or make_client(headers=_HEADERS)

        async with client:
            async for record in _stream_and_cache(client):
                yield record

    # If any error occurs on the feed, treat it as fatal and exit
    except Exception as error:
        logger.fatal(
            "Fatal error while fetching Euronext records: %s",
            error,
            exc_info=True,
        )
        sys.exit(1)


async def _stream_and_cache(client: AsyncClient) -> RecordStream:
    """
    Asynchronously stream unique Euronext equity records, cache them, and yield each.

    Args:
        client (AsyncClient): The asynchronous HTTP client used for requests.

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

    save_cache("euronext_records", buffer)
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
    # records per DataTables page
    page_size = 100

    # shared queue for all producers to enqueue records
    queue: asyncio.Queue[EquityRecord | None] = asyncio.Queue()

    # spawn one producer task per MIC
    producers = [
        asyncio.create_task(_produce_mic(client, mic, page_size, queue))
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
    page_size: int,
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
        async for record in _stream_mic_records(client, mic, page_size):
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
    page_size: int,
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
        payload = _build_payload(offset, draw_count, page_size)
        response = await client.post(mic_request_url, data=payload)
        response.raise_for_status()

        # deserialise JSON payload
        result = response.json()

        # parse each row in the response and yield it
        for record in map(_parse_row, result.get("aaData", [])):
            yield record

        # total rows on the server
        total_records = int(result.get("iTotalRecords", 0))

        # determine if final page reached
        if offset + page_size >= total_records:
            break

        # advance offset to next page and increment draw counter
        offset, draw_count = offset + page_size, draw_count + 1


def _build_payload(start: int, draw: int, size: int) -> dict[str, int]:
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
