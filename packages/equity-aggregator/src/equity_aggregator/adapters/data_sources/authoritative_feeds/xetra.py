# authoritative_feeds/xetra.py

import asyncio
import logging
from collections.abc import AsyncIterator, Callable
from functools import lru_cache

import httpx

from equity_aggregator.adapters.data_sources._cache import load_cache, save_cache
from equity_aggregator.adapters.data_sources._utils._client_factory import (
    make_client_factory,
)

logger = logging.getLogger(__name__)

_XETRA_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "User-Agent": "Mozilla/5.0",
    "Content-Type": "application/json; charset=UTF-8",
    "Referer": "https://www.boerse-frankfurt.de/",
    "Origin": "https://www.boerse-frankfurt.de",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

_XETRA_SEARCH_URL = "https://api.boerse-frankfurt.de/v1/search/equity_search"
_PAGE_SIZE = 100

ClientFactory = Callable[..., httpx.AsyncClient]
_DEFAULT_CLIENT_FACTORY: ClientFactory = make_client_factory(headers=_XETRA_HEADERS)


@lru_cache(maxsize=1)
def get_client() -> httpx.AsyncClient:
    return _DEFAULT_CLIENT_FACTORY()


async def fetch_equity_records(
    client_factory: ClientFactory = _DEFAULT_CLIENT_FACTORY,
) -> AsyncIterator[dict[str, object]]:
    """
    Asynchronously stream all Xetra equity records, using cache if available.

    If cached records exist, yields them directly. Otherwise, fetches all records
    from the Xetra feed, streams them as they arrive, and caches the results for
    future use.

    Args:
        client_factory (ClientFactory, optional): Callable that returns an
            httpx.AsyncClient for HTTP requests. Defaults to _DEFAULT_CLIENT_FACTORY.

    Yields:
        dict[str, object]: A normalised equity record from the Xetra feed.

    Returns:
        AsyncIterator[dict[str, object]]: An async iterator yielding equity records.
    """
    cached = load_cache("xetra_records")

    if cached:
        logger.info("Loaded %d Xetra records from cache.", len(cached))
        for record in cached:
            yield record
        return

    async for record in _fetch_and_cache(client_factory):
        yield record


async def _fetch_and_cache(
    client_factory: ClientFactory,
) -> AsyncIterator[dict[str, object]]:
    """
    Asynchronously fetch and cache all equity records from the Xetra feed.

    Streams each record as it is retrieved. On successful completion, caches all
    records locally and logs the total number of records fetched.

    Args:
        client_factory (ClientFactory): A callable that returns an AsyncClient.

    Yields:
        dict[str, object]: A normalised equity record.

    Returns:
        AsyncIterator[dict[str, object]]: An async iterator of equity records.
    """
    buffer: list[dict[str, object]] = []

    # stream, deduplicate, accumulate
    async for record in _unique_by_key(
        _download_equity_records(client_factory),
        lambda record: record.get("isin"),
    ):
        buffer.append(record)
        yield record

    # only cache if the download loop completed successfully
    save_cache("xetra_records", buffer)
    logger.info("Saved %d Xetra records to cache.", len(buffer))


async def _unique_by_key(
    async_iter: AsyncIterator[dict[str, object]],
    key_func: Callable[[dict[str, object]], object],
) -> AsyncIterator[dict[str, object]]:
    """
    Yield only the first item for each unique key from an async iterator.

    Iterates over the provided async iterator and yields each item only if its key,
    as determined by the key_func, has not been seen before. This ensures that only
    unique items by the specified key are yielded.

    Args:
        async_iter (AsyncIterator[dict[str, object]]): Async iterator to deduplicate.
        key_func (Callable[[dict[str, object]], object]): Function to extract the
            deduplication key from each item.

    Returns:
        AsyncIterator[dict[str, object]]: Async iterator yielding unique items by key.
    """
    seen: set[object] = set()
    async for record in async_iter:
        key = key_func(record)
        if key in seen:
            continue
        seen.add(key)
        yield record


async def _download_equity_records(
    client_factory: ClientFactory,
) -> AsyncIterator[dict[str, object]]:
    """
    Asynchronously downloads equity records using a provided client factory.

    Args:
        client_factory (ClientFactory): A callable that returns an asynchronous client
            context manager for connecting to the data source.

    Yields:
        dict[str, object]: A dictionary representing a single equity record retrieved
            from the data source.

    Returns:
        AsyncIterator[dict[str, object]]: An asynchronous iterator yielding equity
            record dictionaries.
    """
    client = get_client()

    async for record in _stream_records_from_client(client):
        yield record


async def _stream_records_from_client(
    client: httpx.AsyncClient,
) -> AsyncIterator[dict[str, object]]:
    """
    Asynchronously yield all equity records available using the provided HTTP client.

    Fetches the first page to yield early results and determine total records. If more
    records exist, streams remaining records from subsequent pages.

    Args:
        client (httpx.AsyncClient): The asynchronous HTTP client used for requests.

    Yields:
        dict[str, object]: Normalised equity record from the Xetra feed.

    Returns:
        AsyncIterator[dict[str, object]]: An async iterator yielding equity records.
    """
    page = await _try_fetch_page(client, offset=0)
    if not page:
        return

    # Extract records from the first page and yield them
    first_records = _extract_equity_records(page)
    page_size = len(first_records)
    for record in first_records:
        yield record

    # If total records are less than or equal to page size, no more pages to fetch
    total = _get_total_records(page)
    if total <= page_size:
        return

    # Calculate offsets for remaining pages and stream them
    offsets = range(page_size, total, page_size)
    async for record in _stream_remaining_records(client, offsets):
        yield record


async def _try_fetch_page(
    client: httpx.AsyncClient,
    offset: int,
) -> dict[str, object] | None:
    """
    Attempt to fetch a page of records from the Xetra data source at the given offset.

    Uses the provided HTTPX async client to retrieve a single page of data. If an
    HTTP status or read error occurs, logs a warning and returns None.

    Args:
        client (httpx.AsyncClient): The asynchronous HTTP client for the request.
        offset (int): The pagination offset for the records to fetch.

    Returns:
        dict[str, object] | None: The page of records as a dictionary if successful,
        otherwise None if an error occurs.
    """
    try:
        return await _fetch_page(client, offset)
    except (httpx.HTTPStatusError, httpx.ReadError) as error:
        logger.warning("Xetra API error offset %s: %s", offset, error)
        return None


async def _fetch_page(
    client: httpx.AsyncClient,
    offset: int,
) -> dict[str, object] | None:
    """
    Asynchronously fetch a single page of equity records from the Xetra feed.

    Sends a POST request to the Xetra search endpoint with the specified offset
    to retrieve a page of equity records. Raises an exception if the HTTP
    response status is not successful.

    Args:
        client (httpx.AsyncClient): The asynchronous HTTP client used for the request.
        offset (int): The pagination offset for the records to fetch.

    Returns:
        dict[str, object] | None: Parsed JSON response containing the page of records
        or None if an error occurs.

    Raises:
        httpx.HTTPStatusError: If the response status is not 2xx.
    """
    try:
        response = await client.post(
            _XETRA_SEARCH_URL,
            json=_build_payload(offset),
        )
        response.raise_for_status()
        return response.json()

    except (httpx.HTTPStatusError, httpx.ReadError, ValueError) as error:
        root = error.__context__ or error.__cause__
        label = repr(root) if root else type(error).__name__
        logger.warning("Xetra [%s] %s → %r", offset, _XETRA_SEARCH_URL, label)
        return None


async def _stream_remaining_records(
    client: httpx.AsyncClient,
    offsets: range,
) -> AsyncIterator[dict[str, object]]:
    """
    Asynchronously yield equity records for all pages at the specified offsets.

    Args:
        client (httpx.AsyncClient): The asynchronous HTTP client to use for requests.
        offsets (range): Range of integer offsets for paginated API requests.

    Yields:
        dict[str, object]: Normalised equity record from each fetched page.

    Returns:
        AsyncIterator[dict[str, object]]: Async iterator yielding equity records.
    """
    async for page_json in _fetch_remaining_pages(client, offsets):
        for record in _extract_equity_records(page_json):
            yield record


def _extract_equity_records(page_json: dict[str, object]) -> list[dict[str, object]]:
    """
    Normalise raw page data into equity records.

    Args:
        page_json (dict[str, object]): JSON page with 'data' list.
    Returns:
        list[dict[str, object]]: List of formatted equity records.
    """
    equity_records = page_json.get("data", [])
    return [
        {
            "name": item["name"]["originalValue"],
            "wkn": item.get("wkn", ""),
            "isin": item.get("isin", ""),
            "slug": item.get("slug", ""),
            "mic": "XETR",
            "currency": "EUR",
            "overview": item.get("overview", {}),
            "performance": item.get("performance", {}),
            "key_data": item.get("keyData", {}),
            "sustainability": item.get("sustainability", {}),
        }
        for item in equity_records
    ]


async def _fetch_remaining_pages(
    client: httpx.AsyncClient,
    offsets: range,
) -> AsyncIterator[dict[str, object]]:
    """
    Concurrently fetch and yield JSON pages of equity records for the given offsets.

    For each offset in the provided range, launches an asynchronous request to fetch
    a page of equity records. Yields each successfully fetched page's JSON as soon as
    it is available. If a request fails, logs a warning and continues with the rest.

    Args:
        client (httpx.AsyncClient): The asynchronous HTTP client for API requests.
        offsets (range): Range of integer offsets for paginated API requests.

    Yields:
        dict[str, object]: Parsed JSON response for each successfully fetched page.

    Returns:
        AsyncIterator[dict[str, object]]: An async iterator yielding page JSONs.
    """

    coroutines = [_try_fetch_page(client, offset) for offset in offsets]

    for future in asyncio.as_completed(coroutines):
        page = await future
        if page:
            yield page


def _build_payload(offset: int) -> dict[str, object]:
    """
    Build the JSON payload for a search request.

    Args:
        offset (int): Pagination offset.
    Returns:
        dict[str, object]: Request payload dict.
    """
    return {
        "stockExchanges": ["XETR"],
        "lang": "en",
        "offset": offset,
        "limit": _PAGE_SIZE,
    }


def _get_total_records(page_json: dict[str, object]) -> int:
    """
    Extract total record count from page JSON.

    Args:
        page_json (dict[str, object]): Response JSON containing recordsTotal.
    Returns:
        int: Total number of records.
    """
    return int(page_json.get("recordsTotal", 0))
