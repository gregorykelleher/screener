# authoritative_feeds/lse.py

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

_LSE_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "User-Agent": "Mozilla/5.0",
    "Content-Type": "application/json; charset=UTF-8",
    "Referer": "https://www.londonstockexchange.com/",
    "Origin": "https://www.londonstockexchange.com",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

_LSE_SEARCH_URL = "https://api.londonstockexchange.com/api/v1/components/refresh"
_PAGE_SIZE = 100

ClientFactory = Callable[..., httpx.AsyncClient]
_DEFAULT_CLIENT_FACTORY: ClientFactory = make_client_factory(headers=_LSE_HEADERS)


@lru_cache(maxsize=1)
def get_client() -> httpx.AsyncClient:
    return _DEFAULT_CLIENT_FACTORY()


async def fetch_equity_records(
    client_factory: ClientFactory = _DEFAULT_CLIENT_FACTORY,
) -> AsyncIterator[dict[str, object]]:
    """
    Asynchronously stream all LSE equity records, using cache if available.

    If cached records exist, yields them directly. Otherwise, fetches all records
    from the LSE feed, streams them as they arrive, and caches the results for
    future use.

    Args:
        client_factory (ClientFactory, optional): Callable that returns an
            httpx.AsyncClient for HTTP requests. Defaults to _DEFAULT_CLIENT_FACTORY.

    Yields:
        dict[str, object]: A normalised equity record from the LSE feed.

    Returns:
        AsyncIterator[dict[str, object]]: An async iterator yielding equity records.
    """
    cached = load_cache("lse_records")
    if cached:
        logger.info("Loaded %d LSE records from cache.", len(cached))
        for record in cached:
            yield record
        return

    async for record in _fetch_and_cache(client_factory):
        yield record


async def _fetch_and_cache(
    client_factory: ClientFactory,
) -> AsyncIterator[dict[str, object]]:
    """
    Asynchronously fetch and cache all equity records from the LSE feed.

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
    save_cache("lse_records", buffer)
    logger.info("Saved %d LSE records to cache.", len(buffer))


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
        if key is None:
            yield record
            continue

        if key in seen:
            continue

        seen.add(key)
        yield record


async def _download_equity_records(
    client_factory: ClientFactory,
) -> AsyncIterator[dict[str, object]]:
    """
    Asynchronously downloads equity records from the LSE authoritative feed.

    Args:
        client_factory (ClientFactory): A factory function that returns an asynchronous
            HTTP client instance for making requests to the LSE feed.

    Yields:
        dict[str, object]: A dictionary representing a single equity record retrieved
            from the LSE feed.

    Raises:
        Any exceptions raised by the client or during data retrieval will propagate.
    """
    client = get_client()

    async for record in _stream_equity_records(client):
        yield record


def _build_payload(page: int) -> dict:
    """
    Build the JSON payload for the LSE API to fetch equity data for a given page.

    Args:
        page (int): The page number to request from the LSE API.

    Returns:
        dict: The payload with path, parameters, and components for the API request.
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
                    f"&showonlylse=true&page={page}&size={_PAGE_SIZE}"
                ),
            },
        ],
    }


def _parse_equities(data: dict) -> tuple[list[dict], int | None]:
    """
    Parse the LSE API response to extract equity records and the total page count.

    Args:
        data (dict): The JSON response from the LSE API.

    Returns:
        tuple[list[dict], int | None]: A tuple containing:
            - A list of equity record dictionaries.
            - The total number of pages (int), or None if not available.
    """
    if data is None:
        logger.debug("LSE API returned None data")
        return [], None

    price_explorer_data = next(
        (
            content_item
            for content_item in data.get("content", [])
            if content_item.get("name") == "priceexplorersearch"
        ),
        None,
    )

    if not price_explorer_data:
        return [], None

    value = price_explorer_data.get("value", {})
    return value.get("content", []), value.get("totalPages")


async def _fetch_page(
    client: httpx.AsyncClient,
    payload: dict,
) -> dict:
    """
    Send a POST request to the LSE API and return the parsed JSON response.

    Args:
        client (httpx.AsyncClient): The asynchronous HTTP client used to make
            the POST request.
        payload (dict): The JSON payload to include in the POST request body.

    Returns:
        dict: The first element of the JSON response from the LSE API, containing
            the requested data.
    """
    response = await client.post(_LSE_SEARCH_URL, json=payload)
    response.raise_for_status()
    return response.json()[0]


async def _try_fetch_page(client: httpx.AsyncClient, page: int) -> dict | None:
    """
    Attempts to fetch a page asynchronously using the provided HTTP client and payload.
    Handles HTTP status/read errors gracefully by logging a warning and returning None.

    Args:
        client (httpx.AsyncClient): The asynchronous HTTP client to use for the request.
        page (int): The page number to fetch.

    Returns:
        dict | None: The response data as a dictionary if successful, otherwise None if
        HTTP status or read error occurs.
    """
    try:
        payload = _build_payload(page)
        return await _fetch_page(client, payload)

    except (httpx.HTTPStatusError, httpx.ReadError) as error:
        root = error.__context__ or error.__cause__
        label = repr(root) if root else type(error).__name__
        logger.warning("LSE %s → %s", _LSE_SEARCH_URL, label)
        return None


async def _stream_equity_records(
    client: httpx.AsyncClient,
) -> AsyncIterator[dict[str, object]]:
    """
    Asynchronously stream individual equity records from the LSE API.

    Fetches the first page of equity records and yields each record. If additional
    pages exist, fetches the remaining pages concurrently and yields each record as
    they are retrieved.

    Args:
        client (httpx.AsyncClient): The asynchronous HTTP client used to make
            requests to the LSE API.

    Returns:
        AsyncIterator[dict[str, object]]: An async iterator yielding individual
            equity record dictionaries from all available pages.
    """
    async for records_pages in _equity_pages(client):
        for record in records_pages:
            yield record


async def _equity_pages(
    client: httpx.AsyncClient,
) -> AsyncIterator[list[dict[str, object]]]:
    """
    Asynchronously yield lists of equity records from the LSE API, one page at a time.

    Fetches the first page of equity records and yields its list. If additional pages
    exist, fetches and yields each remaining page's records as they are retrieved.

    Args:
        client (httpx.AsyncClient): The asynchronous HTTP client used to make
            requests to the LSE API.

    Returns:
        AsyncIterator[list[dict[str, object]]]: An async iterator yielding lists of
            equity records for each page, in the order they are fetched.
    """
    first_records, total_pages = await _fetch_first_page(client)
    if not first_records:
        return

    yield first_records

    if total_pages and total_pages > 1:
        async for records_page in _fetch_remaining_pages(client, total_pages):
            yield records_page


async def _fetch_first_page(
    client: httpx.AsyncClient,
) -> tuple[list[dict[str, object]], int | None]:
    """
    Fetches the first page of equity records from the LSE API, parses the response,
    and returns a tuple containing the list of records and the total number of pages.

    Args:
        client (httpx.AsyncClient): The asynchronous HTTP client used to make the
            request to the LSE API.

    Returns:
        tuple[list[dict[str, object]], int | None]: A tuple where the first element
            is a list of equity record dictionaries, and the second element is the
            total number of pages (int) or None if unavailable or on error.
    """
    raw = await _try_fetch_page(client, 1)
    if not raw:
        return [], None
    return _parse_equities(raw)


async def _fetch_remaining_pages(
    client: httpx.AsyncClient,
    total_pages: int,
) -> AsyncIterator[list[dict[str, object]]]:
    """
    Concurrently fetch and yield equity records for pages 2 through total_pages.

    Args:
        client (httpx.AsyncClient): The asynchronous HTTP client used for requests.
        total_pages (int): The total number of pages to fetch (including page 1).

    Yields:
        list[dict[str, object]]: A list of equity records for each fetched page.

    Returns:
        AsyncIterator[list[dict[str, object]]]: An async iterator yielding lists of
        equity records for each page, in the order they complete.
    """
    pages = range(2, total_pages + 1)
    tasks = [_try_fetch_page(client, p) for p in pages]

    for future in asyncio.as_completed(tasks):
        raw = await future
        if not raw:
            continue
        equities, _ = _parse_equities(raw)
        yield equities
