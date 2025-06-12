# authoritative_feeds/xetra.py

import asyncio
import logging
from collections.abc import AsyncIterator, Callable

import httpx

from equity_aggregator.adapters.data_sources._cache import load_cache, save_cache
from equity_aggregator.adapters.data_sources._utils._client_factory import (
    make_client_factory,
)

logger = logging.getLogger(__name__)

_XETRA_SEARCH_URL = "https://api.boerse-frankfurt.de/v1/search/equity_search"
_PAGE_SIZE = 100

ClientFactory = Callable[..., httpx.AsyncClient]
_DEFAULT_CLIENT_FACTORY: ClientFactory = make_client_factory()


async def fetch_equity_records(
    client_factory: ClientFactory = _DEFAULT_CLIENT_FACTORY,
) -> AsyncIterator[dict[str, object]]:
    """
    Stream all Xetra records, with caching to avoid redundant fetches.

    Args:
        client_factory (ClientFactory): Callable returning an AsyncClient.
    Yields:
        dict[str, object]: Normalised equity record.
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

    Args:
        async_iter (AsyncIterator[dict[str, object]]): The async iterator to deduplicate.
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
    Yield all equity records by fetching pages concurrently.

    The initial page (offset 0) is fetched first to stream early results
    and to read `recordsTotal` to compute remaining offsets. Subsequent
    pages are gathered in parallel and yielded as they arrive.

    Args:
        client_factory (ClientFactory): Callable returning an AsyncClient.
    Yields:
        dict[str, object]: Formatted equity record.
    """
    async with client_factory() as client:
        first_page = await _fetch_page(client, offset=0)
        first_records = _extract_equity_records(first_page)

        for record in first_records:
            yield record

        total_records = _get_total_records(first_page)
        page_size = len(first_records)

        # if no records or only one page, nothing more to fetch
        if page_size == 0 or total_records <= page_size:
            return

        # compute offsets based on actual page size
        remaining_offsets = range(page_size, total_records, page_size)

        # fetch remaining pages with cancellation support
        async for page in _fetch_remaining_pages(client, remaining_offsets):
            for record in _extract_equity_records(page):
                yield record


async def _fetch_remaining_pages(
    client: httpx.AsyncClient,
    offsets: range,
) -> AsyncIterator[dict[str, object]]:
    """
    Fetch multiple pages of equity records concurrently and yield each page.

    Args:
        client (httpx.AsyncClient): HTTP client for making requests.
        offsets (range): Offsets for paginated requests.

    Yields:
        dict[str, object]: JSON response for each fetched page.
    """

    tasks: list[asyncio.Task[dict[str, object]]] = []

    async with asyncio.TaskGroup() as task_group:
        for offset in offsets:
            tasks.append(task_group.create_task(_fetch_page(client, offset)))

    for task in tasks:
        yield task.result()


async def _fetch_page(client: httpx.AsyncClient, offset: int) -> dict[str, object]:
    """
    Fetch a single page of equity records from the Xetra feed at the specified offset.

    Args:
        client (httpx.AsyncClient): The asynchronous HTTP client to use for the request.
        offset (int): The pagination offset for the records to fetch.

    Returns:
        dict[str, object]: The parsed JSON response containing the page of records.
    """

    response = await client.post(_XETRA_SEARCH_URL, json=_build_search_payload(offset))

    response.raise_for_status()

    return response.json()


def _build_search_payload(offset: int) -> dict[str, object]:
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
