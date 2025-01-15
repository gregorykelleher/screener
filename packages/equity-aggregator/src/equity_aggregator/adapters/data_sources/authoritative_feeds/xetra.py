# xetra/xetra.py

import asyncio
import logging
from collections.abc import AsyncIterator, Callable
from typing import Any

import httpx

from equity_aggregator.adapters.data_sources._cache import load_cache, save_cache

logger = logging.getLogger(__name__)

_URL = "https://api.boerse-frankfurt.de/v1/search/equity_search"

_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "User-Agent": "Mozilla/5.0",
    "Content-Type": "application/json; charset=UTF-8",
    "Referer": "https://www.boerse-frankfurt.de/",
    "Origin": "https://www.boerse-frankfurt.de",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}


async def fetch_equities(
    page_size: int = 100,
    concurrency: int = 8,
) -> list[dict]:
    """
    Fetch a list of unique, valid equities from the Xetra API asynchronously.

    Args:
        page_size (int): Number of equities to fetch per API page. Default is 100.
        concurrency (int): Maximum number of concurrent API requests. Default is 8.

    Returns:
        list[dict]: List of unique equities, each as a dictionary.
    """
    # load from cache if available (refresh every 24 hours by default)
    cached = load_cache("xetra_equities")
    if cached is not None:
        logger.info("Loaded Xetra raw equities from cache.")
        return cached

    logger.info("Fetching Xetra raw equities from Xetra API.")

    semaphore = asyncio.Semaphore(concurrency)
    async with httpx.AsyncClient(headers=_HEADERS, timeout=20.0) as client:
        # stream all pages
        stream = _xetra_pages(client, page_size, semaphore)

        deduped = _unique_by_key(stream, key_func=lambda eq: eq["isin"])

        raw_equities = [item async for item in deduped]

        logger.debug(
            f"Fetched {len(raw_equities)} unique raw equities from Xetra API.",
        )

        # persist to cache and return
        save_cache("xetra_equities", raw_equities)
        logger.info("Saved Xetra raw equities to cache.")
        return raw_equities


def _build_payload(offset: int, limit: int) -> dict:
    """
    Build the JSON payload for a POST request to the Xetra equities API.

    Args:
        offset (int): The starting index for records to fetch.
        limit (int): The maximum number of records to fetch.

    Returns:
        dict: The JSON payload to be sent in the POST request.
    """
    return {
        "indices": [],
        "regions": [],
        "countries": [],
        "sectors": [],
        "types": [],
        "forms": [],
        "segments": [],
        "markets": [],
        "stockExchanges": ["XETR"],
        "lang": "en",
        "offset": offset,
        "limit": limit,
        "sorting": "TURNOVER",
        "sortOrder": "DESC",
    }


def _parse_equities(items: list[dict]) -> list[dict]:
    """
    Normalises a list of raw equity items from the Xetra API into a standardized
    dictionary format expected by the pipeline.

    Args:
        items (list[dict]): A list of dictionaries, each representing an equity item
            as returned by the Xetra API.

    Returns:
        list[dict]: A list of normalised equity dictionaries containing keys such as
            'name', 'wkn', 'isin', 'slug', 'mic', 'currency', 'overview',
            'performance', 'key_data', and 'sustainability'.
    """
    out: list[dict] = []
    for item in items:
        out.append(
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
            },
        )
    return out


async def _unique_by_key(
    aiter: AsyncIterator[dict],
    key_func: Callable[[dict], Any],
) -> AsyncIterator[dict]:
    """
    Yield only the first item for each unique key from an async iterator.

    Args:
        aiter (AsyncIterator[dict]): An async iterator yielding dictionaries.
        key_func (Callable[[dict], Any]): Function to extract a unique key from each
            dictionary.

    Yields:
        dict: The first occurrence of each unique key found in the iterator.

    Returns:
        AsyncIterator[dict]: An async iterator yielding unique dictionaries by key.
    """
    seen: set = set()
    async for item in aiter:
        k = key_func(item)
        if k in seen:
            continue
        seen.add(k)
        yield item


async def _fetch_page(
    client: httpx.AsyncClient,
    payload: dict,
    sem: asyncio.Semaphore,
) -> dict:
    """
    Perform a single asynchronous POST request to the Xetra API and return the
    parsed JSON response.

    Args:
        client (httpx.AsyncClient): The HTTP client used for making requests.
        payload (dict): The JSON payload to send in the POST request.
        sem (asyncio.Semaphore): Semaphore to limit concurrent requests.

    Returns:
        dict: The parsed JSON response from the Xetra API.
    """
    async with sem:
        resp = await client.post(_URL, json=payload)
        resp.raise_for_status()
        return resp.json()


async def _xetra_pages(
    client: httpx.AsyncClient,
    page_size: int,
    sem: asyncio.Semaphore,
) -> AsyncIterator[dict]:
    """
    Asynchronously stream all equity pages from the Xetra API.

    The first request determines the total number of records. Subsequent pages are
    fetched concurrently for efficiency.

    Args:
        client (httpx.AsyncClient): The HTTP client for making requests.
        page_size (int): Number of equities to fetch per API page.
        sem (asyncio.Semaphore): Semaphore to limit concurrent requests.

    Yields:
        dict: A normalised equity dictionary for each equity found.

    Returns:
        AsyncIterator[dict]: An async iterator yielding equity dictionaries.
    """
    # first request
    first_raw = await _fetch_page(client, _build_payload(0, page_size), sem)
    records_total = first_raw.get("recordsTotal", 0)
    for equity in _parse_equities(first_raw.get("data", [])):
        yield equity

    # No extra pages?
    if records_total <= page_size:
        return

    # remaining pages concurrently
    offsets = range(page_size, records_total, page_size)
    tasks = [
        asyncio.create_task(_fetch_page(client, _build_payload(off, page_size), sem))
        for off in offsets
    ]

    for coro in asyncio.as_completed(tasks):
        data = await coro
        for equity in _parse_equities(data.get("data", [])):
            yield equity
