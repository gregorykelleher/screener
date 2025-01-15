# lse/lse.py

import asyncio
import logging
from collections.abc import AsyncIterator, Callable
from typing import Any

import httpx

from equity_aggregator.adapters.data_sources._cache import load_cache, save_cache

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/134.0.0.0 Safari/537.36"
    ),
}

_URL = "https://api.londonstockexchange.com/api/v1/components/refresh"


async def fetch_equities(
    page_size: int = 100,
    concurrency: int = 8,
) -> list[dict]:
    """
    Asynchronously fetches a list of unique, valid equities from the LSE API.

    Args:
        page_size (int, optional): No. of equities to fetch per page. Defaults to 100.
        concurrency (int, optional): Max number of concurrent requests. Defaults to 8.

    Returns:
        list[dict]: A list of unique equities (deduplicated by ISIN) as dictionaries.
    """
    # load from cache if available (refresh every 24 hours by default)
    cached = load_cache("lse_equities")
    if cached is not None:
        logger.info("Loaded LSE raw equities from cache.")
        return cached

    logger.info("Fetching LSE raw equities from LSE API.")

    semaphore = asyncio.Semaphore(concurrency)
    async with httpx.AsyncClient(headers=_HEADERS, timeout=20.0) as client:
        # stream all pages
        stream = _lse_pages(client, page_size, semaphore)

        deduped = _unique_by_key(stream, key_func=lambda eq: eq["isin"])

        raw_equities = [item async for item in deduped]

        logger.debug(
            f"Fetched {len(raw_equities)} unique raw equities from LSE API.",
        )

        # persist to cache and return
        save_cache("lse_equities", raw_equities)
        logger.info("Saved LSE raw equities to cache.")
        return raw_equities


def _build_payload(page: int, page_size: int) -> dict:
    """
    Constructs the JSON payload required for fetching equity data from the LSE API for a
    specific page and page size.

    Args:
        page (int): The page number to retrieve.
        page_size (int): The number of items per page.

    Returns:
        dict: The JSON payload containing path, parameters, and components for the API
            request.
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


def _parse_equities(data: dict) -> tuple[list[dict], int | None]:
    """
    Parses the LSE API response to extract equities and the total number of pages.

    Args:
        data (dict): The JSON response from the LSE API.

    Returns:
        tuple[list[dict], int | None]: A tuple containing a list of equity dictionaries
        and the total number of pages (or None if not available).
    """
    comp = next(
        (c for c in data.get("content", []) if c.get("name") == "priceexplorersearch"),
        None,
    )
    if not comp:
        return [], None
    value = comp.get("value", {})
    return value.get("content", []), value.get("totalPages")


async def _unique_by_key(
    aiter: AsyncIterator[dict],
    key_func: Callable[[dict], Any],
) -> AsyncIterator[dict]:
    """
    Asynchronously yields only the first item for each unique key, as determined by
    key_func.

    Args:
        aiter (AsyncIterator[dict]): An async iterator yielding dictionaries.
        key_func (Callable): A function that extracts key from each item for uniqueness.

    Yields:
        dict: The first occurrence of each unique key from the async iterator.
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
    semaphore: asyncio.Semaphore,
) -> dict:
    """
    Sends a POST request to the LSE API and returns the parsed JSON response.

    Args:
        client (httpx.AsyncClient): The async HTTP client for making requests.
        payload (dict): The JSON payload to send in the POST request.
        semaphore (asyncio.Semaphore): Semaphore to limit concurrent requests.

    Returns:
        dict: The first element of the JSON response from the LSE API.
    """
    async with semaphore:
        resp = await client.post(_URL, json=payload)
        resp.raise_for_status()
        return resp.json()[0]  # API wraps payload list in a single-element array


async def _lse_serial_pages(
    client: httpx.AsyncClient,
    page_size: int,
    semaphore: asyncio.Semaphore,
) -> AsyncIterator[dict]:
    """
    Asynchronously fetches and yields equities from the LSE API one page at a time,
    starting from page 1, until an empty result is returned. Used when the total number
    of pages is unknown (i.e., totalPages is None).

    Args:
        client (httpx.AsyncClient): The async HTTP client for making requests.
        page_size (int): The number of equities to fetch per page.
        semaphore (asyncio.Semaphore): Semaphore to limit concurrent requests.

    Yields:
        dict: An equity dictionary from the LSE API.
    """
    page = 1
    while True:
        raw = await _fetch_page(client, _build_payload(page, page_size), semaphore)
        equities, _ = _parse_equities(raw)
        if not equities:
            break
        for equity in equities:
            yield equity
        page += 1


async def _lse_concurrent_pages(
    client: httpx.AsyncClient,
    page_size: int,
    semaphore: asyncio.Semaphore,
    total_pages: int,
) -> AsyncIterator[dict]:
    """
    Asynchronously fetches and yields equities from pages 1 to total_pages - 1 of the
    LSE API concurrently.

    Args:
        client (httpx.AsyncClient): The async HTTP client for making requests.
        page_size (int): The number of equities to fetch per page.
        semaphore (asyncio.Semaphore): Semaphore to limit concurrent requests.
        total_pages (int): The total number of pages to fetch.

    Yields:
        dict: An equity dictionary from the LSE API.
    """
    tasks = [
        asyncio.create_task(
            _fetch_page(client, _build_payload(page, page_size), semaphore),
        )
        for page in range(1, total_pages)
    ]
    for coro in asyncio.as_completed(tasks):
        raw = await coro
        for equity in _parse_equities(raw)[0]:
            yield equity


async def _lse_pages(
    client: httpx.AsyncClient,
    page_size: int,
    semaphore: asyncio.Semaphore,
) -> AsyncIterator[dict]:
    """
    Fetches all pages of equities from the LSE API, yielding each equity as a dict.

    Args:
        client (httpx.AsyncClient): The async HTTP client for making requests.
        page_size (int): The number of equities to fetch per page.
        semaphore (asyncio.Semaphore): Semaphore to limit concurrent requests.

    Yields:
        dict: An equity dictionary from the LSE API.
    """
    first_raw = await _fetch_page(client, _build_payload(0, page_size), semaphore)

    equities, total_pages = _parse_equities(first_raw)

    for equity in equities:
        yield equity

    if total_pages is None:
        async for equity in _lse_serial_pages(client, page_size, semaphore):
            yield equity
    else:
        async for equity in _lse_concurrent_pages(
            client,
            page_size,
            semaphore,
            total_pages,
        ):
            yield equity
