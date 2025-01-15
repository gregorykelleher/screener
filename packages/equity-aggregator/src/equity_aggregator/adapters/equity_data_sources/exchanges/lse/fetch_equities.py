# lse/fetch_equities.py

import httpx
import asyncio
import logging
from typing import AsyncIterator, Dict, List, Set, Tuple

from equity_aggregator.adapters.equity_data_sources._cache import load_cache, save_cache

logger = logging.getLogger(__name__)

# ───────────────────────── constants ────────────────────────────────────────

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/134.0.0.0 Safari/537.36"
    )
}

_URL = "https://api.londonstockexchange.com/api/v1/components/refresh"

# ───────────────────────── public coroutine & façade ────────────────────────


async def fetch_equities(
    page_size: int = 100,
    concurrency: int = 8,
    use_cache: bool = True,
) -> List[Dict]:
    """
    Async coroutine; returns a list of unique (per ISIN), valid equities.
    """

    # load from cache if available (refresh every 24 hours)
    cached = load_cache("lse_equities", ttl_minutes=1_440)
    if cached is not None and use_cache:
        logger.info("Loaded LSE raw equities from cache.")
        return cached

    logger.info("Fetching LSE raw equities from LSE API.")

    semaphore = asyncio.Semaphore(concurrency)
    async with httpx.AsyncClient(headers=_HEADERS, timeout=20.0) as client:
        # stream all pages
        stream = _lse_pages(client, page_size, semaphore)

        deduped = _unique_by_key(stream, key_func=lambda eq: eq["isin"])

        raw_equities = [item async for item in deduped]

        # persist to cache and return
        if use_cache:
            save_cache("lse_equities", raw_equities)
        return raw_equities


# ───────────────────────── pure helpers ────────────────────────────────


def _build_payload(page: int, page_size: int) -> Dict:
    """
    Builds the JSON payload for the given page and page size.
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
            }
        ],
    }


def _parse_equities(data: Dict) -> Tuple[List[Dict], int | None]:
    """
    Extracts equities and total_pages from the LSE response.
    Returns a tuple: (list of equities, total_pages).
    """
    comp = next(
        (c for c in data.get("content", []) if c.get("name") == "priceexplorersearch"),
        None,
    )
    if not comp:
        return [], None
    value = comp.get("value", {})
    return value.get("content", []), value.get("totalPages")


async def _unique_by_key(aiter, key_func):
    """
    A simple generator that yields only the first item
    for each unique key_func(item).
    """
    seen: Set = set()
    async for item in aiter:
        k = key_func(item)
        if k in seen:
            continue
        seen.add(k)
        yield item


# ───────────────────────── async I/O layer ──────────────────────────────────


async def _fetch_page(
    client: httpx.AsyncClient,
    payload: Dict,
    semaphore: asyncio.Semaphore,
) -> Dict:
    """
    Executes the POST request and returns the first element of the JSON response.
    """
    async with semaphore:
        resp = await client.post(_URL, json=payload)
        resp.raise_for_status()
        return resp.json()[0]  # API wraps payload list in a single-element array


async def _lse_pages(
    client: httpx.AsyncClient,
    page_size: int,
    sem: asyncio.Semaphore,
) -> AsyncIterator[Dict]:
    """
    Asynchronously fetches pages of equities from LSE
    """
    # first request to discover totalPages
    first_raw = await _fetch_page(client, _build_payload(0, page_size), sem)
    equities, total_pages = _parse_equities(first_raw)
    for eq in equities:
        yield eq

    # If total_pages is absent, fall back to serial crawl until empty page.
    if total_pages is None:
        page = 1
        while True:
            raw = await _fetch_page(client, _build_payload(page, page_size), sem)
            equities, _ = _parse_equities(raw)
            if not equities:
                break
            for eq in equities:
                yield eq
            page += 1
        return

    # fire off remaining pages concurrently
    tasks = [
        asyncio.create_task(_fetch_page(client, _build_payload(p, page_size), sem))
        for p in range(1, total_pages)
    ]
    for coro in asyncio.as_completed(tasks):
        raw = await coro
        for eq in _parse_equities(raw)[0]:
            yield eq
