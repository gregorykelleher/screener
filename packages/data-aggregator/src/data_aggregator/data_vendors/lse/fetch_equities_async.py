# lse/fetch_equities_async.py

import asyncio
from typing import AsyncIterator, Dict, List, Set, Tuple

import httpx

# ───────────────────────── constants ────────────────────────────────────────

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/134.0.0.0 Safari/537.36"
    )
}

_URL = "https://api.londonstockexchange.com/api/v1/components/refresh"

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


async def _unique_by_key_async(aiter, key_func):
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


# ───────────────────────── public coroutine & façade ────────────────────────


async def fetch_equities_async(
    page_size: int = 100,
    concurrency: int = 8,
) -> List[Dict]:
    """
    Coroutine returning a list[dict] with one entry per unique ISIN.
    """
    sem = asyncio.Semaphore(concurrency)
    async with httpx.AsyncClient(headers=_HEADERS, timeout=20.0) as client:
        stream = _lse_pages(client, page_size, sem)
        deduped = _unique_by_key_async(stream, key_func=lambda e: e.get("isin"))
        return [item async for item in deduped]
