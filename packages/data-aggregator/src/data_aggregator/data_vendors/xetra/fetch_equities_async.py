# xetra/fetch_equities_async.py

import asyncio
from typing import AsyncIterator, Dict, List, Set

import httpx

# ───────────────────────── constants ───────────────────────────────────

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

# ───────────────────────── pure helpers ────────────────────────────────


def _build_payload(offset: int, limit: int) -> Dict:
    """
    JSON body for each POST.
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


def _parse_equities(items: List[Dict]) -> List[Dict]:
    """
    Normalise API items to the equity dict our pipeline expects.
    """
    out: List[Dict] = []
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
            }
        )
    return out


async def _unique_by_key_async(aiter, key_func):
    """
    Async generator that yields only the first item for each unique key.
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
    client: httpx.AsyncClient, payload: Dict, sem: asyncio.Semaphore
) -> Dict:
    """
    Single async POST; returns parsed JSON dict.
    """
    async with sem:
        resp = await client.post(_URL, json=payload)
        resp.raise_for_status()
        return resp.json()


async def _xetra_pages(
    client: httpx.AsyncClient,
    page_size: int,
    sem: asyncio.Semaphore,
) -> AsyncIterator[Dict]:
    """
    Stream all pages. First call discovers total record count, then remaining
    pages are fired concurrently.
    """
    # ── first request ───────────────────────────────────────────────────────
    first_raw = await _fetch_page(client, _build_payload(0, page_size), sem)
    records_total = first_raw.get("recordsTotal", 0)
    for eq in _parse_equities(first_raw.get("data", [])):
        yield eq

    # No extra pages?
    if records_total <= page_size:
        return

    # ── remaining pages concurrently ────────────────────────────────────────
    offsets = range(page_size, records_total, page_size)
    tasks = [
        asyncio.create_task(_fetch_page(client, _build_payload(off, page_size), sem))
        for off in offsets
    ]

    for coro in asyncio.as_completed(tasks):
        data = await coro
        for eq in _parse_equities(data.get("data", [])):
            yield eq


# ───────────────────────── public coroutine & façade ────────────────────────


async def fetch_equities_async(
    page_size: int = 100,
    concurrency: int = 8,
) -> List[Dict]:
    """
    Coroutine returning list[dict], one entry per unique ISIN.
    """
    sem = asyncio.Semaphore(concurrency)
    async with httpx.AsyncClient(headers=_HEADERS, timeout=20.0) as client:
        stream = _xetra_pages(client, page_size, sem)
        deduped = _unique_by_key_async(stream, key_func=lambda e: e["isin"])
        return [item async for item in deduped]
