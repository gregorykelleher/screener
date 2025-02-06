# euronext/fetch_equities_async.py

import asyncio
import re
from functools import partial
from typing import AsyncIterator, Iterable, List, Dict

import httpx

# --- constants -------------------------------------------------------------

_COUNTRY_TO_MIC: Dict[str, str] = {
    "France": "XPAR",
    "Netherlands": "XAMS",
    "Belgium": "XBRU",
    "Ireland": "XMSM",
    "Portugal": "XLIS",
    "Italy": "MTAA",
    "Norway": "XOSL",
}

_HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "User-Agent": "Mozilla/5.0",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
}

# --- pure helpers ------------------------------------------------------


async def _unique_by_key_async(aiter, key_func):
    """
    Async generator that yields only the first item for each unique key.
    """
    seen = set()
    async for item in aiter:
        k = key_func(item)
        if k in seen:
            continue
        seen.add(k)
        yield item


def _build_payload(start: int, length: int, draw: int) -> Dict:
    """
    Builds the form payload required by the Euronext POST endpoint.
    """
    return {
        "draw": draw,
        "start": start,
        "length": length,
        "iDisplayLength": length,
        "iDisplayStart": start,
    }


def _parse_equities(aa_data) -> Iterable[Dict]:
    """
    Parses the "aaData" rows from the Euronext JSON response and yields a dict per equity:
      - name
      - symbol
      - isin
      - mics
      - currency
      - last_price
    """
    for row in aa_data:
        # Extract the equity name from the anchor tag in column 1.
        name_match = re.search(r">(.*?)<", row[1])
        name = name_match.group(1).strip() if name_match else row[1].strip()

        # Extract the MIC code from the div in column 4.
        mic_match = re.search(r">(.*?)<", row[4])
        mic_str = mic_match.group(1).strip() if mic_match else row[4].strip()

        # Split into a list (or fallback to empty if no MIC string).
        mics = [m.strip() for m in mic_str.split(",")] if mic_str else []

        # Extract currency and last price from column 5.
        # Example in row[5]:
        #   <div class='text-right pd_currency_es'>
        #       EUR <span class='pd_last_price_es'>31.00</span>
        #   </div>
        cp_match = re.search(r">([A-Z]{3})\s*<span[^>]*>([\d\.,]+)</span>", row[5])

        # Extract currency and last price from the match or fallback to empty strings.
        currency, last_price = cp_match.groups() if cp_match else ("", "")

        yield {
            "name": name,
            "symbol": row[3].strip(),
            "isin": row[2].strip(),
            "mics": mics,
            "currency": currency,
            "last_price": last_price,
        }


# --- async I/O layer --------------------------------------------------------


async def _fetch_page(
    client: httpx.AsyncClient,
    mic_code: str,
    payload: Dict,
    semaphore: asyncio.Semaphore,
):
    """
    Makes a single POST request to retrieve data from the Euronext live endpoint.
    Returns the JSON response as a dict.
    """
    url = f"https://live.euronext.com/en/pd_es/data/stocks?mics={mic_code}"
    async with semaphore:
        r = await client.post(url, data=payload)
        r.raise_for_status()
        return r.json()


async def _country_pages(
    client: httpx.AsyncClient,
    mic_code: str,
    page_size: int,
    semaphore: asyncio.Semaphore,
) -> AsyncIterator[Dict]:
    """
    Fetch *all* pages for a single country.
    Strategy:
      1. Hit page 0 to discover totalRecords.
      2. Fire off tasks for the remaining pages concurrently.
    """
    first_payload = _build_payload(0, page_size, draw=1)
    first = await _fetch_page(client, mic_code, first_payload, semaphore)
    for eq in _parse_equities(first["aaData"]):
        yield eq

    total = first.get("iTotalRecords", 0)
    if total <= page_size:  # nothing more to fetch
        return

    # Prep remaining requests
    starts = range(page_size, total, page_size)
    make_payload = partial(_build_payload, length=page_size)
    tasks = [
        asyncio.create_task(
            _fetch_page(
                client,
                mic_code,
                make_payload(start=s, draw=idx + 2),
                semaphore,
            )
        )
        for idx, s in enumerate(starts)
    ]

    # Await tasks as they complete
    for coro in asyncio.as_completed(tasks):
        data = await coro
        for eq in _parse_equities(data["aaData"]):
            yield eq


# --- public coroutine -------------------------------------------------------


async def fetch_equities_async(
    page_size: int = 100,
    concurrency: int = 8,  # max simultaneous HTTP requests
) -> List[Dict]:
    """
    Async entry point; returns a list of unique equities.
    """
    semaphore = asyncio.Semaphore(concurrency)
    async with httpx.AsyncClient(headers=_HEADERS, timeout=20.0) as client:

        async def _collect(mic: str) -> list[dict]:
            return [
                eq async for eq in _country_pages(client, mic, page_size, semaphore)
            ]

        tasks = [asyncio.create_task(_collect(mic)) for mic in _COUNTRY_TO_MIC.values()]

        async def _flatten():
            for coro in asyncio.as_completed(tasks):
                for item in await coro:
                    yield item

        deduped = _unique_by_key_async(_flatten(), key_func=lambda eq: eq["isin"])

        # Because equities_iter is async, gather items lazily
        return [item async for item in deduped]
