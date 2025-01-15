# authoritative_feeds/euronext.py

import asyncio
import logging
import re
from collections.abc import AsyncIterator, Callable, Hashable, Iterable
from functools import partial
from typing import Any

import httpx

from equity_aggregator.adapters.data_sources._cache import load_cache, save_cache

logger = logging.getLogger(__name__)

_COUNTRY_TO_MIC: dict[str, str] = {
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


async def fetch_equities(
    page_size: int = 100,
    concurrency: int = 8,
) -> list[dict]:
    """
    Fetch a list of unique, valid equities from the Euronext API or cache.

    Args:
        page_size (int, optional): Number of records to fetch per page. Defaults to 100.
        concurrency (int, optional): Max number of concurrent requests. Defaults to 8.

    Returns:
        list[dict]: List of unique equities, each as a dictionary with keys:
            - name (str): Equity name.
            - symbol (str): Ticker symbol.
            - isin (str): ISIN code.
            - mics (list[str]): List of MIC codes.
            - currency (str): Currency code.
            - last_price (str): Last traded price as a string.
    """
    # load from cache if available (refresh every 24 hours by default)
    cached = load_cache("euronext_equities")
    if cached is not None:
        logger.info("Loaded Euronext raw equities from cache.")
        return cached

    logger.info("Fetching Euronext raw equities from Euronext API.")

    # fetch and process equities
    semaphore = asyncio.Semaphore(concurrency)
    async with httpx.AsyncClient(headers=_HEADERS, timeout=20.0) as client:
        # merge all country‐streams into one
        stream = _multi_country_pages(client, page_size, semaphore)

        deduped = _unique_by_key(stream, key_func=lambda eq: eq["isin"])

        raw_equities = [item async for item in deduped]

        logger.debug(
            f"Fetched {len(raw_equities)} unique raw equities from Euronext API.",
        )

        # persist to cache and return
        save_cache("euronext_equities", raw_equities)
        logger.info("Saved Euronext raw equities to cache.")
        return raw_equities


async def _unique_by_key(
    aiter: AsyncIterator,
    key_func: Callable[[Any], Hashable],
) -> AsyncIterator[Any]:
    """
    Async generator that yields the first item for each unique key from async iterable.

    Args:
        aiter (AsyncIterable): An asynchronous iterable of items to filter.
        key_func (Callable): A function that takes an item and returns a hashable key
            used to determine uniqueness.

    Yields:
        Any: The first occurrence of each unique key in the input async iterable.
    """
    seen = set()
    async for item in aiter:
        k = key_func(item)
        if k in seen:
            continue
        seen.add(k)
        yield item


async def _multi_country_pages(
    client: httpx.AsyncClient,
    page_size: int,
    sem: asyncio.Semaphore,
) -> AsyncIterator[dict]:
    """
    Fetch and yield equities from all Euronext MICs concurrently.

    Args:
        client (httpx.AsyncClient): The HTTP client for making requests.
        page_size (int): Number of records to fetch per page.
        sem (asyncio.Semaphore): Semaphore to limit concurrent requests.

    Yields:
        dict: Parsed equity data for each record from all supported MICs.
    """

    async def _collect(mic: str) -> list[dict]:
        return [equity async for equity in _country_pages(client, mic, page_size, sem)]

    tasks = [asyncio.create_task(_collect(mic)) for mic in _COUNTRY_TO_MIC.values()]
    for coro in asyncio.as_completed(tasks):
        for equity in await coro:
            yield equity


def _build_payload(start: int, length: int, draw: int) -> dict:
    """
    Build the form data payload for the Euronext POST endpoint.

    Args:
        start (int): The starting index of records to fetch (pagination offset).
        length (int): The number of records to fetch per page.
        draw (int): The draw counter for DataTables (used for request tracking).

    Returns:
        dict: The payload dictionary to be sent as form data in the POST request.
    """
    return {
        "draw": draw,
        "start": start,
        "length": length,
        "iDisplayLength": length,
        "iDisplayStart": start,
    }


def _parse_equities(aa_data: Iterable) -> Iterable[dict]:
    """
    Parse the "aaData" rows from the Euronext JSON response and yield a dict per equity.

    Args:
        aa_data (Iterable): Rows from the "aaData" field of the Euronext API response.

    Yields:
        dict: Parsed equity data with keys:
            - name (str): Equity name.
            - symbol (str): Ticker symbol.
            - isin (str): ISIN code.
            - mics (list[str]): List of MIC codes.
            - currency (str): Currency code.
            - last_price (str): Last traded price as a string.
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


async def _fetch_page(
    client: httpx.AsyncClient,
    mic_code: str,
    payload: dict,
    semaphore: asyncio.Semaphore,
) -> dict:
    """
    Make a single POST request to the Euronext live endpoint for a given MIC code.

    Args:
        client (httpx.AsyncClient): The HTTP client for making requests.
        mic_code (str): The MIC code representing the country exchange.
        payload (dict): The form data payload for the POST request.
        semaphore (asyncio.Semaphore): Semaphore to limit concurrent requests.

    Returns:
        dict: The JSON response from the Euronext API as a dictionary.
    """
    url = f"https://live.euronext.com/en/pd_es/data/stocks?mics={mic_code}"
    async with semaphore:
        try:
            response = await client.post(url, data=payload)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as error:
            logger.warning("Euronext API 500 for %s: %s", url, error)
            return {"aaData": [], "iTotalRecords": 0}
        except httpx.ReadError as error:
            logger.warning("Network/Read error for %s: %s", url, error)
            return {"aaData": [], "iTotalRecords": 0}


async def _country_pages(
    client: httpx.AsyncClient,
    mic_code: str,
    page_size: int,
    semaphore: asyncio.Semaphore,
) -> AsyncIterator[dict]:
    """
    Fetch all equity pages for a single country (MIC) from the Euronext API.

    Strategy:
      1. Fetch the first page to determine the total number of records.
      2. Concurrently fetch all remaining pages using the provided semaphore.

    Args:
        client (httpx.AsyncClient): The HTTP client for making requests.
        mic_code (str): The MIC code representing the country exchange.
        page_size (int): Number of records per page.
        semaphore (asyncio.Semaphore): Semaphore to limit concurrent requests.

    Yields:
        dict: Parsed equity data for each record.
    """
    first_payload = _build_payload(0, page_size, draw=1)
    first = await _fetch_page(client, mic_code, first_payload, semaphore)
    for equity in _parse_equities(first["aaData"]):
        yield equity

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
            ),
        )
        for idx, s in enumerate(starts)
    ]

    # Await tasks as they complete
    for coro in asyncio.as_completed(tasks):
        data = await coro
        for equity in _parse_equities(data["aaData"]):
            yield equity
