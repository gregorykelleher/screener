# authoritative_feeds/euronext.py

import asyncio
import logging
import re
from collections.abc import AsyncIterator, Callable, Iterable
from functools import lru_cache

import httpx

from equity_aggregator.adapters.data_sources._cache import load_cache, save_cache
from equity_aggregator.adapters.data_sources._utils import make_client_factory

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

_EURONEXT_HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "User-Agent": "Mozilla/5.0",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://live.euronext.com",
    "Referer": "https://live.euronext.com/en/markets",
    "Accept-Encoding": "gzip, deflate",
}

_EURONEXT_SEARCH_URL = "https://live.euronext.com/en/pd_es/data/stocks"
_PAGE_SIZE = 100

ClientFactory = Callable[..., httpx.AsyncClient]
_DEFAULT_CLIENT_FACTORY: ClientFactory = make_client_factory(headers=_EURONEXT_HEADERS)


@lru_cache(maxsize=1)
def get_client() -> httpx.AsyncClient:
    return _DEFAULT_CLIENT_FACTORY()


async def fetch_equity_records(
    client_factory: ClientFactory = _DEFAULT_CLIENT_FACTORY,
) -> AsyncIterator[dict[str, object]]:
    """
    Asynchronously stream all Euronext equity records, using cache if available.

    If cached records exist, yields them directly. Otherwise, fetches all records
    from the Euronext feed, streams them as they arrive, and caches the results for
    future use.

    Args:
        client_factory (ClientFactory, optional): Callable that returns an
            httpx.AsyncClient for HTTP requests. Defaults to _DEFAULT_CLIENT_FACTORY.

    Yields:
        dict[str, object]: A normalised equity record from the Xetra feed.

    Returns:
        AsyncIterator[dict[str, object]]: An async iterator yielding equity records.
    """
    cached = load_cache("euronext_records")

    if cached:
        logger.info("Loaded %d Euronext records from cache.", len(cached))
        for record in cached:
            yield record
        return

    async for record in _fetch_and_cache(client_factory):
        yield record


async def _fetch_and_cache(
    client_factory: ClientFactory,
) -> AsyncIterator[dict[str, object]]:
    """
    Asynchronously fetch and cache all equity records from the Euronext feed.

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
    save_cache("euronext_records", buffer)
    logger.info("Saved %d Euronext records to cache.", len(buffer))


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

    async for record in _fetch_equity_records_from_mics(client):
        yield record


async def _fetch_equity_records_from_mics(
    client: httpx.AsyncClient,
) -> AsyncIterator[dict[str, object]]:
    """
    Asynchronously fetch and yield equity records from all supported Euronext MICs.

    Launches concurrent tasks to retrieve equities for each MIC code, yielding each
    parsed equity record as soon as it is available.

    Args:
        client (httpx.AsyncClient): The HTTP client used for making requests.

    Yields:
        dict[str, object]: Parsed equity data for each record from all supported MICs.

    Returns:
        AsyncIterator[dict[str, object]]: An async iterator yielding equity records.
    """
    client = get_client()

    async def _collect(mic: str) -> list[dict[str, object]]:
        return [equity async for equity in _country_pages(client, mic)]

    tasks = [asyncio.create_task(_collect(mic)) for mic in _COUNTRY_TO_MIC.values()]
    for coroutine in asyncio.as_completed(tasks):
        try:
            equities = await coroutine
        except (httpx.HTTPStatusError, httpx.ReadError) as error:
            logger.warning("Euronext API error fetching MIC batch: %s", error)
            continue
        for equity in equities:
            yield equity


def _build_payload(start: int, length: int, draw: int) -> dict[str, int]:
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


def _parse_equities(aa_data: Iterable) -> Iterable[dict[str, object]]:
    """
    Parse the "aaData" rows from the Euronext JSON response and yield a dict for each
    equity.

    Args:
        aa_data (Iterable): Iterable of rows from the "aaData" field of the Euronext
            API response. Each row is a list of HTML strings representing equity data.

    Yields:
        dict[str, object]: Parsed equity data with the following keys:
            - name (str): Equity name.
            - symbol (str): Ticker symbol.
            - isin (str): ISIN code.
            - mics (list[str]): List of MIC codes.
            - currency (str): Currency code (e.g., "EUR").
            - last_price (str): Last traded price as a string.

    Returns:
        Iterable[dict[str, object]]: An iterable of dictionaries, each representing a
            single equity record.
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


async def _country_pages(
    client: httpx.AsyncClient,
    mic_code: str,
) -> AsyncIterator[dict[str, object]]:
    """
    Asynchronously fetch and yield all equity records for a single MIC code.

    Fetches the first page of equity records for the specified MIC code. If the
    first page is available, yields each parsed equity record from it. Then,
    asynchronously fetches and yields records from all remaining pages.

    Args:
        client (httpx.AsyncClient): The HTTP client used for making requests.
        mic_code (str): The MIC code representing the Euronext market to query.

    Yields:
        dict[str, object]: Parsed equity data for each record in the MIC.

    Returns:
        AsyncIterator[dict[str, object]]: An async iterator yielding equity records.
    """
    first_page = await _fetch_first_page(client, mic_code)

    if not first_page:
        return

    for record in _parse_equities(first_page["aaData"]):
        yield record
    async for record in _fetch_remaining_pages(client, mic_code, first_page):
        yield record


async def _fetch_first_page(
    client: httpx.AsyncClient,
    mic_code: str,
) -> dict[str, object] | None:
    """
    Asynchronously fetch the first page of equity records for a given MIC code.

    Sends a POST request to the Euronext endpoint to retrieve the first page of
    equities for the specified MIC code. Returns the parsed JSON response if
    successful, or None if an error occurs.

    Args:
        client (httpx.AsyncClient): The HTTP client used for making requests.
        mic_code (str): The MIC code representing the Euronext market to query.

    Returns:
        dict[str, object] | None: The parsed JSON response for the first page, or
        None if the request fails or the response is invalid.
    """
    payload = _build_payload(0, _PAGE_SIZE, draw=1)
    url = f"{_EURONEXT_SEARCH_URL}?mics={mic_code}"

    try:
        response = await client.post(
            url,
            data=payload,
        )
        response.raise_for_status()
        return response.json()

    except (httpx.HTTPStatusError, httpx.ReadError, ValueError) as error:
        root = error.__context__ or error.__cause__
        label = repr(root) if root else type(error).__name__
        logger.warning("Euronext [%s] %s → %s", mic_code, url, label)
        return None


async def _fetch_remaining_pages(
    client: httpx.AsyncClient,
    mic_code: str,
    first_page: dict[str, object],
) -> AsyncIterator[dict[str, object]]:
    """
    Asynchronously fetch and yield all remaining equity records for a given MIC code,
    starting from page 2.

    Args:
        client (httpx.AsyncClient): The HTTP client used for requests.
        mic_code (str): The MIC code representing the Euronext market.
        first_page (dict[str, object]): The parsed JSON response from the first page,
            must include "iTotalRecords".

    Yields:
        dict[str, object]: Parsed equity record from each subsequent page.

    Returns:
        AsyncIterator[dict[str, object]]: An async iterator yielding equity records.
    """
    total_records = first_page.get("iTotalRecords", 0)
    page_size = _PAGE_SIZE

    if total_records <= page_size:
        return

    url = f"{_EURONEXT_SEARCH_URL}?mics={mic_code}"

    for draw, start in enumerate(range(page_size, total_records, page_size), start=2):
        payload = _build_payload(start=start, length=page_size, draw=draw)
        try:
            response = await client.post(url, data=payload)
            response.raise_for_status()
            page_data = response.json().get("aaData", [])

        except (httpx.HTTPStatusError, httpx.ReadError, ValueError) as exc:
            logger.warning("Euronext page %d error for %s: %s", draw, mic_code, exc)
            continue

        for record in _parse_equities(page_data):
            yield record
