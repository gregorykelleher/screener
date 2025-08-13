# api/summary.py

import logging
from collections.abc import Iterable, Mapping

import httpx

from ..session import YFSession

logger = logging.getLogger(__name__)


async def get_quote_summary(
    session: YFSession,
    ticker: str,
    modules: Iterable[str] | None = None,
) -> dict[str, object] | None:
    """
    Fetch and flatten Yahoo Finance quoteSummary modules for a given ticker.

    This coroutine retrieves detailed equity data for the specified ticker symbol
    from Yahoo Finance's quoteSummary endpoint. It requests all specified modules
    in a single call, then merges the resulting module dictionaries into a single
    flat mapping for convenience.

    Args:
        session (YFSession): The Yahoo Finance session for making HTTP requests.
        ticker (str): The stock symbol to fetch (e.g., "AAPL").
        modules (Iterable[str] | None): Optional iterable of module names to
            retrieve. If None, uses the default modules from the session config.

    Returns:
        dict[str, object] | None: A flattened dictionary containing all fields from
        the requested modules, or None if no data is found.
    """

    modules = tuple(modules or session.config.modules)

    url = session.config.quote_summary_url + ticker

    response = await session.get(
        url,
        params={
            "modules": ",".join(modules),
            "corsDomain": "finance.yahoo.com",
            "formatted": "false",
            "symbol": ticker,
            "lang": "en-US",
            "region": "US",
        },
    )

    status = response.status_code

    # 401/500/502 → fallback
    if status in {
        httpx.codes.UNAUTHORIZED,
        httpx.codes.INTERNAL_SERVER_ERROR,
        httpx.codes.BAD_GATEWAY,
    }:
        return await _get_quote_summary_fallback(session, ticker)

    # 429 after back-off → treat as “no data” so the caller logs it cleanly
    if status == httpx.codes.TOO_MANY_REQUESTS:
        raise LookupError(f"HTTP 429 Too Many Requests for {ticker}")

    # everything else: try to parse
    raw = response.json().get("quoteSummary", {}).get("result", [])
    if raw:
        return _flatten_module_dicts(modules, raw[0])

    # empty result
    raise LookupError("Quote Summary endpoint returned nothing.")


async def _get_quote_summary_fallback(
    session: YFSession,
    ticker: str,
) -> dict[str, object] | None:
    """
    Fallback: fetch basic quote data from Yahoo Finance's v7 /finance/quote endpoint.

    This coroutine is used if the main quoteSummary endpoint returns no data. It
    retrieves a basic set of quote fields for the given ticker symbol from the
    fallback endpoint.

    Args:
        session (YFSession): The Yahoo Finance session for making HTTP requests.
        ticker (str): The stock symbol to fetch (e.g., "AAPL").

    Returns:
        dict[str, object] | None: The first quote dictionary from the response if
        available, otherwise None.
    """
    resp = await session.get(
        session.config.quote_summary_fallback_url,
        params={
            "corsDomain": "finance.yahoo.com",
            "formatted": "false",
            "symbols": ticker,
            "lang": "en-US",
            "region": "US",
        },
    )
    resp.raise_for_status()
    results = resp.json().get("quoteResponse", {}).get("result", [])
    return results[0] if results else None


def _flatten_module_dicts(
    modules: Iterable[str],
    payload: Mapping[str, object],
) -> dict[str, object]:
    """
    Merge and flatten module dictionaries from a Yahoo Finance API payload.

    For each module name in `modules`, if the corresponding value in `payload` is a
    dictionary, its key-value pairs are merged into a single dictionary. Keys from
    later modules can overwrite those from earlier modules.

    Args:
        modules (Iterable[str]): Module names to extract and merge from the payload.
        payload (Mapping[str, object]): Mapping of module names to their data
            (typically from the Yahoo Finance API response).

    Returns:
        dict[str, object]: A merged dictionary containing all key-value pairs from
        the specified module dictionaries found in the payload.
    """
    merged: dict[str, object] = {}
    for module in modules:
        if (value := payload.get(module)) and isinstance(value, dict):
            merged.update(value)
    return merged
