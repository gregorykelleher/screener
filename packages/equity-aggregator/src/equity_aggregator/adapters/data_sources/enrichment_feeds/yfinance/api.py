# yfinance/api.py

import logging
from collections.abc import Iterable, Mapping

import httpx
from rapidfuzz import fuzz, utils

from .session import YFSession

logger = logging.getLogger(__name__)


async def search_quotes(
    session: YFSession,
    query: str,
) -> list[dict]:
    """
    Asynchronously search Yahoo Finance for equities matching a query string.

    This coroutine sends a GET request to Yahoo Finance's search API using the
    provided query. The response is parsed for a "quotes" field, which should
    contain a list of quote dictionaries. Only quotes where "quoteType" is
    "EQUITY" are included in the result.

    Args:
        session (YFSession): The Yahoo Finance session for making HTTP requests.
        query (str): The search query (symbol, name, ISIN, or CUSIP).

    Returns:
        list[dict]: List of quote dictionaries for equities matching the query.
    """
    try:
        response = await session.get(
            session.config.search_url,
            params={"q": query},
        )
        response.raise_for_status()
        raw_data = response.json().get("quotes", [])

    except httpx.HTTPError as error:
        logger.error("HTTP error during search for %s: %s", query, error)
        return []

    except Exception as error:
        logger.error("Unexpected error during search for %s: %s", query, error)
        return []

    # filter out non-equity quotes
    return [quote for quote in raw_data if quote.get("quoteType") == "EQUITY"]


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
    response.raise_for_status()
    raw = response.json().get("quoteSummary", {}).get("result", [])

    if raw:
        return _flatten_module_dicts(modules, raw[0])

    return await _get_quote_summary_fallback(session, ticker)


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


def pick_best_symbol(
    quotes: list[dict],
    *,
    name_key: str,
    expected_name: str,
    expected_symbol: str,
    min_score: int = 0,
) -> str | None:
    """
    Select the best-matching symbol from a list of Yahoo Finance quotes using
    fuzzy matching.

    For each quote, this function computes a combined fuzzy score based on the
    similarity between the quote's symbol and the expected symbol, and between the
    quote's name (using `name_key`) and the expected name. Quote with the highest
    combined score is selected if its score meets or exceeds `min_score`. If no
    quote meets the threshold, None is returned.

    Args:
        quotes (list[dict]): List of quote dictionaries, each with at least a
            "symbol" key and a name field specified by `name_key`.
        name_key (str): The key in each quote dict for equity name
            (e.g., "longname").
        expected_name (str): The expected equity name to match against.
        expected_symbol (str): The expected ticker symbol to match against.
        min_score (int, optional): Minimum combined fuzzy score required to accept a
            match. Defaults to 0.

    Returns:
        str | None: Best-matching symbol if a suitable match is found, else None.
    """

    if not quotes:
        return None

    # compute fuzzy scores for each quote
    scored = [
        _score_quote(
            quote,
            name_key=name_key,
            expected_symbol=expected_symbol,
            expected_name=expected_name,
        )
        for quote in quotes
    ]

    # compute the best score and symbol from the scored list
    best_score, best_symbol, best_name = max(scored, key=lambda t: t[0])

    # if the best score is below the minimum threshold, return None
    if best_score < min_score:
        return None

    # otherwise, return the best symbol found
    return best_symbol


def _score_quote(
    quote: dict,
    *,
    name_key: str,
    expected_symbol: str,
    expected_name: str,
) -> tuple[int, str, str]:
    """
    Compute a combined fuzzy score for a Yahoo Finance quote.

    This function calculates the sum of the fuzzy string similarity between the
    quote's symbol and the expected symbol, and between the quote's name (using
    `name_key`) and the expected name. The result is a tuple containing the total
    score, the actual symbol, and the actual name.

    Args:
        quote (dict): The quote dictionary containing at least a "symbol" key and
            a name field specified by `name_key`.
        name_key (str): The key in the quote dict for the equity name.
        expected_symbol (str): The expected ticker symbol to match against.
        expected_name (str): The expected equity name to match against.

    Returns:
        tuple[int, str, str]: A tuple of (total_score, actual_symbol, actual_name),
            where total_score is the sum of the symbol and name fuzzy scores.
    """
    actual_symbol = quote["symbol"]
    actual_name = quote.get(name_key, "<no-name>")

    symbol_score = fuzz.ratio(
        actual_symbol,
        expected_symbol,
        processor=utils.default_process,
    )
    name_score = fuzz.WRatio(
        actual_name,
        expected_name,
        processor=utils.default_process,
    )

    total_score = symbol_score + name_score
    return total_score, actual_symbol, actual_name
