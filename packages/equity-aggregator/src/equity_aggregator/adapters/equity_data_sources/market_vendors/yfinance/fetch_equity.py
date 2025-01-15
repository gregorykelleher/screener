# yfinance/fetch_equity.py

import asyncio
import logging
from typing import Dict, List

import httpx
import yfinance as yf
from rapidfuzz import fuzz

from equity_aggregator.adapters.equity_data_sources._cache import (
    load_cache_entry,
    save_cache_entry,
)

logger = logging.getLogger(__name__)


async def fetch_equity(
    symbol: str,
    name: str,
    *,
    isin: str | None = None,
    cusip: str | None = None,
    use_cache: bool = True,
) -> Dict:
    """
    Retrieve equity information from Yahoo Finance for a given symbol and name, with optional caching.

    Args:
        symbol (str): The equity symbol used as the cache key (e.g., "AAPL").
        name (str): The name of the equity.
        isin (str | None, optional): The International Securities Identification Number. Defaults to None.
        cusip (str | None, optional): The Committee on Uniform Securities Identification Procedures number. Defaults to None.
        use_cache (bool, optional): Whether to use cached results if available. Defaults to True.

    Returns:
        Dict: A dictionary containing the equity information as returned by Yahoo Finance.

    Details:
        - The cache key is always the incoming `symbol`, not any discovered Yahoo symbol.
        - Cached entries are valid for 1,440 minutes (24 hours).
        - If data is not found in the cache or `use_cache` is False, data is fetched from Yahoo Finance.
        - On cache hit, logs the event and returns cached data.
        - Data is fetched using ISIN, CUSIP, or a fuzzy match on name and symbol.
    """
    if use_cache:
        cached = load_cache_entry("yfinance_equities", symbol, ttl_minutes=1_440)
        if cached:
            logger.info("Cache hit for %s", symbol)
            return cached

    # fetch data from Yahoo Finance
    data = await _fetch_yf_equity_data(
        isin=isin,
        cusip=cusip,
        name=name,
        symbol=symbol,
    )

    if use_cache and data:
        save_cache_entry("yfinance_equities", symbol, data)
    return data


async def _fetch_yf_equity_data(
    *,
    isin: str | None = None,
    cusip: str | None = None,
    name: str,
    symbol: str,
) -> Dict:
    """
    Asynchronously fetch detailed equity data from Yahoo Finance using ISIN, CUSIP, name, and symbol.

    This function attempts to retrieve equity information in the following order:
        1. If an ISIN is provided, search for the equity using the ISIN and return the best match.
        2. If a CUSIP is provided (and ISIN did not yield results), search using the CUSIP.
        3. If neither identifier yields results, perform a fuzzy search using the provided name and symbol.
        4. If no data is found by any method, log a warning and return an empty dictionary.

    Args:
        isin (str | None): The International Securities Identification Number of the equity.
        cusip (str | None): The Committee on Uniform Securities Identification Procedures number.
        name (str): The name of the equity.
        symbol (str): The ticker symbol of the equity.

    Returns:
        Dict: A dictionary containing the equity data if found, otherwise an empty dictionary.
    """
    fetchers = [
        # if ISIN is provided, try to find equity data using it
        (lambda: _find_via_isin(isin, expected_name=name, expected_symbol=symbol))
        if isin
        else None,
        # if CUSIP is provided, try to find equity data using it
        (lambda: _find_via_cusip(cusip, expected_name=name, expected_symbol=symbol))
        if cusip
        else None,
        # if neither ISIN nor CUSIP is provided, try to find equity data using name and symbol
        lambda: _find_via_name_symbol(name=name, symbol=symbol),
    ]

    # run in sequence, returning the first truthy result
    for fetch in fetchers:
        if fetch is None:
            continue
        result = await fetch()
        if result:
            if "symbol" in result:
                logger.info(
                    "Auto-discovered Yahoo symbol %s for input symbol %s",
                    result["symbol"],
                    symbol,
                )
            return result

    logger.warning("Yahoo returned no data for %s / %s / %s", symbol, isin, name)
    return {}


async def _find_via_isin(
    isin: str, *, expected_name: str, expected_symbol: str
) -> Dict | None:
    """
    Asynchronously retrieves equity data using the provided ISIN (International Securities Identification Number).

    Args:
        isin (str): The ISIN identifier for the equity to search for.
        expected_name (str): The expected name of the equity for validation.
        expected_symbol (str): The expected symbol of the equity for validation.

    Returns:
        Dict | None: A dictionary containing the equity data if found, otherwise None.
    """
    return await _find_by_identifier(
        identifier=isin,
        expected_name=expected_name,
        expected_symbol=expected_symbol,
    )


async def _find_via_cusip(
    cusip: str, *, expected_name: str, expected_symbol: str
) -> Dict | None:
    """
    Asynchronously finds equity data using a CUSIP identifier.

    Args:
        cusip (str): The CUSIP identifier for the equity.
        expected_name (str): The expected name of the equity.
        expected_symbol (str): The expected symbol of the equity.

    Returns:
        Dict | None: A dictionary containing the equity data if found, otherwise None.
    """
    return await _find_by_identifier(
        identifier=cusip,
        expected_name=expected_name,
        expected_symbol=expected_symbol,
    )


async def _find_by_identifier(
    *,
    identifier: str,
    expected_name: str,
    expected_symbol: str,
) -> Dict | None:
    """
    Shared implementation for identifier-based equity lookup (ISIN or CUSIP).

    Steps:
        1. Search Yahoo Finance quotes using the provided identifier value.
        2. Filter results to those containing both 'symbol' and 'longname'.
        3. If multiple candidates exist, select the best match using a combined fuzzy score
           based on similarity to the expected name and symbol.
        4. Retrieve and return detailed equity information for the selected symbol using yfinance.

    Args:
        id_type (str): The identifier value to search for (ISIN or CUSIP).
        expected_name (str): The expected name of the equity for fuzzy matching.
        expected_symbol (str): The expected symbol of the equity for fuzzy matching.

    Returns:
        Dict | None: A dictionary with detailed equity data if found, otherwise None.
    """
    quotes = await _search_quotes(identifier)

    # filter quotes to those that have both 'symbol' and 'longname'
    viable = [q for q in quotes if q.get("symbol") and q.get("longname")]
    if not viable:
        return None

    # if only one viable candidate, use it directly
    if len(viable) == 1:
        chosen = viable[0]["symbol"]
    else:
        chosen = _pick_best_symbol(
            viable,
            name_key="longname",
            expected_name=expected_name,
            expected_symbol=expected_symbol,
        )
        if not chosen:
            return None

    return await _get_info(chosen)


async def _find_via_name_symbol(*, name: str, symbol: str) -> Dict | None:
    """
    Asynchronously attempts to find an equity by performing a fuzzy match against the provided name and symbol.

    This function searches for potential equity quotes using the given name or symbol, then selects the best match
    based on a combination of fuzzy matching on both the name and symbol. A higher threshold is used to ensure
    accuracy in the match. If a suitable match is found, detailed information about the equity is retrieved.

    Args:
        name (str): The expected name of the equity to search for.
        symbol (str): The expected symbol (ticker) of the equity to search for.

    Returns:
        Dict | None: A dictionary containing detailed information about the matched equity if found,
        otherwise None.
    """
    quotes = await _search_quotes(name or symbol)
    if not quotes:
        return None

    chosen = _pick_best_symbol(
        quotes,
        name_key="shortname",
        expected_name=name,
        expected_symbol=symbol,
        min_score=150,  # set a higher threshold for fuzzy matching on name + symbol
    )
    if not chosen:
        return None

    logger.info("Fuzzy(name=%r, symbol=%r) → %s", name, symbol, chosen)
    return await _get_info(chosen)


def _pick_best_symbol(
    quotes: List[Dict],
    *,
    name_key: str,
    expected_name: str,
    expected_symbol: str,
    min_score: int = 0,
) -> str | None:
    """
    Select the best matching symbol from a list of quotes using fuzzy matching.

    Calculates a combined fuzzy score for each quote based on symbol and name similarity.
    Returns the symbol with the highest score if it meets or exceeds min_score, else None.

    Args:
        quotes (List[Dict]): List of quote dicts with symbol and name.
        name_key (str): Key for the name field in each quote.
        expected_name (str): Name to match.
        expected_symbol (str): Symbol to match.
        min_score (int, optional): Minimum score to accept a match.

    Returns:
        str | None: Best-matching symbol, or None.
    """
    scored = [
        (
            fuzz.ratio(quote.get("symbol", ""), expected_symbol)
            + fuzz.token_sort_ratio(quote.get(name_key, ""), expected_name),
            quote["symbol"],
        )
        for quote in quotes
    ]

    # compute the best score and symbol from the scored list
    best_score, best_symbol = max(scored, key=lambda t: t[0])

    # if the best score is below the minimum threshold, return None
    if best_score < min_score:
        logger.info(
            "Best fuzzy score %d below threshold %d for %s / %s",
            best_score,
            min_score,
            expected_name,
            expected_symbol,
        )
        return None

    # otherwise, return the best symbol found
    return best_symbol


async def _search_quotes(query: str) -> List[Dict]:
    """
    Asynchronously searches Yahoo Finance for equities matching the given query string.

    This function sends an HTTP GET request to Yahoo Finance's search API endpoint with the provided query.
    It uses a custom User-Agent header to mimic a browser and sets a timeout for the request.
    The response is expected to be JSON and should contain a "quotes" field, which is a list of quote dictionaries.
    Only quotes where the "quoteType" is "EQUITY" are returned.

    Args:
        query (str): The search query string, typically a symbol, name, or ISIN.

    Returns:
        List[Dict]: A list of quote dictionaries for equities matching the query.
    """
    url = "https://query1.finance.yahoo.com/v1/finance/search"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/115.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json",
    }
    async with httpx.AsyncClient(headers=headers, timeout=5.0) as client:
        response = await client.get(url, params={"q": query})
        response.raise_for_status()
        raw_data = response.json().get("quotes", [])

    # Filter out non-equity quotes
    return [quote for quote in raw_data if quote.get("quoteType") == "EQUITY"]


async def _get_info(ticker: str) -> Dict:
    """
    Asynchronously retrieves information for a given equity ticker using yfinance, with error handling.

    This function wraps the `yf.Ticker(ticker).info` call in a thread to avoid blocking the event loop.
    It logs HTTP errors at the debug level and any other unexpected exceptions at the warning level.
    If an error occurs or no information is found, an empty dictionary is returned.

    Args:
        ticker (str): The equity ticker symbol to fetch information for.

    Returns:
        Dict: A dictionary containing the ticker's information, or an empty dictionary if an error occurs.
    """
    try:
        return await asyncio.to_thread(lambda: yf.Ticker(ticker).info or {})

    except httpx.HTTPError as exc:
        logger.debug("HTTP error for %s → %s", ticker, exc, exc_info=False)

    except Exception as exc:
        logger.warning("Unexpected error for %s → %s", ticker, exc, exc_info=False)

    return {}
