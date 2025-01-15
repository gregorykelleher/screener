# yfinance/yfinance.py

import asyncio
import logging

import httpx
import yfinance as yf
from rapidfuzz import fuzz

from equity_aggregator.adapters.data_sources._cache import (
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
) -> dict:
    """
    Retrieve equity information from Yahoo Finance for a given symbol and name, with
    optional caching.

    This function attempts to fetch detailed equity data using ISIN, CUSIP, or a
    fuzzy match on the provided name and symbol.

    Args:
        symbol (str): The equity symbol used as the cache key (e.g., "AAPL").
        name (str): The name of the equity.
        isin (str | None, optional): The International Securities Identification Number.
        cusip (str | None, optional): The Committee on Uniform Securities Identification
            Procedures number.

    Returns:
        dict: A dictionary containing the equity information as returned by
            Yahoo Finance. Returns an empty dictionary if no data is found.

    Notes:
        - The cache key is always the incoming `symbol`, not discovered Yahoo symbol.
        - Cached entries are valid for 1,440 minutes (24 hours).
        - On cache hit, logs the event and returns cached data.
    """

    cached = load_cache_entry("yfinance_equities", symbol)
    if cached:
        logger.debug("Loading symbol %s from cache", symbol)
        return cached

    logger.debug(
        "Fetching yfinance data for symbol=%s, name=%s, isin=%s, cusip=%s",
        symbol,
        name,
        isin,
        cusip,
    )

    # fetch data from Yahoo Finance
    data = await _fetch_yf_equity_data(
        isin=isin,
        cusip=cusip,
        name=name,
        symbol=symbol,
    )

    if data:
        logger.debug("Saving yfinance data to cache for symbol %s", symbol)
        save_cache_entry("yfinance_equities", symbol, data)
    else:
        logger.warning(
            "No data found for symbol %s (name=%s, isin=%s, cusip=%s)",
            symbol,
            name,
            isin,
            cusip,
        )
    return data


async def _fetch_yf_equity_data(
    *,
    isin: str | None = None,
    cusip: str | None = None,
    name: str,
    symbol: str,
) -> dict:
    """
    Asynchronously fetch detailed equity data from Yahoo Finance using ISIN, CUSIP,
    name, and symbol.

    The function attempts to retrieve equity information in the following order:
        1. If ISIN is provided, search for equity using ISIN and return the best match.
        2. If CUSIP is provided (and ISIN did not yield results), search using CUSIP.
        3. If neither identifier yields results, perform a fuzzy search using provided
           name and symbol.
        4. If no data is found by any method, log a warning and return empty dictionary.

    Args:
        isin (str | None): The International Securities Identification Number of equity.
        cusip (str | None): The Committee on Uniform Securities Identification
            Procedures number.
        name (str): The name of the equity.
        symbol (str): The ticker symbol of the equity.

    Returns:
        dict: A dictionary containing equity data if found, otherwise empty dictionary.
    """
    fetchers = [
        (lambda: _find_via_isin(isin, expected_name=name, expected_symbol=symbol))
        if isin
        else None,
        (lambda: _find_via_cusip(cusip, expected_name=name, expected_symbol=symbol))
        if cusip
        else None,
        lambda: _find_via_name_symbol(name=name, symbol=symbol),
    ]

    valid_fetchers = [f for f in fetchers if f]

    for fetch in valid_fetchers:
        result = await fetch()
        if not result:
            logger.debug(
                "fetcher %r returned no result for symbol=%s, name=%s",
                fetch,
                symbol,
                name,
            )
            continue

        _log_discovered_symbol(result, symbol)
        return result

    logger.warning("yfinance returned no data for symbol=%s, name=%s", symbol, name)
    return {}


def _log_discovered_symbol(result: dict, requested_symbol: str) -> None:
    """
    Log when the discovered Yahoo Finance symbol differs from the requested symbol.

    Args:
        result (dict): The dictionary containing the discovered equity data, expected
            to include a "symbol" key.
        requested_symbol (str): The symbol originally requested by the user.

    Returns:
        None
    """
    discovered = result.get("symbol")
    if discovered:
        logger.debug(
            "Discovered yfinance symbol %s for requested symbol %s",
            discovered,
            requested_symbol,
        )


async def _find_via_isin(
    isin: str,
    *,
    expected_name: str,
    expected_symbol: str,
) -> dict | None:
    """
    Asynchronously retrieve equity data using the provided ISIN.

    This function searches Yahoo Finance for an equity matching the given ISIN.
    It validates candidates using fuzzy matching against the expected name and symbol,
    and returns detailed equity information if a suitable match is found.

    Args:
        isin (str): The ISIN identifier for the equity to search for.
        expected_name (str): The expected name of the equity for fuzzy validation.
        expected_symbol (str): The expected symbol of the equity for fuzzy validation.

    Returns:
        dict | None: A dictionary with detailed equity data if found, otherwise None.
    """
    return await _find_by_identifier(
        identifier=isin,
        expected_name=expected_name,
        expected_symbol=expected_symbol,
    )


async def _find_via_cusip(
    cusip: str,
    *,
    expected_name: str,
    expected_symbol: str,
) -> dict | None:
    """
    Asynchronously retrieve equity data using the provided CUSIP identifier.

    This function searches Yahoo Finance for an equity matching the given CUSIP.
    It validates candidates using fuzzy matching against the expected name and symbol,
    and returns detailed equity information if a suitable match is found.

    Args:
        cusip (str): The CUSIP identifier for the equity to search for.
        expected_name (str): The expected name of the equity for fuzzy validation.
        expected_symbol (str): The expected symbol of the equity for fuzzy validation.

    Returns:
        dict | None: A dictionary with detailed equity data if found, otherwise None.
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
) -> dict | None:
    """
    Look up equity data using an identifier (ISIN or CUSIP) with fuzzy validation.

    This function searches Yahoo Finance for equities matching the given identifier,
    then selects the best candidate using fuzzy matching against the expected name and
    symbol. If a suitable match is found, detailed equity information is returned.

    Args:
        identifier (str): The identifier value to search for (ISIN or CUSIP).
        expected_name (str): The expected equity name for fuzzy matching.
        expected_symbol (str): The expected equity symbol for fuzzy matching.

    Returns:
        dict | None: Detailed equity data if a match is found, otherwise None.
    """
    quotes = await _search_quotes(identifier)

    # filter quotes to those that have both 'symbol' and 'longname'
    viable = [
        quote for quote in quotes if quote.get("symbol") and quote.get("longname")
    ]

    if not viable:
        logger.debug("No viable quotes found for identifier %s", identifier)
        return None

    # if only one viable candidate, use it directly
    if len(viable) == 1:
        chosen = viable[0]["symbol"]
        logger.debug(
            "Single viable candidate found for identifier %s: %s",
            identifier,
            chosen,
        )
    else:
        chosen = _pick_best_symbol(
            viable,
            name_key="longname",
            expected_name=expected_name,
            expected_symbol=expected_symbol,
        )
        if not chosen:
            logger.debug(
                "No suitable candidate found after fuzzy matching for identifier %s",
                identifier,
            )
            return None

    return await _get_info(chosen)


async def _find_via_name_symbol(
    *,
    name: str,
    symbol: str,
) -> dict | None:
    """
    Asynchronously search for an equity by fuzzy matching the provided name and symbol.

    This function queries Yahoo Finance for potential equity quotes using the given name
    or symbol. It then selects the best match based on a combined fuzzy score of both
    the name and symbol, using a higher threshold to ensure accuracy. If a suitable
    match is found, detailed information about the equity is retrieved.

    Args:
        name (str): The expected name of the equity to search for.
        symbol (str): The expected ticker symbol of the equity to search for.

    Returns:
        dict | None: Information about the matched equity if found, otherwise None.
    """
    quotes = await _search_quotes(name or symbol)
    if not quotes:
        logger.debug("No quotes found for name=%s or symbol=%s", name, symbol)
        return None

    chosen = _pick_best_symbol(
        quotes,
        name_key="shortname",
        expected_name=name,
        expected_symbol=symbol,
        min_score=150,  # set a higher threshold for fuzzy matching on name + symbol
    )
    if not chosen:
        logger.debug(
            "No suitable candidate found after fuzzy matching for name=%s, symbol=%s",
            name,
            symbol,
        )
        return None

    logger.info(
        "Discovered %s symbol to %s using fuzzy-matching",
        symbol,
        chosen,
    )

    logger.debug("Fuzzy(name=%r, symbol=%r) → %s", name, symbol, chosen)
    return await _get_info(chosen)


def _pick_best_symbol(
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
    quote's name (using `name_key`) and the expected name. The quote with the highest
    combined score is selected if its score meets or exceeds `min_score`. If no quote
    meets the threshold, None is returned.

    Args:
        quotes (list[dict]): List of quote dictionaries, each with at least a "symbol"
            key and a name field specified by `name_key`.
        name_key (str): The key in each quote dict for equity name (e.g., "longname").
        expected_name (str): The expected equity name to match against.
        expected_symbol (str): The expected ticker symbol to match against.
        min_score (int, optional): Minimum combined fuzzy score required to accept a
            match. Defaults to 0.

    Returns:
        str | None: Best-matching symbol if a suitable match is found, otherwise None.
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
    logger.debug(
        "Best fuzzy score for %s/%s: %d (symbol: %s)",
        expected_name,
        expected_symbol,
        best_score,
        best_symbol,
    )

    # if the best score is below the minimum threshold, return None
    if best_score < min_score:
        logger.debug(
            "Best fuzzy score %d below threshold %d for %s / %s",
            best_score,
            min_score,
            expected_name,
            expected_symbol,
        )
        return None

    # otherwise, return the best symbol found
    return best_symbol


async def _search_quotes(query: str) -> list[dict]:
    """
    Asynchronously searches Yahoo Finance for equities matching the given query string.

    This function sends an HTTP GET request to Yahoo Finance's search API endpoint with
    the provided query. It uses a custom User-Agent header to mimic a browser and sets a
    timeout for the request. The response is expected to be JSON and should contain a
    "quotes" field, which is a list of quote dictionaries. Only quotes where the
    "quoteType" is "EQUITY" are returned.

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
    logger.debug("Searching yfinance for query: %s", query)
    async with httpx.AsyncClient(headers=headers, timeout=5.0) as client:
        try:
            response = await client.get(url, params={"q": query})
            response.raise_for_status()
            raw_data = response.json().get("quotes", [])
        except httpx.HTTPError as exc:
            logger.error(
                "HTTP error during yfinance search for query %s: %s",
                query,
                exc,
            )
            return []
        except Exception as exc:
            logger.error(
                "Unexpected error during yfinance search for query %s: %s",
                query,
                exc,
            )
            return []

    # Filter out non-equity quotes
    return [quote for quote in raw_data if quote.get("quoteType") == "EQUITY"]


async def _get_info(ticker: str) -> dict:
    """
    Asynchronously retrieve detailed information for given equity ticker using yfinance.

    This function wraps the `yf.Ticker(ticker).info` call in a thread to avoid blocking
    the event loop. It handles HTTP errors and unexpected exceptions gracefully, logging
    them at the appropriate level. If an error occurs or no information is found, an
    empty dictionary is returned.

    Args:
        ticker (str): The equity ticker symbol to fetch information for.

    Returns:
        dict: A dictionary containing the ticker's information, or an empty dictionary
            if an error occurs or no data is found.
    """
    try:
        logger.debug("Fetching info for ticker: %s", ticker)
        return await asyncio.to_thread(lambda: yf.Ticker(ticker).info or {})
    except httpx.HTTPError as exc:
        logger.error("HTTP error for %s → %s", ticker, exc, exc_info=False)
    except Exception as exc:
        logger.warning("Unexpected error for %s → %s", ticker, exc, exc_info=False)
    return {}
