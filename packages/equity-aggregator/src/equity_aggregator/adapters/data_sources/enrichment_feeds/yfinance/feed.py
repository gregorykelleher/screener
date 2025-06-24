# yfinance/feed.py

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from equity_aggregator.adapters.data_sources._cache import (
    load_cache_entry,
    save_cache_entry,
)
from equity_aggregator.schemas import YFinanceFeedData

from .api import (
    get_info,
    log_discovered_symbol,
    pick_best_symbol,
    search_quotes,
)
from .config import FeedConfig
from .session import YFSession

logger = logging.getLogger(__name__)


@asynccontextmanager
async def open_yfinance_feed(
    *,
    config: FeedConfig | None = None,
) -> AsyncIterator["YFinanceFeed"]:
    """
    Asynchronous context manager for creating and managing a YFinanceFeed instance.

    This function initialises a YFSession and yields a YFinanceFeed object for use
    within an async context. The session is automatically closed when the context
    exits, ensuring proper resource cleanup.

    Args:
        config (FeedConfig | None, optional): Optional configuration for Yahoo Finance
            endpoints and modules. If not provided, the default FeedConfig is used.

    Yields:
        YFinanceFeed: An instance of YFinanceFeed with an active session.

    Returns:
        AsyncIterator[YFinanceFeed]: An async iterator yielding a YFinanceFeed object.
    """
    config = config or FeedConfig()
    session = YFSession(config)
    try:
        yield YFinanceFeed(session, config)
    finally:
        await session.aclose()


class YFinanceFeed:
    """
    Asynchronous Yahoo Finance equity data fetcher with caching support.

    Provides methods to retrieve detailed equity information from Yahoo Finance
    using symbol, name, ISIN, or CUSIP. Results are cached for efficiency.

    Args:
        session (YFSession): The Yahoo Finance session instance.
        config (FeedConfig | None, optional): Optional configuration for Yahoo
            Finance endpoints and modules. If not provided, uses session config.

    Attributes:
        model (YFinanceFeedData): Validation model for Yahoo Finance fetched data.

    Returns:
        None
    """

    __slots__ = ("_session", "_config")

    model = YFinanceFeedData

    def __init__(
        self,
        session: YFSession,
        config: FeedConfig | None = None,
    ) -> None:
        """
        Initialise a YFinanceFeed instance.

        Args:
            session (YFSession): The Yahoo Finance session instance.
            config (FeedConfig | None, optional): Optional configuration for Yahoo
                Finance endpoints and modules. If not provided, uses session config.

        Returns:
            None
        """
        self._session = session
        self._config = config or session.config

    async def fetch_equity(
        self,
        *,
        symbol: str,
        name: str,
        isin: str | None = None,
        cusip: str | None = None,
    ) -> dict:
        """
        Fetch equity data for a given symbol from Yahoo Finance, using cache if
        available.

        If the data for the specified symbol is present in the cache, it is returned
        immediately. Otherwise, fetches the data from Yahoo Finance, stores it in the
        cache, and returns the result.

        Args:
            symbol (str): The ticker symbol of the equity to fetch.
            name (str): The name of the equity.
            isin (str | None, optional): The ISIN of the equity. Defaults to None.
            cusip (str | None, optional): The CUSIP of the equity. Defaults to None.

        Returns:
            dict: A dictionary containing the fetched equity data, or None if not found.
        """
        cached = load_cache_entry("yfinance_equities", symbol)
        if cached:
            logger.debug("Loading symbol %s from cache", symbol)
            return cached

        logger.debug(
            "Fetching Yahoo Finance data for symbol=%s, name=%s, isin=%s, cusip=%s",
            symbol,
            name,
            isin,
            cusip,
        )

        yf_record = await self._retrieve_yf_equity_data(
            symbol=symbol,
            name=name,
            isin=isin,
            cusip=cusip,
        )

        if yf_record:
            logger.debug("Saving Yahoo Finance data to cache for symbol %s", symbol)
            save_cache_entry("yfinance_equities", symbol, yf_record)
        else:
            logger.warning(
                "No data found for symbol %s (name=%s, isin=%s, cusip=%s)",
                symbol,
                name,
                isin,
                cusip,
            )
        return yf_record

    async def _retrieve_yf_equity_data(
        self,
        name: str,
        symbol: str,
        isin: str | None = None,
        cusip: str | None = None,
    ) -> dict:
        """
        Asynchronously fetch detailed equity data from Yahoo Finance using ISIN,
        CUSIP, name, and symbol.

        Attempts to retrieve equity information in the following order:
            1. If ISIN is provided, search for equity using ISIN and return best match.
            2. If CUSIP is provided (and ISIN didn't yield results), search using CUSIP.
            3. If neither identifier yields results, perform fuzzy search using provided
                name and symbol.
            4. If no data found by any method, log warning and return empty dictionary.

        Args:
            name (str): The name of the equity.
            symbol (str): The ticker symbol of the equity.
            isin (str | None, optional): The ISIN of the equity. Defaults to None.
            cusip (str | None, optional): The CUSIP of the equity. Defaults to None.

        Returns:
            dict: Dictionary containing equity data if found, else empty dictionary.
        """
        search_methods: list[tuple[str | None, dict | None]] = [
            (isin, None),
            (cusip, None),
            (None, None),  # placeholder for fuzzy search
        ]

        result: dict = {}

        for identifier, _ in search_methods:
            if identifier:
                result = await self._find_by_identifier(identifier, name, symbol)
            else:
                result = await self._find_via_name_symbol(name, symbol)

            if result:
                log_discovered_symbol(result, symbol)
                break

        if not result:
            logger.warning(
                "Yahoo Finance returned no data for symbol=%s, name=%s",
                symbol,
                name,
            )

        return result

    async def _find_by_identifier(
        self,
        identifier: str,
        expected_name: str,
        expected_symbol: str,
    ) -> dict | None:
        """
        Look up equity data using an identifier (ISIN or CUSIP) with fuzzy validation.

        Searches Yahoo Finance for equities matching the given identifier, then selects
        best candidate using fuzzy matching against the expected name and symbol. If a
        suitable match is found, detailed equity information is returned.

        Args:
            identifier (str): The identifier value to search for (ISIN or CUSIP).
            expected_name (str): The expected equity name for fuzzy matching.
            expected_symbol (str): The expected equity symbol for fuzzy matching.

        Returns:
            dict | None: Detailed equity data if a match is found, otherwise None.
        """
        quotes = await search_quotes(self._session, identifier)

        viable = [
            quote for quote in quotes if quote.get("symbol") and quote.get("longname")
        ]

        if not viable:
            logger.debug("No viable quotes found for identifier %s", identifier)
            return None

        if len(viable) == 1:
            chosen = viable[0]["symbol"]
            logger.debug(
                "Single viable candidate found for identifier %s: %s",
                identifier,
                chosen,
            )
        else:
            chosen = pick_best_symbol(
                viable,
                name_key="longname",
                expected_name=expected_name,
                expected_symbol=expected_symbol,
                min_score=150,
            )
            if not chosen:
                logger.debug(
                    "No suitable candidate found fuzzy matching for identifier %s",
                    identifier,
                )
                return None

        return await get_info(self._session, chosen, modules=self._config.modules)

    async def _find_via_name_symbol(
        self,
        name: str,
        symbol: str,
    ) -> dict | None:
        """
        Asynchronously search for equity by fuzzy matching the provided name and symbol.

        Queries Yahoo Finance for potential equity quotes using given name or symbol.
        Selects the best match based on a combined fuzzy score of both the name and
        symbol, using a higher threshold to ensure accuracy. If a suitable match is
        found, detailed information about the equity is retrieved.

        Args:
            name (str): The expected name of the equity to search for.
            symbol (str): The expected ticker symbol of the equity to search for.

        Returns:
            dict | None: Information about the matched equity if found, otherwise None.
        """
        quotes = await search_quotes(self._session, name or symbol)

        if not quotes:
            logger.debug("No quotes found for name=%s or symbol=%s", name, symbol)
            return None

        chosen = pick_best_symbol(
            quotes,
            name_key="shortname",
            expected_name=name,
            expected_symbol=symbol,
            min_score=150,
        )
        if not chosen:
            logger.debug(
                "No suitable candidate found fuzzy matching for name=%s, symbol=%s",
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
        return await get_info(self._session, chosen, modules=self._config.modules)
