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


def _filter_equities(quotes: list[dict]) -> list[dict]:
    """
    Filter out any quotes lacking a symbol or longname.

    Args:
        quotes (list[dict]): Raw list of quote dicts from Yahoo Finance.

    Returns:
        list[dict]: Only those quotes that have both 'symbol' and 'longname'.
    """
    return [q for q in quotes if q.get("symbol") and q.get("longname")]


def _choose_symbol(
    viable: list[dict],
    *,
    name_key: str,
    expected_name: str,
    expected_symbol: str,
    min_score: int,
) -> str | None:
    """
    Select the best symbol from a list of filtered quotes.

    1. If there is exactly one candidate, return it.
    2. If multiple candidates share the same name_key value, return the first.
    3. Otherwise perform fuzzy matching with pick_best_symbol.

    Args:
        viable (list[dict]): Filtered list of quote dicts.
        name_key (str): Field name to use for equity name comparison ('longname' or 'shortname').
        expected_name (str): The expected equity name.
        expected_symbol (str): The expected equity ticker symbol.
        min_score (int): Minimum combined fuzzy score to accept a match.

    Returns:
        str | None: The chosen symbol, or None if no candidate meets the threshold.
    """
    if len(viable) == 1:
        return viable[0]["symbol"]

    names = {q[name_key] for q in viable}
    if len(names) == 1:
        return viable[0]["symbol"]

    return pick_best_symbol(
        viable,
        name_key=name_key,
        expected_name=expected_name,
        expected_symbol=expected_symbol,
        min_score=min_score,
    )


@asynccontextmanager
async def open_yfinance_feed(
    *,
    config: FeedConfig | None = None,
) -> AsyncIterator["YFinanceFeed"]:
    """
    Context manager to create and close a YFinanceFeed instance.

    Args:
        config (FeedConfig | None, optional): Custom feed configuration; defaults to default FeedConfig.

    Yields:
        YFinanceFeed: An initialized feed with an active session.
    """
    config = config or FeedConfig()
    session = YFSession(config)
    try:
        yield YFinanceFeed(session, config)
    finally:
        await session.aclose()


class YFinanceFeed:
    """
    Asynchronous Yahoo Finance feed with caching and fuzzy lookup.

    Provides fetch_equity() to retrieve enriched equity data by symbol, name, ISIN or CUSIP.

    Attributes:
        _session (YFSession): HTTP session for Yahoo Finance.
        _config (FeedConfig): Endpoints and modules configuration.
        _min_score (int): Minimum fuzzy score threshold.
    """

    __slots__ = ("_session", "_config")
    model = YFinanceFeedData
    _min_score = 150

    def __init__(self, session: YFSession, config: FeedConfig | None = None) -> None:
        """
        Initialise with an active YFSession and optional custom FeedConfig.

        Args:
            session (YFSession): The Yahoo Finance HTTP session.
            config (FeedConfig | None, optional): Feed configuration; defaults to session.config.
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
        Retrieve equity data by symbol, using cache, identifiers, or fuzzy lookup.

        Steps:
        1. Return cached entry if present.
        2. Try ISIN then CUSIP exact search via _try_identifier.
        3. Fallback to fuzzy name/symbol search via _try_name_symbol.
        4. Warn and return empty dict if no data found.

        Args:
            symbol (str): Requested ticker symbol.
            name (str): Expected company name.
            isin (str | None): Optional ISIN identifier.
            cusip (str | None): Optional CUSIP identifier.

        Returns:
            dict: Enriched equity data, or empty dict if not found.
        """
        # 1) cache lookup
        if record := load_cache_entry("yfinance_equities", symbol):
            logger.debug("Loading symbol %s from cache", symbol)
            return record

        logger.debug(
            "Fetching Yahoo Finance data for symbol=%s, name=%s, isin=%s, cusip=%s",
            symbol,
            name,
            isin,
            cusip,
        )

        # 2) try identifiers
        for identifier in (isin, cusip):
            if not identifier:
                continue
            if result := await self._try_identifier(identifier, name, symbol):
                return self._log_and_cache_record(symbol, result)

        # 3) fallback to name/symbol fuzzy
        if result := await self._try_name_symbol(name or symbol, name, symbol):
            return self._log_and_cache_record(symbol, result)

        # 4) nothing found
        logger.warning(
            "No data found for name=%r (symbol=%s). "
            "Tried ISIN=%r, CUSIP=%r, and name/symbol lookup.",
            name,
            symbol,
            isin,
            cusip,
        )
        return {}

    async def _try_identifier(
        self,
        identifier: str,
        expected_name: str,
        expected_symbol: str,
    ) -> dict | None:
        """
        Search Yahoo Finance by ISIN or CUSIP identifier and fetch full info.

        1. Query API with identifier.
        2. Filter quotes to those with symbol+longname.
        3. Choose best symbol via _choose_symbol.
        4. Fetch detailed data if match found.

        Args:
            identifier (str): ISIN or CUSIP string.
            expected_name (str): Expected company name.
            expected_symbol (str): Expected ticker symbol.

        Returns:
            dict | None: Fetched equity data or None if no valid candidate.
        """
        logger.debug(
            "Searching Yahoo Finance by identifier=%s "
            "(expected symbol=%s, expected_name=%s)",
            identifier,
            expected_symbol,
            expected_name,
        )
        quotes = await search_quotes(self._session, identifier)
        viable = _filter_equities(quotes)
        if not viable:
            logger.debug(
                "No viable identifier results for identifier=%s (expected symbol=%s)",
                identifier,
                expected_symbol,
            )
            return None

        chosen = _choose_symbol(
            viable,
            name_key="longname",
            expected_name=expected_name,
            expected_symbol=expected_symbol,
            min_score=self._min_score,
        )
        if not chosen:
            logger.debug(
                "Identifier fuzzy-match failed for identifier=%s (threshold=%d)",
                identifier,
                self._min_score,
            )
            return None

        logger.debug(
            "Identifier resolved identifier=%s → symbol=%s",
            identifier,
            chosen,
        )
        return await get_info(
            self._session,
            chosen,
            modules=self._config.modules,
        )

    async def _try_name_symbol(
        self,
        query: str,
        expected_name: str,
        expected_symbol: str,
    ) -> dict | None:
        """
        Perform fuzzy search by company name or symbol and fetch full info.

        1. Query API with name or symbol.
        2. Filter quotes to those with symbol+longname.
        3. Choose best symbol via _choose_symbol (using 'shortname').
        4. Fetch detailed data if match found.

        Args:
            query (str): Company name or ticker symbol to search.
            expected_name (str): Expected company name.
            expected_symbol (str): Expected ticker symbol.

        Returns:
            dict | None: Fetched equity data or None if no valid candidate.
        """
        logger.debug(
            "Searching Yahoo Finance by name/symbol=%s (expected symbol=%s)",
            query,
            expected_symbol,
        )
        quotes = await search_quotes(self._session, query)
        viable = _filter_equities(quotes)
        if not viable:
            logger.warning(
                "No viable name/symbol results for %r (expected symbol=%r)",
                query,
                expected_symbol,
            )
            return None

        chosen = _choose_symbol(
            viable,
            name_key="shortname",
            expected_name=expected_name,
            expected_symbol=expected_symbol,
            min_score=self._min_score,
        )
        if not chosen:
            logger.debug(
                "Name/symbol fuzzy-match failed for %s (threshold=%d)",
                query,
                self._min_score,
            )
            return None

        logger.debug(
            "Name/symbol resolved query=%s → symbol=%s",
            query,
            chosen,
        )

        return await get_info(
            self._session,
            chosen,
            modules=self._config.modules,
        )

    def _log_and_cache_record(self, requested_symbol: str, record: dict) -> dict:
        """
        Log discovery, save to cache, and return the fetched record.

        Args:
            requested_symbol (str): The symbol originally requested.
            record (dict): The fetched equity data.

        Returns:
            dict: The same record, after logging and caching.
        """
        log_discovered_symbol(record, requested_symbol)
        save_cache_entry("yfinance_equities", requested_symbol, record)
        return record
