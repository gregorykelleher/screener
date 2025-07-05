# yfinance/feed.py

import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

from equity_aggregator.adapters.data_sources._cache import (
    load_cache_entry,
    save_cache_entry,
)
from equity_aggregator.schemas import YFinanceFeedData

from .api import (
    get_info,
    pick_best_symbol,
    search_quotes,
)
from .config import FeedConfig
from .errors import (
    EmptySummaryError,
    LowFuzzyScoreError,
    NoEquityDataError,
    NoQuotesError,
)
from .session import YFSession

logger = logging.getLogger(__name__)

LookupFn = Callable[..., Awaitable[dict | None]]


@asynccontextmanager
async def open_yfinance_feed(
    *,
    config: FeedConfig | None = None,
) -> AsyncIterator["YFinanceFeed"]:
    """
    Context manager to create and close a YFinanceFeed instance.

    Args:
        config (FeedConfig | None, optional): Custom feed configuration; defaults to
            default FeedConfig.

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

    Provides fetch_equity() to retrieve equity data by symbol, name, ISIN or CUSIP.

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
            config (FeedConfig | None, optional): Feed configuration; defaults to
                session.config.
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

        Steps, in order:
        1. Return cached entry if present.
        2. Try exact lookup via ISIN, then CUSIP.
        3. Fallback to fuzzy name/symbol search.
        4. Return empty dict if nothing found.

        Returns:
            dict: Enriched equity data, or empty dict if not found.
        """
        if record := load_cache_entry("yfinance_equities", symbol):
            return record

        # try identifiers first
        lookups: list[tuple[LookupFn, str]] = [
            (self._try_identifier, identifier)
            for identifier in (isin, cusip)
            if identifier
        ]

        # fallback to fuzzy search
        lookups.append((self._try_name_symbol, name or symbol))

        result: dict | None = {}
        for fn, arg in lookups:
            result = await fn(arg, name, symbol)
            if result:
                break
        else:
            # no break → nothing found
            result = {}

        if result:
            save_cache_entry("yfinance_equities", symbol, result)
            return result

        raise EmptySummaryError(symbol or name)

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
        quotes = await search_quotes(self._session, identifier)

        if not quotes:
            raise NoQuotesError(identifier)

        viable = _filter_equities(quotes)

        if not viable:
            raise NoEquityDataError(identifier)

        chosen = _choose_symbol(
            viable,
            name_key="longname",
            expected_name=expected_name,
            expected_symbol=expected_symbol,
            min_score=self._min_score,
        )

        if not chosen:
            raise LowFuzzyScoreError(identifier)

        info = await get_info(
            self._session,
            chosen,
            modules=self._config.modules,
        )

        if info is None:
            raise EmptySummaryError(chosen)

        return info

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
        3. Choose best symbol via _choose_symbol (using 'longname').
        4. Fetch detailed data if match found.

        Args:
            query (str): Company name or ticker symbol to search.
            expected_name (str): Expected company name.
            expected_symbol (str): Expected ticker symbol.

        Returns:
            dict | None: Fetched equity data or None if no valid candidate.
        """
        quotes = await search_quotes(self._session, query)
        viable = _filter_equities(quotes)

        if not quotes:
            raise NoQuotesError()

        if not viable:
            raise NoEquityDataError()

        chosen = _choose_symbol(
            viable,
            name_key="longname",
            expected_name=expected_name,
            expected_symbol=expected_symbol,
            min_score=self._min_score,
        )

        if not chosen:
            raise LowFuzzyScoreError()

        info = await get_info(
            self._session,
            chosen,
            modules=self._config.modules,
        )

        if info is None:
            raise EmptySummaryError()

        return info


def _filter_equities(quotes: list[dict]) -> list[dict]:
    """
    Filter out any quotes lacking a longname or symbol.

    Args:
        quotes (list[dict]): Raw list of quote dicts from Yahoo Finance.

    Returns:
        list[dict]: Only those quotes that have both 'longname' and 'symbol'.
    """
    return [quote for quote in quotes if quote.get("longname") and quote.get("symbol")]


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
        name_key (str): Field name to use for equity name comparison ('longname').
        expected_name (str): The expected equity name.
        expected_symbol (str): The expected equity ticker symbol.
        min_score (int): Minimum combined fuzzy score to accept a match.

    Returns:
        str | None: The chosen symbol, or None if no candidate meets the threshold.
    """
    if len(viable) == 1:
        return viable[0]["symbol"]

    names = {quote.get(name_key) for quote in viable if quote.get(name_key)}

    if len(names) == 1:
        return viable[0]["symbol"]

    return pick_best_symbol(
        viable,
        name_key=name_key,
        expected_name=expected_name,
        expected_symbol=expected_symbol,
        min_score=min_score,
    )
