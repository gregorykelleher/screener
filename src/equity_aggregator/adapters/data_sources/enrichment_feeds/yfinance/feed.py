# yfinance/feed.py

import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from itertools import filterfalse

from equity_aggregator.schemas import YFinanceFeedData
from equity_aggregator.storage import (
    load_cache_entry,
    save_cache_entry,
)

from .api import (
    get_quote_summary,
    search_quotes,
)
from .config import FeedConfig
from .session import YFSession
from .utils import pick_best_symbol

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
        YFinanceFeed: An initialised feed with an active session.
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

    # Data model associated with the Yahoo Finance feed
    model = YFinanceFeedData

    # Minimum fuzzy matching score
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
    ) -> dict | None:
        """
        Fetch enriched equity data using symbol, name, ISIN, or CUSIP.

        The method performs the following steps:
          1. Checks for a cached entry for the given symbol and returns it if found.
          2. Attempts an exact lookup using ISIN and CUSIP, if provided.
          3. Falls back to a fuzzy search using the name or symbol.
          4. Raises LookupError if no data is found from any source.

        Args:
            symbol (str): Ticker symbol of the equity.
            name (str): Full name of the equity.
            isin (str | None): ISIN identifier, if available.
            cusip (str | None): CUSIP identifier, if available.

        Returns:
            dict | None: Enriched equity data if found, otherwise None.

        Raises:
            LookupError: If no matching equity data is found.
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
        lookups.append((self._try_name_or_symbol, name or symbol))

        for fn, arg in lookups:
            try:
                data = await fn(arg, name, symbol)
            except LookupError:
                continue
            if data:
                save_cache_entry("yfinance_equities", symbol, data)
                return data

        raise LookupError("Quote Summary endpoint returned nothing.")

    async def _try_identifier(
        self,
        identifier: str,
        expected_name: str,
        expected_symbol: str,
    ) -> dict | None:
        """
        Attempt to fetch equity data from Yahoo Finance using an ISIN or CUSIP.

        This method:
          1. Searches Yahoo Finance for quotes matching the identifier.
          2. Filters results to those with both a symbol and a name.
          3. Selects the best candidate using fuzzy matching.
          4. Retrieves detailed quote summary data for the chosen symbol.

        Args:
            identifier (str): The ISIN or CUSIP to search for.
            expected_name (str): The expected company or equity name.
            expected_symbol (str): The expected ticker symbol.

        Returns:
            dict | None: Detailed equity data if a suitable match is found, else None.

        Raises:
            LookupError: If no valid candidate is found or quote summary is unavailable.
        """
        quotes = await search_quotes(self._session, identifier)

        if not quotes:
            raise LookupError("Quote Search endpoint returned nothing.")

        viable = _filter_equities(quotes)

        if not viable:
            raise LookupError("No viable candidates found.")

        chosen = _choose_symbol(
            viable,
            expected_name=expected_name,
            expected_symbol=expected_symbol,
            min_score=self._min_score,
        )

        if not chosen:
            raise LookupError("Low Fuzzy Score.")

        info = await get_quote_summary(
            self._session,
            chosen,
            modules=self._config.modules,
        )

        if info is None:
            raise LookupError("Quote Summary endpoint returned nothing.")

        return info

    async def _try_name_or_symbol(
        self,
        query: str,
        expected_name: str,
        expected_symbol: str,
    ) -> dict | None:
        """
        Attempt to retrieve a quote summary for an equity using a name or symbol query.

        This method searches Yahoo Finance using the provided query string and the
        expected symbol. For each search term, it:
          1. Retrieves quote candidates.
          2. Filters out entries lacking a name or symbol.
          3. Selects the best match using fuzzy logic.
          4. Fetches and returns the detailed quote summary for the chosen symbol.

        Args:
            query (str): Primary search string, typically a company name or symbol.
            expected_name (str): Expected equity name for fuzzy matching.
            expected_symbol (str): Expected ticker symbol for fuzzy matching.

        Returns:
            dict | None: Quote summary dictionary if a suitable match is found,
            otherwise None.

        Raises:
            LookupError: If no suitable candidate is found after all queries.
        """

        searches = tuple(dict.fromkeys((query, expected_symbol)))

        for term in searches:
            # search for quotes
            quotes = await search_quotes(self._session, term)
            if not quotes:
                continue

            # filter out any without name or symbol
            viable = _filter_equities(quotes)
            if not viable:
                continue

            # pick best symbol via fuzzy matching
            symbol = _choose_symbol(
                viable,
                expected_name=expected_name,
                expected_symbol=expected_symbol,
                min_score=self._min_score,
            )
            if not symbol:
                continue

            # fetch and return the quote summary
            return await get_quote_summary(
                self._session,
                symbol,
                modules=self._config.modules,
            )

        # Nothing matched
        raise LookupError("No candidate matched.")


def _filter_equities(quotes: list[dict]) -> list[dict]:
    """
    Filter out any quotes lacking a longname or symbol.

    Note:
        The Yahoo Finance search quote query endpoint returns 'longname' and 'shortname'
        fields in lowercase.

    Args:
        quotes (list[dict]): Raw list of quote dicts from Yahoo Finance.

    Returns:
        list[dict]: Only those quotes that have both 'longname' and 'symbol'.
    """
    return [
        quote
        for quote in quotes
        if (quote.get("longname") or quote.get("shortname")) and quote.get("symbol")
    ]


def _choose_symbol(
    viable: list[dict],
    *,
    expected_name: str,
    expected_symbol: str,
    min_score: int,
) -> str | None:
    """
    Select the most appropriate symbol from a list of viable Yahoo Finance quote dicts.

    If only one candidate is present, its symbol is returned. If multiple candidates
    exist, the function attempts to select the best match by comparing the expected
    name and symbol to the 'longname' and 'shortname' fields of each candidate. If
    all candidates share the same name, the first such symbol is returned. Otherwise,
    fuzzy matching is performed using pick_best_symbol, which considers the expected
    name, expected symbol, and a minimum score threshold.

    Args:
        viable (list[dict]): List of filtered Yahoo Finance quote dictionaries.
        expected_name (str): Expected company or equity name for fuzzy matching.
        expected_symbol (str): Expected ticker symbol for fuzzy matching.
        min_score (int): Minimum fuzzy score required to accept a match.

    Returns:
        str | None: The selected symbol if a suitable candidate is found, else None.
    """

    # if there’s only one candidate, return its symbol immediately
    if len(viable) == 1:
        return viable[0]["symbol"]

    def select_best_symbol(name_key: str) -> str | None:
        """
        Selects the best symbol from a list of candidates based on provided name key.

        Examines the specified name field (e.g., 'longname' or 'shortname')
        across all viable candidates. If all candidate names are identical, it returns
        the corresponding symbol. Otherwise, it applies fuzzy matching against the
        expected name or symbol to determine the best match.

        Args:
            name_key (str): The key in each candidate dict to use for name comparison
                (e.g., 'longname' or 'shortname').

        Returns:
            str | None: Selected symbol if suitable candidate is found, otherwise None.
        """

        # gather all names under the given key
        candidate_names = [quote[name_key] for quote in viable if quote.get(name_key)]

        if not candidate_names:
            return None

        # all names identical → pick first matching symbol
        if len({*candidate_names}) == 1:
            return next(quote["symbol"] for quote in viable if quote.get(name_key))

        # otherwise perform fuzzy matching
        return pick_best_symbol(
            viable,
            name_key=name_key,
            expected_name=expected_name,
            expected_symbol=expected_symbol,
            min_score=min_score,
        )

    # try 'longname' then 'shortname', return first non-None result
    return next(
        filterfalse(
            lambda x: x is None,
            map(select_best_symbol, ("longname", "shortname")),
        ),
        None,
    )
