# yfinance/session.py

from collections.abc import Mapping

import httpx

from equity_aggregator.adapters.data_sources._utils import make_client

from .config import FeedConfig


class YFSession:
    """
    Async wrapper for httpx.AsyncClient that manages Yahoo Finance crumb tokens.

    Handles Yahoo Finance anti-CSRF crumb tokens required for authenticated API
    calls. Bootstraps the session by visiting key Yahoo domains and fetches the
    crumb token as needed. Automatically injects the crumb into requests to quote
    endpoints.

    Args:
        config (FeedConfig): Yahoo Finance feed configuration.
        client (httpx.AsyncClient | None, optional): Optional HTTP client. If not
            provided, a new client is created.

    Returns:
        None
    """

    __slots__ = ("_client", "_config", "_crumb")

    def __init__(
        self,
        config: FeedConfig,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._config = config
        self._client = client or make_client()
        self._crumb: str | None = None

    @property
    def config(self) -> FeedConfig:
        """
        Gets the configuration for the feed.

        Returns:
            FeedConfig: The configuration object associated with this feed instance.
        """
        return self._config

    async def get(
        self,
        url: str,
        *,
        params: Mapping[str, str] | None = None,
    ) -> httpx.Response:
        """
        Asynchronously perform a GET request, auto-injecting crumb if required.

        Args:
            url (str): The URL to request.
            params (Mapping[str, str] | None, optional): Query parameters for the
                request. Defaults to None.

        Returns:
            httpx.Response: The HTTP response object.
        """
        if self._requires_crumb(url):
            ticker = self._extract_ticker(url)
            await self._bootstrap_and_fetch_crumb(ticker)

        if params is None:
            params = {}
        if self._crumb and url.startswith(self._config.quote_base):
            params = {**params, "crumb": self._crumb}

        return await self._client.get(url, params=params)

    async def aclose(self) -> None:
        """
        Asynchronously close the underlying HTTP client.

        Args:
            None

        Returns:
            None
        """
        await self._client.aclose()

    def _requires_crumb(self, url: str) -> bool:
        """
        Determine if a crumb token is required for the given URL.

        Args:
            url (str): The URL to check.

        Returns:
            bool: True if crumb is needed, False otherwise.
        """
        return self._crumb is None and url.startswith(self._config.quote_base)

    def _extract_ticker(self, url: str) -> str:
        """
        Extract the ticker symbol from a Yahoo Finance quote URL.

        Args:
            url (str): The quote URL.

        Returns:
            str: The extracted ticker symbol.
        """
        remainder = url[len(self._config.quote_base) :]
        first_segment = remainder.split("/", 1)[0]

        return first_segment.split("?", 1)[0].split("#", 1)[0]

    async def _bootstrap_and_fetch_crumb(self, ticker: str) -> None:
        """
        Bootstrap session cookies and fetch the Yahoo Finance crumb token.

        Args:
            ticker (str): The ticker symbol for which to initialise the session.

        Returns:
            None
        """
        for seed in (
            "https://fc.yahoo.com",
            "https://finance.yahoo.com",
            f"https://finance.yahoo.com/quote/{ticker}",
        ):
            await self._client.get(seed)
        resp = await self._client.get(self._config.crumb_url)
        resp.raise_for_status()
        self._crumb = resp.text.strip().strip('"')
