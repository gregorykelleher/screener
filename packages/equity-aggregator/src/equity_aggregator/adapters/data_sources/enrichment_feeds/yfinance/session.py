# yfinance/session.py

import asyncio
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
    """

    __slots__ = ("_client", "_config", "_crumb", "_crumb_lock")

    # shared semaphore to cap concurrent streams to maximum limits for HTTP/2
    _stream_semaphore: asyncio.Semaphore = asyncio.Semaphore(100)

    def __init__(
        self,
        config: FeedConfig,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._config = config
        self._client = client or make_client()
        self._crumb: str | None = None
        self._crumb_lock = asyncio.Lock()

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
        Perform an asynchronous GET request with crumb handling and retry logic.

        This method injects the Yahoo Finance crumb token if required, bootstraps
        the session as needed, and retries once on HTTP 401 errors. It also
        enforces a concurrency cap using a shared semaphore to avoid exceeding
        HTTP/2 stream limits.

        Args:
            url (str): The URL to send the GET request to.
            params (Mapping[str, str] | None, optional): Query parameters to include
                in the request. Defaults to None.

        Returns:
            httpx.Response: The HTTP response object from the GET request.
        """
        async with self.__class__._stream_semaphore:
            # merge requested parameters with anti-CSRF token (i.e. crumb)
            merged_params = self._attach_crumb(url, dict(params or {}))

            # perform the GET request with retry logic in case of crumb expiration
            return await self._fetch_with_retry(url, merged_params)

    def _attach_crumb(self, url: str, params: dict[str, str]) -> dict[str, str]:
        """
        Attach the authentication crumb to the request parameters if required.

        This method checks if a crumb value is available and if the provided URL starts
        with the configured quote base URL. If both conditions are met, it adds the
        crumb to the request parameters dictionary.

        Args:
            url (str): The URL to which the request will be sent.
            params (dict[str, str]): The dictionary of request parameters.

        Returns:
            dict[str, str]: The updated dictionary of request parameters, potentially
                including the crumb.
        """
        if self._crumb and url.startswith(self._config.quote_summary_url):
            params["crumb"] = self._crumb
        return params

    async def _fetch_with_retry(
        self,
        url: str,
        params: dict[str, str],
    ) -> httpx.Response:
        """
        Perform a GET request with crumb handling and retry on HTTP 401 errors.

        Attempts to fetch the given URL with the provided parameters. If a 401
        Unauthorized error is encountered, the method will re-bootstrap the session,
        refresh the crumb token, and retry the request once.

        Args:
            url (str): The URL to send the GET request to.
            params (dict[str, str]): Query parameters to include in the request.

        Returns:
            httpx.Response: The HTTP response object from the GET request.

        Raises:
            httpx.HTTPStatusError: If the request fails with a non-401 HTTP error,
                or if the retry also fails.
        """
        try:
            response = await self._client.get(url, params=params)
            response.raise_for_status()
            return response

        except httpx.HTTPStatusError as error:
            if error.response.status_code != httpx.codes.UNAUTHORIZED:
                raise

            ticker = self._extract_ticker(url)

            await self._bootstrap_and_fetch_crumb(ticker)

            params["crumb"] = self._crumb

            response = await self._client.get(url, params=params)
            response.raise_for_status()
            return response

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
        return self._crumb is None and url.startswith(self._config.quote_summary_url)

    def _extract_ticker(self, url: str) -> str:
        """
        Extract the ticker symbol from a Yahoo Finance quote URL.

        Args:
            url (str): The quote URL.

        Returns:
            str: The extracted ticker symbol.
        """
        remainder = url[len(self._config.quote_summary_url) :]
        first_segment = remainder.split("/", 1)[0]

        return first_segment.split("?", 1)[0].split("#", 1)[0]

    async def _bootstrap_and_fetch_crumb(self, ticker: str) -> None:
        """
        Bootstrap and fetch the Yahoo Finance crumb token once per session.

        This method visits key Yahoo Finance domains to initialise session cookies,
        then retrieves the anti-CSRF crumb token required for authenticated API
        requests. The crumb is cached for future use. Thread safety is ensured
        using an async lock.

        Args:
            ticker (str): The ticker symbol used to initialise the session.

        Returns:
            None
        """
        if self._crumb is not None:
            return

        async with self._crumb_lock:
            if self._crumb is not None:
                return

            for seed in (
                "https://fc.yahoo.com",
                "https://finance.yahoo.com",
                f"https://finance.yahoo.com/quote/{ticker}",
            ):
                await self._client.get(seed)

            response = await self._client.get(self._config.crumb_url)
            response.raise_for_status()
            self._crumb = response.text.strip().strip('"')
