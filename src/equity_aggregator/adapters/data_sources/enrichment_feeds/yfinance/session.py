# yfinance/session.py

import asyncio
import logging
from collections.abc import Mapping

import httpx

from equity_aggregator.adapters.data_sources._utils import make_client

from .config import FeedConfig
from .utils import backoff_delays

logger: logging.Logger = logging.getLogger(__name__)


class YFSession:
    """
    Asynchronous session for Yahoo Finance JSON endpoints.

    This class manages HTTP requests to Yahoo Finance, handling authentication,
    rate limits, and crumb renewal. It is lightweight and reusable, maintaining
    only a client and session state. Concurrency is limited by a shared
    semaphore to respect Yahoo's HTTP/2 stream restriction.

    Args:
        config (FeedConfig): Immutable feed configuration.
        client (httpx.AsyncClient | None): Optional pre-configured HTTP client.

    Returns:
        None
    """

    __slots__: tuple[str, ...] = ("_client", "_config", "_crumb", "_crumb_lock")

    # Limit HTTP/2 concurrent streams to 1; in effect serialising requests.
    # This is workaround to alleviate reliability issues with Yahoo Finance's endpoints.
    # With this approach, the HTTP/2 protocol is still in use, without having to drop to
    # HTTP/1.1.
    _concurrent_streams: asyncio.Semaphore = asyncio.Semaphore(1)

    def __init__(
        self,
        config: FeedConfig,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        """
        Initialise a new YFSession for Yahoo Finance JSON endpoints.

        Args:
            config (FeedConfig): Immutable feed configuration.
            client (httpx.AsyncClient | None): Optional pre-configured HTTP client.

        Returns:
            None
        """
        self._config: FeedConfig = config
        self._client: httpx.AsyncClient = client or make_client()
        self._crumb: str | None = None
        self._crumb_lock: asyncio.Lock = asyncio.Lock()

    @property
    def config(self) -> FeedConfig:
        """
        Gets the immutable configuration associated with this session.

        Args:
            None

        Returns:
            FeedConfig: The configuration object bound to this session instance.
        """
        return self._config

    async def aclose(self) -> None:
        """
        Asynchronously close the underlying HTTP client.

        Args:
            None

        Returns:
            None
        """
        await self._client.aclose()

    async def get(
        self,
        url: str,
        *,
        params: Mapping[str, str] | None = None,
    ) -> httpx.Response:
        """
        Perform a resilient asynchronous GET request to Yahoo Finance endpoints.

        This method injects the crumb if required, renews it on a single 401
        response, and applies exponential backoff on 429 responses. Concurrency
        is limited to comply with Yahoo's HTTP/2 stream limits.

        Args:
            url (str): Absolute URL to request.
            params (Mapping[str, str] | None): Optional query parameters.

        Returns:
            httpx.Response: The successful HTTP response.
        """
        async with self.__class__._concurrent_streams:
            merged_params: dict[str, str] = self._attach_crumb(url, dict(params or {}))
            return await self._fetch_with_retry(url, merged_params)

    async def _safe_get(
        self,
        url: str,
        params: dict[str, str],
        *,
        retries: int = 3,
    ) -> httpx.Response:
        """
        Perform a GET request with up to 3 retries on HTTP/2 protocol errors.

        If a ProtocolError occurs, the HTTP client is reset and the request
        retried. After 3 failed attempts, the last exception is raised.

        Args:
            url (str): The absolute URL to request.
            params (dict[str, str]): Query parameters for the request.

        Returns:
            httpx.Response: The successful HTTP response.

        Raises:
            httpx.ProtocolError: If all retry attempts fail.
        """
        last_exc: Exception | None = None

        for attempt in range(1, retries + 1):
            try:
                return await self._client.get(url, params=params)
            except httpx.ProtocolError as exc:
                last_exc = exc
                logger.debug(
                    (
                        "HTTP/2 protocol error (%s) on %s - "
                        "recreating client (attempt %d/3)"
                    ),
                    exc,
                    url,
                    attempt,
                )
                await self._reset_client()

        assert last_exc is not None
        raise last_exc

    async def _fetch_with_retry(
        self,
        url: str,
        params: Mapping[str, str],
    ) -> httpx.Response:
        """
        Perform a single GET request, transparently handling 401 and invalid responses.

        If a 401 (Unauthorized) is received, the crumb is renewed and the request
        retried once. If other 4xx or 5xx responses are received, exponential backoff
        is applied and the request is retried up to the configured limit.

        Args:
            url (str): The absolute URL to request.
            params (Mapping[str, str]): Query parameters after crumb injection.

        Returns:
            httpx.Response: The successful HTTP response.

        Raises:
            httpx.HTTPStatusError: If the final response is not successful.
        """
        # make mutable copy
        params = dict(params)

        response = await self._safe_get(url, params)

        # handle 401 Unauthorized
        if response.status_code == httpx.codes.UNAUTHORIZED:
            response = await self._renew_crumb_once(url, params)

        # Retry transient server / rate‑limit responses
        retryable = {
            httpx.codes.TOO_MANY_REQUESTS,  # 429
            httpx.codes.INTERNAL_SERVER_ERROR,  # 500
            httpx.codes.BAD_GATEWAY,  # 502
            httpx.codes.SERVICE_UNAVAILABLE,  # 503
            httpx.codes.GATEWAY_TIMEOUT,  # 504
        }

        if response.status_code in retryable:
            response = await self._get_with_backoff(url, params, response)

        # If the response is still not successful after retries, raise an error
        if response.status_code in retryable:
            raise LookupError(f"HTTP {response.status_code} after retries for {url}")

        return response

    async def _reset_client(self) -> None:
        """
        Reset the HTTP client instance asynchronously.

        Closes the current client and creates a new one. Also clears the crumb
        to ensure session state is refreshed after protocol errors.

        Args:
            None

        Returns:
            None
        """
        self._crumb = None
        await self._client.aclose()
        self._client = make_client()

    async def _renew_crumb_once(
        self,
        url: str,
        params: dict[str, str],
    ) -> httpx.Response:
        """
        Refresh the crumb after a 401 Unauthorized and retry the request.

        This method extracts the ticker from the URL, fetches a new crumb,
        updates the query parameters, and replays the GET request.

        Args:
            url (str): The original request URL.
            params (dict[str, str]): Mutable query parameters.

        Returns:
            httpx.Response: Response after retrying with a fresh crumb.
        """
        ticker: str = self._extract_ticker(url)

        await self._bootstrap_and_fetch_crumb(ticker)

        params["crumb"] = self._crumb

        return await self._client.get(url, params=params)

    async def _get_with_backoff(
        self,
        url: str,
        params: dict[str, str],
        response: httpx.Response,
    ) -> httpx.Response:
        """
        Retry a GET request after receiving any 4xx or 5xx response,
        using exponential backoff.

        Retries up to `max_attempts` times, waiting for delays generated by
        `backoff_delays()`. Each retry uses `_safe_get`, which handles protocol
        errors. If a non-retryable response is received, it is returned immediately.

        Args:
            url (str): The absolute URL to request.
            params (dict[str, str]): Query parameters for the request.
            response (httpx.Response): The initial 429 response.

        Returns:
            httpx.Response: The successful HTTP response or the last response
                after all retries.
        """
        max_attempts = 5

        retryable = {
            httpx.codes.TOO_MANY_REQUESTS,  # 429
            httpx.codes.INTERNAL_SERVER_ERROR,  # 500
            httpx.codes.BAD_GATEWAY,  # 502
            httpx.codes.SERVICE_UNAVAILABLE,  # 503
            httpx.codes.GATEWAY_TIMEOUT,  # 504
        }

        for attempt, delay in enumerate(backoff_delays(attempts=max_attempts), 1):
            if response.status_code not in retryable:
                return response

            logger.debug(
                "%d %s – sleeping %.1fs (attempt %d/%d)",
                response.status_code,
                url,
                delay,
                attempt,
                max_attempts,
            )
            await asyncio.sleep(delay)

            try:
                response = await self._safe_get(url, params)
            except httpx.ProtocolError:
                raise

        return response

    def _attach_crumb(
        self,
        url: str,
        params: dict[str, str],
    ) -> dict[str, str]:
        """
        Inject the anti-CSRF crumb into query parameters if required.

        The crumb is added only for quote-summary endpoint requests when available.
        If the crumb is not set or the URL does not match, the original parameters
        are returned unchanged.

        Args:
            url (str): Target request URL.
            params (dict[str, str]): Query parameters to update.

        Returns:
            dict[str, str]: Updated query parameters with crumb if needed.
        """
        # needs_crumb = self._crumb is not None and url.startswith(
        #     self._config.quote_summary_url,
        # )

        # if not needs_crumb:
        #     return params

        # return {**params, "crumb": self._crumb}
        return params

    def _extract_ticker(self, url: str) -> str:
        """
        Extract the ticker symbol from a Yahoo Finance quote-summary URL.

        Args:
            url (str): The quote-summary endpoint URL.

        Returns:
            str: The ticker symbol found in the URL path.
        """
        remainder: str = url[len(self._config.quote_summary_url) :]

        first_segment: str = remainder.split("/", 1)[0]

        return first_segment.split("?", 1)[0].split("#", 1)[0]

    async def _bootstrap_and_fetch_crumb(self, ticker: str) -> None:
        """
        Initialise session cookies and retrieve the anti-CSRF crumb.

        This method primes the session by making requests to Yahoo Finance endpoints
        using the provided ticker, then fetches the crumb required for authenticated
        requests. The crumb is cached for future use and protected by a lock.

        Args:
            ticker (str): Symbol used to prime the session.

        Returns:
            None
        """
        if self._crumb is not None:
            return

        async with self._crumb_lock:
            if self._crumb is not None:
                return
            seeds: tuple[str, ...] = (
                "https://fc.yahoo.com",
                "https://finance.yahoo.com",
                f"https://finance.yahoo.com/quote/{ticker}",
            )
            for seed in seeds:
                await self._client.get(seed)

            resp: httpx.Response = await self._client.get(self._config.crumb_url)
            resp.raise_for_status()
            self._crumb = resp.text.strip().strip('"')
