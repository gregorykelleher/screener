# reference_lookup/exchange_rate_api.py

import logging
import os
import sys
from decimal import Decimal
from itertools import starmap

import httpx

from equity_aggregator.adapters.data_sources._utils import make_client
from equity_aggregator.storage import load_cache, save_cache

logger = logging.getLogger(__name__)


async def retrieve_conversion_rates(
    client: httpx.AsyncClient | None = None,
    *,
    cache_key: str = "exchange_rate_api",
) -> dict[str, Decimal]:
    """
    Fetch the latest currency conversion rates from the ExchangeRateApi.

    Args:
        client (AsyncClient | None): Optional HTTP client to use for requests.
        cache_key (str): The key under which to cache the records.

    Returns:
        dict[str, Decimal]: Mapping of currency codes to their Decimal conversion rates.

    Raises:
        EnvironmentError: If the API key is missing.
        httpx.HTTPError: For network or HTTP status errors.
        Exception: For API-level failures or invalid responses.

    Notes:
        Uses a cache to avoid unnecessary API calls. Cache is refreshed every 24 hours.
    """
    cached = load_cache(cache_key)

    if cached is not None:
        logger.debug("Loaded exchange rates from cache.")
        return cached

    logger.info("Fetching exchange rates from ExchangeRateApi API.")

    # fetch from API and validate
    api_key = _get_api_key()
    url = _build_url(api_key)

    try:
        # obtain an HTTP client if not provided
        client = client or make_client()

        rates = await _get_rates(client, url)

        # persist retrieved conversion rates to cache and return
        save_cache(cache_key, rates)
        logger.info("Saved exchange rates to cache.")
        return rates

    # If any error occurs on the request, treat it as fatal and exit
    except Exception as error:
        logger.fatal(
            "Fatal error while fetching exchange rates: %s",
            error,
            exc_info=True,
        )
        sys.exit(1)


def _get_api_key() -> str:
    """
    Fetches the Exchange Rate API key from the environment variable.

    Raises:
        OSError: If the 'EXCHANGE_RATE_API_KEY' environment variable is not set.

    Returns:
        str: The API key retrieved from the environment.
    """
    key = os.getenv("EXCHANGE_RATE_API_KEY")
    if not key:
        logger.error("EXCHANGE_RATE_API_KEY environment variable is not set.")
        raise OSError("EXCHANGE_RATE_API_KEY environment variable is not set.")
    logger.debug("Exchange Rate API key loaded from environment.")
    return key


def _build_url(api_key: str) -> str:
    """
    Constructs the API endpoint URL for retrieving the latest USD exchange rates.

    Args:
        api_key (str): The API key required to authenticate with the exchange rate API.

    Returns:
        str: The fully formatted URL to access the latest USD exchange rates.
    """
    return f"https://v6.exchangerate-api.com/v6/{api_key}/latest/USD"


async def _get_rates(client: httpx.AsyncClient, url: str) -> dict[str, Decimal]:
    """
    Asynchronously fetches, validates, and converts currency exchange rates.

    Args:
        client (AsyncClient): An asynchronous HTTP client used to make the request.
        url (str): The URL endpoint to fetch exchange rate data from.

    Returns:
        dict[str, Decimal]: A dictionary mapping currency codes to their exchange rates
            as Decimal objects.
    """
    async with client:
        payload = await _fetch_and_validate(client, url)
    return dict(
        starmap(_convert_rate, payload["conversion_rates"].items()),
    )


async def _fetch_and_validate(client: httpx.AsyncClient, url: str) -> dict:
    """
    Fetches and validates FX rates from the given URL using an asynchronous HTTP GET
    request.

    Performs the request, raises an exception for any HTTP or API error, and returns
    the parsed JSON payload if successful.

    Args:
        url (str): The URL to fetch FX rates from.
        client (httpx.AsyncClient): The HTTP client to use for making the request.

    Returns:
        dict: The parsed JSON payload containing FX rates.

    Raises:
        httpx.HTTPStatusError: If the HTTP request returns an unsuccessful status code.
        ValueError: If the API response indicates an error or invalid payload.
    """
    try:
        response = await client.get(url)
        response.raise_for_status()
        payload = response.json()
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error while fetching exchange rates: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error while fetching exchange rates: {e}")
        raise

    logger.debug("Exchange rates response received and parsed.")

    _assert_success(payload)
    return payload


def _convert_rate(key: str, rate: float) -> tuple[str, Decimal]:
    """
    Converts a currency rate value to a Decimal for improved precision.

    Args:
        key (str): The identifier or symbol for the currency pair or rate.
        rate (float): The exchange rate value to be converted.

    Returns:
        tuple[str, Decimal]: A tuple containing the original key and the rate as a
            Decimal object.
    """
    return key, Decimal(str(rate))


def _assert_success(payload: dict) -> None:
    """
    Checks if the API response indicates success; raises an Exception otherwise.

    Args:
        payload (dict): The response payload from the Exchange Rate API. Must contain
            a 'result' key indicating the status, and may contain an 'error-type' key
            describing the error.

    Raises:
        Exception: If the 'result' key is not 'success', raises an Exception with the
            error type from the payload or 'Unknown error' if not provided.

    Returns:
        None
    """
    if payload.get("result") != "success":
        error = payload.get("error-type", "Unknown error")

        logger.error(f"Exchange Rate API error: {error}")

        raise Exception(f"Exchange Rate API error: {error}")
