# adapters/exchange_rate_api.py

import os
import httpx
import logging

from typing import Dict
from decimal import Decimal
from itertools import starmap

from equity_aggregator.adapters.equity_data_sources._cache import load_cache, save_cache

logger = logging.getLogger(__name__)


async def retrieve_conversion_rates() -> Dict[str, Decimal]:
    """
    Fetch the latest currency conversion rates.

    Returns:
        A dictionary mapping currency codes to their Decimal conversion rates.
    Raises:
        EnvironmentError if API key missing;
        httpx.HTTPError for network or status errors;
        Exception for API-level failures.
    """

    # load from cache if available (refresh every 24 hours)
    cached = load_cache("exchange_rate_api", ttl_minutes=1_440)
    if cached is not None:
        logger.info("Loaded exchange rates from cache.")
        return cached

    logger.info("Fetching exchange rates from ExchangeRateApi API")

    # fetch from API and validate
    api_key = _get_api_key()
    url = _build_url(api_key)

    response = await _fetch_and_validate(url)

    # convert the conversion rates to Decimal and return as a dictionary
    rates = dict(starmap(_convert_rate, response["conversion_rates"].items()))

    # persist retrieved conversion rates to cache and return
    save_cache("exchange_rate_api", rates)
    return rates


def _get_api_key() -> str:
    """
    Retrieve the API key from the environment.
    """
    key = os.getenv("EXCHANGE_RATE_API_KEY")
    if not key:
        raise EnvironmentError("EXCHANGE_RATE_API_KEY environment variable is not set.")
    return key


def _build_url(api_key: str) -> str:
    """
    Build the API URL using the provided API key.
    """
    return f"https://v6.exchangerate-api.com/v6/{api_key}/latest/USD"


def _convert_rate(key: str, rate: float) -> tuple[str, Decimal]:
    """
    Convert a (key, rate) pair to a (key, Decimal(rate)) pair.
    """
    return key, Decimal(str(rate))


async def _fetch_and_validate(url: str) -> Dict:
    """
    Perform the HTTP GET for FX rates, raise for any HTTP or API error,
    and return the parsed JSON payload.
    """
    async with httpx.AsyncClient() as client:
        response = await client.get(url, timeout=10.0)
        response.raise_for_status()
        payload = response.json()

    _assert_success(payload)
    return payload


def _assert_success(payload: Dict) -> None:
    """
    Raise if the API indicates a failure.
    """
    if payload.get("result") != "success":
        error = payload.get("error-type", "Unknown error")
        raise Exception(f"Exchange Rate API error: {error}")
