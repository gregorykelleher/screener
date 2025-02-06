# currency_exchange/currency_converter.py

import os
import requests
from functools import cache
from decimal import Decimal
from itertools import starmap


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


def _fetch_conversion_data(url: str) -> dict:
    """
    Fetch conversion data from the API and return the JSON response.
    Raises an error if the API response is not successful.
    """
    response = requests.get(url)
    response.raise_for_status()  # will raise an HTTPError for bad responses
    data = response.json()
    if data.get("result") != "success":
        error_type = data.get("error-type", "Unknown error")
        raise Exception(f"Exchange Rate API error: {error_type}")
    return data.get("conversion_rates", {})


def _convert_rate(key: str, rate: float) -> tuple[str, Decimal]:
    """
    Convert a (key, rate) pair to a (key, Decimal(rate)) pair.
    """
    return key, Decimal(str(rate))


@cache
def retrieve_conversion_rates() -> dict[str, Decimal]:
    """
    Fetch the latest currency conversion rates.

    Returns:
        A dictionary mapping currency codes to their Decimal conversion rates.
    Raises:
        EnvironmentError: If the API key is not set.
        Exception: For API errors.
    """
    api_key = _get_api_key()
    url = _build_url(api_key)
    raw_rates = _fetch_conversion_data(url)

    # Convert the conversion rates to Decimal and return as a dictionary
    return dict(starmap(_convert_rate, raw_rates.items()))
