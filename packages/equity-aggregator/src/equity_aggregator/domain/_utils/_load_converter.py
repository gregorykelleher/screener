# _utils/_load_converter.py

import logging
from collections.abc import Callable
from decimal import Decimal

from equity_aggregator.adapters import retrieve_conversion_rates
from equity_aggregator.schemas import RawEquity

logger = logging.getLogger(__name__)

type RawEquityConverter = Callable[[RawEquity], RawEquity]


def _build_usd_converter_loader() -> Callable[[], RawEquityConverter]:
    """
    Creates an async loader that fetches FX rates once and returns a USD converter.

    The returned loader asynchronously retrieves and caches FX rates on the first call,
    then returns a callable that converts RawEquity instances to USD using those rates.

    Args:
        None

    Returns:
        Callable[[], EquityConverter]:
            An async function that, when awaited, returns a callable for converting
            RawEquity objects to USD.
    """
    converter_fn: RawEquityConverter | None = None

    async def loader() -> RawEquityConverter:
        nonlocal converter_fn
        if converter_fn is None:
            # first call: retrieve conversion rates and build the converter function
            rates = await retrieve_conversion_rates()
            converter_fn = _build_usd_converter(rates)
        return converter_fn

    return loader


# one-time async USD converter: first await builds & caches, then reuses
get_usd_converter = _build_usd_converter_loader()


def _build_usd_converter(rates: dict[str, Decimal]) -> RawEquityConverter:
    """
    Creates a converter function to transform a RawEquity's price and market cap to USD.

    Args:
        rates (dict[str, Decimal]): A mapping from currency codes to their USD
        conversion rates.

    Returns:
        EquityConverter: A function that takes a RawEquity object and
            returns a new RawEquity with its respective fields converted to USD, unless
            conversion should be skipped.
    """

    def convert(equity: RawEquity) -> RawEquity:
        if _should_skip_conversion(equity):
            return equity

        rate = _get_rate_for_currency(equity.currency, rates)

        update_values = {"currency": "USD", **_build_field_updates(equity, rate)}

        return equity.model_copy(update=update_values)

    return convert


def _should_skip_conversion(equity: RawEquity) -> bool:
    """
    Determines whether the conversion process should be skipped for a given equity.

    The function checks if both the last price and market cap are missing, if the
    currency is not specified, or if the currency is already USD. In any of these
    cases, conversion is deemed unnecessary and the function returns True.

    Args:
        equity (RawEquity): The equity object containing last price, market cap,
            and currency information.

    Returns:
        bool: True if conversion should be skipped, False otherwise.
    """
    last_price = equity.last_price
    market_cap = equity.market_cap
    currency = equity.currency

    return (
        (last_price is None and market_cap is None)
        or currency is None
        or currency == "USD"
    )


def _get_rate_for_currency(currency: str, rates: dict[str, Decimal]) -> Decimal:
    """
    Retrieve the FX rate for a given currency from a dictionary of rates.

    Args:
        currency (str): The currency code (e.g., 'USD', 'EUR') for which to obtain
            the FX rate.
        rates (dict[str, Decimal]): A mapping of currency codes to their FX rates.

    Returns:
        Decimal: The FX rate corresponding to the specified currency.

    Raises:
        ValueError: If the FX rate for the given currency is not found in the rates
            dictionary.
    """
    rate = rates.get(currency)
    if rate is None:
        raise ValueError(f"Missing FX rate for currency {currency}")
    return rate


def _build_field_updates(equity: RawEquity, rate: Decimal) -> dict[str, Decimal]:
    """
    Builds a dictionary of updated equity fields converted to USD using the given rate.

    Args:
        equity (RawEquity): The equity object containing fields to be updated.
        rate (Decimal): The conversion rate to USD.

    Returns:
        dict[str, Decimal]: A dictionary with updated field names as keys and their
            converted USD values as values. Only includes fields present in the equity
            object and not None.
    """
    updates: dict[str, Decimal] = {}

    if equity.last_price is not None:
        updates["last_price"] = _convert_to_usd(equity.last_price, rate)

    if equity.market_cap is not None:
        updates["market_cap"] = _convert_to_usd(equity.market_cap, rate)

    return updates


def _convert_to_usd(figure: Decimal, rate: Decimal) -> Decimal:
    """
    Converts figure from a foreign currency to USD using the provided FX rate.

    Args:
        figure (Decimal): The monetary value in the foreign currency.
        rate (Decimal): The FX rate, representing amount of foreign currency per 1 USD.

    Returns:
        Decimal: The equivalent value in USD, rounded to two decimal places.

    Raises:
        ValueError: If the FX rate is zero.
    """
    if rate == 0:
        raise ValueError("FX rate cannot be zero")

    return (figure / rate).quantize(Decimal("0.01"))
