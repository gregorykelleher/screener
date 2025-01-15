# _utils/_fx.py

import logging
from decimal import Decimal
from typing import Callable

from equity_aggregator.adapters import retrieve_conversion_rates
from equity_aggregator.schemas import RawEquity

logger = logging.getLogger(__name__)


async def get_usd_converter() -> Callable[[RawEquity], RawEquity]:
    """
    Asynchronously loads and caches FX rates, returning a function that converts
    RawEquity instances to USD.
    """
    # load the conversion rates from Exchange Rate API or cache
    rates = await retrieve_conversion_rates()

    # build and return the USD converter function
    return _build_usd_converter(rates)


def _build_usd_converter(rates: dict[str, Decimal]) -> Callable[[RawEquity], RawEquity]:
    """
    Creates a USD converter function using the provided FX rates.

    The returned function takes a RawEquity instance and converts its price to USD
    using the captured rates mapping. This closure ensures the rates are reused
    efficiently without needing to pass them on each call.
    """

    def convert(equity: RawEquity) -> RawEquity:
        last_price = equity.last_price
        currency = equity.currency

        # no-op if missing data or already USD
        if last_price is None or currency is None or currency.upper() == "USD":
            return equity

        rate = rates.get(currency.upper())

        if rate is None:
            logger.warning("No FX rate for currency %s: ", currency)
            raise ValueError(f"Missing FX rate for currency: {currency}")

        last_price_usd = _convert_to_usd(last_price, rate)

        return equity.model_copy(
            update={"last_price": last_price_usd, "currency": "USD"}
        )

    return convert


def _convert_to_usd(figure: Decimal, rate: Decimal) -> Decimal:
    """
    Convert figure in foreign currency to USD.

    rate is foreign-currency per USD.
    """
    if rate == 0:
        raise ValueError("FX rate cannot be zero")

    return (figure / rate).quantize(Decimal("0.01"))
