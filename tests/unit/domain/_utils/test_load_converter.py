# pipeline/test_load_converter.py

from decimal import Decimal

import pytest

from equity_aggregator.domain._utils._load_converter import (
    _build_usd_converter,
    _convert_to_usd,
)
from equity_aggregator.schemas import RawEquity

pytestmark = pytest.mark.unit


def test_fx_conversion_divides_by_rate() -> None:
    """
    ARRANGE: 1 EUR, rate 0.8 EUR per USD
    ACT:     _convert_to_usd
    ASSERT:  actual == 1.25 USD
    """
    expected = Decimal("1.25")

    actual = _convert_to_usd(Decimal("1"), Decimal("0.8"))

    assert actual == expected


def test_fx_zero_rate_raises() -> None:
    """
    ARRANGE: rate == 0
    ACT:     _convert_to_usd
    ASSERT:  ValueError raised
    """
    with pytest.raises(ValueError):
        _convert_to_usd(Decimal("1"), Decimal("0"))


def test_eur_price_converted_correctly() -> None:
    """
    ARRANGE: 6.47 EUR, rate 0.8999 EUR per USD
    ACT:     converter(last_price)
    ASSERT:  last_price == 7.19 USD
    """
    rates = {"EUR": Decimal("0.8999")}

    convert = _build_usd_converter(rates)

    equity = RawEquity(
        name="ALTRI",
        symbol="ALT",
        currency="EUR",
        last_price=Decimal("6.47"),
    )

    actual = convert(equity)

    assert actual.last_price == Decimal("7.19")


def test_eur_conversion_sets_currency_to_usd() -> None:
    """
    ARRANGE: 6.47 EUR, rate 0.8999 EUR per USD
    ACT:     converter(currency)
    ASSERT:  currency == "USD"
    """
    rates = {"EUR": Decimal("0.8999")}

    convert = _build_usd_converter(rates)

    equity = RawEquity(
        name="ALTRI",
        symbol="ALT",
        currency="EUR",
        last_price=Decimal("6.47"),
    )

    actual = convert(equity)

    assert actual.currency == "USD"


def test_market_cap_converted_correctly() -> None:
    """
    ARRANGE: market_cap only, rate 0.5 EUR per USD
    ACT:     converter(market_cap)
    ASSERT:  market_cap == 4.00 USD
    """
    rates = {"EUR": Decimal("0.5")}

    convert = _build_usd_converter(rates)

    equity = RawEquity(
        name="CAP",
        symbol="CAP",
        currency="EUR",
        market_cap=Decimal("2"),
    )

    actual = convert(equity)

    assert actual.market_cap == Decimal("4.00")


def test_none_currency_returns_same_object() -> None:
    """
    ARRANGE: currency is None, last_price exists
    ACT:     converter(equity)
    ASSERT:  object unchanged
    """
    rates = {"EUR": Decimal("0.8")}

    convert = _build_usd_converter(rates)

    equity = RawEquity(
        name="NONE",
        symbol="NON",
        currency=None,
        last_price=Decimal("10"),
    )

    actual = convert(equity)

    assert actual is equity


def test_rounding_precision_quantizes_to_two_decimal() -> None:
    """
    ARRANGE: 1 EUR, rate 3 EUR per USD
    ACT:     converter(last_price)
    ASSERT:  last_price == 0.33 USD
    """
    rates = {"EUR": Decimal("3")}

    convert = _build_usd_converter(rates)

    equity = RawEquity(
        name="ROUND",
        symbol="RND",
        currency="EUR",
        last_price=Decimal("1"),
    )

    actual = convert(equity)

    assert actual.last_price == Decimal("0.33")


def test_preserve_name_and_symbol_after_conversion() -> None:
    """
    ARRANGE: equity with name and symbol
    ACT:     converter(equity)
    ASSERT:  name and symbol unchanged
    """
    rates = {"EUR": Decimal("2")}

    convert = _build_usd_converter(rates)

    equity = RawEquity(
        name="PRESERVE",
        symbol="PSV",
        currency="EUR",
        last_price=Decimal("2"),
        market_cap=Decimal("4"),
    )

    actual = convert(equity)

    assert (actual.name, actual.symbol) == (equity.name, equity.symbol)


def test_missing_rate_raises_value_error() -> None:
    """
    ARRANGE: currency not in rates
    ACT:     converter(equity)
    ASSERT:  ValueError raised
    """
    rates = {}  # no EUR entry
    convert = _build_usd_converter(rates)

    equity = RawEquity(
        name="MISSING",
        symbol="MIS",
        currency="EUR",
        last_price=Decimal("1.00"),
    )

    with pytest.raises(ValueError):
        convert(equity)


def test_already_usd_returns_same_object() -> None:
    """
    ARRANGE: equity already in USD
    ACT:     converter(equity)
    ASSERT:  object unchanged
    """
    rates = {"EUR": Decimal("0.9")}
    convert = _build_usd_converter(rates)

    equity = RawEquity(
        name="USD CORP",
        symbol="USD",
        currency="USD",
        last_price=Decimal("5"),
    )

    actual = convert(equity)

    assert actual is equity


def test_none_price_returns_same_object() -> None:
    """
    ARRANGE: last_price is None
    ACT:     converter(equity)
    ASSERT:  object unchanged
    """
    rates = {"EUR": Decimal("0.9")}
    convert = _build_usd_converter(rates)

    equity = RawEquity(
        name="NOPRICE",
        symbol="NP",
        currency="EUR",
        last_price=None,
    )

    actual = convert(equity)

    assert actual is equity


def test_usd_converter_applies_to_multiple_equities() -> None:
    """
    ARRANGE: three equities in EUR, GBP, USD
    ACT:     _build_usd_converter and apply to list
    ASSERT:  currencies all USD and prices converted/divided once
    """
    equities = [
        RawEquity(name="E", symbol="E", currency="EUR", last_price=Decimal("1")),
        RawEquity(name="G", symbol="G", currency="GBP", last_price=Decimal("2")),
        RawEquity(name="D", symbol="D", currency="USD", last_price=Decimal("3")),
    ]

    rates = {"EUR": Decimal("0.8"), "GBP": Decimal("0.5")}  # USD omitted on purpose

    convert = _build_usd_converter(rates)

    actual_prices = [convert(e).last_price for e in equities]

    expected = [Decimal("1.25"), Decimal("4"), Decimal("3")]

    assert actual_prices == expected


def test_convert_both_price_and_market_cap() -> None:
    """
    ARRANGE: equity with both last_price and market_cap in EUR
    ACT:     converter(equity)
    ASSERT:  both last_price and market_cap converted to USD
    """
    rates = {"EUR": Decimal("2")}

    convert = _build_usd_converter(rates)

    equity = RawEquity(
        name="BOTH",
        symbol="BTH",
        currency="EUR",
        last_price=Decimal("4.00"),
        market_cap=Decimal("6.00"),
    )

    actual = convert(equity)

    assert (actual.last_price, actual.market_cap) == (Decimal("2.00"), Decimal("3.00"))


def test_skip_when_no_price_and_no_market_cap() -> None:
    """
    ARRANGE: equity with no last_price and no market_cap
    ACT:     converter(equity)
    ASSERT:  object unchanged
    """
    rates = {"EUR": Decimal("1")}

    convert = _build_usd_converter(rates)

    equity = RawEquity(
        name="NONE",
        symbol="NON",
        currency="EUR",
        last_price=None,
        market_cap=None,
    )

    assert convert(equity) is equity
