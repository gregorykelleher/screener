# pipeline/test_normalise.py

from decimal import Decimal

import pytest

from equity_aggregator.domain._utils._convert import (
    _build_usd_converter,
    _convert_to_usd,
)
from equity_aggregator.schemas import RawEquity

pytestmark = pytest.mark.unit


def test_fx_conversion_divides_by_rate() -> None:
    """
    ARRANGE:
    1 EUR, rate 0.8 EUR per USD
    ACT:     _convert_to_usd
    ASSERT:  result == 1.25 USD
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


def test_normalise_to_usd_vectorised() -> None:
    """
    ARRANGE: three equities in EUR, GBP, USD
    ACT:     _normalise_to_usd
    ASSERT:  currencies all USD, prices converted/divided once
    """
    equities = [
        RawEquity(
            name="E",
            symbol="E",
            currency="EUR",
            last_price=Decimal("1"),
        ),  # 1 / 0.8 = 1.25
        RawEquity(
            name="G",
            symbol="G",
            currency="GBP",
            last_price=Decimal("2"),
        ),  # 2 / 0.5 = 4
        RawEquity(
            name="D",
            symbol="D",
            currency="USD",
            last_price=Decimal("3"),
        ),  # no change
    ]

    rates = {"EUR": Decimal("0.8"), "GBP": Decimal("0.5")}  # USD omitted on purpose

    # Act
    convert = _build_usd_converter(rates)
    actual = [convert(e) for e in equities]

    expected_prices = [Decimal("1.25"), Decimal("4"), Decimal("3")]

    assert [e.last_price for e in actual] == expected_prices
