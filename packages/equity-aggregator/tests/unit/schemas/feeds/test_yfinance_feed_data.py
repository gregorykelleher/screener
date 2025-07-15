# feeds/test_yfinance_feed_data.py

from decimal import Decimal

import pytest
from pydantic import ValidationError

from equity_aggregator.schemas import YFinanceFeedData

pytestmark = pytest.mark.unit


def test_strips_extra_fields() -> None:
    """
    ARRANGE: input with unexpected extra field
    ACT:     construct YFinanceFeedData
    ASSERT:  extra field is not present on the model
    """
    raw = {
        "longName": "Foo Inc",
        "underlyingSymbol": "FOO",
        "currency": "USD",
        "currentPrice": None,
        "marketCap": None,
    }

    actual = YFinanceFeedData(**raw, unexpected="FIELD")

    assert not hasattr(actual, "unexpected")


def test_preserves_optional_none_fields() -> None:
    """
    ARRANGE: currency and marketCap set to None
    ACT:     construct YFinanceFeedData
    ASSERT:  optional field 'currency' is preserved as None
    """
    raw = {
        "longName": "Foo Inc",
        "underlyingSymbol": "FOO",
        "currency": None,
        "currentPrice": 1.23,
        "marketCap": None,
    }

    actual = YFinanceFeedData(**raw)

    assert actual.currency is None


def test_accepts_various_numeric_types() -> None:
    """
    ARRANGE: currentPrice and marketCap with several numeric/string/Decimal types
    ACT:     construct YFinanceFeedData for each payload
    ASSERT:  values are preserved exactly
    """
    for price in (123, 123.45, "123.45", Decimal("123.45")):
        raw = {
            "longName": "Foo Inc",
            "underlyingSymbol": "FOO",
            "currency": "USD",
            "currentPrice": price,
            "marketCap": price,
        }

        actual = YFinanceFeedData(**raw)

        assert (actual.last_price, actual.market_cap) == (price, price)


def test_missing_required_raises() -> None:
    """
    ARRANGE: omit required 'longName'
    ACT:     construct YFinanceFeedData
    ASSERT:  raises ValidationError
    """
    incomplete = {
        "underlyingSymbol": "FOO",
        "currency": "USD",
        "currentPrice": 1,
    }

    with pytest.raises(ValidationError):
        YFinanceFeedData(**incomplete)


def test_normalises_and_preserves_whitespace() -> None:
    """
    ARRANGE: raw fields include padding/whitespace
    ACT:     construct YFinanceFeedData
    ASSERT:  whitespace in 'name' is retained (no trimming at this layer)
    """
    raw = {
        "longName": "  Padded Name  ",
        "underlyingSymbol": " PAD ",
        "currency": " usd ",
        "currentPrice": "1,23",
        "marketCap": None,
    }

    actual = YFinanceFeedData(**raw)

    assert actual.name == "  Padded Name  "


def test_last_price_string_with_comma_preserved() -> None:
    """
    ARRANGE: currentPrice as string using comma decimal
    ACT:     construct YFinanceFeedData
    ASSERT:  last_price is preserved as string
    """
    raw = {
        "longName": "Foo Inc",
        "underlyingSymbol": "FOO",
        "currency": "USD",
        "currentPrice": "1,23",
        "marketCap": None,
    }

    actual = YFinanceFeedData(**raw)

    assert actual.last_price == "1,23"
