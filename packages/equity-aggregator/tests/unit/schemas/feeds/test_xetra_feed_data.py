# feeds/test_xetra_feed_data.py

from decimal import Decimal

import pytest
from pydantic import ValidationError

from equity_aggregator.schemas import XetraFeedData

pytestmark = pytest.mark.unit


def test_strips_extra_fields() -> None:
    """
    ARRANGE: input with unexpected extra field
    ACT:     construct XetraFeedData
    ASSERT:  extra field is not present on the model
    """
    payload = {
        "name": "Foo",
        "wkn": "F",
        "mic": "XETR",
        "currency": None,
        "overview": {"lastPrice": None},
        "key_data": {"marketCapitalisation": None},
    }

    actual = XetraFeedData(**payload, unexpected="FIELD")

    assert not hasattr(actual, "unexpected")


def test_missing_required_raises() -> None:
    """
    ARRANGE: input missing required 'name' field
    ACT:     construct XetraFeedData
    ASSERT:  raises ValidationError
    """
    incomplete = {
        "wkn": "F",
        "mic": "XETR",
        "currency": None,
        "overview": {"lastPrice": None},
        "key_data": {"marketCapitalisation": None},
    }

    with pytest.raises(ValidationError):
        XetraFeedData(**incomplete)


def test_mics_default_to_xetr() -> None:
    """
    ARRANGE: omit 'mic' field
    ACT:     construct XetraFeedData
    ASSERT:  mics defaults to ['XETR']
    """
    payload = {
        "name": "Foo",
        "wkn": "F",
        "currency": "EUR",
        "overview": {"lastPrice": 1.0},
        "key_data": {"marketCapitalisation": 1000},
    }

    actual = XetraFeedData(**payload)

    assert actual.mics == ["XETR"]


def test_symbol_maps_from_wkn() -> None:
    """
    ARRANGE: provide 'wkn' field
    ACT:     construct XetraFeedData
    ASSERT:  symbol is set from wkn
    """
    payload = {
        "name": "Foo",
        "wkn": "WKN123",
        "mic": "XETR",
        "currency": "EUR",
        "overview": {"lastPrice": 1.0},
        "key_data": {"marketCapitalisation": 1000},
    }

    actual = XetraFeedData(**payload)

    assert actual.symbol == "WKN123"


def test_last_price_and_market_cap_types() -> None:
    """
    ARRANGE: last_price and market_cap as int, float, str, Decimal
    ACT:     construct XetraFeedData for each type
    ASSERT:  values are preserved as given
    """
    for candidate in (123, 123.45, "123.45", Decimal("123.45")):
        payload = {
            "name": "Foo",
            "wkn": "F",
            "mic": "XETR",
            "currency": "EUR",
            "overview": {"lastPrice": candidate},
            "key_data": {"marketCapitalisation": candidate},
        }

        actual = XetraFeedData(**payload)

        assert actual.last_price == candidate


def test_last_price_can_be_none() -> None:
    """
    ARRANGE: last_price is None
    ACT:     construct XetraFeedData
    ASSERT:  last_price is preserved as None
    """
    payload = {
        "name": "Foo",
        "wkn": "F",
        "mic": "XETR",
        "currency": "EUR",
        "overview": {"lastPrice": None},
        "key_data": {"marketCapitalisation": 1000},
    }

    actual = XetraFeedData(**payload)

    assert actual.last_price is None


def test_market_cap_can_be_none() -> None:
    """
    ARRANGE: market_cap is None
    ACT:     construct XetraFeedData
    ASSERT:  market_cap is preserved as None
    """
    payload = {
        "name": "Foo",
        "wkn": "F",
        "mic": "XETR",
        "currency": "EUR",
        "overview": {"lastPrice": 1.0},
        "key_data": {"marketCapitalisation": None},
    }

    actual = XetraFeedData(**payload)

    assert actual.market_cap is None


def test_currency_case_and_whitespace_preserved() -> None:
    """
    ARRANGE: currency is lowercase and padded
    ACT:     construct XetraFeedData
    ASSERT:  currency is preserved as given (no uppercase enforcement)
    """
    payload = {
        "name": "Foo",
        "wkn": "F",
        "mic": "XETR",
        "currency": " eur ",
        "overview": {"lastPrice": 10},
        "key_data": {"marketCapitalisation": 1000},
    }

    actual = XetraFeedData(**payload)

    assert actual.currency == " eur "


def test_omits_isin_sets_none() -> None:
    """
    ARRANGE: omit 'isin' field
    ACT:     construct XetraFeedData
    ASSERT:  isin is set to None
    """
    payload = {
        "name": "Foo",
        "wkn": "F",
        "mic": "XETR",
        "currency": "EUR",
        "overview": {"lastPrice": 1.0},
        "key_data": {"marketCapitalisation": 1000},
    }

    actual = XetraFeedData(**payload)

    assert actual.isin is None


def test_last_price_string_with_comma() -> None:
    """
    ARRANGE: last_price as string with comma decimal
    ACT:     construct XetraFeedData
    ASSERT:  last_price is preserved as string
    """
    payload = {
        "name": "Foo",
        "wkn": "F",
        "mic": "XETR",
        "currency": "EUR",
        "overview": {"lastPrice": "1,23"},
        "key_data": {"marketCapitalisation": 1000},
    }

    actual = XetraFeedData(**payload)

    assert actual.last_price == "1,23"


def test_mics_from_mic_field() -> None:
    """
    ARRANGE: provide 'mic' field
    ACT:     construct XetraFeedData
    ASSERT:  mics is set as a list containing mic
    """
    payload = {
        "name": "Foo",
        "wkn": "F",
        "mic": "XETR",
        "currency": "EUR",
        "overview": {"lastPrice": 1.0},
        "key_data": {"marketCapitalisation": 1000},
    }

    actual = XetraFeedData(**payload)

    assert actual.mics == ["XETR"]
