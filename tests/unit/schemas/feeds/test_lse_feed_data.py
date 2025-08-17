# feeds/test_lse_feed_data.py

from decimal import Decimal

import pytest
from pydantic import ValidationError

from equity_aggregator.schemas import LseFeedData

pytestmark = pytest.mark.unit


def test_strips_extra_fields() -> None:
    """
    ARRANGE: input with unexpected extra field
    ACT:     construct LseFeedData
    ASSERT:  extra field is not present on the model
    """
    payload = {
        "issuername": "Foo",
        "tidm": "F",
        "currency": None,
        "lastprice": None,
        "marketcapitalization": None,
        "mics": ["XLON"],
    }

    actual = LseFeedData(**payload, unexpected="FIELD")

    assert not hasattr(actual, "unexpected")


def test_missing_required_raises() -> None:
    """
    ARRANGE: input missing required 'issuername' field
    ACT:     construct LseFeedData
    ASSERT:  raises ValidationError
    """
    incomplete = {
        "tidm": "F",
        "currency": None,
        "lastprice": None,
        "marketcapitalization": None,
        "mics": ["XLON"],
    }

    with pytest.raises(ValidationError):
        LseFeedData(**incomplete)


def test_mics_default_to_xlon() -> None:
    """
    ARRANGE: omit 'mics' field
    ACT:     construct LseFeedData
    ASSERT:  mics defaults to ['XLON']
    """
    payload = {
        "issuername": "Foo",
        "tidm": "F",
        "currency": "GBP",
        "lastprice": 1.0,
        "marketcapitalization": 1000,
    }

    actual = LseFeedData(**payload)

    assert actual.mics == ["XLON"]


def test_symbol_maps_from_tidm() -> None:
    """
    ARRANGE: provide 'tidm' field
    ACT:     construct LseFeedData
    ASSERT:  symbol is set from tidm
    """
    payload = {
        "issuername": "Foo",
        "tidm": "TIDM123",
        "currency": "GBP",
        "lastprice": 1.0,
        "marketcapitalization": 1000,
        "mics": ["XLON"],
    }

    actual = LseFeedData(**payload)

    assert actual.symbol == "TIDM123"


def test_last_price_and_market_cap_types() -> None:
    """
    ARRANGE: lastprice and marketcapitalization as int, float, str, Decimal
    ACT:     construct LseFeedData for each type
    ASSERT:  values are preserved as given
    """
    for candidate in (123, 123.45, "123.45", Decimal("123.45")):
        payload = {
            "issuername": "Foo",
            "tidm": "F",
            "currency": "GBP",
            "lastprice": candidate,
            "marketcapitalization": candidate,
            "mics": ["XLON"],
        }

        actual = LseFeedData(**payload)

        assert actual.last_price == candidate


def test_last_price_can_be_none() -> None:
    """
    ARRANGE: lastprice is None
    ACT:     construct LseFeedData
    ASSERT:  last_price is preserved as None
    """
    payload = {
        "issuername": "Foo",
        "tidm": "F",
        "currency": "GBP",
        "lastprice": None,
        "marketcapitalization": 1000,
        "mics": ["XLON"],
    }

    actual = LseFeedData(**payload)

    assert actual.last_price is None


def test_market_cap_can_be_none() -> None:
    """
    ARRANGE: marketcapitalization is None
    ACT:     construct LseFeedData
    ASSERT:  market_cap is preserved as None
    """
    payload = {
        "issuername": "Foo",
        "tidm": "F",
        "currency": "GBP",
        "lastprice": 1.0,
        "marketcapitalization": None,
        "mics": ["XLON"],
    }

    actual = LseFeedData(**payload)

    assert actual.market_cap is None


def test_currency_case_and_whitespace_preserved() -> None:
    """
    ARRANGE: currency is lowercase and padded
    ACT:     construct LseFeedData
    ASSERT:  currency is preserved as given (no uppercase enforcement)
    """
    payload = {
        "issuername": "Foo",
        "tidm": "F",
        "currency": " gbp ",
        "lastprice": 10,
        "marketcapitalization": 1000,
        "mics": ["XLON"],
    }

    actual = LseFeedData(**payload)

    assert actual.currency == " gbp "


def test_omits_isin_sets_none() -> None:
    """
    ARRANGE: omit 'isin' field
    ACT:     construct LseFeedData
    ASSERT:  isin is set to None
    """
    payload = {
        "issuername": "Foo",
        "tidm": "F",
        "currency": "GBP",
        "lastprice": 1.0,
        "marketcapitalization": 1000,
        "mics": ["XLON"],
    }

    actual = LseFeedData(**payload)

    assert actual.isin is None


def test_last_price_string_with_comma() -> None:
    """
    ARRANGE: lastprice as string with comma decimal
    ACT:     construct LseFeedData
    ASSERT:  last_price is preserved as string
    """
    payload = {
        "issuername": "Foo",
        "tidm": "F",
        "currency": "GBP",
        "lastprice": "1,23",
        "marketcapitalization": 1000,
        "mics": ["XLON"],
    }

    actual = LseFeedData(**payload)

    assert actual.last_price == "1,23"


def test_mics_from_field() -> None:
    """
    ARRANGE: provide 'mics' field
    ACT:     construct LseFeedData
    ASSERT:  mics is set as given
    """
    payload = {
        "issuername": "Foo",
        "tidm": "F",
        "currency": "GBP",
        "lastprice": 1.0,
        "marketcapitalization": 1000,
        "mics": ["XLON", "XOFF"],
    }

    actual = LseFeedData(**payload)

    assert actual.mics == ["XLON", "XOFF"]


def test_gbx_currency_converts_price_and_currency() -> None:
    """
    ARRANGE: currency is GBX and lastprice is pence string
    ACT:     construct LseFeedData
    ASSERT:  last_price is converted to pounds and currency to GBP
    """
    payload = {
        "issuername": "Foo",
        "tidm": "F",
        "currency": "GBX",
        "lastprice": "123,45",
        "marketcapitalization": 1000,
        "mics": ["XLON"],
    }

    actual = LseFeedData(**payload)

    assert actual.last_price == Decimal("1.2345")


def test_gbx_currency_converts_currency_to_gbp() -> None:
    """
    ARRANGE: currency is GBX
    ACT:     construct LseFeedData
    ASSERT:  currency is converted to GBP
    """
    payload = {
        "issuername": "Foo",
        "tidm": "F",
        "currency": "GBX",
        "lastprice": "123,45",
        "marketcapitalization": 1000,
        "mics": ["XLON"],
    }

    actual = LseFeedData(**payload)

    assert actual.currency == "GBP"


def test_gbx_currency_handles_invalid_lastprice() -> None:
    """
    ARRANGE: currency is GBX and lastprice is not a number
    ACT:     construct LseFeedData
    ASSERT:  last_price is None (conversion fails)
    """
    payload = {
        "issuername": "Foo",
        "tidm": "F",
        "currency": "GBX",
        "lastprice": "not_a_number",
        "marketcapitalization": 1000,
        "mics": ["XLON"],
    }

    actual = LseFeedData(**payload)

    assert actual.last_price is None and actual.currency == "GBP"


def test_gbx_currency_with_none_lastprice() -> None:
    """
    ARRANGE: currency is GBX and lastprice is None
    ACT:     construct LseFeedData
    ASSERT:  last_price is None
    """
    payload = {
        "issuername": "Foo",
        "tidm": "F",
        "currency": "GBX",
        "lastprice": None,
        "marketcapitalization": 1000,
        "mics": ["XLON"],
    }

    actual = LseFeedData(**payload)

    assert actual.last_price is None and actual.currency == "GBP"


def test_extra_field_is_ignored() -> None:
    """
    ARRANGE: input with an extra unexpected field
    ACT:     construct LseFeedData
    ASSERT:  extra field is not present on the model
    """
    payload = {
        "issuername": "Real Name",
        "tidm": "SYM",
        "currency": "GBP",
        "lastprice": 1.0,
        "marketcapitalization": 1000,
        "mics": ["XLON"],
        "extra": "should be ignored",
    }

    actual = LseFeedData(**payload)

    assert not hasattr(actual, "extra")


def test_accepts_various_last_price_types() -> None:
    """
    ARRANGE: lastprice as int, float, str, Decimal
    ACT:     construct LseFeedData for each type
    ASSERT:  last_price is preserved as given
    """
    for candidate in (123, 123.45, "123.45", Decimal("123.45")):
        payload = {
            "issuername": "Foo",
            "tidm": "F",
            "currency": "GBP",
            "lastprice": candidate,
            "marketcapitalization": 1000,
            "mics": ["XLON"],
        }

        actual = LseFeedData(**payload)

        assert actual.last_price == candidate
