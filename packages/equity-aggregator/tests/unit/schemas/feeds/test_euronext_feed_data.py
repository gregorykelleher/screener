# feeds/test_euronext_feed_data.py

from decimal import Decimal

import pytest
from pydantic import ValidationError

from equity_aggregator.schemas import EuronextFeedData

pytestmark = pytest.mark.unit


def test_strips_extra_fields() -> None:
    """
    ARRANGE: input with unexpected extra field
    ACT:     construct EuronextFeedData
    ASSERT:  extra field is not present on the actualect
    """
    payload = {
        "name": "Foo",
        "symbol": "F",
        "mics": [],
        "currency": None,
        "last_price": None,
    }

    actual = EuronextFeedData(**payload, unexpected="FIELD")

    assert not hasattr(actual, "unexpected")


def test_converts_last_price_to_str_or_decimal() -> None:
    """
    ARRANGE: input with last_price as a float
    ACT:     construct EuronextFeedData
    ASSERT:  last_price is preserved as float
    """

    # Set a float value for last_price
    last_price = 123.45

    raw_price = {
        "name": "N",
        "symbol": "S",
        "mics": [],
        "currency": None,
        "last_price": last_price,
    }

    actual = EuronextFeedData(**raw_price)

    assert actual.last_price == last_price


def test_missing_required_raises() -> None:
    """
    ARRANGE: input missing required 'name' field
    ACT:     construct EuronextFeedData
    ASSERT:  raises ValidationError
    """
    incomplete = {
        "symbol": "S",
        "mics": [],
        "currency": None,
        "last_price": None,
    }

    with pytest.raises(ValidationError):
        EuronextFeedData(**incomplete)


def test_normalises_and_requires_fields() -> None:
    """
    ARRANGE: valid input with extra whitespace and extra field
    ACT:     construct EuronextFeedData
    ASSERT:  name is preserved as-is (no cleaning at this stage)
    """
    raw = {
        "name": " Acme Inc.  ",
        "symbol": " acme  ",
        "isin": None,
        "mics": ["XPAR"],
        "currency": "eur",
        "last_price": "123,45",
        "extra_field": "ignored",
    }
    actual = EuronextFeedData(**raw)

    assert actual.name == " Acme Inc.  "


def test_missing_required_fields_raise() -> None:
    """
    ARRANGE: omit 'mics' field
    ACT:     construct EuronextFeedData
    ASSERT:  raises ValidationError
    """
    incomplete = {
        "name": "Foo",
        "symbol": "F",
        "currency": "EUR",
        "last_price": 1.0,
    }

    with pytest.raises(ValidationError):
        EuronextFeedData(**incomplete)


def test_mics_must_be_list_of_strings() -> None:
    """
    ARRANGE: mics is not a list
    ACT:     construct EuronextFeedData
    ASSERT:  raises ValidationError
    """
    payload = {
        "name": "Foo",
        "symbol": "F",
        "isin": None,
        "mics": "notalist",
        "currency": "EUR",
        "last_price": None,
    }

    with pytest.raises(ValidationError):
        EuronextFeedData(**payload)


def test_preserves_optional_none_fields() -> None:
    """
    ARRANGE: optional fields set to None
    ACT:     construct EuronextFeedData
    ASSERT:  optional field 'isin' is preserved as None
    """
    payload = {
        "name": "Foo",
        "symbol": "F",
        "isin": None,
        "mics": ["XPAR"],
        "currency": None,
        "last_price": None,
    }

    actual = EuronextFeedData(**payload)

    assert actual.isin is None


def test_accepts_various_last_price_types() -> None:
    """
    ARRANGE: last_price as int, float, str, Decimal
    ACT:     construct EuronextFeedData for each type
    ASSERT:  last_price is preserved as given
    """
    for candidate in (123, 123.45, "123.45", Decimal("123.45")):
        payload = {
            "name": "Foo",
            "symbol": "F",
            "isin": None,
            "mics": ["XPAR"],
            "currency": "EUR",
            "last_price": candidate,
        }

        actual = EuronextFeedData(**payload)

        assert actual.last_price == candidate


def test_currency_case_and_whitespace_preserved() -> None:
    """
    ARRANGE: currency is lowercase and padded
    ACT:     construct EuronextFeedData
    ASSERT:  currency is preserved as given (no uppercase enforcement)
    """
    raw = {
        "name": "Foo",
        "symbol": "F",
        "isin": None,
        "mics": ["XPAR"],
        "currency": " eur ",
        "last_price": 10,
    }

    actual = EuronextFeedData(**raw)

    assert actual.currency == " eur "


def test_last_price_string_with_comma() -> None:
    """
    ARRANGE: last_price as string with comma decimal
    ACT:     construct EuronextFeedData
    ASSERT:  last_price is preserved as string
    """
    payload = {
        "name": "Foo",
        "symbol": "F",
        "mics": ["XPAR"],
        "currency": "EUR",
        "last_price": "1,23",
    }

    actual = EuronextFeedData(**payload)

    assert actual.last_price == "1,23"


def test_missing_optional_isin_sets_none() -> None:
    """
    ARRANGE: omit 'isin' field
    ACT:     construct EuronextFeedData
    ASSERT:  isin is set to None
    """
    payload = {
        "name": "Foo",
        "symbol": "F",
        "mics": ["XPAR"],
        "currency": "EUR",
        "last_price": 1.0,
    }

    actual = EuronextFeedData(**payload)

    assert actual.isin is None


def test_last_price_can_be_none() -> None:
    """
    ARRANGE: last_price is None
    ACT:     construct EuronextFeedData
    ASSERT:  last_price is preserved as None
    """
    payload = {
        "name": "Foo",
        "symbol": "F",
        "mics": ["XPAR"],
        "currency": "EUR",
        "last_price": None,
    }

    actual = EuronextFeedData(**payload)

    assert actual.last_price is None


def test_mics_can_be_empty_list() -> None:
    """
    ARRANGE: mics is an empty list
    ACT:     construct EuronextFeedData
    ASSERT:  mics is preserved as empty list
    """
    payload = {
        "name": "Foo",
        "symbol": "F",
        "mics": [],
        "currency": "EUR",
        "last_price": 1.0,
    }

    actual = EuronextFeedData(**payload)

    assert actual.mics == []


def test_extra_field_is_ignored() -> None:
    """
    ARRANGE: input with an extra unexpected field
    ACT:     construct EuronextFeedData
    ASSERT:  extra field is not present on the model
    """
    payload = {
        "name": "Real Name",
        "symbol": "SYM",
        "mics": ["XPAR"],
        "currency": "EUR",
        "last_price": 1.0,
        "extra": "should be ignored",
    }

    actual = EuronextFeedData(**payload)

    assert not hasattr(actual, "extra")
