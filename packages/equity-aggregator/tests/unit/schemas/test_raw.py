# schemas/test_raw.py

from decimal import Decimal

import pytest
from pydantic import ValidationError

from equity_aggregator.schemas.raw import RawEquity

pytestmark = pytest.mark.unit


def test_valid_minimal_raw_equity() -> None:
    """
    ARRANGE: minimal valid RawEquity input
    ACT:     construct RawEquity
    ASSERT:  all required fields are set, optionals are None
    """
    payload = {
        "name": "ACME CORP",
        "symbol": "ACME",
    }

    actual = RawEquity(**payload)

    assert actual.name == "ACME CORP"


def test_all_fields_valid() -> None:
    """
    ARRANGE: all fields provided and valid
    ACT:     construct RawEquity
    ASSERT:  all fields are set as expected
    """
    payload = {
        "name": "ACME CORP",
        "symbol": "ACME",
        "isin": "US0378331005",
        "cik": "0000320193",
        "cusip": "037833100",
        "share_class_figi": "BBG001S5N8V8",
        "mics": ["XNAS", "XLON"],
        "currency": "USD",
        "last_price": Decimal("123.45"),
        "market_cap": Decimal("1000000000"),
    }

    actual = RawEquity(**payload)

    assert actual.isin == "US0378331005"


def test_missing_required_name_raises() -> None:
    """
    ARRANGE: missing required 'name'
    ACT:     construct RawEquity
    ASSERT:  raises ValidationError
    """
    payload = {
        "symbol": "ACME",
    }

    with pytest.raises(ValidationError):
        RawEquity(**payload)


def test_missing_required_symbol_raises() -> None:
    """
    ARRANGE: missing required 'symbol'
    ACT:     construct RawEquity
    ASSERT:  raises ValidationError
    """
    payload = {
        "name": "ACME CORP",
    }

    with pytest.raises(ValidationError):
        RawEquity(**payload)


def test_invalid_isin_raises() -> None:
    """
    ARRANGE: invalid ISIN format
    ACT:     construct RawEquity
    ASSERT:  raises ValidationError
    """
    payload = {
        "name": "ACME CORP",
        "symbol": "ACME",
        "isin": "INVALID!",
    }

    with pytest.raises(ValidationError):
        RawEquity(**payload)


def test_isin_lowercase_is_uppercased() -> None:
    """
    ARRANGE: valid ISIN supplied in lowercase
    ACT:     construct RawEquity
    ASSERT:  ISIN is upper-cased on the model
    """
    payload = {
        "name": "ACME CORP",
        "symbol": "ACME",
        "isin": "us0378331005",
    }

    equity = RawEquity(**payload)

    assert equity.isin == "US0378331005"


def test_invalid_currency_raises() -> None:
    """
    ARRANGE: invalid currency code
    ACT:     construct RawEquity
    ASSERT:  raises ValidationError
    """
    payload = {
        "name": "ACME CORP",
        "symbol": "ACME",
        "currency": "US1",
    }

    with pytest.raises(ValidationError):
        RawEquity(**payload)


def test_negative_last_price_raises() -> None:
    """
    ARRANGE: negative last_price
    ACT:     construct RawEquity
    ASSERT:  raises ValidationError
    """
    payload = {
        "name": "ACME CORP",
        "symbol": "ACME",
        "last_price": -10,
    }

    with pytest.raises(ValidationError):
        RawEquity(**payload)


def test_negative_market_cap_raises() -> None:
    """
    ARRANGE: negative market_cap
    ACT:     construct RawEquity
    ASSERT:  raises ValidationError
    """
    payload = {
        "name": "ACME CORP",
        "symbol": "ACME",
        "market_cap": -1000,
    }

    with pytest.raises(ValidationError):
        RawEquity(**payload)


def test_empty_name_raises() -> None:
    """
    ARRANGE: empty name string
    ACT:     construct RawEquity
    ASSERT:  raises ValidationError
    """
    payload = {
        "name": "",
        "symbol": "ACME",
    }

    with pytest.raises(ValidationError):
        RawEquity(**payload)


def test_empty_symbol_raises() -> None:
    """
    ARRANGE: empty symbol string
    ACT:     construct RawEquity
    ASSERT:  raises ValidationError
    """
    payload = {
        "name": "ACME CORP",
        "symbol": "",
    }

    with pytest.raises(ValidationError):
        RawEquity(**payload)


def test_mics_accepts_none_or_list() -> None:
    """
    ARRANGE: mics as None and as list
    ACT:     construct RawEquity for both
    ASSERT:  mics is set as expected
    """
    payload_none = {
        "name": "ACME CORP",
        "symbol": "ACME",
        "mics": None,
    }

    actual_none = RawEquity(**payload_none)

    assert actual_none.mics is None

    payload_list = {
        "name": "ACME CORP",
        "symbol": "ACME",
        "mics": ["XNAS", "XLON"],
    }

    actual_list = RawEquity(**payload_list)

    assert actual_list.mics == ["XNAS", "XLON"]


def test_mic_lowercase_uppercased() -> None:
    """
    ARRANGE: lowercase MIC format
    ACT:     construct RawEquity
    ASSERT:  mic is uppercased
    """
    equity = RawEquity(name="ACME", symbol="ACME", mics=["xnys"])

    assert equity.mics == ["XNYS"]


def test_currency_accepts_none() -> None:
    """
    ARRANGE: currency is None
    ACT:     construct RawEquity
    ASSERT:  currency is set to None
    """
    payload = {
        "name": "ACME CORP",
        "symbol": "ACME",
        "currency": None,
    }

    actual = RawEquity(**payload)

    assert actual.currency is None


def test_last_price_accepts_none() -> None:
    """
    ARRANGE: last_price is None
    ACT:     construct RawEquity
    ASSERT:  last_price is set to None
    """
    payload = {
        "name": "ACME CORP",
        "symbol": "ACME",
        "last_price": None,
    }

    actual = RawEquity(**payload)

    assert actual.last_price is None


def test_market_cap_accepts_none() -> None:
    """
    ARRANGE: market_cap is None
    ACT:     construct RawEquity
    ASSERT:  market_cap is set to None
    """
    payload = {
        "name": "ACME CORP",
        "symbol": "ACME",
        "market_cap": None,
    }

    actual = RawEquity(**payload)

    assert actual.market_cap is None


def test_extra_field_is_ignored() -> None:
    """
    ARRANGE: input with an extra unexpected field
    ACT:     construct RawEquity
    ASSERT:  extra field is not present on the model
    """
    payload = {
        "name": "ACME CORP",
        "symbol": "ACME",
        "extra": "should be ignored",
    }

    actual = RawEquity(**payload)

    assert not hasattr(actual, "extra")


def test_identifiers_accept_none() -> None:
    """
    ARRANGE: identifiers as None
    ACT:     construct RawEquity
    ASSERT:  isin is None
    """
    payload = {
        "name": "ACME CORP",
        "symbol": "ACME",
        "isin": None,
        "cusip": None,
        "cik": None,
        "share_class_figi": None,
    }

    actual = RawEquity(**payload)

    assert actual.isin is None


def test_identifiers_accept_valid() -> None:
    """
    ARRANGE: identifiers as valid values
    ACT:     construct RawEquity
    ASSERT:  share_class_figi is set as expected
    """
    payload = {
        "name": "ACME CORP",
        "symbol": "ACME",
        "isin": "US0378331005",
        "cusip": "037833100",
        "cik": "0000320193",
        "share_class_figi": "BBG001S5N8V8",
    }

    actual = RawEquity(**payload)

    assert actual.share_class_figi == "BBG001S5N8V8"


def test_mics_accepts_empty_list() -> None:
    """
    ARRANGE: mics as empty list
    ACT:     construct RawEquity
    ASSERT:  mics is empty list
    """
    payload = {
        "name": "ACME CORP",
        "symbol": "ACME",
        "mics": [],
    }

    actual = RawEquity(**payload)

    assert actual.mics is None


def test_mics_rejects_nonlist() -> None:
    """
    ARRANGE: mics as string
    ACT:     construct RawEquity
    ASSERT:  raises ValidationError
    """
    payload = {
        "name": "ACME CORP",
        "symbol": "ACME",
        "mics": "XNAS",
    }

    with pytest.raises(ValidationError):
        RawEquity(**payload)


def test_currency_accepts_lowercase() -> None:
    """
    ARRANGE: currency as lowercase
    ACT:     construct RawEquity
    ASSERT:  currency is uppercased
    """
    payload = {
        "name": "ACME CORP",
        "symbol": "ACME",
        "currency": "usd",
    }

    actual = RawEquity(**payload)

    assert actual.currency == "USD"


def test_currency_accepts_padded() -> None:
    """
    ARRANGE: currency as padded string
    ACT:     construct RawEquity
    ASSERT:  currency is uppercased and stripped
    """
    payload = {
        "name": "ACME CORP",
        "symbol": "ACME",
        "currency": " usd ",
    }

    actual = RawEquity(**payload)

    assert actual.currency == "USD"


def test_currency_rejects_too_long() -> None:
    """
    ARRANGE: currency too long
    ACT:     construct RawEquity
    ASSERT:  raises ValidationError
    """
    payload = {
        "name": "ACME CORP",
        "symbol": "ACME",
        "currency": "USDA",
    }

    with pytest.raises(ValidationError):
        RawEquity(**payload)


def test_currency_rejects_too_short() -> None:
    """
    ARRANGE: currency too short
    ACT:     construct RawEquity
    ASSERT:  raises ValidationError
    """
    payload = {
        "name": "ACME CORP",
        "symbol": "ACME",
        "currency": "US",
    }

    with pytest.raises(ValidationError):
        RawEquity(**payload)


def test_last_price_accepts_str() -> None:
    """
    ARRANGE: last_price as string
    ACT:     construct RawEquity
    ASSERT:  last_price is Decimal
    """
    payload = {
        "name": "ACME CORP",
        "symbol": "ACME",
        "last_price": "123.45",
    }

    actual = RawEquity(**payload)

    assert actual.last_price == Decimal("123.45")


def test_last_price_accepts_float() -> None:
    """
    ARRANGE: last_price as float
    ACT:     construct RawEquity
    ASSERT:  last_price is Decimal
    """
    payload = {
        "name": "ACME CORP",
        "symbol": "ACME",
        "last_price": 123.45,
    }

    actual = RawEquity(**payload)

    assert actual.last_price == Decimal("123.45")


def test_last_price_accepts_int() -> None:
    """
    ARRANGE: last_price as int
    ACT:     construct RawEquity
    ASSERT:  last_price is Decimal
    """
    payload = {
        "name": "ACME CORP",
        "symbol": "ACME",
        "last_price": 123,
    }

    actual = RawEquity(**payload)

    assert actual.last_price == Decimal("123")


def test_last_price_accepts_decimal() -> None:
    """
    ARRANGE: last_price as Decimal
    ACT:     construct RawEquity
    ASSERT:  last_price is Decimal
    """
    payload = {
        "name": "ACME CORP",
        "symbol": "ACME",
        "last_price": Decimal("123.45"),
    }

    actual = RawEquity(**payload)

    assert actual.last_price == Decimal("123.45")


def test_last_price_rejects_negative() -> None:
    """
    ARRANGE: last_price negative
    ACT:     construct RawEquity
    ASSERT:  raises ValidationError
    """
    payload = {
        "name": "ACME CORP",
        "symbol": "ACME",
        "last_price": -1,
    }

    with pytest.raises(ValidationError):
        RawEquity(**payload)


def test_last_price_rejects_invalid() -> None:
    """
    ARRANGE: last_price invalid string
    ACT:     construct RawEquity
    ASSERT:  raises ValidationError
    """
    payload = {
        "name": "ACME CORP",
        "symbol": "ACME",
        "last_price": "not_a_number",
    }

    with pytest.raises(ValidationError):
        RawEquity(**payload)


def test_isin_rejects_invalid_format() -> None:
    """
    ARRANGE: invalid ISIN format
    ACT:     construct RawEquity
    ASSERT:  raises ValidationError
    """
    payload = {
        "name": "ACME CORP",
        "symbol": "ACME",
        "isin": "BAD!",
    }

    with pytest.raises(ValidationError):
        RawEquity(**payload)


def test_cusip_rejects_invalid_format() -> None:
    """
    ARRANGE: invalid CUSIP format
    ACT:     construct RawEquity
    ASSERT:  raises ValidationError
    """
    payload = {
        "name": "ACME CORP",
        "symbol": "ACME",
        "cusip": "BAD!",
    }

    with pytest.raises(ValidationError):
        RawEquity(**payload)


def test_cusip_lowercase_uppercased() -> None:
    """
    ARRANGE: lowercase CUSIP format
    ACT:     construct RawEquity
    ASSERT:  cusip is uppercased
    """
    equity = RawEquity(name="ACME CORP", symbol="ACME", cusip="037833100")

    assert equity.cusip == "037833100"


def test_cik_rejects_invalid_format() -> None:
    """
    ARRANGE: invalid CIK format
    ACT:     construct RawEquity
    ASSERT:  raises ValidationError
    """
    payload = {
        "name": "ACME CORP",
        "symbol": "ACME",
        "cik": "BAD!",
    }

    with pytest.raises(ValidationError):
        RawEquity(**payload)


def test_figi_rejects_invalid_format() -> None:
    """
    ARRANGE: invalid FIGI format
    ACT:     construct RawEquity
    ASSERT:  raises ValidationError
    """
    payload = {
        "name": "ACME CORP",
        "symbol": "ACME",
        "share_class_figi": "BAD!",
    }

    with pytest.raises(ValidationError):
        RawEquity(**payload)


def test_figi_lowercase_uppercased() -> None:
    """
    ARRANGE: lowercase FIGI format
    ACT:     construct RawEquity
    ASSERT:  figi is uppercased
    """
    equity = RawEquity(name="ACME", symbol="ACME", share_class_figi="bbg001s5n8v8")
    assert equity.share_class_figi == "BBG001S5N8V8"
