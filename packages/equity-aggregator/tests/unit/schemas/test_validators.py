# schemas/test_validators.py

from decimal import Decimal

import pytest

from equity_aggregator.schemas import validators

pytestmark = pytest.mark.unit


def test_validate_name_basic() -> None:
    """
    ARRANGE: simple name string
    ACT:     validate_name
    ASSERT:  returns uppercased, punctuation-stripped name
    """
    value = "Acme, Inc."

    actual = validators.validate_name(value)

    assert actual == "ACME INC"


def test_validate_name_whitespace_and_punct() -> None:
    """
    ARRANGE: name with extra whitespace and punctuation
    ACT:     validate_name
    ASSERT:  returns cleaned, uppercased name
    """
    value = "  Foo-Bar!  Ltd.  "

    actual = validators.validate_name(value)

    assert actual == "FOO BAR LTD"


def test_validate_name_none_raises() -> None:
    """
    ARRANGE: None as name
    ACT:     validate_name
    ASSERT:  raises ValueError
    """
    with pytest.raises(ValueError):
        validators.validate_name(None)


def test_validate_symbol_basic() -> None:
    """
    ARRANGE: simple symbol
    ACT:     validate_symbol
    ASSERT:  returns uppercased symbol
    """
    value = "aapl"

    actual = validators.validate_symbol(value)

    assert actual == "AAPL"


def test_validate_symbol_strips_and_upper() -> None:
    """
    ARRANGE: symbol with whitespace
    ACT:     validate_symbol
    ASSERT:  returns stripped, uppercased symbol
    """
    value = " msft "

    actual = validators.validate_symbol(value)

    assert actual == "MSFT"


def test_validate_symbol_none_raises() -> None:
    """
    ARRANGE: None as symbol
    ACT:     validate_symbol
    ASSERT:  raises ValueError
    """
    with pytest.raises(ValueError):
        validators.validate_symbol(None)


def test_validate_id_str() -> None:
    """
    ARRANGE: string id with whitespace and lowercase
    ACT:     validate_id
    ASSERT:  returns stripped, uppercased id
    """
    value = " abcd1234 "

    actual = validators.validate_id(value)

    assert actual == "ABCD1234"


def test_validate_id_non_str() -> None:
    """
    ARRANGE: non-string id (int)
    ACT:     validate_id
    ASSERT:  returns value as-is
    """
    non_string_id = 1234
    value = non_string_id

    actual = validators.validate_id(value)

    assert actual == non_string_id


def test_validate_mics_none() -> None:
    """
    ARRANGE: mics is None
    ACT:     validate_mics
    ASSERT:  returns None
    """
    value = None

    actual = validators.validate_mics(value)

    assert actual is None


def test_validate_mics_empty_list() -> None:
    """
    ARRANGE: mics is empty list
    ACT:     validate_mics
    ASSERT:  returns None
    """
    value = []

    actual = validators.validate_mics(value)

    assert actual is None


def test_validate_mics_valid_list() -> None:
    """
    ARRANGE: valid mics list with duplicates and whitespace
    ACT:     validate_mics
    ASSERT:  returns unique, uppercased, stripped mics
    """
    value = [" xlon ", "XNAS", "xlon", None]

    actual = validators.validate_mics(value)

    assert actual == ["XLON", "XNAS"]


def test_validate_mics_invalid_length_raises() -> None:
    """
    ARRANGE: mics list with invalid length code
    ACT:     validate_mics
    ASSERT:  raises ValueError
    """
    value = ["XLONN"]

    with pytest.raises(ValueError):
        validators.validate_mics(value)


def test_validate_currency_basic() -> None:
    """
    ARRANGE: valid currency string
    ACT:     validate_currency
    ASSERT:  returns uppercased currency
    """
    value = "usd"

    actual = validators.validate_currency(value)

    assert actual == "USD"


def test_validate_currency_none() -> None:
    """
    ARRANGE: currency is None
    ACT:     validate_currency
    ASSERT:  returns None
    """
    value = None

    actual = validators.validate_currency(value)

    assert actual is None


def test_validate_currency_empty() -> None:
    """
    ARRANGE: currency is empty string
    ACT:     validate_currency
    ASSERT:  returns None
    """
    value = "  "

    actual = validators.validate_currency(value)

    assert actual is None


def test_validate_last_price_valid() -> None:
    """
    ARRANGE: valid price string
    ACT:     validate_last_price
    ASSERT:  returns Decimal value
    """
    value = "123.45"

    actual = validators.validate_last_price(value)

    assert actual == Decimal("123.45")


def test_validate_last_price_none() -> None:
    """
    ARRANGE: last_price is None
    ACT:     validate_last_price
    ASSERT:  returns None
    """
    value = None

    actual = validators.validate_last_price(value)

    assert actual is None


def test_validate_last_price_invalid_raises() -> None:
    """
    ARRANGE: invalid price string
    ACT:     validate_last_price
    ASSERT:  raises ValueError
    """
    value = "not_a_number"

    with pytest.raises(ValueError):
        validators.validate_last_price(value)


def test_validate_last_price_negative_raises() -> None:
    """
    ARRANGE: negative price string
    ACT:     validate_last_price
    ASSERT:  raises ValueError
    """
    value = "-10"

    with pytest.raises(ValueError):
        validators.validate_last_price(value)


def test_validate_last_price_eu_format() -> None:
    """
    ARRANGE: price in European format
    ACT:     validate_last_price
    ASSERT:  returns Decimal value
    """
    value = "1.234,56"

    actual = validators.validate_last_price(value)

    assert actual == Decimal("1234.56")


def test_validate_market_cap_valid() -> None:
    """
    ARRANGE: valid market cap string
    ACT:     validate_market_cap
    ASSERT:  returns Decimal value
    """
    value = "1000000"

    actual = validators.validate_market_cap(value)

    assert actual == Decimal("1000000")


def test_validate_market_cap_none() -> None:
    """
    ARRANGE: market_cap is None
    ACT:     validate_market_cap
    ASSERT:  returns None
    """
    value = None

    actual = validators.validate_market_cap(value)

    assert actual is None


def test_validate_market_cap_invalid_raises() -> None:
    """
    ARRANGE: invalid market cap string
    ACT:     validate_market_cap
    ASSERT:  raises ValueError
    """
    value = "not_a_number"

    with pytest.raises(ValueError):
        validators.validate_market_cap(value)


def test_validate_market_cap_negative_raises() -> None:
    """
    ARRANGE: negative market cap string
    ACT:     validate_market_cap
    ASSERT:  raises ValueError
    """
    value = "-1000"

    with pytest.raises(ValueError):
        validators.validate_market_cap(value)


def test__normalise_mic_valid() -> None:
    """
    ARRANGE: valid mic string with whitespace and lowercase
    ACT:     _normalise_mic
    ASSERT:  returns uppercased, stripped mic
    """
    value = " xlon "

    actual = validators._normalise_mic(value)

    assert actual == "XLON"


def test__normalise_mic_none() -> None:
    """
    ARRANGE: mic is None
    ACT:     _normalise_mic
    ASSERT:  returns None
    """
    value = None

    actual = validators._normalise_mic(value)

    assert actual is None


def test__normalise_numeric_text_valid() -> None:
    """
    ARRANGE: valid numeric string with plus sign
    ACT:     _normalise_numeric_text
    ASSERT:  returns cleaned numeric string
    """
    value = "+123.45"

    actual = validators._normalise_numeric_text(value)

    assert actual == "123.45"


def test__normalise_numeric_text_negative_raises() -> None:
    """
    ARRANGE: negative numeric string
    ACT:     _normalise_numeric_text
    ASSERT:  raises ValueError
    """
    value = "-123.45"

    with pytest.raises(ValueError):
        validators._normalise_numeric_text(value)


def test__convert_separators_us_style() -> None:
    """
    ARRANGE: US style number with comma as thousands separator
    ACT:     _convert_separators
    ASSERT:  returns string with no comma
    """
    value = "1,234.56"

    actual = validators._convert_separators(value)

    assert actual == "1234.56"


def test__convert_separators_eu_style() -> None:
    """
    ARRANGE: EU style number with dot as thousands and comma as decimal
    ACT:     _convert_separators
    ASSERT:  returns string with dot as decimal
    """
    value = "1.234,56"

    actual = validators._convert_separators(value)

    assert actual == "1234.56"


def test__convert_separators_only_comma() -> None:
    """
    ARRANGE: number with only comma as decimal
    ACT:     _convert_separators
    ASSERT:  returns string with dot as decimal
    """
    value = "1234,56"

    actual = validators._convert_separators(value)

    assert actual == "1234.56"


def test__convert_separators_no_sep() -> None:
    """
    ARRANGE: number with no separators
    ACT:     _convert_separators
    ASSERT:  returns string unchanged
    """
    value = "123456"

    actual = validators._convert_separators(value)

    assert actual == "123456"
