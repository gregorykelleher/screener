# schemas/test_validators.py

from decimal import Decimal

import pytest

from equity_aggregator.schemas import validators

pytestmark = pytest.mark.unit


def test_require_non_empty_valid() -> None:
    """
    ARRANGE: non-empty string and dummy info
    ACT:     require_non_empty
    ASSERT:  returns the original string
    """
    info = type("Info", (), {"field_name": "field"})()
    value = "FOO"

    actual = validators.require_non_empty(value, info)

    assert actual == "FOO"


def test_require_non_empty_none_raises() -> None:
    """
    ARRANGE: None value and dummy info
    ACT:     require_non_empty
    ASSERT:  raises ValueError
    """
    info = type("Info", (), {"field_name": "field"})()

    with pytest.raises(ValueError):
        validators.require_non_empty(None, info)


def test_require_non_empty_blank_raises() -> None:
    """
    ARRANGE: blank string and dummy info
    ACT:     require_non_empty
    ASSERT:  raises ValueError
    """
    info = type("Info", (), {"field_name": "field"})()
    value = "   "

    with pytest.raises(ValueError):
        validators.require_non_empty(value, info)


def test_to_upper_basic() -> None:
    """
    ARRANGE: simple lower-case string
    ACT:     to_upper
    ASSERT:  returns upper-cased string
    """
    value = "foo"

    actual = validators.to_upper(value)

    assert actual == "FOO"


def test_to_upper_whitespace_and_punct() -> None:
    """
    ARRANGE: string with whitespace and punctuation
    ACT:     to_upper
    ASSERT:  returns cleaned, upper-cased string
    """
    value = "  Foo-Bar!  Ltd.  "

    actual = validators.to_upper(value)

    assert actual == "FOO BAR LTD"


def test_to_upper_optional_none() -> None:
    """
    ARRANGE: None value with required=False
    ACT:     to_upper
    ASSERT:  returns None
    """
    value = None

    actual = validators.to_upper(value)

    assert actual is None


def test_to_upper_blank_string_optional() -> None:
    """
    ARRANGE: blank string with required=False
    ACT:     to_upper
    ASSERT:  returns None
    """
    value = "   "

    actual = validators.to_upper(value)

    assert actual is None


def test_to_unsigned_decimal_valid() -> None:
    """
    ARRANGE: US-style numeric string
    ACT:     to_unsigned_decimal
    ASSERT:  returns correct Decimal
    """
    info = type("Info", (), {"field_name": "field"})()
    value = "123.45"

    actual = validators.to_unsigned_decimal(value, info)

    assert actual == Decimal("123.45")


def test_to_unsigned_decimal_eu_format() -> None:
    """
    ARRANGE: EU-style numeric string
    ACT:     to_unsigned_decimal
    ASSERT:  returns correct Decimal
    """
    info = type("Info", (), {"field_name": "field"})()
    value = "1.234,56"

    actual = validators.to_unsigned_decimal(value, info)

    assert actual == Decimal("1234.56")


def test_to_unsigned_decimal_decimal_input() -> None:
    """
    ARRANGE: Decimal instance
    ACT:     to_unsigned_decimal
    ASSERT:  returns identical Decimal
    """
    info = type("Info", (), {"field_name": "field"})()
    value = Decimal("99.99")

    actual = validators.to_unsigned_decimal(value, info)

    assert actual == Decimal("99.99")


def test_to_unsigned_decimal_float_input() -> None:
    """
    ARRANGE: float value
    ACT:     to_unsigned_decimal
    ASSERT:  returns Decimal representation
    """
    info = type("Info", (), {"field_name": "field"})()
    value = 123.45

    actual = validators.to_unsigned_decimal(value, info)

    assert actual == Decimal("123.45")


def test_to_unsigned_decimal_none() -> None:
    """
    ARRANGE: None value
    ACT:     to_unsigned_decimal
    ASSERT:  returns None
    """
    info = type("Info", (), {"field_name": "field"})()
    value = None

    actual = validators.to_unsigned_decimal(value, info)

    assert actual is None


def test_to_unsigned_decimal_negative_raises() -> None:
    """
    ARRANGE: negative numeric string
    ACT:     to_unsigned_decimal
    ASSERT:  raises ValueError
    """
    info = type("Info", (), {"field_name": "field"})()
    value = "-10"

    with pytest.raises(ValueError):
        validators.to_unsigned_decimal(value, info)


def test_to_unsigned_decimal_invalid_raises() -> None:
    """
    ARRANGE: non-numeric string
    ACT:     to_unsigned_decimal
    ASSERT:  raises ValueError
    """
    info = type("Info", (), {"field_name": "field"})()
    value = "not_a_number"

    with pytest.raises(ValueError):
        validators.to_unsigned_decimal(value, info)


def test_to_analyst_rating_valid_buy() -> None:
    """
    ARRANGE: valid analyst rating string
    ACT:     to_analyst_rating
    ASSERT:  returns canonical upper-cased rating
    """
    value = "buy"

    actual = validators.to_analyst_rating(value)

    assert actual == "BUY"


def test_to_analyst_rating_invalid_value() -> None:
    """
    ARRANGE: invalid analyst rating string
    ACT:     to_analyst_rating
    ASSERT:  returns None
    """
    value = "outperform"

    actual = validators.to_analyst_rating(value)

    assert actual is None


def test__parse_numeric_text_valid() -> None:
    """
    ARRANGE: numeric string with leading '+'
    ACT:     _parse_numeric_text
    ASSERT:  returns cleaned numeric string
    """
    value = "+123.45"

    actual = validators._parse_numeric_text(value)

    assert actual == "123.45"


def test__parse_numeric_text_thousands_sep() -> None:
    """
    ARRANGE: numeric string with thousands separator
    ACT:     _parse_numeric_text
    ASSERT:  returns cleaned numeric string
    """
    value = "1.234,56"

    actual = validators._parse_numeric_text(value)

    assert actual == "1234.56"


def test__parse_numeric_text_none() -> None:
    """
    ARRANGE: None value
    ACT:     _parse_numeric_text
    ASSERT:  returns None
    """
    value = None

    actual = validators._parse_numeric_text(value)

    assert actual is None


def test__parse_numeric_text_negative_preserved() -> None:
    """
    ARRANGE: negative numeric string
    ACT:     _parse_numeric_text
    ASSERT:  value is preserved
    """
    value = "-123.45"

    assert validators._parse_numeric_text(value) == "-123.45"


def test__convert_separators_us_style() -> None:
    """
    ARRANGE: US-style number with ',' thousands separator
    ACT:     _convert_separators
    ASSERT:  returns string without commas
    """
    value = "1,234.56"

    actual = validators._convert_separators(value)

    assert actual == "1234.56"


def test__convert_separators_eu_style() -> None:
    """
    ARRANGE: EU-style number with '.' thousands and ',' decimal
    ACT:     _convert_separators
    ASSERT:  returns dot-decimal string
    """
    value = "1.234,56"

    actual = validators._convert_separators(value)

    assert actual == "1234.56"


def test__convert_separators_only_comma() -> None:
    """
    ARRANGE: number with only comma as decimal
    ACT:     _convert_separators
    ASSERT:  returns dot-decimal string
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


def test__convert_separators_us_large() -> None:
    """
    ARRANGE: large US-style number with multiple commas
    ACT:     _convert_separators
    ASSERT:  returns string without commas
    """
    value = "1,234,567.89"

    actual = validators._convert_separators(value)

    assert actual == "1234567.89"


def test__convert_separators_eu_large() -> None:
    """
    ARRANGE: large EU-style number with multiple dots and one comma
    ACT:     _convert_separators
    ASSERT:  returns dot-decimal string
    """
    value = "1.234.567,89"

    actual = validators._convert_separators(value)

    assert actual == "1234567.89"
