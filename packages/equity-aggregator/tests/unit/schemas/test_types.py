# schemas/test_types.py

from decimal import Decimal

import pytest
from pydantic import TypeAdapter, ValidationError

from equity_aggregator.schemas.types import (
    CurrencyStr,
    CUSIPStr,
    FIGIStr,
    ISINStr,
    MICStr,
    NonEmptyStr,
    NonNegDecimal,
)

pytestmark = pytest.mark.unit


def test_non_empty_str_valid() -> None:
    """
    ARRANGE: valid non-empty string
    ACT:     validate NonEmptyStr
    ASSERT:  value is preserved
    """
    value = TypeAdapter(NonEmptyStr).validate_python("foo")
    assert value == "foo"


def test_non_empty_str_strips_and_rejects_empty() -> None:
    """
    ARRANGE: whitespace-only string
    ACT:     validate NonEmptyStr
    ASSERT:  raises ValidationError
    """
    with pytest.raises(ValidationError):
        TypeAdapter(NonEmptyStr).validate_python("   ")


def test_isin_valid() -> None:
    """
    ARRANGE: valid ISIN string (lowercase)
    ACT:     validate ISINStr
    ASSERT:  value is preserved
    """
    value = TypeAdapter(ISINStr).validate_python("US0378331005")
    assert value == "US0378331005"


def test_isin_invalid_length() -> None:
    """
    ARRANGE: ISIN string too short
    ACT:     validate ISINStr
    ASSERT:  raises ValidationError
    """
    with pytest.raises(ValidationError):
        TypeAdapter(ISINStr).validate_python("US123")


def test_isin_invalid_pattern() -> None:
    """
    ARRANGE: ISIN with invalid character
    ACT:     validate ISINStr
    ASSERT:  raises ValidationError
    """
    with pytest.raises(ValidationError):
        TypeAdapter(ISINStr).validate_python("US!378331005")


def test_cusip_valid() -> None:
    """
    ARRANGE: valid CUSIP string
    ACT:     validate CUSIPStr
    ASSERT:  value is uppercased
    """
    value = TypeAdapter(CUSIPStr).validate_python("037833100")
    assert value == "037833100"


def test_cusip_invalid_length() -> None:
    """
    ARRANGE: CUSIP string too short
    ACT:     validate CUSIPStr
    ASSERT:  raises ValidationError
    """
    with pytest.raises(ValidationError):
        TypeAdapter(CUSIPStr).validate_python("03783")


def test_cusip_invalid_pattern() -> None:
    """
    ARRANGE: CUSIP with invalid character
    ACT:     validate CUSIPStr
    ASSERT:  raises ValidationError
    """
    with pytest.raises(ValidationError):
        TypeAdapter(CUSIPStr).validate_python("03783310!")


# --- FIGIStr ---
def test_figi_valid() -> None:
    """
    ARRANGE: valid FIGI string
    ACT:     validate FIGIStr
    ASSERT:  value is uppercased
    """
    value = TypeAdapter(FIGIStr).validate_python("BBG001S5N8V8")
    assert value == "BBG001S5N8V8"


def test_figi_invalid_length() -> None:
    """
    ARRANGE: FIGI string too short
    ACT:     validate FIGIStr
    ASSERT:  raises ValidationError
    """
    with pytest.raises(ValidationError):
        TypeAdapter(FIGIStr).validate_python("BBG001S5N8")


def test_figi_invalid_pattern() -> None:
    """
    ARRANGE: FIGI with invalid character
    ACT:     validate FIGIStr
    ASSERT:  raises ValidationError
    """
    with pytest.raises(ValidationError):
        TypeAdapter(FIGIStr).validate_python("BBG001S5N8!!")


def test_mic_valid() -> None:
    """
    ARRANGE: valid MIC string (lowercase)
    ACT:     validate MICStr
    ASSERT:  value is preserved
    """
    value = TypeAdapter(MICStr).validate_python("XLON")
    assert value == "XLON"


def test_mic_invalid_length() -> None:
    """
    ARRANGE: MIC string too short
    ACT:     validate MICStr
    ASSERT:  raises ValidationError
    """
    with pytest.raises(ValidationError):
        TypeAdapter(MICStr).validate_python("XL")


def test_mic_invalid_pattern() -> None:
    """
    ARRANGE: MIC with invalid character
    ACT:     validate MICStr
    ASSERT:  raises ValidationError
    """
    with pytest.raises(ValidationError):
        TypeAdapter(MICStr).validate_python("X!ON")


def test_currency_valid() -> None:
    """
    ARRANGE: valid currency string (lowercase)
    ACT:     validate CurrencyStr
    ASSERT:  value is preserved
    """
    value = TypeAdapter(CurrencyStr).validate_python("USD")
    assert value == "USD"


def test_currency_invalid_length() -> None:
    """
    ARRANGE: currency string too short
    ACT:     validate CurrencyStr
    ASSERT:  raises ValidationError
    """
    with pytest.raises(ValidationError):
        TypeAdapter(CurrencyStr).validate_python("US")


def test_currency_invalid_pattern() -> None:
    """
    ARRANGE: currency with invalid character
    ACT:     validate CurrencyStr
    ASSERT:  raises ValidationError
    """
    with pytest.raises(ValidationError):
        TypeAdapter(CurrencyStr).validate_python("US$")


def test_nonnegdecimal_valid() -> None:
    """
    ARRANGE: valid non-negative decimal
    ACT:     validate NonNegDecimal
    ASSERT:  value is preserved
    """
    value = TypeAdapter(NonNegDecimal).validate_python(Decimal("123.45"))
    assert value == Decimal("123.45")


def test_nonnegdecimal_zero() -> None:
    """
    ARRANGE: zero value
    ACT:     validate NonNegDecimal
    ASSERT:  value is zero
    """
    value = TypeAdapter(NonNegDecimal).validate_python(0)
    assert value == 0


def test_nonnegdecimal_negative() -> None:
    """
    ARRANGE: negative value
    ACT:     validate NonNegDecimal
    ASSERT:  raises ValidationError
    """
    with pytest.raises(ValidationError):
        TypeAdapter(NonNegDecimal).validate_python(-1)


def test_non_empty_str_strips_whitespace() -> None:
    """
    ARRANGE: string with leading/trailing whitespace
    ACT:     validate NonEmptyStr
    ASSERT:  value is stripped
    """
    value = TypeAdapter(NonEmptyStr).validate_python(" hello world   ")
    assert value == "hello world"


def test_nonnegdecimal_invalid_string() -> None:
    """
    ARRANGE: invalid string input
    ACT:     validate NonNegDecimal
    ASSERT:  raises ValidationError
    """

    with pytest.raises(ValidationError):
        TypeAdapter(NonNegDecimal).validate_python("not_a_number")
