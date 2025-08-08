# schemas/test_types.py

from decimal import Decimal

import pytest
from pydantic import TypeAdapter, ValidationError

from equity_aggregator.schemas.types import (
    AnalystRatingStrOpt,
    CIKStrOpt,
    CurrencyStrOpt,
    CUSIPStrOpt,
    FIGIStrOpt,
    ISINStrOpt,
    MICStrOpt,
    SignedDecOpt,
    UnsignedDecOpt,
    UpperStrOpt,
    UpperStrReq,
)

pytestmark = pytest.mark.unit


def test_non_empty_str_valid() -> None:
    """
    ARRANGE: valid non-empty string
    ACT:     validate NonEmptyStr
    ASSERT:  value is preserved
    """
    value = TypeAdapter(UpperStrReq).validate_python("foo")
    assert value == "FOO"


def test_non_empty_str_strips_and_rejects_empty() -> None:
    """
    ARRANGE: whitespace-only string
    ACT:     validate UpperStrReq
    ASSERT:  raises ValidationError
    """
    with pytest.raises(ValidationError):
        TypeAdapter(UpperStrReq).validate_python("   ")


def test_isin_valid() -> None:
    """
    ARRANGE: valid ISIN string (lowercase)
    ACT:     validate ISINStr
    ASSERT:  value is preserved
    """
    value = TypeAdapter(ISINStrOpt).validate_python("US0378331005")
    assert value == "US0378331005"


def test_isin_invalid_length() -> None:
    """
    ARRANGE: ISIN string too short
    ACT:     validate ISINStr
    ASSERT:  raises ValidationError
    """
    with pytest.raises(ValidationError):
        TypeAdapter(ISINStrOpt).validate_python("US123")


def test_isin_invalid_pattern() -> None:
    """
    ARRANGE: ISIN with invalid character
    ACT:     validate ISINStr
    ASSERT:  raises ValidationError
    """
    with pytest.raises(ValidationError):
        TypeAdapter(ISINStrOpt).validate_python("US!378331005")


def test_cusip_valid() -> None:
    """
    ARRANGE: valid CUSIP string
    ACT:     validate CUSIPStr
    ASSERT:  value is uppercased
    """
    value = TypeAdapter(CUSIPStrOpt).validate_python("037833100")
    assert value == "037833100"


def test_cusip_invalid_length() -> None:
    """
    ARRANGE: CUSIP string too short
    ACT:     validate CUSIPStr
    ASSERT:  raises ValidationError
    """
    with pytest.raises(ValidationError):
        TypeAdapter(CUSIPStrOpt).validate_python("03783")


def test_cusip_invalid_pattern() -> None:
    """
    ARRANGE: CUSIP with invalid character
    ACT:     validate CUSIPStr
    ASSERT:  raises ValidationError
    """
    with pytest.raises(ValidationError):
        TypeAdapter(CUSIPStrOpt).validate_python("03783310!")


def test_figi_valid() -> None:
    """
    ARRANGE: valid FIGI string
    ACT:     validate FIGIStr
    ASSERT:  value is uppercased
    """
    value = TypeAdapter(FIGIStrOpt).validate_python("BBG001S5N8V8")
    assert value == "BBG001S5N8V8"


def test_figi_invalid_length() -> None:
    """
    ARRANGE: FIGI string too short
    ACT:     validate FIGIStr
    ASSERT:  raises ValidationError
    """
    with pytest.raises(ValidationError):
        TypeAdapter(FIGIStrOpt).validate_python("BBG001S5N8")


def test_figi_invalid_pattern() -> None:
    """
    ARRANGE: FIGI with invalid character
    ACT:     validate FIGIStr
    ASSERT:  raises ValidationError
    """
    with pytest.raises(ValidationError):
        TypeAdapter(FIGIStrOpt).validate_python("BBG001S5N8!!")


def test_mic_valid() -> None:
    """
    ARRANGE: valid MIC string (lowercase)
    ACT:     validate MICStr
    ASSERT:  value is preserved
    """
    value = TypeAdapter(MICStrOpt).validate_python("XLON")
    assert value == "XLON"


def test_mic_invalid_length() -> None:
    """
    ARRANGE: MIC string too short
    ACT:     validate MICStr
    ASSERT:  raises ValidationError
    """
    with pytest.raises(ValidationError):
        TypeAdapter(MICStrOpt).validate_python("XL")


def test_mic_invalid_pattern() -> None:
    """
    ARRANGE: MIC with invalid character
    ACT:     validate MICStr
    ASSERT:  raises ValidationError
    """
    with pytest.raises(ValidationError):
        TypeAdapter(MICStrOpt).validate_python("X!ON")


def test_currency_valid() -> None:
    """
    ARRANGE: valid currency string (lowercase)
    ACT:     validate CurrencyStr
    ASSERT:  value is preserved
    """
    value = TypeAdapter(CurrencyStrOpt).validate_python("USD")
    assert value == "USD"


def test_currency_invalid_length() -> None:
    """
    ARRANGE: currency string too short
    ACT:     validate CurrencyStr
    ASSERT:  raises ValidationError
    """
    with pytest.raises(ValidationError):
        TypeAdapter(CurrencyStrOpt).validate_python("US")


def test_currency_invalid_pattern() -> None:
    """
    ARRANGE: currency with invalid character
    ACT:     validate CurrencyStr
    ASSERT:  raises ValidationError
    """
    with pytest.raises(ValidationError):
        TypeAdapter(CurrencyStrOpt).validate_python("US$")


def test_non_empty_str_strips_whitespace() -> None:
    """
    ARRANGE: string with leading/trailing whitespace
    ACT:     validate UpperStrReq
    ASSERT:  value is stripped
    """
    value = TypeAdapter(UpperStrReq).validate_python(" hello world   ")
    assert value == "HELLO WORLD"


def test_cik_valid() -> None:
    """
    ARRANGE: valid CIK string (10 digits, zero-padded)
    ACT:     validate CIKStr
    ASSERT:  value is preserved
    """
    value = TypeAdapter(CIKStrOpt).validate_python("0000320193")
    assert value == "0000320193"


def test_upper_str_req_valid() -> None:
    """
    ARRANGE: lower-case string
    ACT:     validate UpperStrReq
    ASSERT:  value is upper-cased
    """
    value = TypeAdapter(UpperStrReq).validate_python("foo")
    assert value == "FOO"


def test_upper_str_req_none_invalid() -> None:
    """
    ARRANGE: None value
    ACT:     validate UpperStrReq
    ASSERT:  raises ValidationError
    """
    with pytest.raises(ValidationError):
        TypeAdapter(UpperStrReq).validate_python(None)


def test_upper_str_opt_valid() -> None:
    """
    ARRANGE: lower-case string
    ACT:     validate UpperStrOpt
    ASSERT:  value is upper-cased
    """
    value = TypeAdapter(UpperStrOpt).validate_python("bar")
    assert value == "BAR"


def test_upper_str_opt_none_allowed() -> None:
    """
    ARRANGE: None value
    ACT:     validate UpperStrOpt
    ASSERT:  value is preserved
    """
    value = TypeAdapter(UpperStrOpt).validate_python(None)
    assert value is None


def test_signed_dec_opt_positive_valid() -> None:
    """
    ARRANGE: positive decimal string
    ACT:     validate SignedDecOpt
    ASSERT:  value is converted to Decimal
    """
    value = TypeAdapter(SignedDecOpt).validate_python("123.45")
    assert value == Decimal("123.45")


def test_signed_dec_opt_none_allowed() -> None:
    """
    ARRANGE: None value
    ACT:     validate SignedDecOpt
    ASSERT:  value is preserved
    """
    value = TypeAdapter(SignedDecOpt).validate_python(None)
    assert value is None


def test_signed_dec_opt_negative_valid() -> None:
    """
    ARRANGE: negative decimal string
    ACT:     validate SignedDecOpt
    ASSERT:  value is preserved
    """
    value = TypeAdapter(SignedDecOpt).validate_python("-123.45")
    assert value == Decimal("-123.45")


def test_unsigned_dec_opt_negative_invalid() -> None:
    """
    ARRANGE: negative decimal string
    ACT:     validate UnsignedDecOpt
    ASSERT:  raises ValidationError
    """
    with pytest.raises(ValidationError):
        TypeAdapter(UnsignedDecOpt).validate_python("-1")


def test_unsigned_dec_opt_positive_valid() -> None:
    """
    ARRANGE: positive decimal string
    ACT:     validate UnsignedDecOpt
    ASSERT:  value is preserved
    """
    value = TypeAdapter(UnsignedDecOpt).validate_python("1")
    assert value == Decimal("1")


def test_analyst_rating_str_valid() -> None:
    """
    ARRANGE: valid analyst rating string
    ACT:     validate AnalystRatingStr
    ASSERT:  value is upper-cased and preserved
    """
    value = TypeAdapter(AnalystRatingStrOpt).validate_python("buy")
    assert value == "BUY"


def test_analyst_rating_str_invalid_value() -> None:
    """
    ARRANGE: invalid analyst rating string
    ACT:     validate AnalystRatingStr
    ASSERT:  value is coerced to None
    """
    value = TypeAdapter(AnalystRatingStrOpt).validate_python("strong buy")
    assert value is None


def test_analyst_rating_str_none_allowed() -> None:
    """
    ARRANGE: None value
    ACT:     validate AnalystRatingStr
    ASSERT:  value is preserved (None allowed)
    """
    value = TypeAdapter(AnalystRatingStrOpt).validate_python(None)
    assert value is None
