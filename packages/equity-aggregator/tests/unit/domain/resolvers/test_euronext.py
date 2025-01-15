# resolvers/test_euronext.py

from decimal import Decimal

import pytest
from pydantic import ValidationError

from equity_aggregator.domain.resolvers._utils import build_raw_equity
from equity_aggregator.schemas import RawEquity

pytestmark = pytest.mark.unit


def test_build_raw_equity_basic() -> None:
    """
    ARRANGE: fully-populated, valid row
    ACT:     call build_raw_equity
    ASSERT:  last_price parsed to Decimal
    """
    raw = {
        "name": "AIB GROUP PLC",
        "symbol": "A5G",
        "isin": "IE00BF0L3536",
        "mics": ["XMSM"],
        "currency": "EUR",
        "last_price": "5.68",
    }

    actual: RawEquity = build_raw_equity(raw)

    assert actual.last_price == Decimal("5.68")


def test_build_raw_equity_missing_price() -> None:
    """
    ARRANGE: row without 'last_price'
    ACT:     call build_raw_equity
    ASSERT:  last_price is set to None
    """
    raw = {
        "name": "Foo plc",
        "symbol": "FOO",
        "isin": "IE00BF0L3536",
        "mics": ["XMSM"],
        "currency": "EUR",
        # 'last_price' omitted
    }

    actual: RawEquity = build_raw_equity(raw)

    # missing last_price is coerced to None
    assert actual.last_price is None


def test_build_raw_equity_invalid_price() -> None:
    """
    ARRANGE: row with non-numeric 'last_price'
    ACT:     call build_raw_equity
    ASSERT:  raises ValidationError
    """
    raw = {
        "name": "Foo plc",
        "symbol": "FOO",
        "isin": "IE00BF0L3536",
        "mics": ["XMSM"],
        "currency": "EUR",
        "last_price": "None",
    }

    with pytest.raises(ValidationError):
        build_raw_equity(raw)


def test_build_raw_equity_comma_price() -> None:
    """
    ARRANGE: European style comma decimal
    ACT:     call build_raw_equity
    ASSERT:  converted to Decimal with dot
    """
    raw = {
        "name": "Foo plc",
        "symbol": "FOO",
        "isin": "IE00BF0L3536",
        "mics": ["XMSM"],
        "currency": "EUR",
        "last_price": "1,23",
    }

    actual: RawEquity = build_raw_equity(raw)

    assert actual.last_price == Decimal("1.23")


def test_build_raw_equity_name_symbol_isin_normalised() -> None:
    """
    ARRANGE: fields contain lowercase and extra spaces
    ACT:     call build_raw_equity
    ASSERT:  actual.name is upper-cased and de-spaced
    """
    raw = {
        "name": "  acme    corp  ",
        "symbol": "bar ",
        "isin": " ie00bf0l3536 ",
        "mics": ["XPAR"],
        "currency": "EUR",
        "last_price": "1.00",
    }

    actual: RawEquity = build_raw_equity(raw)

    assert actual.name == "ACME CORP"


def test_build_raw_equity_negative_price() -> None:
    """
    ARRANGE: row with negative last_price
    ACT:     call build_raw_equity
    ASSERT:  raises ValidationError
    """
    raw = {
        "name": "NEGATIVE PRICE PLC",
        "symbol": "NEG",
        "isin": "IE00BF0L3536",
        "currency": "EUR",
        "last_price": "-10.5",
    }

    with pytest.raises(ValidationError):
        build_raw_equity(raw)


def test_build_raw_equity_none_price() -> None:
    """
    ARRANGE: last_price present but None
    ACT:     call build_raw_equity
    ASSERT:  last_price is set to None
    """
    raw = {
        "name": "NONE PRICE PLC",
        "symbol": "NON",
        "isin": "IE00BF0L3536",
        "currency": "EUR",
        "last_price": None,
    }

    actual: RawEquity = build_raw_equity(raw)

    # explicit None last_price is coerced to None
    assert actual.last_price is None


def test_build_raw_equity_missing_isin() -> None:
    """
    ARRANGE: row without 'isin'
    ACT:     call build_raw_equity
    ASSERT:  model builds successfully (ISIN is optional)
    """
    raw = {
        "name": "NO ISIN PLC",
        "symbol": "NOI",
        "currency": "EUR",
        "last_price": "1.00",
    }

    actual: RawEquity = build_raw_equity(raw)

    assert actual.isin is None


def test_build_raw_equity_invalid_isin_char() -> None:
    """
    ARRANGE: ISIN contains '!'
    ACT:     call build_raw_equity
    ASSERT:  raises ValidationError
    """
    raw = {
        "name": "BAD ISIN PLC",
        "symbol": "BAD",
        "isin": "IE00BF0L353!",
        "currency": "EUR",
        "last_price": "1.00",
    }

    with pytest.raises(ValidationError):
        build_raw_equity(raw)


def test_build_raw_equity_empty_name() -> None:
    """
    ARRANGE: name is empty string
    ACT:     call build_raw_equity
    ASSERT:  raises ValidationError
    """
    raw = {
        "name": "",
        "symbol": "EMP",
        "isin": "IE00BF0L3536",
        "currency": "EUR",
        "last_price": "2.00",
    }

    with pytest.raises(ValidationError):
        build_raw_equity(raw)


def test_build_raw_equity_mics_normalised() -> None:
    """
    ARRANGE: two MICs with lowercase & spaces
    ACT:     call build_raw_equity
    ASSERT:  MICs upper-cased and stripped
    """
    raw = {
        "name": "MIC TEST PLC",
        "symbol": "MIC",
        "isin": "IE00BF0L3536",
        "mics": ["xpar ", "xams"],
        "currency": "EUR",
        "last_price": "1.0",
    }

    actual: RawEquity = build_raw_equity(raw)

    assert actual.mics == ["XPAR", "XAMS"]


def test_build_raw_equity_bad_mic_length() -> None:
    """
    ARRANGE: MIC with only 3 chars
    ACT:     call build_raw_equity
    ASSERT:  raises ValidationError
    """
    raw = {
        "name": "BAD MIC LEN",
        "symbol": "BML",
        "isin": "IE00BF0L3536",
        "mics": ["ABC"],  # 3 letters, invalid
        "currency": "EUR",
        "last_price": "1.0",
    }

    with pytest.raises(ValidationError):
        build_raw_equity(raw)


def test_build_raw_equity_missing_mics() -> None:
    """
    ARRANGE: no 'mics' key
    ACT:     call build_raw_equity
    ASSERT:  financial.mics is None
    """
    raw = {
        "name": "NO MICS PLC",
        "symbol": "NOM",
        "isin": "IE00BF0L3536",
        "currency": "EUR",
        "last_price": "1.0",
    }

    actual: RawEquity = build_raw_equity(raw)

    assert actual.mics is None


def test_build_raw_equity_currency_whitespace_lowercase() -> None:
    """
    ARRANGE: currency supplied as ' eur ' (lowercase & padded)
    ACT:     call build_raw_equity
    ASSERT:  currency normalised to 'EUR'
    """
    raw = {
        "name": "WHITE SPACE PLC",
        "symbol": "WSP",
        "isin": "IE00BF0L3536",
        "mics": ["XLON"],
        "currency": " eur ",
        "last_price": "2.50",
    }

    actual: RawEquity = build_raw_equity(raw)

    assert actual.currency == "EUR"


def test_build_raw_equity_currency_with_digit() -> None:
    """
    ARRANGE: currency contains a digit ('US1')
    ACT:     call build_raw_equity
    ASSERT:  raises ValidationError
    """
    raw = {
        "name": "DIGIT CURR PLC",
        "symbol": "DCP",
        "isin": "IE00BF0L3536",
        "mics": ["XPAR"],
        "currency": "US1",  # invalid
        "last_price": "3.00",
    }

    with pytest.raises(ValidationError):
        build_raw_equity(raw)


def test_build_raw_equity_currency_too_long() -> None:
    """
    ARRANGE: currency length > 3 ('EURO')
    ACT:     call build_raw_equity
    ASSERT:  raises ValidationError
    """
    raw = {
        "name": "LONG CURR PLC",
        "symbol": "LCP",
        "isin": "IE00BF0L3536",
        "mics": ["XAMS"],
        "currency": "EURO",  # invalid length
        "last_price": "4.00",
    }

    with pytest.raises(ValidationError):
        build_raw_equity(raw)


def test_build_raw_equity_currency_missing() -> None:
    """
    ARRANGE: omit 'currency' field
    ACT:     call build_raw_equity
    ASSERT:  missing currency is set to None
    """
    raw = {
        "name": "NO CURR PLC",
        "symbol": "NCP",
        "isin": "IE00BF0L3536",
        "mics": ["XMSM"],
        "last_price": "5.00",
    }

    actual: RawEquity = build_raw_equity(raw)

    # missing currency is coerced to None
    assert actual.currency is None


def test_build_raw_equity_symbol_normalised() -> None:
    """
    ARRANGE: symbol given as ' bar '
    ACT:     call build_raw_equity
    ASSERT:  actual.symbol becomes 'BAR'
    """
    raw = {
        "name": "BAR PLC",
        "symbol": " bar ",
        "isin": "IE00BF0L3536",
        "mics": ["XLON"],
        "currency": "GBP",
        "last_price": "3.33",
    }

    actual: RawEquity = build_raw_equity(raw)

    assert actual.symbol == "BAR"


def test_build_raw_equity_price_zero() -> None:
    """
    ARRANGE: last_price of '0'
    ACT:     call build_raw_equity
    ASSERT:  last_price parsed to Decimal('0')
    """
    raw = {
        "name": "ZERO PRICE PLC",
        "symbol": "ZPP",
        "isin": "IE00BF0L3536",
        "mics": ["XPAR"],
        "currency": "EUR",
        "last_price": "0",
    }

    actual: RawEquity = build_raw_equity(raw)

    assert actual.last_price == Decimal("0")


def test_build_raw_equity_price_scientific_invalid() -> None:
    """
    ARRANGE: last_price set to '1e6' (scientific notation)
    ACT:     call build_raw_equity
    ASSERT:  raises ValidationError
    """
    raw = {
        "name": "SCI PRICE PLC",
        "symbol": "SCI",
        "isin": "IE00BF0L3536",
        "mics": ["XAMS"],
        "currency": "EUR",
        "last_price": "1e6",
    }

    with pytest.raises(ValidationError):
        build_raw_equity(raw)


def test_build_raw_equity_price_thousands_sep_normalised() -> None:
    """
    ARRANGE: last_price with thousands separator '1,234.56'
    ACT:     call build_raw_equity
    ASSERT:  last_price is normalised to Decimal('1234.56')
    """
    raw = {
        "name": "SEP PRICE PLC",
        "symbol": "SEP",
        "isin": "IE00BF0L3536",
        "mics": ["XMSM"],
        "currency": "EUR",
        "last_price": "1,234.56",
    }

    actual: RawEquity = build_raw_equity(raw)

    # thousands separator is stripped → Decimal('1234.56')
    assert actual.last_price == Decimal("1234.56")


def test_build_raw_equity_duplicate_mics_deduped() -> None:
    """
    ARRANGE: duplicate MIC codes
    ACT:     call build_raw_equity
    ASSERT:  duplicates removed, order preserved
    """
    raw = {
        "name": "DUP MIC PLC",
        "symbol": "DUP",
        "isin": "IE00BF0L3536",
        "mics": ["XPAR", "xpar ", "XPAR", "XLON"],
        "currency": "EUR",
        "last_price": "1.11",
    }

    actual: RawEquity = build_raw_equity(raw)

    assert actual.mics == ["XPAR", "XLON"]


def test_build_raw_equity_currency_mixed_case() -> None:
    """
    ARRANGE: currency 'gBp' (mixed case)
    ACT:     call build_raw_equity
    ASSERT:  currency upper-cased to 'GBP'
    """
    raw = {
        "name": "MIX CURR PLC",
        "symbol": "MCP",
        "isin": "IE00BF0L3536",
        "mics": ["XLON"],
        "currency": "gBp",
        "last_price": "6.66",
    }

    actual: RawEquity = build_raw_equity(raw)

    assert actual.currency == "GBP"
