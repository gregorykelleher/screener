# resolvers/test_xetra.py

from decimal import Decimal

import pytest
from pydantic import ValidationError

from equity_aggregator.domain.resolvers._utils import build_raw_equity
from equity_aggregator.schemas import RawEquity

pytestmark = pytest.mark.unit


def test_build_raw_equity_xetra_basic() -> None:
    """
    ARRANGE: overview.lastPrice present, EUR currency
    ACT:     call build_raw_equity
    ASSERT:  last_price parsed to Decimal
    """
    raw = {
        "name": "BASF SE",
        "wkn": "BAS",
        "isin": "DE000BASF111",
        "currency": "EUR",
        "last_price": "45.67",
        "mics": ["XETR"],
    }

    actual: RawEquity = build_raw_equity(raw, symbol_key="wkn")

    assert actual.last_price == Decimal("45.67")


def test_build_raw_equity_xetra_missing_price() -> None:
    """
    ARRANGE: no overview.lastPrice
    ACT:     call build_raw_equity
    ASSERT:  missing last_price is set to None
    """
    raw = {
        "name": "NO PRICE AG",
        "wkn": "NOP",
        "isin": "DE000BASF111",
        "currency": "EUR",
        # neither 'overview' nor 'last_price' present
    }

    actual: RawEquity = build_raw_equity(raw, symbol_key="wkn")

    assert actual.last_price is None


def test_build_raw_equity_xetra_comma_price() -> None:
    """
    ARRANGE: lastPrice '12,34' with comma decimal
    ACT:     call build_raw_equity
    ASSERT:  converted to Decimal('12.34')
    """
    raw = {
        "name": "COMMA AG",
        "wkn": "COM",
        "isin": "DE000BASF111",
        "currency": "EUR",
        "last_price": "12,34",
    }

    actual: RawEquity = build_raw_equity(raw, symbol_key="wkn")

    assert actual.last_price == Decimal("12.34")


def test_build_raw_equity_xetra_negative_price() -> None:
    """
    ARRANGE: lastPrice '-1'
    ACT:     call build_raw_equity
    ASSERT:  raises ValidationError
    """
    raw = {
        "name": "NEG PRICE AG",
        "wkn": "NEG",
        "isin": "DE000BASF111",
        "currency": "EUR",
        "last_price": "-1",
    }

    with pytest.raises(ValidationError):
        build_raw_equity(raw, symbol_key="wkn")


def test_build_raw_equity_xetra_thousands_sep_normalised() -> None:
    """
    ARRANGE: lastPrice '1,234.00' with thousands separator
    ACT:     call build_raw_equity
    ASSERT:  normalised to Decimal('1234.00')
    """
    raw = {
        "name": "SEP AG",
        "wkn": "SEP",
        "isin": "DE000BASF111",
        "currency": "EUR",
        "last_price": "1,234.00",
    }

    actual: RawEquity = build_raw_equity(raw, symbol_key="wkn")
    assert actual.last_price == Decimal("1234.00")


def test_build_raw_equity_xetra_default_mic() -> None:
    """
    ARRANGE: omit 'mics'
    ACT:     call build_raw_equity
    ASSERT:  financial.mics == ['XETR']
    """
    raw = {
        "name": "DEF MIC AG",
        "wkn": "DEF",
        "isin": "DE000BASF111",
        "currency": "EUR",
        "last_price": "10.00",
    }

    actual: RawEquity = build_raw_equity(
        raw,
        symbol_key="wkn",
        default_mics=["XETR"],
    )

    assert actual.mics == ["XETR"]


def test_build_raw_equity_xetra_duplicate_mics_deduped() -> None:
    """
    ARRANGE: duplicate MICs with mixed case
    ACT:     call build_raw_equity
    ASSERT:  dedup → ['XETR']
    """
    raw = {
        "name": "DUP MIC AG",
        "wkn": "DUP",
        "isin": "DE000BASF111",
        "currency": "EUR",
        "last_price": "9.99",
        "mics": ["xetr", "XETR", "XETR"],
    }

    actual: RawEquity = build_raw_equity(raw, symbol_key="wkn")

    assert actual.mics == ["XETR"]


def test_build_raw_equity_xetra_currency_digit_invalid() -> None:
    """
    ARRANGE: currency 'EU1'
    ACT:     call build_raw_equity
    ASSERT:  raises ValidationError
    """
    raw = {
        "name": "BAD CURR AG",
        "wkn": "BAD",
        "isin": "DE000BASF111",
        "currency": "EU1",
        "overview": {"lastPrice": "5.55"},
    }

    with pytest.raises(ValidationError):
        build_raw_equity(raw)
