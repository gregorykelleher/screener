# resolvers/test_lse.py

from decimal import Decimal

import pytest
from pydantic import ValidationError

from equity_aggregator.domain.resolvers._utils import build_raw_equity
from equity_aggregator.domain.resolvers.lse import _convert_gbx_to_gbp
from equity_aggregator.schemas import RawEquity

pytestmark = pytest.mark.unit


def test_build_raw_equity_lse_basic() -> None:
    """
    ARRANGE: fully-populated, valid GBP row
    ACT:     call build_raw_equity
    ASSERT:  last_price parsed to Decimal
    """
    raw = {
        "issuername": "ACME PLC",
        "tidm": "ACM",
        "isin": "GB00B03MLX29",
        "currency": "GBP",
        "lastprice": "12.34",
        "mics": ["XLON"],
    }

    actual: RawEquity = build_raw_equity(
        raw,
        name_key="issuername",
        symbol_key="tidm",
        mics_key="mics",
        currency_key="currency",
        last_price_key="lastprice",
        default_mics=["XLON"],
    )

    assert actual.last_price == Decimal("12.34")


def test_build_raw_equity_gbx_conversion() -> None:
    """
    ARRANGE: currency 'GBX', lastprice in pence ('150')
    ACT:     call build_raw_equity
    ASSERT:  price÷100, currency switched to GBP
    """
    raw = {
        "issuername": "GBX TEST PLC",
        "tidm": "GBX",
        "isin": "GB00B03MLX29",
        "currency": "GBX",
        "lastprice": "150",
        # no mics → default XLON
    }

    actual: RawEquity = build_raw_equity(
        _convert_gbx_to_gbp(raw),
        name_key="issuername",
        symbol_key="tidm",
        mics_key="mics",
        currency_key="currency",
        last_price_key="lastprice",
        default_mics=["XLON"],
    )

    assert actual.last_price == Decimal("1.5") and actual.currency == "GBP"


def test_build_raw_equity_gbx_invalid_price() -> None:
    """
    ARRANGE: GBX row with non-numeric lastprice
    ACT:     call build_raw_equity
    ASSERT:  raises ValidationError
    """
    raw = {
        "issuername": "GBX FAIL PLC",
        "tidm": "GBXF",
        "isin": "GB00B03MLX29",
        "currency": "GBX",
        "lastprice": "None",
    }

    with pytest.raises(ValidationError):
        build_raw_equity(
            raw,
            name_key="issuername",
            symbol_key="tidm",
            mics_key="mics",
            currency_key="currency",
            last_price_key="lastprice",
            default_mics=["XLON"],
        )


def test_build_raw_equity_lse_missing_price() -> None:
    """
    ARRANGE: GBP row without 'lastprice'
    ACT:     call build_raw_equity
    ASSERT:  missing lastprice is set to None
    """
    raw = {
        "issuername": "NO PRICE PLC",
        "tidm": "NOP",
        "isin": "GB00B03MLX29",
        "currency": "GBP",
        # lastprice omitted
    }

    actual: RawEquity = build_raw_equity(
        raw,
        name_key="issuername",
        symbol_key="tidm",
        mics_key="mics",
        currency_key="currency",
        last_price_key="lastprice",
        default_mics=["XLON"],
    )

    # lastprice is optional, so None is valid
    assert actual.last_price is None


def test_build_raw_equity_lse_comma_price() -> None:
    """
    ARRANGE: European comma decimal in lastprice
    ACT:     call build_raw_equity
    ASSERT:  parsed to Decimal with dot
    """
    raw = {
        "issuername": "COMMA PLC",
        "tidm": "COM",
        "isin": "GB00B03MLX29",
        "currency": "GBP",
        "lastprice": "3,21",
    }

    actual: RawEquity = build_raw_equity(
        raw,
        name_key="issuername",
        symbol_key="tidm",
        mics_key="mics",
        currency_key="currency",
        last_price_key="lastprice",
        default_mics=["XLON"],
    )

    assert actual.last_price == Decimal("3.21")


def test_build_raw_equity_lse_default_mic() -> None:
    """
    ARRANGE: row without 'mics'
    ACT:     call build_raw_equity
    ASSERT:  mics == ['XLON']
    """
    raw = {
        "issuername": "DEFAULT MIC PLC",
        "tidm": "DEF",
        "isin": "GB00B03MLX29",
        "currency": "GBP",
        "lastprice": "1.00",
    }

    actual: RawEquity = build_raw_equity(
        raw,
        name_key="issuername",
        symbol_key="tidm",
        mics_key="mics",
        currency_key="currency",
        last_price_key="lastprice",
        default_mics=["XLON"],
    )

    assert actual.mics == ["XLON"]


def test_build_raw_equity_lse_duplicate_mics_deduped() -> None:
    """
    ARRANGE: duplicated 'XLON' mixed case/whitespace
    ACT:     call build_raw_equity
    ASSERT:  dedup → ['XLON']
    """
    raw = {
        "issuername": "DUP MIC PLC",
        "tidm": "DUP",
        "isin": "GB00B03MLX29",
        "currency": "GBP",
        "lastprice": "2.22",
        "mics": ["xlon ", "XLON", "XLON"],
    }

    actual: RawEquity = build_raw_equity(
        raw,
        name_key="issuername",
        symbol_key="tidm",
        mics_key="mics",
        currency_key="currency",
        last_price_key="lastprice",
        default_mics=["XLON"],
    )

    assert actual.mics == ["XLON"]


def test_build_raw_equity_lse_currency_digit_invalid() -> None:
    """
    ARRANGE: currency 'GB1'
    ACT:     call build_raw_equity
    ASSERT:  raises ValidationError
    """
    raw = {
        "issuername": "BAD CURR PLC",
        "tidm": "BAD",
        "isin": "GB00B03MLX29",
        "currency": "GB1",
        "lastprice": "2.00",
    }

    with pytest.raises(ValidationError):
        build_raw_equity(
            raw,
            name_key="issuername",
            symbol_key="tidm",
            mics_key="mics",
            currency_key="currency",
            last_price_key="lastprice",
            default_mics=["XLON"],
        )


def test_build_raw_equity_lse_negative_price() -> None:
    """
    ARRANGE: negative lastprice in GBP
    ACT:     call build_raw_equity
    ASSERT:  raises ValidationError
    """
    raw = {
        "issuername": "NEG PRICE PLC",
        "tidm": "NEG",
        "isin": "GB00B03MLX29",
        "currency": "GBP",
        "lastprice": "-5.0",
    }

    with pytest.raises(ValidationError):
        build_raw_equity(
            raw,
            name_key="issuername",
            symbol_key="tidm",
            mics_key="mics",
            currency_key="currency",
            last_price_key="lastprice",
            default_mics=["XLON"],
        )
