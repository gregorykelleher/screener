# tests/unit/domain/pipeline/transforms/test_parse.py

import asyncio
from collections.abc import AsyncIterable
from decimal import Decimal

import pytest

from equity_aggregator.domain.pipeline.resolve import FeedRecord
from equity_aggregator.domain.pipeline.transforms.parse import parse
from equity_aggregator.schemas import (
    EuronextFeedData,
    LseFeedData,
    RawEquity,
    XetraFeedData,
)

pytestmark = pytest.mark.unit


def _run_parse(records: list[FeedRecord]) -> list[RawEquity]:
    """
    Runs the asynchronous 'parse' function on a list of FeedRecord objects and returns
    the resulting list of RawEquity objects.

    Args:
        records (list[FeedRecord]): A list of FeedRecord instances to be parsed.

    Returns:
        list[RawEquity]: A list of RawEquity objects produced by parsing input records.
    """

    async def source() -> AsyncIterable[FeedRecord]:
        for record in records:
            yield record

    async def runner() -> list[RawEquity]:
        return [equity async for equity in parse(source())]

    return asyncio.run(runner())


def test_parse_valid_euronext_record_yields_raw_equity() -> None:
    """
    ARRANGE: a valid EuronextFeedData record
    ACT:     run parse() over that single record
    ASSERT:  yields exactly one RawEquity with the expected tuple of fields
    """
    raw = {
        "name": "TEST",
        "symbol": "TST",
        "isin": None,
        "mics": ["MIC1"],
        "currency": "USD",
        "last_price": Decimal("10"),
    }
    record = FeedRecord(EuronextFeedData, raw)

    actual = _run_parse([record])

    assert [
        (
            equity.name,
            equity.symbol,
            equity.isin,
            equity.mics,
            equity.currency,
            equity.last_price,
        )
        for equity in actual
    ] == [("TEST", "TST", None, ["MIC1"], "USD", Decimal("10"))]


def test_parse_valid_lse_record_converts_gbx_and_defaults_mics() -> None:
    """
    ARRANGE: a LseFeedData record with GBX currency, pence lastprice, and no mics
    ACT:     run parse() over that single record
    ASSERT:  yields exactly one RawEquity with converted price and default mics
    """
    raw = {
        "issuername": "LSE CO",
        "tidm": "LSEC",
        "isin": None,
        "mics": None,
        "currency": "GBX",
        "lastprice": "123,45",  # 123.45 pence => £1.2345
        "marketcapitalization": "2000",
    }
    record = FeedRecord(LseFeedData, raw)

    actual = _run_parse([record])

    assert [
        (
            equity.name,
            equity.symbol,
            equity.currency,
            equity.last_price,
            equity.market_cap,
            equity.mics,
        )
        for equity in actual
    ] == [("LSE CO", "LSEC", "GBP", Decimal("1.2345"), Decimal("2000"), ["XLON"])]


def test_parse_valid_xetra_record_defaults_mics_and_flattens_fields() -> None:
    """
    ARRANGE: a XetraFeedData record with nested overview/key_data and missing mic
    ACT:     run parse() over that single record
    ASSERT:  yields exactly one RawEquity with flattened fields and default mic
    """
    raw = {
        "name": "XETRA LTD",
        "wkn": "XTL",
        "isin": "DE1234567890",
        "mic": None,
        "currency": "EUR",
        "overview": {"lastPrice": "50.00"},
        "key_data": {"marketCapitalisation": 5000},
    }
    record = FeedRecord(XetraFeedData, raw)

    actual = _run_parse([record])

    assert [
        (
            equity.name,
            equity.symbol,
            equity.isin,
            equity.mics,
            equity.currency,
            equity.last_price,
            equity.market_cap,
        )
        for equity in actual
    ] == [
        (
            "XETRA LTD",
            "XTL",
            "DE1234567890",
            ["XETR"],
            "EUR",
            Decimal("50.00"),
            Decimal("5000"),
        ),
    ]


def test_parse_handles_string_price_in_euronext() -> None:
    """
    ARRANGE: an EuronextFeedData record with last_price as a string
    ACT:     run parse() over that single record
    ASSERT:  yields exactly one RawEquity with Decimal-converted last_price
    """
    raw = {
        "name": "STR",
        "symbol": "ST",
        "isin": None,
        "mics": ["MIC1"],
        "currency": "USD",
        "last_price": "99.99",
    }
    record = FeedRecord(EuronextFeedData, raw)

    actual = _run_parse([record])

    assert [equity.last_price for equity in actual] == [Decimal("99.99")]


def test_parse_ignores_extra_fields() -> None:
    """
    ARRANGE: an EuronextFeedData record with unexpected extra keys
    ACT:     run parse() over that single record
    ASSERT:  yields exactly one RawEquity and ignores extra keys
    """
    raw = {
        "name": "EXTRA",
        "symbol": "EX",
        "isin": None,
        "mics": [],
        "currency": "USD",
        "last_price": Decimal("1"),
        "unexpected": "value",
        "another": 123,
    }
    record = FeedRecord(EuronextFeedData, raw)

    actual = _run_parse([record])

    assert all(
        "unexpected" not in equity.__dict__
        and equity.name == "EXTRA"
        and equity.last_price == Decimal("1")
        for equity in actual
    )


def test_parse_skips_invalid_records_across_feeds() -> None:
    """
    ARRANGE: a mix of valid and invalid records from multiple feeds
    ACT:     run parse() over that list
    ASSERT:  yields exactly one RawEquity with the valid symbol
    """

    valid = {
        "name": "GOOD",
        "symbol": "GDX",
        "isin": None,
        "mics": [],
        "currency": "USD",
        "last_price": Decimal("5"),
    }
    missing_symbol = {
        "name": "BAD1",
        "isin": None,
        "mics": [],
        "currency": "USD",
        "last_price": Decimal("5"),
    }
    missing_name_gbx = {
        "tidm": "B2",
        "isin": None,
        "mics": [],
        "currency": "GBX",
        "lastprice": "not_a_number",
        "marketcapitalization": "100",
    }
    missing_overview = {
        "name": "X",
        "wkn": "WX",
        "isin": None,
        "mic": "XETR",
        "currency": "EUR",
        "overview": {},
        "key_data": {},
    }

    records = [
        FeedRecord(EuronextFeedData, valid),
        FeedRecord(EuronextFeedData, missing_symbol),
        FeedRecord(LseFeedData, missing_name_gbx),
        FeedRecord(XetraFeedData, missing_overview),
    ]

    actual = _run_parse(records)

    assert [equity.symbol for equity in actual] == ["GDX", "WX"]


def test_parse_lse_record_non_gbx_pass_through() -> None:
    """
    ARRANGE: a LseFeedData record with non-GBX currency and numeric lastprice
    ACT:     run parse() over that single record
    ASSERT:  yields exactly one RawEquity with last_price unchanged & currency unchanged
    """

    raw = {
        "issuername": "ABC LTD",
        "tidm": "ABCL",
        "isin": None,
        "mics": ["XLON"],
        "currency": "GBP",
        "lastprice": "250",
        "marketcapitalization": "5000",
    }

    record = FeedRecord(LseFeedData, raw)

    actual = _run_parse([record])

    assert [(equity.last_price, equity.currency) for equity in actual] == [
        (Decimal("250"), "GBP"),
    ]


def test_parse_lse_gbx_with_none_lastprice() -> None:
    """
    ARRANGE: a LseFeedData record with GBX currency and no lastprice
    ACT:     run parse() over that single record
    ASSERT:  yields exactly one RawEquity with last_price None and currency 'GBP'
    """
    raw = {
        "issuername": "XYZ PLC",
        "tidm": "XYZ",
        "isin": None,
        "mics": None,
        "currency": "GBX",
        "lastprice": None,
        "marketcapitalization": "1000",
    }
    record = FeedRecord(LseFeedData, raw)

    actual = _run_parse([record])

    assert [
        (
            equity.name,
            equity.symbol,
            equity.currency,
            equity.last_price,
            equity.market_cap,
            equity.mics,
        )
        for equity in actual
    ] == [("XYZ PLC", "XYZ", "GBP", None, Decimal("1000"), ["XLON"])]


def test_parse_xetra_only_key_data() -> None:
    """
    ARRANGE: a XetraFeedData record with only key_data and empty overview
    ACT:     run parse() over that single record
    ASSERT:  yields exactly one RawEquity with last_price None and market_cap set
    """
    raw = {
        "name": "KEY CORP",
        "wkn": "KEY",
        "isin": "DE000KEY0001",
        "mic": "XETR",
        "currency": "EUR",
        "overview": {},
        "key_data": {"marketCapitalisation": Decimal("3000")},
    }
    record = FeedRecord(XetraFeedData, raw)

    actual = _run_parse([record])

    assert [(equity.last_price, equity.market_cap) for equity in actual] == [
        (None, Decimal("3000")),
    ]


def test_parse_missing_mics_euronext_skipped() -> None:
    """
    ARRANGE: a EuronextFeedData record missing 'mics'
    ACT:     run parse() over that single record
    ASSERT:  yields no RawEquity
    """
    raw = {
        "name": "MISS",
        "symbol": "MS",
        "isin": None,
        # "mics" key omitted
        "currency": "USD",
        "last_price": Decimal("5"),
    }
    record = FeedRecord(EuronextFeedData, raw)

    actual = _run_parse([record])

    assert actual == []


def test_parse_string_and_int_price_types_and_ordering() -> None:
    """
    ARRANGE: two EuronextFeedData records with last_price as int and float
    ACT:     run parse() over those records
    ASSERT:  yields two RawEquity in same order with correct Decimal prices
    """
    raw_int = {
        "name": "INT",
        "symbol": "IT",
        "isin": None,
        "mics": ["MIC1"],
        "currency": "USD",
        "last_price": 7,
    }
    raw_float = {
        "name": "FLT",
        "symbol": "FT",
        "isin": None,
        "mics": ["MIC1"],
        "currency": "USD",
        "last_price": 8.5,
    }

    records = [
        FeedRecord(EuronextFeedData, raw_int),
        FeedRecord(EuronextFeedData, raw_float),
    ]

    actual = _run_parse(records)

    assert [equity.last_price for equity in actual] == [
        Decimal("7"),
        Decimal("8.5"),
    ]


def test_parse_preserves_input_order_across_feeds() -> None:
    """
    ARRANGE: one Euronext, one LSE, one Xetra record in a known order
    ACT:     run parse() over that list
    ASSERT:  yields RawEquity instances in the same order
    """

    raw_euronext_data = {
        "name": "E1",
        "symbol": "E1",
        "isin": None,
        "mics": ["MIC1"],
        "currency": "USD",
        "last_price": Decimal("1"),
    }
    raw_lse_data = {
        "issuername": "L2",
        "tidm": "L2",
        "isin": None,
        "mics": ["XLON"],
        "currency": "GBP",
        "lastprice": "200",
        "marketcapitalization": "300",
    }
    raw_xetra_data = {
        "name": "X3",
        "wkn": "X3",
        "isin": "DE0000000003",
        "mic": "XETR",
        "currency": "EUR",
        "overview": {"lastPrice": "5"},
        "key_data": {"marketCapitalisation": "50"},
    }

    records = [
        FeedRecord(EuronextFeedData, raw_euronext_data),
        FeedRecord(LseFeedData, raw_lse_data),
        FeedRecord(XetraFeedData, raw_xetra_data),
    ]

    actual = _run_parse(records)

    assert [equity.symbol for equity in actual] == ["E1", "L2", "X3"]
