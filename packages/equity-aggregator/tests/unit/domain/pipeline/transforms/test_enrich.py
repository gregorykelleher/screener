# tests/test_enrich.py

import asyncio
from collections.abc import AsyncIterable
from decimal import Decimal

import pytest

from equity_aggregator.domain.pipeline.transforms.enrich import (
    ValidatorFunc,
    _enrich_with_feed,
    _has_missing_fields,
    _make_validator,
    _replace_none_with_enriched,
    _safe_fetch,
    enrich,
)
from equity_aggregator.schemas import RawEquity

pytestmark = pytest.mark.unit


class GoodFeedData:
    @staticmethod
    def model_validate(record: dict[str, object]) -> "GoodFeedData":
        class _Inner:
            def model_dump(self) -> dict[str, object]:
                return record

        return _Inner()


class BadFeedData:
    @staticmethod
    def model_validate(record: dict[str, object]) -> object:
        raise ValueError("invalid")


class PartialFeedData:
    @staticmethod
    def model_validate(record: dict[str, object]) -> "PartialFeedData":
        class _Inner:
            # drop market_cap so RawEquity.model_validate will fail
            def model_dump(self) -> dict[str, object]:
                d = record.copy()
                d.pop("market_cap", None)
                return d

        return _Inner()


def _create_validator(
    model_cls: type,
) -> ValidatorFunc:
    """
    Creates and returns a validator function for the specified feed model class.

    Args:
        model_cls (type): The class of the feed model for which to create a validator.

    Returns:
        ValidatorFunc: A function that validates instances of the given model class.
    """
    return _make_validator(model_cls)


def test_has_missing_fields_true_when_any_field_none() -> None:
    """
    ARRANGE: a RawEquity with some None fields
    ACT:     call _has_missing_fields
    ASSERT:  returns True
    """
    incomplete = RawEquity(
        name="ABC",
        symbol="ABC",
        isin=None,  # missing
        mics=["XLON"],
        currency="USD",
        last_price=Decimal("10"),
        market_cap=Decimal("1000"),
    )

    assert _has_missing_fields(incomplete) is True


def test_has_missing_fields_false_when_all_fields_present() -> None:
    """
    ARRANGE: a RawEquity with no None fields
    ACT:     call _has_missing_fields
    ASSERT:  returns False
    """
    complete = RawEquity(
        name="XYZ",
        symbol="XYZ",
        isin="ISIN00000001",
        cusip="037833100",
        share_class_figi="BBG000BLNNH6",
        mics=["XLON"],
        currency="USD",
        last_price=Decimal("5"),
        market_cap=Decimal("500"),
    )

    assert _has_missing_fields(complete) is False


def test_replace_none_with_enriched_fills_only_none_fields() -> None:
    """
    ARRANGE: source with last_price None, enriched with both fields set
    ACT:     call _replace_none_with_enriched
    ASSERT:  new object has last_price from enriched, but keeps source market_cap
    """
    source = RawEquity(
        name="SRC",
        symbol="SRC",
        isin="ISIN00000002",
        mics=["XLON"],
        currency="USD",
        last_price=None,
        market_cap=Decimal("300"),
    )

    enriched = RawEquity(
        name="SRC",
        symbol="SRC",
        isin="ISIN00000002",
        mics=["XLON"],
        currency="USD",
        last_price=Decimal("25"),
        market_cap=Decimal("999"),
    )

    merged = _replace_none_with_enriched(source, enriched)

    assert (merged.last_price, merged.market_cap) == (
        Decimal("25"),
        Decimal("300"),
    )


def test_enrich_passes_through_when_no_missing_fields() -> None:
    """
    ARRANGE: an async stream of fully-populated RawEquity objects
    ACT:     run enrich() over that stream
    ASSERT:  yields the same objects in order, unchanged
    """
    first_equity = RawEquity(
        name="ONE",
        symbol="ONE",
        isin="ISIN00000003",
        mics=["XLON"],
        currency="USD",
        last_price=Decimal("1"),
        market_cap=Decimal("100"),
    )

    second_equity = RawEquity(
        name="TWO",
        symbol="TWO",
        isin="ISIN00000004",
        mics=["XLON"],
        currency="USD",
        last_price=Decimal("2"),
        market_cap=Decimal("200"),
    )

    async def source() -> AsyncIterable[RawEquity]:
        yield first_equity
        yield second_equity

    async def runner() -> list[RawEquity]:
        return [equity async for equity in enrich(source())]

    actual = asyncio.run(runner())

    symbols = sorted(equity.symbol for equity in actual)

    assert symbols == ["ONE", "TWO"]


def test__enrich_with_feed_skips_when_no_missing() -> None:
    """
    ARRANGE: fully populated RawEquity, dummy fetcher that would error if called
    ACT:     call _enrich_with_feed
    ASSERT:  returns the same object without calling fetcher
    """

    async def should_not_be_called() -> dict[str, object]:
        raise AssertionError("Fetcher was called")

    full = RawEquity(
        name="FULL",
        symbol="FULL",
        isin="ISIN00000004",
        cusip="037833100",
        share_class_figi="BBG000BLNNH6",
        mics=["XLON"],
        currency="USD",
        last_price=Decimal("4"),
        market_cap=Decimal("40"),
    )

    actual = asyncio.run(_enrich_with_feed(full, (should_not_be_called, object)))

    assert actual is full


def test_safe_fetch_timeout_returns_none() -> None:
    """
    ARRANGE: a slow fetcher that exceeds the timeout
    ACT:     call _safe_fetch with a small wait_timeout
    ASSERT:  returns None
    """

    async def slow_fetcher() -> dict[str, object]:
        await asyncio.sleep(0.05)
        return {"foo": "bar"}

    src = RawEquity(
        name="TST",
        symbol="TST",
        isin="ISIN00000004",
        cusip="037833100",
        share_class_figi="BBG000BLNNH6",
        mics=["XLON"],
        currency="USD",
        last_price=Decimal("2"),
        market_cap=Decimal("20"),
    )

    actual = asyncio.run(_safe_fetch(src, slow_fetcher, "Slow", wait_timeout=0.01))

    assert actual is None


def test_safe_fetch_exception_returns_none() -> None:
    """
    ARRANGE: a fetcher that raises an exception
    ACT:     call _safe_fetch
    ASSERT:  returns None
    """

    async def bad_fetcher() -> dict[str, object]:
        raise RuntimeError("failure")

    source = RawEquity(
        name="TST",
        symbol="TST",
        isin="ISIN00000004",
        cusip="037833100",
        share_class_figi="BBG000BLNNH6",
        mics=["XLON"],
        currency="USD",
        last_price=Decimal("2"),
        market_cap=Decimal("20"),
    )

    actual = asyncio.run(_safe_fetch(source, bad_fetcher, "Bad", wait_timeout=1.0))

    assert actual is None


def test_safe_fetch_success_returns_dict() -> None:
    """
    ARRANGE: a fetcher that returns quickly
    ACT:     call _safe_fetch
    ASSERT:  returns the dict unchanged
    """

    async def quick_fetcher(
        symbol: str,
        name: str,
        isin: str | None,
        cusip: str | None,
    ) -> dict[str, object]:
        _ = (symbol, name, isin, cusip)
        return {"foo": "bar"}

    source = RawEquity(
        name="A",
        symbol="A",
        isin="ISIN00000004",
        cusip="037833100",
        mics=["XLON"],
        currency="USD",
        last_price=Decimal("1"),
        market_cap=Decimal("1"),
    )

    actual = asyncio.run(_safe_fetch(source, quick_fetcher, "Quick", wait_timeout=1.0))

    assert actual == {"foo": "bar"}


def test_enrich_empty_stream_yields_nothing() -> None:
    """
    ARRANGE: an async stream that never yields
    ACT:     run enrich()
    ASSERT:  yields empty list
    """

    async def empty_src() -> AsyncIterable[RawEquity]:
        if False:
            yield

    async def runner() -> list[RawEquity]:
        return await asyncio.gather(*[equity async for equity in enrich(empty_src())])

    actual = asyncio.run(runner())
    assert actual == []


def test_has_missing_fields_counts_optional_fields() -> None:
    """
    ARRANGE: a RawEquity missing an optional field (cusip)
    ACT:     call _has_missing_fields
    ASSERT:  returns True
    """
    # cusip and share_class_figi default to None if not provided
    incomplete = RawEquity(
        name="OPT",
        symbol="OPT",
        isin="ISIN00000004",
        mics=["XLON"],
        currency="USD",
        last_price=Decimal("1"),
        market_cap=Decimal("10"),
    )

    assert _has_missing_fields(incomplete) is True


def test_replace_none_with_enriched_leaves_none_when_enriched_also_none() -> None:
    """
    ARRANGE: source has two None fields, enriched also None for those
    ACT:     call _replace_none_with_enriched
    ASSERT:  both fields remain None
    """
    source = RawEquity(
        name="SRC2",
        symbol="SRC2",
        isin=None,
        cusip=None,
        mics=["XLON"],
        currency="USD",
        last_price=None,
        market_cap=None,
    )

    enriched = RawEquity(
        name="SRC2",
        symbol="SRC2",
        isin=None,
        cusip=None,
        mics=["XLON"],
        currency="USD",
        last_price=None,
        market_cap=None,
    )

    merged = _replace_none_with_enriched(source, enriched)

    assert (merged.isin, merged.cusip, merged.last_price, merged.market_cap) == (
        None,
        None,
        None,
        None,
    )


def test_make_validator_returns_raw_equity() -> None:
    """
    ARRANGE: a validator from GoodFeedData
    ACT:     validate a record
    ASSERT:  returns a RawEquity
    """
    validator = _create_validator(GoodFeedData)

    raw_record = {
        "name": "VAL",
        "symbol": "VAL",
        "isin": "ISIN00000004",
        "mics": ["XLON"],
        "currency": "USD",
        "last_price": Decimal("3"),
        "market_cap": Decimal("30"),
    }

    assert isinstance(validator(raw_record), RawEquity)


def test_make_validator_returns_none_on_error() -> None:
    """
    ARRANGE: a validator from BadFeedData
    ACT:     validate a record
    ASSERT:  returns None
    """
    validator = _create_validator(BadFeedData)

    raw_record = {
        "name": "VAL",
        "symbol": "VAL",
        "isin": "ISIN00000004",
        "mics": ["XLON"],
        "currency": "USD",
        "last_price": Decimal("3"),
        "market_cap": Decimal("30"),
    }

    assert validator(raw_record) is None


def test_enrich_with_feed_falls_back_on_empty_dict() -> None:
    """
    ARRANGE: a RawEquity instance
    ACT:     call _enrich_with_feed with an empty fetcher
    ASSERT:  returns the original RawEquity unchanged
    """

    async def empty_fetcher() -> dict[str, object]:
        return {}

    source = RawEquity(
        name="E",
        symbol="E",
        isin=None,
        cusip=None,
        mics=["XLON"],
        currency="USD",
        last_price=None,
        market_cap=None,
    )

    actual = asyncio.run(_enrich_with_feed(source, (empty_fetcher, GoodFeedData)))

    assert actual is source


def test_make_validator_returns_none_for_partial_missing() -> None:
    """
    ARRANGE: a validator from PartialFeedData that has missing fields
    ACT:     validate a record
    ASSERT:  returns None since RawEquity.model_validate will fail
    """

    raw = {
        "name": "X",
        "symbol": "X",
        "isin": "I",
        "mics": ["XLON"],
        "currency": "USD",
        "last_price": Decimal("2"),
        "market_cap": Decimal("20"),
    }

    validator = _make_validator(PartialFeedData)

    assert validator(raw) is None


def test_safe_fetch_times_out_and_returns_none() -> None:
    """
    ARRANGE: slow fetcher that honours the signature and sleeps past the timeout
    ACT:     call _safe_fetch with a tight wait_timeout
    ASSERT:  returns None (TimeoutError branch)
    """

    async def slow_fetcher(
        **kwargs: dict[str, object],
    ) -> dict[str, object]:
        await asyncio.sleep(0.05)
        return {"ignored": True}

    src = RawEquity(
        name="TO",
        symbol="TO",
        isin="ISIN00000005",
        mics=["XLON"],
        currency="USD",
        last_price=Decimal("0"),
        market_cap=Decimal("0"),
    )

    actual = asyncio.run(_safe_fetch(src, slow_fetcher, "Slow", wait_timeout=0.01))

    assert actual is None


def test_enrich_with_feed_completes_success_path() -> None:
    """
    ARRANGE:  source missing financials; fetcher returns a full record
    ACT:      call _enrich_with_feed
    ASSERT:   enriched RawEquity contains the fetched last_price & market_cap
    """

    async def good_fetcher(
        symbol: str,
        name: str,
        isin: str | None,
        cusip: str | None,
    ) -> dict[str, object]:
        _ = (symbol, name, isin, cusip)
        return {
            "name": name,
            "symbol": symbol,
            "isin": isin,
            "cusip": cusip,
            "mics": ["XLON"],
            "currency": "USD",  # already USD ⇒ converter is no-op
            "last_price": Decimal("123"),
            "market_cap": Decimal("4567"),
        }

    source = RawEquity(
        name="OK",
        symbol="OK",
        isin="ISIN00000006",
        mics=["XLON"],
        currency="USD",
        last_price=None,
        market_cap=None,
    )

    enriched = asyncio.run(_enrich_with_feed(source, (good_fetcher, GoodFeedData)))

    assert (enriched.last_price, enriched.market_cap) == (
        Decimal("123"),
        Decimal("4567"),
    )
