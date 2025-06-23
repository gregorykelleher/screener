# tests/test_identify.py

import asyncio
from collections.abc import AsyncIterable, Sequence

import pytest

from equity_aggregator.domain.pipeline.transforms.identify import (
    _generate_updates,
    _update,
    identify,
)
from equity_aggregator.schemas import RawEquity

pytestmark = pytest.mark.unit


def _make_dummy_equity(
    name: str = "A",
    symbol: str = "A",
    share_class_figi: str | None = None,
    isin: str = "ISIN00000001",
    cusip: str = "037833100",
) -> RawEquity:
    return RawEquity(
        name=name,
        symbol=symbol,
        isin=isin,
        cusip=cusip,
        mics=["XLON"],
        currency="USD",
        last_price=1,
        market_cap=10,
        share_class_figi=share_class_figi,
    )


def test_update_returns_none_if_figi_none() -> None:
    """
    ARRANGE: a RawEquity and metadata with figi None
    ACT:     call _update
    ASSERT:  returns None
    """
    raw_equity = _make_dummy_equity()

    identification_metadata = ("B", "B", None)

    assert _update(raw_equity, identification_metadata) is None


def test_update_returns_actual_equity() -> None:
    """
    ARRANGE: a RawEquity and metadata with valid figi
    ACT:     call _update
    ASSERT:  returns actual RawEquity
    """
    raw_equity = _make_dummy_equity()

    identification_metadata = ("B", "B", "BBG000BLNNH6")

    actual_equity = _update(raw_equity, identification_metadata)

    assert actual_equity.share_class_figi == "BBG000BLNNH6"


def test_generate_updates_yields_only_with_figi() -> None:
    """
    ARRANGE: list of RawEquity and metadata, some with figi None
    ACT:     call _generate_updates
    ASSERT:  yields only actual RawEquity with figi
    """
    raw_equities = [_make_dummy_equity("A", "A"), _make_dummy_equity("B", "B")]

    id_metadata = [
        ("A", "A", "BBG000BLNNH6"),
        ("B", "B", None),
    ]

    actual_equities = list(
        _generate_updates(raw_equities, id_metadata),
    )

    assert len(actual_equities) == 1


def test_generate_updates_applies_metadata() -> None:
    """
    ARRANGE: RawEquity and metadata with new name/symbol/figi
    ACT:     call _generate_updates
    ASSERT:  yields actual RawEquity with new fields
    """
    raw_equities = [_make_dummy_equity("A", "A")]

    id_metadata = [
        ("NEWNAME", "NEWSYM", "BBG000BLNNH6"),
    ]

    actual_equity = next(_generate_updates(raw_equities, id_metadata))

    assert (
        actual_equity.name,
        actual_equity.symbol,
        actual_equity.share_class_figi,
    ) == ("NEWNAME", "NEWSYM", "BBG000BLNNH6")


def test_identify_empty_stream_yields_nothing() -> None:
    """
    ARRANGE: an async stream that yields nothing
    ACT:     run identify()
    ASSERT:  yields nothing
    """

    async def empty_src() -> AsyncIterable[RawEquity]:
        if False:
            yield

    async def runner() -> list[RawEquity]:
        return [raw_equity async for raw_equity in identify(empty_src())]

    actual = asyncio.run(runner())
    assert actual == []


def test_update_retains_original_name_and_symbol_when_metadata_is_none() -> None:
    """
    ARRANGE: a RawEquity and metadata with None for name and symbol, valid figi
    ACT:     call _update
    ASSERT:  returns actual RawEquity with original name and symbol
    """
    raw_equity = _make_dummy_equity(name="OriginalName", symbol="OriginalSymbol")

    identification_metadata = (None, None, "BBG000BLNNH6")

    actual_equity = _update(raw_equity, identification_metadata)

    assert (
        actual_equity.name,
        actual_equity.symbol,
    ) == ("ORIGINALNAME", "ORIGINALSYMBOL")


def test_generate_updates_empty_inputs() -> None:
    """
    ARRANGE: empty lists for raw_equities and metadata
    ACT:     call _generate_updates
    ASSERT:  yields nothing
    """
    raw_equities = []

    id_metadata = []

    actual_equities = list(
        _generate_updates(raw_equities, id_metadata),
    )

    assert actual_equities == []


def test_generate_updates_metadata_shorter_than_equities() -> None:
    """
    ARRANGE: more raw_equities than metadata
    ACT:     call _generate_updates
    ASSERT:  only pairs up to the length of metadata are processed
    """
    raw_equities = [
        _make_dummy_equity("A", "A"),
        _make_dummy_equity("B", "B"),
    ]

    id_metadata = [
        ("A", "A", "BBG000BLNNH6"),
    ]

    actual_equities = list(
        _generate_updates(raw_equities, id_metadata),
    )

    assert len(actual_equities) == 1


def test_generate_updates_metadata_longer_than_equities() -> None:
    """
    ARRANGE: more metadata than raw_equities
    ACT:     call _generate_updates
    ASSERT:  only pairs up to the length of raw_equities are processed
    """
    raw_equities = [_make_dummy_equity("A", "A")]

    id_metadata = [
        ("A", "A", "BBG000BLNNH6"),
        ("B", "B", "BBG000BLNNH6"),
    ]

    actual_equities = list(
        _generate_updates(raw_equities, id_metadata),
    )

    assert len(actual_equities) == 1


def test_update_fallback_symbol_only() -> None:
    """
    ARRANGE: RawEquity and metadata with new name, None symbol, valid figi
    ACT:     call _update
    ASSERT:  name is actual, symbol falls back to original
    """
    raw_equity = _make_dummy_equity(name="OrigName", symbol="OrigSym")

    id_metadata = ("NewName", None, "FIGI123")

    actual = _update(raw_equity, id_metadata)

    assert (actual.name, actual.symbol) == ("NewName", "ORIGSYM")


def test_update_does_not_mutate_original() -> None:
    """
    ARRANGE: RawEquity and metadata with valid figi
    ACT:     call _update
    ASSERT:  original RawEquity remains unchanged and returned object is actual
    """
    raw_equity = _make_dummy_equity(name="X", symbol="Y", share_class_figi=None)

    id_metadata = ("X2", "Y2", "FIGI999")

    actual = _update(raw_equity, id_metadata)

    assert (
        raw_equity.name,
        raw_equity.symbol,
        raw_equity.share_class_figi,
        actual.name,
        actual.symbol,
        actual.share_class_figi,
    ) == ("X", "Y", None, "X2", "Y2", "FIGI999")


def test_generate_updates_preserves_input_order() -> None:
    """
    ARRANGE: three RawEquity items with interleaved figi validity
    ACT:     call _generate_updates
    ASSERT:  yields only the valid ones, in original order
    """
    first_equity = _make_dummy_equity("A", "A")
    second_equity = _make_dummy_equity("B", "B")
    third_equity = _make_dummy_equity("C", "C")

    raw_equities = [first_equity, second_equity, third_equity]

    id_metadata = [
        ("A", "A", "FIG1"),
        ("B", "B", None),
        ("C", "C", "FIG3"),
    ]

    actual = list(_generate_updates(raw_equities, id_metadata))

    # should yield first_equity then third_equity, in that order
    assert [u.share_class_figi for u in actual] == ["FIG1", "FIG3"]


def test_identify_yields_only_with_figi() -> None:
    """
    ARRANGE: async stream of two RawEquity, only first gets FIGI
    ACT:     run identify()
    ASSERT:  yields only updated RawEquity with FIGI
    """
    equities = [
        _make_dummy_equity("A", "A"),
        _make_dummy_equity("B", "B"),
    ]

    async def src() -> AsyncIterable[RawEquity]:
        for equity in equities:
            yield equity

    async def fetch_fn(_: Sequence[RawEquity]) -> list[tuple[str, str, str | None]]:
        return [
            ("A", "A", "FIGI1"),
            ("B", "B", None),
        ]

    async def runner() -> list[RawEquity]:
        return [equity async for equity in identify(src(), fetch_fn=fetch_fn)]

    actual = asyncio.run(runner())

    assert [equity.share_class_figi for equity in actual] == ["FIGI1"]


async def test_identify_batches_entire_stream() -> None:
    """
    ARRANGE: async stream of two RawEquity
    ACT:     run identify()
    ASSERT:  fetch_fn receives the complete batch of RawEquity
    """
    equities = [
        _make_dummy_equity("X", "X"),
        _make_dummy_equity("Y", "Y"),
    ]
    captured: list[RawEquity] = []

    async def src() -> AsyncIterable[RawEquity]:
        for equity in equities:
            yield equity

    async def fetch_fn(batch: Sequence[RawEquity]) -> list[tuple[str, str, str | None]]:
        captured.extend(batch)
        return [
            ("X", "X", None),
            ("Y", "Y", None),
        ]

    async for _ in identify(src(), fetch_fn=fetch_fn):
        pass

    assert tuple(captured) == tuple(equities)


def test_identify_updates_fields_from_metadata() -> None:
    """
    ARRANGE: async stream with one RawEquity, metadata provides new name/symbol/FIGI
    ACT:     run identify()
    ASSERT:  returned RawEquity has updated fields
    """
    equity = _make_dummy_equity("OldName", "OldSym")

    async def src() -> AsyncIterable[RawEquity]:
        yield equity

    async def fetch_fn(_: Sequence[RawEquity]) -> list[tuple[str, str, str | None]]:
        return [("NewName", "NewSym", "FIGI123")]

    async def runner() -> list[RawEquity]:
        return [equity async for equity in identify(src(), fetch_fn=fetch_fn)]

    actual = asyncio.run(runner())[0]

    assert (
        actual.name,
        actual.symbol,
        actual.share_class_figi,
    ) == ("NewName", "NewSym", "FIGI123")
