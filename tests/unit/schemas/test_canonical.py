# schemas/test_canonical.py

from collections.abc import AsyncIterable

import pytest
from pydantic import ValidationError

from equity_aggregator.domain.pipeline.transforms.canonicalise import canonicalise
from equity_aggregator.schemas.canonical import CanonicalEquity
from equity_aggregator.schemas.raw import RawEquity

pytestmark = pytest.mark.unit


async def _stream(items: list[RawEquity]) -> AsyncIterable[RawEquity]:
    for item in items:
        yield item


def test_canonical_equity_from_raw_success() -> None:
    """
    ARRANGE: valid RawEquity with share_class_figi
    ACT:     build CanonicalEquity via from_raw
    ASSERT:  identity.share_class_figi equals expected FIGI
    """
    raw = RawEquity(
        name="Foo Corp.",
        symbol="FOO",
        share_class_figi="BBG000000001",
        isin="US1234567890",
        cusip="123456789",
    )

    canonical = CanonicalEquity.from_raw(raw)

    assert canonical.identity.share_class_figi == "BBG000000001"


def test_canonical_equity_from_raw_requires_figi() -> None:
    """
    ARRANGE: RawEquity missing FIGI
    ACT:     call CanonicalEquity.from_raw
    ASSERT:  raises ValidationError
    """
    raw = RawEquity(
        name="Bar Corp.",
        symbol="BAR",
        isin=None,
        cusip=None,
    )

    with pytest.raises(ValidationError):
        CanonicalEquity.from_raw(raw)


def test_canonical_equity_from_raw_requires_figi_blank() -> None:
    """
    ARRANGE: RawEquity with blank share_class_figi
    ACT:     call CanonicalEquity.from_raw
    ASSERT:  raises ValidationError
    """
    raw_equity = RawEquity(
        name="Baz Corp.",
        symbol="BAZ",
        share_class_figi="",
    )

    with pytest.raises(ValidationError):
        CanonicalEquity.from_raw(raw_equity)


async def test_canonicalise_emits_canonicals() -> None:
    """
    ARRANGE: async stream with two valid RawEquity items
    ACT:     consume canonicalise
    ASSERT:  FIGIs preserved in order
    """
    raw_equities = [
        RawEquity(name="Alpha Corp", symbol="ALPHA", share_class_figi="BBG000000001"),
        RawEquity(name="Beta Ltd", symbol="BETA", share_class_figi="BBG000000002"),
    ]

    canonicals = [equity async for equity in canonicalise(_stream(raw_equities))]

    assert [equity.identity.share_class_figi for equity in canonicals] == [
        "BBG000000001",
        "BBG000000002",
    ]


async def test_canonicalise_raises_on_invalid_item() -> None:
    """
    ARRANGE: async stream with one invalid RawEquity (missing FIGI)
    ACT:    consume canonicalise
    ASSERT:  raises ValidationError
    """
    raw_equities = [
        RawEquity(name="Gamma plc", symbol="GAMMA"),  # no share_class_figi
    ]

    with pytest.raises(ValidationError):
        _ = [equity async for equity in canonicalise(_stream(raw_equities))]
