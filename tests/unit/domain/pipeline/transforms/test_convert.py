# transforms/test_convert.py

import asyncio
from collections.abc import AsyncGenerator
from decimal import Decimal

import pytest

from equity_aggregator.domain.pipeline.transforms import convert
from equity_aggregator.schemas import RawEquity
from equity_aggregator.storage import save_cache

pytestmark = pytest.mark.unit


def _run(
    stream: AsyncGenerator[RawEquity, None],
) -> list[RawEquity]:
    """
    Runs the asynchronous conversion of a stream of RawEquity objects and returns the
    results as a list.

    This function sets up a cache for exchange rates, then asynchronously collects all
    converted RawEquity objects from the provided stream using the `convert` function.

    Args:
        stream (AsyncGenerator[RawEquity, None]): An asynchronous generator yielding
            RawEquity objects to be converted.

    Returns:
        list[RawEquity]: A list of converted RawEquity objects.
    """

    async def runner() -> list[RawEquity]:
        """
        Asynchronously runs the conversion process on a stream of equities after setting
        up a mock exchange rate cache.

        Args:
            None

        Returns:
            list[RawEquity]: A list of RawEquity objects resulting from the conversion.
        """
        save_cache(
            "exchange_rate_api",
            {"EUR": Decimal("0.5"), "GBP": Decimal("0.25")},
        )
        return [equity async for equity in convert(stream)]

    return asyncio.run(runner())


def test_convert_empty_stream_yields_nothing() -> None:
    """
    ARRANGE: an empty async generator of RawEquity
    ACT:     convert over that generator
    ASSERT:  the result is an empty list
    """

    async def empty_gen() -> AsyncGenerator[RawEquity, None]:
        if False:
            yield  # pragma: no cover

    assert _run(empty_gen()) == []


def test_usd_equities_pass_through_unchanged() -> None:
    """
    ARRANGE: two equities already in USD
    ACT:     convert over that generator
    ASSERT:  same objects in same order
    """

    first_equity = RawEquity(
        name="A",
        symbol="A",
        currency="USD",
        last_price=Decimal("1"),
    )
    second_equity = RawEquity(
        name="B",
        symbol="B",
        currency="USD",
        last_price=Decimal("2"),
    )

    async def usd_gen() -> AsyncGenerator[RawEquity, None]:
        yield first_equity
        yield second_equity

    actual = _run(usd_gen())

    assert actual == [first_equity, second_equity]


def test_none_currency_pass_through_unchanged() -> None:
    """
    ARRANGE: one equity with currency=None
    ACT:     convert over that generator
    ASSERT:  same object returned
    """
    equity = RawEquity(name="X", symbol="X", currency=None, last_price=Decimal("5"))

    async def gen() -> AsyncGenerator[RawEquity, None]:
        yield equity

    actual = _run(gen())
    assert actual == [equity]


def test_eur_to_usd_conversion_and_usd_unaffected() -> None:
    """
    ARRANGE: one EUR equity and one USD equity
    ACT:     convert over that generator
    ASSERT:  EUR last_price divided by rate, USD unchanged
    """

    eur = RawEquity(name="E", symbol="E", currency="EUR", last_price=Decimal("1"))
    usd = RawEquity(name="U", symbol="U", currency="USD", last_price=Decimal("3"))

    async def mix_gen() -> AsyncGenerator[RawEquity, None]:
        yield eur
        yield usd

    actual = _run(mix_gen())

    # EUR @ rate 0.5 → 1 / 0.5 = 2.00
    assert (actual[0].last_price, actual[1].last_price) == (
        Decimal("2.00"),
        Decimal("3"),
    )
