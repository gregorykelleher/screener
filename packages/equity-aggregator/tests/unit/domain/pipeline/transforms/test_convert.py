# transforms/test_convert.py

import asyncio
import os
from collections.abc import AsyncGenerator, Iterator
from decimal import Decimal

import httpx
import pytest
from respx import MockRouter

from equity_aggregator.domain.pipeline.transforms import convert
from equity_aggregator.schemas import RawEquity

pytestmark = pytest.mark.unit


# small fixture to stub the external Exchange Rate API call
@pytest.fixture(autouse=True)
def fake_fx_api(respx_mock: MockRouter) -> Iterator[None]:
    """
    Fixture to set the EXCHANGE_RATE_API_KEY environment variable and mock the HTTP
    call to the external exchange rate API, returning fixed conversion rates for tests.

    Args:
        respx_mock (MockRouter): The respx mock router for HTTPX requests.

    Yields:
        None: This is a pytest fixture for use in test setup.
    """
    os.environ["EXCHANGE_RATE_API_KEY"] = "dummy-key"
    url = "https://v6.exchangerate-api.com/v6/dummy-key/latest/USD"
    respx_mock.get(url).mock(
        return_value=httpx.Response(
            200,
            json={
                "result": "success",
                "conversion_rates": {
                    "EUR": 0.5,  # 1 USD == 0.5 EUR → 1 EUR == 2 USD
                    "GBP": 0.25,  # 1 USD == 0.25 GBP → 1 GBP == 4 USD
                },
            },
        ),
    )
    yield


# helper function to run the async convert() function and return the results
def _run(stream: AsyncGenerator[RawEquity, None]) -> list[RawEquity]:
    async def runner() -> list[RawEquity]:
        return [raw_equity async for raw_equity in convert(stream)]

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
