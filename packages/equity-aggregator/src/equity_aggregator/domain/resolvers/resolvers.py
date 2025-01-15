# transforms/resolvers.py

import asyncio
from typing import AsyncIterable

from equity_aggregator.schemas import RawEquity

from .exchanges import (
    resolve_euronext_equities,
    resolve_xetra_equities,
    resolve_lse_equities,
)

from .market_vendors import resolve_yfinance_equities


async def resolve_exchange_raw_equities() -> AsyncIterable[RawEquity]:
    """
    Fetches and resolves raw equities from various exchanges.

    - Launch Euronext, Xetra and LSE fetches concurrently.
    - As soon as one exchange dataset is ready, yield its equities one by one.
    - Keeps memory flat and starts downstream processing immediately.
    """

    tasks = [
        asyncio.create_task(resolve_euronext_equities()),
        asyncio.create_task(resolve_xetra_equities()),
        asyncio.create_task(resolve_lse_equities()),
    ]

    for task in asyncio.as_completed(tasks):
        # Wait for the task to complete and get the result
        resolved_raw_equities = await task

        # Yield each resolved raw equity
        for raw_equity in resolved_raw_equities:
            yield raw_equity


async def resolve_market_vendors_raw_equities(
    raw_equities: AsyncIterable[RawEquity],
) -> AsyncIterable[RawEquity]:
    """
    Receives stream of unique raw equities (from exchanges), resolves them,
    and enriches records with additional data sourced from market vendors.

    - Launch yfinance fetches concurrently.
    - As soon as one exchange dataset is ready, yield its equities one by one.
    - Keeps memory flat and starts downstream processing immediately.
    """
    async for resolved_raw_equities in resolve_yfinance_equities(raw_equities):
        yield resolved_raw_equities
