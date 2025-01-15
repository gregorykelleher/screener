# transforms/enrich.py

from typing import AsyncIterable

from equity_aggregator.domain.resolvers import (
    resolve_market_vendors_raw_equities as resolve,
)
from equity_aggregator.schemas import RawEquity


async def enrich(
    raw_equities_stream: AsyncIterable[RawEquity],
) -> AsyncIterable[RawEquity]:
    """
    Receives stream of unique raw equities (from exchanges)
    and enriches records with additional data sourced from market vendors.
    """
    # resolve and yield enriched raw equities
    async for enriched in resolve(raw_equities_stream):
        yield enriched
