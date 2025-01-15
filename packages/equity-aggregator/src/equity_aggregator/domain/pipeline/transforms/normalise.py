# transforms/normalise.py

import logging
from typing import AsyncIterable

from equity_aggregator.schemas import RawEquity
from equity_aggregator.domain._utils import get_usd_converter

logger = logging.getLogger(__name__)


async def normalise(
    raw_equities: AsyncIterable[RawEquity],
) -> AsyncIterable[RawEquity]:
    """
    Normalise each RawEquity record to USD as it is streamed.

    - Fetches the FX rates once.
    - Builds a converter function that uses these rates for conversion.
    - Iterates over the input raw equities, yielding each with its price converted to USD.
    """
    convert_to_usd = await get_usd_converter()

    async for equity in raw_equities:
        yield convert_to_usd(equity)
