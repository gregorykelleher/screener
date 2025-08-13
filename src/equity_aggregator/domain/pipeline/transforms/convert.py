# transforms/convert.py

import logging
from collections.abc import AsyncIterable

from equity_aggregator.domain._utils import get_usd_converter
from equity_aggregator.schemas import RawEquity

logger = logging.getLogger(__name__)


async def convert(
    raw_equities: AsyncIterable[RawEquity],
) -> AsyncIterable[RawEquity]:
    """
    Convert each RawEquity record's price to USD as it is streamed.

    This function:
      - Fetches FX rates once and builds a converter for price conversion.
      - Iterates over the input stream, yielding each RawEquity with its price in USD.

    Args:
        raw_equities (AsyncIterable[RawEquity]): Stream of RawEquity records
            to convert.

    Returns:
        AsyncIterable[RawEquity]: Stream of RawEquity records with prices converted
            to USD.
    """
    convert_to_usd = await get_usd_converter()
    converted_count = 0

    async for equity in raw_equities:
        converted_count += 1
        yield convert_to_usd(equity)

    logger.info("Converted %d raw equities.", converted_count)
