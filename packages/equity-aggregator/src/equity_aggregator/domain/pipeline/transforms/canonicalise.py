# transforms/canonicalise.py

import logging
from collections.abc import AsyncIterable, AsyncIterator

from equity_aggregator.schemas import RawEquity
from equity_aggregator.schemas.canonical import CanonicalEquity

logger = logging.getLogger(__name__)


async def canonicalise(
    raw_equities: AsyncIterable[RawEquity],
) -> AsyncIterator[CanonicalEquity]:
    """
    Asynchronously converts a stream of RawEquity objects into CanonicalEquity objects.

    Each RawEquity is validated and transformed into a CanonicalEquity. If a required
    field (such as share_class_figi) is missing or invalid, a ValidationError is raised.

    Args:
        raw_equities (AsyncIterable[RawEquity]): An asynchronous iterable of RawEquity
            instances to be canonicalised.

    Yields:
        CanonicalEquity: The canonicalised equity object corresponding to each input.

    Raises:
        ValidationError: If incoming RawEquity is missing any required fields or is
            invalid.
    """
    canonicalised_count = 0

    async for raw_equity in raw_equities:
        canonicalised_count += 1
        yield CanonicalEquity.from_raw(raw_equity)

    logger.info("Canonicalised %d equities.", canonicalised_count)
