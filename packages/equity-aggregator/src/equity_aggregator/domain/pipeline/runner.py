# pipeline/runner.py

import logging
from collections.abc import AsyncIterable
from typing import TypeVar

# from equity_aggregator.domain.pipeline.old_resolve import resolve
from equity_aggregator.domain.pipeline.resolve import resolve
from equity_aggregator.schemas import RawEquity

from .transforms import convert, deduplicate, enrich, identify, parse

logger = logging.getLogger(__name__)


T = TypeVar("T")


# TODO: temp take function to limit the number of items processed (to be removed)
async def take(
    stream: AsyncIterable[T],
    n: int,
) -> AsyncIterable[T]:
    """Yield at most the first n items from the incoming async stream."""
    count = 0
    async for item in stream:
        if count >= n:
            break
        yield item
        count += 1


async def aggregate_equity_profiles() -> list[RawEquity]:
    """
    Fetch and process raw equity data from authoritative feeds, returning unique equity
    profiles.

    This function streams raw equities through a pipeline of transforms:
      - convert: Convert prices to reference currency (USD).
      - identify: Attach identification metadata to each raw equity.
      - deduplicate: Merge duplicate raw equities.
      - enrich: Add additional data to each equity profile.

    Args:
        None

    Returns:
        list[RawEquity]: A list of unique, fully enriched equity profiles.
    """
    # resolve the stream of raw equities
    stream = resolve()

    # arrange the pipeline stages
    transforms = (
        parse,
        convert,
        identify,
        deduplicate,
        # enrich,
        # canonicalise,
    )

    # pipe stream through each transform sequentially
    for stage in transforms:
        stream = stage(stream)

    # TODO: take only the first 10 equities for now
    stream = take(stream, 10)
    stream = enrich(stream)

    # materialise the stream
    return [equity async for equity in stream]
