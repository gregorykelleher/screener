# pipeline/runner.py

import logging
from typing import AsyncIterable, List, TypeVar

from equity_aggregator.domain.resolvers import resolve_exchange_raw_equities as resolve
from equity_aggregator.schemas import RawEquity

from .transforms import normalise, identify, deduplicate, enrich

logger = logging.getLogger(__name__)


T = TypeVar("T")


# TODO: temp take function to limit the number of items processed (to be removed)
async def take(
    stream: AsyncIterable[T],
    n: int,
) -> AsyncIterable[T]:
    """
    Yield at most the first n items from the incoming async stream.
    """
    count = 0
    async for item in stream:
        if count >= n:
            break
        yield item
        count += 1


async def aggregate_equity_profiles() -> List[RawEquity]:
    """
    Fetch exchange raw equities and stream records for the
    transforms pipeline and return a fully-materialised list
    of unique equity profiles.

    The transforms pipeline consists of the following stages:
    - normalise: convert prices to reference currency USD
    - identify: attach shareClassFIGI to each raw equity
    - deduplicate: merge raw equity duplicates
    """
    # resolve the stream of raw equities
    stream = resolve()

    # arrange the pipeline stages
    transforms = (
        normalise,
        identify,
        deduplicate,
        # enrich,
        # canonicalise,
    )

    # pipe stream through each transform sequentially
    for stage in transforms:
        stream = stage(stream)

    # TODO: take only the first 10 equities for now
    stream = take(stream, 25)
    stream = enrich(stream)

    # materialise the stream
    return [equity async for equity in stream]
