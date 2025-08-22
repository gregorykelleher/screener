# pipeline/runner.py

import logging

from equity_aggregator.domain.pipeline.resolve import resolve
from equity_aggregator.schemas import RawEquity

from .transforms import canonicalise, convert, deduplicate, enrich, identify, parse

logger = logging.getLogger(__name__)


async def aggregate_canonical_equities() -> list[RawEquity]:
    """
    Fetch and process raw equity data from authoritative feeds, returning unique
        canonical equities.

    This function streams raw equities through a pipeline of transforms:
      - convert: Convert prices to reference currency (USD).
      - identify: Attach identification metadata to each raw equity.
      - deduplicate: Merge duplicate raw equities.
      - enrich: Add additional data to each raw equity.
      - canonicalise: Convert raw equities to canonical form.

    Args:
        None

    Returns:
        list[RawEquity]: A list of unique, fully enriched canonical equities.
    """
    # resolve the stream of raw equities
    stream = resolve()

    # arrange the pipeline stages
    transforms = (
        parse,
        convert,
        identify,
        deduplicate,
        enrich,
        canonicalise,
    )

    # pipe stream through each transform sequentially
    for stage in transforms:
        stream = stage(stream)

    # materialise and return the stream
    return [equity async for equity in stream]
