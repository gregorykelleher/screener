# transforms/deduplicate.py

import logging
from collections.abc import AsyncIterable
from itertools import groupby

from equity_aggregator.domain._utils._merge import merge
from equity_aggregator.schemas import RawEquity

logger = logging.getLogger(__name__)


async def deduplicate(
    raw_equities: AsyncIterable[RawEquity],
) -> AsyncIterable[RawEquity]:
    """
    Deduplicate and merge RawEquity records by their `share_class_figi`.

    Consumes an asynchronous stream of RawEquity objects, groups them by their
    `share_class_figi` attribute, and merges each group into a single RawEquity using
    the `merge` function. Logs the total number of records, the number of unique FIGIs,
    and the number of duplicates found.

    Args:
        raw_equities (AsyncIterable[RawEquity]): Async iterable of RawEquity records to
            deduplicate and merge.

    Yields:
        RawEquity: The merged RawEquity record for each unique `share_class_figi`.
    """

    # materialise all raw equities into a list
    aggregated_raw_equities = [raw_equity async for raw_equity in raw_equities]

    total = len(aggregated_raw_equities)
    unique = len({equity.share_class_figi for equity in aggregated_raw_equities})

    duplicates = total - unique

    logger.info(
        "Detected %d duplicate raw equities → merged into %d unique raw equities",
        duplicates,
        unique,
    )

    # sort by ShareClassFIGI
    aggregated_raw_equities.sort(key=lambda equity: equity.share_class_figi)

    # group by ShareClassFIGI and merge each group
    for _, group in groupby(
        aggregated_raw_equities,
        key=lambda equity: equity.share_class_figi,
    ):
        yield merge(list(group))
