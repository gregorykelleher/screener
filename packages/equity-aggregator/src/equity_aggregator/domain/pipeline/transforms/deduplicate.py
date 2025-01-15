# transforms/deduplicate.py

import logging
from collections import defaultdict
from typing import AsyncIterable, Dict, List

from equity_aggregator.schemas import RawEquity

from ._merge import merge

logger = logging.getLogger(__name__)


async def deduplicate(
    raw_equities: AsyncIterable[RawEquity],
) -> AsyncIterable[RawEquity]:
    """
    Merge all RawEquity records by their `share_class_figi`
    yielding one merged record per FIGI.

    - Consume the stream of raw equities once, grouping each record by its `share_class_figi`.
    - Log the number of records merged (i.e. deduplicated).
    - Merge the equities in each group and yield the result.
    """

    # group equities by their share_class_figi
    groups_by_figi: Dict[str | None, List[RawEquity]] = defaultdict(list)

    async for equity in raw_equities:
        groups_by_figi[equity.share_class_figi].append(equity)

    # log the number of duplicates
    _log_number_deduplicates(groups_by_figi)

    # Merge and yield the equities in each group
    for group in groups_by_figi.values():
        yield merge(group)


def _log_number_deduplicates(groups: Dict[str | None, List[RawEquity]]) -> None:
    """
    Log how many raw equities were merged.
    """
    total = sum(len(lst) for lst in groups.values())
    unique = len(groups)
    logger.info(
        "Deduplicated %d total raw equities → %d unique raw equities (merged %d duplicates)",
        total,
        unique,
        total - unique,
    )
