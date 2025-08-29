# pipeline/seed.py

import asyncio
import logging

from equity_aggregator.storage.data_store import save_canonical_equities

from .runner import aggregate_canonical_equities

logger = logging.getLogger(__name__)


def seed_canonical_equities() -> None:
    """
    Runs the canonical equities aggregation pipeline and seeds the database.

    This function executes the aggregation pipeline to collect canonical equities,
    then saves them to the SQLite data store.

    Args:
        None

    Returns:
        None
    """
    aggregated_canonical_equities = asyncio.run(aggregate_canonical_equities())

    save_canonical_equities(aggregated_canonical_equities)

    logger.info(
        "Saved %d canonical equities to the database",
        len(aggregated_canonical_equities),
    )
