# equity_aggregator/__main__.py

import asyncio
import logging
import time

from equity_aggregator import (
    aggregate_canonical_equities,
    configure_logging,
    export_canonical_equities_to_jsonl_gz,
    save_canonical_equities,
)

logger = logging.getLogger(__name__)


def main() -> None:
    configure_logging()

    start = time.monotonic()

    # execute equity aggregation pipeline
    canonical_equities = asyncio.run(aggregate_canonical_equities())

    # save canonical equities to database
    save_canonical_equities(canonical_equities)

    # export canonical equities to JSONL GZ
    export_canonical_equities_to_jsonl_gz()

    logger.info(f"Found {len(canonical_equities)} canonical equities.")

    duration = time.monotonic() - start

    logger.info("Completed in %.2f seconds", duration)


if __name__ == "__main__":
    main()
