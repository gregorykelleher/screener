# equity_aggregator/__main__.py

import asyncio
import logging
import time

from equity_aggregator import aggregate_equity_profiles, configure_logging
from equity_aggregator.adapters.data_sources.authoritative_feeds.xetra import (
    fetch_equity_records as fetch_xetra_equities,
)

logger = logging.getLogger(__name__)


async def main_async() -> None:
    """Async entrypoint: stream and print Xetra equities."""
    logger = logging.getLogger(__name__)
    logger.info("Starting Xetra equities fetch…")

    # Stream each record as it arrives
    async for equity in fetch_xetra_equities():
        print(equity)
        print()

    logger.info("Finished streaming Xetra equities.")


def main() -> None:
    configure_logging()

    import logging

    logger = logging.getLogger(__name__)

    start = time.monotonic()
    profiles = asyncio.run(aggregate_equity_profiles())

    if not profiles:
        logger.error("No equity profiles found.")
        return
    logger.info(f"Found {len(profiles)} equity profiles.")
    for p in profiles:
        logger.info(
            "{name=%s, symbol=%s, isin=%s, FIGI=%s, mics=%s, currency=%s, "
            "last_price=%s, market_cap=%s}",
            p.name,
            p.symbol,
            p.isin or "-",
            p.share_class_figi or "-",
            ",".join(p.mics or []),
            p.currency or "-",
            p.last_price if p.last_price is not None else "-",
            p.market_cap if p.market_cap is not None else "-",
        )

    duration = time.monotonic() - start
    logging.info("Completed in %.2f seconds", duration)


if __name__ == "__main__":
    main()
