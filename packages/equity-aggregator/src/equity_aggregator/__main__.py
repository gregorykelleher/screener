# equity_aggregator/__main__.py

import asyncio
import time

from equity_aggregator import aggregate_equity_profiles, configure_logging


def main() -> None:
    """Entrypoint: run the async main function."""
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
