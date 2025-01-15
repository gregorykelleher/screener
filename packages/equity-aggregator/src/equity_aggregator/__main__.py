# equity_aggregator/__main__.py

import logging
import asyncio
import time

from equity_aggregator import aggregate_equity_profiles

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(module)-20s %(levelname)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)


def main() -> None:
    """
    Entrypoint: run the async main function.
    """
    start = time.monotonic()
    profiles = asyncio.run(aggregate_equity_profiles())

    if not profiles:
        logging.error("No equity profiles found.")
        return
    logging.info(f"Found {len(profiles)} equity profiles.")
    for p in profiles:
        logging.info(
            "Equity: name=%s, symbol=%s, isin=%s, FIGI=%s, mics=%s, currency=%s, price=%s",
            p.name,
            p.symbol,
            p.isin or "-",
            p.share_class_figi or "-",
            ",".join(p.mics or []),
            p.currency or "-",
            p.last_price if p.last_price is not None else "-",
        )

    duration = time.monotonic() - start
    logging.info("Completed in %.2f seconds", duration)


if __name__ == "__main__":
    main()
