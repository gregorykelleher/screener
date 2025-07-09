# equity_aggregator/__main__.py

import asyncio
import logging
import time

from equity_aggregator import aggregate_equity_profiles, configure_logging

logger = logging.getLogger(__name__)


def main() -> None:
    configure_logging()

    # try:
    #     start_time = time.perf_counter()
    #     flat = asyncio.run(_get_info("MSFT"))
    #     elapsed = time.perf_counter() - start_time
    #     logger.info("`_get_info` completed in %.3f seconds", elapsed)
    # except Exception as exc:
    #     logger.exception("Unhandled exception during fetch")
    #     return

    # if not flat:
    #     logger.error("No data returned for MSFT")
    #     return

    # print(json.dumps(flat, indent=2))

    start = time.monotonic()
    profiles = asyncio.run(aggregate_equity_profiles())

    if not profiles:
        logger.error("No equity profiles found.")
        return
    logger.info(f"Found {len(profiles)} equity profiles.")
    for p in profiles:
        logger.debug(
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
    logger.info("Completed in %.2f seconds", duration)


if __name__ == "__main__":
    main()
