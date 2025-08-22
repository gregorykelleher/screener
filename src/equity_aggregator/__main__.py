# equity_aggregator/__main__.py

import asyncio
import logging
import time
from itertools import starmap

from equity_aggregator.domain.pipeline import aggregate_canonical_equities
from equity_aggregator.logging_config import configure_logging
from equity_aggregator.storage import (
    save_canonical_equities_json,
    save_canonical_equities_sql,
)

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
    canonical_equities = asyncio.run(aggregate_canonical_equities())

    save_canonical_equities_json(canonical_equities)
    save_canonical_equities_sql(canonical_equities)

    if not canonical_equities:
        logger.error("No canonical equities found.")
        return
    logger.info(f"Found {len(canonical_equities)} canonical equities.")
    for p in canonical_equities:
        fields = p.model_dump()

        def normalise(value: object) -> str:
            if value in (None, "", [], {}):
                return "-"
            if isinstance(value, list):
                return ",".join(map(str, value))
            return str(value)

        serialised = ", ".join(
            starmap(lambda k, v: f"{k}={normalise(v)}", fields.items()),
        )

        logger.debug("{%s}", serialised)

    duration = time.monotonic() - start
    logger.info("Completed in %.2f seconds", duration)


if __name__ == "__main__":
    main()
