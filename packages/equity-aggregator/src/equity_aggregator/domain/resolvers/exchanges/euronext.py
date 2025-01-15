# exchanges/euronext.py

import logging

from equity_aggregator.schemas import RawEquity
from equity_aggregator.adapters import fetch_equities_euronext
from equity_aggregator.domain.resolvers.utils import build_raw_equity

logger = logging.getLogger(__name__)


async def resolve_euronext_equities() -> list[RawEquity]:
    """
    Extracts the raw equities (from cache if available) and
    resolves the raw equities into a list of RawEquity instances.
    """
    raw_equities = await fetch_equities_euronext()

    logger.info("Found %d unique Euronext raw equities", len(raw_equities))

    return [
        build_raw_equity(
            equity,
            name_key="name",
            symbol_key="symbol",
            isin_key="isin",
            cik_key=None,
            mics_key="mics",
            currency_key="currency",
            last_price_key="last_price",
            default_mics=[],
        )
        for equity in raw_equities
    ]
