# exchanges/xetra.py

import logging
from typing import List

from equity_aggregator.schemas import RawEquity
from equity_aggregator.adapters import fetch_equities_xetra
from equity_aggregator.domain.resolvers.utils import build_raw_equity

logger = logging.getLogger(__name__)


async def resolve_xetra_equities() -> List[RawEquity]:
    """
    Extracts the raw equities (from cache if available) and
    resolves the raw equities into a list of RawEquity instances.
    """
    raw_equities = await fetch_equities_xetra()

    logger.info("Found %d unique Xetra raw equities", len(raw_equities))

    return [
        build_raw_equity(
            # merge in a unified last_price key
            {**equity, "last_price": equity.get("overview", {}).get("lastPrice")},
            name_key="name",
            symbol_key="wkn",
            isin_key="isin",
            cik_key=None,
            mics_key="mics",
            currency_key="currency",
            last_price_key="last_price",
            default_mics=["XETR"],
        )
        for equity in raw_equities
    ]
