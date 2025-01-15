# exchanges/lse.py

import re
import logging
from decimal import Decimal
from typing import List, Optional

from equity_aggregator.schemas import RawEquity
from equity_aggregator.adapters import fetch_equities_lse
from equity_aggregator.domain.resolvers.utils import build_raw_equity

logger = logging.getLogger(__name__)


async def resolve_lse_equities() -> List[RawEquity]:
    """
    Extracts the raw equities (from cache if available) and
    resolves the raw equities into a list of RawEquity instances.
    """
    raw_equities = await fetch_equities_lse()

    logger.info("Found %d unique LSE raw equities", len(raw_equities))

    return [
        build_raw_equity(
            # convert GBX to GBP
            _convert_gbx_to_gbp(equity),
            name_key="issuername",
            symbol_key="tidm",
            isin_key="isin",
            cik_key=None,
            mics_key="mics",
            currency_key="currency",
            last_price_key="lastprice",
            default_mics=["XLON"],
        )
        for equity in raw_equities
    ]


def _gbx_to_decimal(pence: Optional[str]) -> Optional[Decimal]:
    """
    Turn a pence string like "150" or "1,50" into a Decimal('150') or Decimal('1.50').
    Returns None if the input is None or doesn't match a plain positive number.
    """
    if pence is None:
        return None

    s = str(pence).strip()
    # allow "1,23" → "1.23"
    if "," in s and "." not in s:
        s = s.replace(",", ".")

    # only digits with optional single decimal point
    if not re.fullmatch(r"\d+(?:\.\d+)?", s):
        return None

    return Decimal(s)


def _convert_gbx_to_gbp(raw: dict) -> dict:
    """
    If currency == "GBX", divides lastprice by 100 and sets currency="GBP".
    Leaves everything else untouched.
    """
    eq = raw.copy()
    if eq.get("currency") == "GBX":
        dec_pence = _gbx_to_decimal(eq.get("lastprice"))
        if dec_pence is not None:
            eq["lastprice"] = dec_pence / Decimal("100")
        eq["currency"] = "GBP"
    return eq
