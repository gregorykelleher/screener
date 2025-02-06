# resolvers/lse.py

from decimal import Decimal
from typing import List

from data_aggregator.data_vendors.lse import fetch_equities_async
from data_aggregator.equities_aggregator.schemas import EquityData

from .utils import (
    fetch_or_load_equities_async,
    safe_parse_decimal,
    build_equity_data,
)


async def _load_equities() -> list[dict]:
    def fetch():
        return fetch_equities_async()

    return await fetch_or_load_equities_async(fetch, "lse_equities.json")


async def resolve_lse_equities_async() -> List[EquityData]:
    raw_equities = await _load_equities()
    return [_extract_equity_data(eq) for eq in raw_equities]


def _extract_equity_data(equity: dict) -> EquityData:
    """
    Normalise raw dict → EquityData, converting GBX→GBP where needed.
    """
    equity = _convert_gbx_to_gbp(equity)

    return build_equity_data(
        equity,
        isin_key="isin",
        name_key="issuername",
        symbol_key="tidm",
        currency_key="currency",
        last_price_key="lastprice",
        default_mics=["XLON"],
    )


def _convert_gbx_to_gbp(raw: dict) -> dict:
    """
    Convert price-like fields from GBX (pence) to GBP (pounds).
    """
    eq = dict(raw)  # shallow copy

    if eq.get("currency") != "GBX":
        return eq

    for field in ["lastprice"]:
        val = eq.get(field)
        dec_val = safe_parse_decimal(val)
        if dec_val is not None:
            eq[field] = dec_val / Decimal("100")

    eq["currency"] = "GBP"
    return eq
