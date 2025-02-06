# resolvers/xetra.py

from typing import List

from data_aggregator.data_vendors.xetra import fetch_equities_async
from data_aggregator.equities_aggregator.schemas import EquityData

from .utils import fetch_or_load_equities_async, build_equity_data


async def _load_equities() -> list[dict]:
    """
    Load cached Xetra equities or fetch them asynchronously.
    """

    def fetch():
        return fetch_equities_async()

    return await fetch_or_load_equities_async(fetch, "xetra_equities.json")


async def resolve_xetra_equities_async() -> List[EquityData]:
    raw_equities = await _load_equities()
    return [_extract_equity_data(eq) for eq in raw_equities]


def _extract_equity_data(equity: dict) -> EquityData:
    """
    Normalise raw dict → EquityData, moving nested lastPrice for convenience.
    """
    last_price = equity.get("overview", {}).get("lastPrice")
    eq = {**equity, "last_price": last_price}  # shallow copy w/ unified key

    return build_equity_data(
        eq,
        isin_key="isin",
        name_key="name",
        symbol_key="wkn",
        currency_key="currency",
        last_price_key="last_price",
        default_mics=["XETR"],
    )
