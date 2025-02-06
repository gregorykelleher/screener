# resolvers/euronext.py

from data_aggregator.data_vendors.euronext import fetch_equities_async
from data_aggregator.equities_aggregator.schemas import EquityData
from .utils import fetch_or_load_equities_async, build_equity_data


async def _load_equities():
    return await fetch_or_load_equities_async(
        fetch_equities_async, json_filename="euronext_equities.json"
    )


async def resolve_euronext_equities_async() -> list[EquityData]:
    raw_equities = await _load_equities()
    return [_extract_equity_data(eq) for eq in raw_equities]


def _extract_equity_data(equity: dict) -> EquityData:
    return build_equity_data(
        equity,
        isin_key="isin",
        name_key="name",
        symbol_key="symbol",
        currency_key="currency",
        last_price_key="last_price",
    )
