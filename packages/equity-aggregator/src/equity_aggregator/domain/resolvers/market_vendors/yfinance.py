# market_vendors/yfinance.py

import logging
from typing import AsyncIterable

from equity_aggregator.schemas import RawEquity
from equity_aggregator.adapters import fetch_equity_yfinance
from equity_aggregator.domain._utils import get_usd_converter

from equity_aggregator.domain.resolvers.utils import (
    build_raw_equity,
    has_missing_fields,
    replace_none_with_enriched,
)

logger = logging.getLogger(__name__)


async def resolve_yfinance_equities(
    raw_equities: AsyncIterable[RawEquity],
) -> AsyncIterable[RawEquity]:
    """
    Extracts the raw equities (from cache if available), enriches
    any missing fields from yfinance, normalises to USD, and resolves
    the raw equities into a list of fully-populated RawEquity instances.
    """
    convert_to_usd = await get_usd_converter()

    async for raw_equity in raw_equities:
        enriched = await _enrich_missing_fields_from_yfinance(raw_equity)
        yield convert_to_usd(enriched)


async def _enrich_missing_fields_from_yfinance(raw_equity: RawEquity) -> RawEquity:
    """
    If raw equity has any None fields, fetch corresponding yfinance raw data,
    build a RawEquity out of that, then copy over only the missing
    fields into the raw equity. Otherwise returns raw equity untouched.
    """

    # if raw equity doesn't have any missing fields, just return it
    if not has_missing_fields(raw_equity):
        return raw_equity

    # if raw equity has missing fields, fetch data from yfinance
    # TODO: add support for CUSIP
    raw_data = await fetch_equity_yfinance(
        symbol=raw_equity.symbol,
        name=raw_equity.name,
        isin=raw_equity.isin,
        cusip=None,
    )

    # if yfinance returns empty data, just return the raw equity as is
    if not raw_data:
        return raw_equity

    # Build a RawEquity instance from the fetched data
    yf_raw_equity = build_raw_equity(
        raw_data,
        name_key="longName",
        symbol_key="symbol",
        isin_key=None,
        cik_key=None,
        mics_key=None,
        currency_key="currency",
        last_price_key="currentPrice",
        default_mics=[],
    )

    # Replace None values in raw equity with values from yf_raw_equity
    return replace_none_with_enriched(raw_equity, yf_raw_equity)
