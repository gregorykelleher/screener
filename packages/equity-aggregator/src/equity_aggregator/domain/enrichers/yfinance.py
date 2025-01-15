# enrichers/yfinance.py

import logging

from equity_aggregator.adapters import fetch_equity_yfinance
from equity_aggregator.domain._utils import get_usd_converter
from equity_aggregator.domain.resolvers._utils import build_raw_equity
from equity_aggregator.schemas import RawEquity

logger = logging.getLogger(__name__)


async def enrich_equity_with_yfinance(raw_equity: RawEquity) -> RawEquity:
    """
    Enrich a RawEquity instance by filling missing fields using data from yfinance.

    If all required fields are present in the input, returns the RawEquity unchanged.
    Otherwise, fetches data from yfinance, builds a new RawEquity, and merges only the
    missing fields into the original instance.

    Args:
        raw_equity (RawEquity): The RawEquity instance to enrich. Guaranteed to be USD
            denominated (from earlier normalisation pipeline stage).

    Returns:
        RawEquity: The enriched RawEquity instance with missing fields filled
            if possible.
    """
    # if raw equity doesn't have any missing fields, just return it
    if not has_missing_fields(raw_equity):
        return raw_equity

    # if raw equity has missing fields, fetch data from yfinance
    # pass identifiers individually instead of passing the `RawEquity` instance
    # to maintain strict decoupling between `schemas` and `adapters`
    raw_data = await fetch_equity_yfinance(
        symbol=raw_equity.symbol,
        name=raw_equity.name,
        isin=raw_equity.isin,
        cusip=raw_equity.cusip,
    )

    # if yfinance returns empty data, just return the raw equity as is
    if not raw_data:
        logger.debug(
            "No data returned from yfinance for equity %s (%s)."
            "Returning original raw equity.",
            raw_equity.name,
            raw_equity.symbol,
        )
        return raw_equity

    # Build a RawEquity instance from the fetched data
    yf_raw_equity = build_raw_equity(
        raw_data,
        name_key="longName",
        symbol_key="symbol",
        # TODO: investigate whether these should really be None
        isin_key=None,
        cusip_key=None,
        mics_key=None,
        currency_key="currency",
        last_price_key="currentPrice",
        market_cap_key="marketCap",
        default_mics=[],
    )

    # convert the fetched yf raw equity to USD before merging it with the original
    # raw equity to ensure all values are in the same currency
    convert_to_usd = await get_usd_converter()
    yf_raw_equity_usd = convert_to_usd(yf_raw_equity)

    # Replace None values in raw equity with values from yf_raw_equity
    return replace_none_with_enriched(raw_equity, yf_raw_equity_usd)


def replace_none_with_enriched(
    source: RawEquity,
    enriched: RawEquity,
) -> RawEquity:
    """
    Return new RawEquity instance with missing fields from `source` filled in from
    `enriched`.

    For each field, if `source` has a non-None value, it is kept. If `source` has None,
    the value from `enriched` is used, but only if it is not None. None values in
    `enriched` never overwrite any value in `source`.

    Args:
        source (RawEquity): The original RawEquity instance, possibly with missing
            fields.
        enriched (RawEquity): The RawEquity instance to use for filling missing fields.

    Returns:
        RawEquity: A new RawEquity instance with missing fields filled from `enriched`.
    """
    # dump enriched, don’t include any None values
    enriched_data = enriched.model_dump(exclude_none=True)

    # pick only the keys where source is None
    to_update = {
        field: value
        for field, value in enriched_data.items()
        if getattr(source, field) is None
    }

    # return a copy of source with just those missing fields filled in
    return source.model_copy(update=to_update)


def has_missing_fields(equity: RawEquity) -> bool:
    """
    Checks if any field in a RawEquity Pydantic model instance is missing
    (i.e., set to None).

    Args:
        equity (RawEquity): The Pydantic model instance to check for missing fields.

    Returns:
        bool: True if any field in the model is None, otherwise False.
    """
    # Dump all the fields, then check for any None
    return any(value is None for value in equity.model_dump().values())
