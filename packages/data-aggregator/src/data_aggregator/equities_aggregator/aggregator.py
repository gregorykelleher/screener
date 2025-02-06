# equities_aggregator/aggregator.py

import asyncio
from decimal import Decimal
from collections import defaultdict
from statistics import median
from typing import Dict, Optional, List

from ..currency_converter import retrieve_conversion_rates

from .resolvers.euronext import resolve_euronext_equities_async
from .resolvers.xetra import resolve_xetra_equities_async
from .resolvers.lse import resolve_lse_equities_async

from .schemas import FinancialEquityData, EquityData


async def _fetch_aggregated_equity_data_async() -> list[EquityData]:
    eur, xet, lse = await asyncio.gather(
        resolve_euronext_equities_async(),
        resolve_xetra_equities_async(),
        resolve_lse_equities_async(),
    )
    # flatten
    return [*eur, *xet, *lse]


def _convert_financial_data_to_usd(
    financial: FinancialEquityData, conversion_rates: dict[str, Decimal]
) -> FinancialEquityData:
    """
    Convert the financial data's last_price into USD (the reference base currency).
    If the financial data is already in USD, return it unchanged.
    """
    if financial.currency != "USD" and financial.last_price is not None:
        rate = conversion_rates.get(financial.currency)
        if rate is None:
            raise ValueError(
                f"No conversion rate provided for {financial.currency} to USD"
            )
        # Since the rate is defined as USD as the base currency, invert the rate to convert currency -> USD.
        converted_last_price = (financial.last_price / rate).quantize(Decimal("0.01"))
        return financial.model_copy(
            update={"last_price": converted_last_price, "currency": "USD"}
        )
    return financial


def _convert_equity_to_usd(
    eq: EquityData, conversion_rates: dict[str, Decimal]
) -> EquityData:
    """Return a new EquityData instance with its financial data normalized to USD."""
    if eq.financial:
        normalized_financial = _convert_financial_data_to_usd(
            eq.financial, conversion_rates
        )
        return eq.model_copy(update={"financial": normalized_financial})
    return eq


def _normalize_equities_to_usd(
    equities: List[EquityData], conversion_rates: Dict[str, Decimal]
) -> List[EquityData]:
    """
    Convert a list of EquityData objects so each one has financial data in USD.
    """
    return [_convert_equity_to_usd(e, conversion_rates) for e in equities]


def _group_equities_by_isin(equities: List[EquityData]) -> Dict[str, List[EquityData]]:
    """Group EquityData objects by their canonical ISIN."""
    groups = defaultdict(list)
    for eq in equities:
        groups[eq.canonical.isin].append(eq)
    return groups


def _merge_financial_data(
    equities: List[EquityData],
) -> FinancialEquityData:
    """
    Merge FinancialEquityData from a list of EquityData objects that share the same canonical ISIN.

    - Computes the median of all available last_price values.
    - Combine all distinct MICS.
    - Assume final currency is 'USD' if any financial data is present.
    """
    # Extract all financial data entries
    financials = [eq.financial for eq in equities if eq.financial is not None]

    # Collect available last_price values.
    last_prices = [f.last_price for f in financials if f.last_price is not None]

    merged_last_price: Optional[Decimal] = None
    if last_prices:
        # Calculate median and ensure the result is rounded to two decimals.
        merged_last_price = median(last_prices).quantize(Decimal("0.01"))

    # Merge mics by taking the union.
    all_mics = {mic for f in financials for mic in (f.mics or [])}
    merged_mics = list(all_mics) if all_mics else None

    merged_currency = "USD" if financials else None

    return FinancialEquityData(
        mics=merged_mics,
        currency=merged_currency,
        last_price=merged_last_price,
    )


def _merge_equities(equities: List[EquityData]) -> EquityData:
    """
    Merge all EquityData objects in a group (sharing the same canonical ISIN) into a single record.
    """
    # Assume all equities in the group share the same canonical data.
    canonical = equities[0].canonical
    merged_financial = _merge_financial_data(equities)
    return EquityData(
        canonical=canonical,
        financial=merged_financial,
    )


async def aggregate_and_normalise_equities_async() -> List[EquityData]:
    """
    High-level function to:
      1. Fetch all equities,
      2. Retrieve currency conversion rates,
      3. Convert them to USD,
      4. Group & merge them by ISIN,
      5. Return the final list of unique EquityData objects.
    """
    # 1) Fetch raw equities asynchronously
    raw_equities = await _fetch_aggregated_equity_data_async()

    # 2) Get conversion rates
    conversion_rates = retrieve_conversion_rates()

    # 3) Normalize to USD
    usd_equities = _normalize_equities_to_usd(raw_equities, conversion_rates)

    # 4) Group by ISIN
    grouped = _group_equities_by_isin(usd_equities)

    # 5) Merge within each group
    return [_merge_equities(group) for group in grouped.values()]
