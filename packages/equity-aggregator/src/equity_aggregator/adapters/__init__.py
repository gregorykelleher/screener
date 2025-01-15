# adapters/__init__.py

from .data_sources.authoritative_feeds import (
    fetch_equities_euronext,
    fetch_equities_lse,
    fetch_equities_xetra,
)
from .data_sources.enrichment_feeds import (
    fetch_equity_identification,
    fetch_equity_yfinance,
    retrieve_conversion_rates,
)

__all__ = [
    # authoritative feeds
    "fetch_equities_euronext",
    "fetch_equities_lse",
    "fetch_equities_xetra",
    # enrichment feeds
    "fetch_equity_yfinance",
    # reference lookup
    "fetch_equity_identification",
    "retrieve_conversion_rates",
]
