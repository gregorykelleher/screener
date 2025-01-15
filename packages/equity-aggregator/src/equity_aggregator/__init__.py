# equity_aggregator/__init__.py

from equity_aggregator.domain.pipeline import aggregate_equity_profiles
from equity_aggregator.logging_config import configure_logging

__all__ = [
    "aggregate_equity_profiles",
    "configure_logging",
]
