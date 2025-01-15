# src/pages/__init__.py

from .dashboards import (
    overview_config,
    movers_config,
    generate_overview,
    generate_insiders,
    generate_movers,
)

from .trackers import (
    generate_favourites,
    generate_watchlist,
    generate_portfolio,
)

from .analyses import (
    generate_asset_analysis,
    generate_insider_analysis,
    generate_risk_analysis,
)

__all__ = [
    "overview_config",
    "movers_config",
    "generate_overview",
    "generate_movers",
    "generate_insiders",
    "generate_favourites",
    "generate_watchlist",
    "generate_portfolio",
    "generate_asset_analysis",
    "generate_insider_analysis",
    "generate_risk_analysis",
]
