# src/__init__.py

from .main_config import config as main_config

from .pages import (
    generate_overview,
    generate_movers,
    generate_insiders,
    generate_favourites,
    generate_watchlist,
    generate_portfolio,
    generate_asset_analysis,
    generate_insider_analysis,
    generate_risk_analysis,
)

from .services import (
    initialise_services,
    retrieve_fmp_client,
    retrieve_database_client,
)

__all__ = [
    "main_config",
    "generate_overview",
    "generate_movers",
    "generate_insiders",
    "generate_favourites",
    "generate_watchlist",
    "generate_portfolio",
    "generate_asset_analysis",
    "generate_insider_analysis",
    "generate_risk_analysis",
    "initialise_services",
    "retrieve_fmp_client",
    "retrieve_database_client",
]
