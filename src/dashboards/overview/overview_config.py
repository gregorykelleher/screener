# config/overview_config.py

from config.config_models import TableModel

from .fetch import(
    fetch_daily_gainers,
    fetch_daily_losers,
    fetch_sector_performance,
    fetch_commodities_performance
)

overview_config = [
    TableModel(
        title="Daily Gainers",
        icon="📈",
        columns_to_hide=["symbol", "change"],
        fetch_func=fetch_daily_gainers,
        columns_mapping={
            "name": "Name",
            "price": "Price",
            "changesPercentage": "Change %"
        },
        default_sort=[{"colId": "changesPercentage", "sort": "desc"}]
    ),
    TableModel(
        title="Daily Losers",
        icon="📉",
        columns_to_hide=["symbol", "change"],
        fetch_func=fetch_daily_losers,
        columns_mapping={
            "name": "Name",
            "price": "Price",
            "changesPercentage": "Change %"
        },
        default_sort=[{"colId": "changesPercentage", "sort": "asc"}]
    ),
    TableModel(
        title="Sectors",
        icon="🏭",
        columns_to_hide=["symbol", "change"],
        fetch_func=fetch_sector_performance,
        columns_mapping={
            "sector": "Sector",
            "price": "Price",
            "changesPercentage": "Change %"
        },
        default_sort=[{"colId": "changesPercentage", "sort": "desc"}]
    ),
    TableModel(
        title="Commodities",
        icon="📦",
        columns_to_hide=["symbol"],
        fetch_func=fetch_commodities_performance,
        columns_mapping={
            "commodity": "Commodity",
            "price": "Price",
            "changesPercentage": "Change %"
        },
        default_sort=[{"colId": "changesPercentage", "sort": "desc"}]
    )
]
