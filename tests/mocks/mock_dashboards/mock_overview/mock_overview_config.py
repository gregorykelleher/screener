# mock_overview/mock_overview_config.py

from config.config_models import TableModel

from .mock_fetch import(
    mock_fetch_daily_gainers,
    mock_fetch_daily_losers,
    mock_fetch_sector_performance,
    mock_fetch_commodities_performance
)

overview_config = [
    TableModel(
        title="Daily Gainers",
        icon="📈",
        columns_to_hide=["symbol", "change"],
        fetch_func=mock_fetch_daily_gainers,
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
        fetch_func=mock_fetch_daily_losers,
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
        fetch_func=mock_fetch_sector_performance,
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
        fetch_func=mock_fetch_commodities_performance,
        columns_mapping={
            "commodity": "Commodity",
            "price": "Price",
            "changesPercentage": "Change %"
        },
        default_sort=[{"colId": "changesPercentage", "sort": "desc"}]
    )
]
