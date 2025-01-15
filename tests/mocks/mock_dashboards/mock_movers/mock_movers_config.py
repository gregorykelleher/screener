# mock_movers/mock_movers_config.py

from config import TableModel

from .mock_fetch import mock_fetch_exchanges

movers_config = [
    TableModel(
        title="Movers",
        icon="📈",
        columns_to_hide=[],
        fetch_func=mock_fetch_exchanges,
        columns_mapping={},
        default_sort=[{}],
    )
]
