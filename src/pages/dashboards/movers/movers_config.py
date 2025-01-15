# movers/movers_config.py

from config import TableModel

from .fetch import fetch_exchanges

movers_config = [
    TableModel(
        title="Movers",
        icon="📈",
        columns_to_hide=[],
        fetch_func=fetch_exchanges,
        columns_mapping={},
        default_sort=[{}],
    )
]
