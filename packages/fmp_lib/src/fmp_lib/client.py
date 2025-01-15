# client.py

from .stock_market import StockMarket


class Client:
    """
    A client to interact with the Financial Modeling Prep (FMP) API.

    :param api_key: Your FMP API key.
    """

    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("API key must be provided.")
        self.api_key = api_key

        # Initialize endpoint-specific classes
        self.stock_market = StockMarket(api_key=self.api_key)
