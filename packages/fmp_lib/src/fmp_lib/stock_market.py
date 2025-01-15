# stock_market.py

from typing import Optional, List, Dict

from .settings import SECTOR_ETF_VALUES, COMMODITY_VALUES
from .url_methods import _return_json_v3


class StockMarket:
    """
    Handles interactions with the Stock Market endpoints of the FMP API.

    :param api_key: Your FMP API key.
    """

    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("API key must be provided to StockMarket.")
        self.api_key = api_key

    def gainers(self) -> Optional[List[Dict]]:
        """
        Retrieve the top gainers from the stock market.

        :return: A list of dictionaries containing gainers data.
        """
        path = "stock_market/gainers"
        query_vars = {"apikey": self.api_key}
        return _return_json_v3(path=path, query_vars=query_vars)

    def losers(self) -> Optional[List[Dict]]:
        """
        Retrieve the top losers from the stock market.

        :return: A list of dictionaries containing losers data.
        """
        path = "stock_market/losers"
        query_vars = {"apikey": self.api_key}
        return _return_json_v3(path=path, query_vars=query_vars)

    def quote(self, symbol: str) -> Optional[List[Dict]]:
        """
        Retrieve the quote for the desire asset.

        :param symbol: The Ticker(s), Index(es), Commodity(ies), etc. symbol to query for.
        :return: A list of dictionaries containing quote data.
        """
        path = f"quote/{symbol}"
        query_vars = {"apikey": self.api_key}
        return _return_json_v3(path=path, query_vars=query_vars)

    def sectors_performance(self) -> Optional[List[Dict]]:
        """
        Retrieve performance data for ETFs linked to market sectors.

        For each sector in SECTOR_ETF_VALUES, fetch the ETF's latest quote using
        the `quote` method and compile its symbol, price, percentage change, name,
        and sector into a dictionary.

        :return: List of dictionaries containing sector performance data.
        """
        performance_data: List[Dict] = []

        for sector, symbol in SECTOR_ETF_VALUES.items():
            quote_data = self.quote(symbol)
            if quote_data:
                quote = quote_data[0]
                performance_data.append(
                    {
                        "sector": sector,
                        "price": quote.get("price"),
                        "symbol": symbol,
                        "changesPercentage": quote.get("changesPercentage"),
                    }
                )

        return performance_data

    def commodities_performance(self) -> Optional[List[Dict]]:
        """
        Retrieve performance data for commodities.

        For each commodity in COMMODITY_VALUES, fetch the latest quote using
        the `quote` method and compile its name, symbol, price, and percentage change
        into a dictionary.

        :return: List of dictionaries containing commodity performance data.
        """
        performance_data: List[Dict] = []

        for commodity_name, symbol in COMMODITY_VALUES.items():
            quote_data = self.quote(symbol)
            if quote_data:
                quote = quote_data[0]
                performance_data.append(
                    {
                        "commodity": commodity_name,
                        "price": quote.get("price"),
                        "symbol": symbol,
                        "changesPercentage": quote.get("changesPercentage"),
                    }
                )

        return performance_data

    def exchanges(self) -> Optional[List[str]]:
        """
        Retrieve a list of exchanges from the API.

        The endpoint returns data as a list of exchange names

        :return: A list of exchange names as strings.
        """
        path = "exchanges-list"
        query_vars = {"apikey": self.api_key}
        return _return_json_v3(path=path, query_vars=query_vars)
