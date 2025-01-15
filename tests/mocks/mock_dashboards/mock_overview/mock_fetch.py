# mock_overview/mock_fetch.py

from tests.mocks.utils import load_mock_json_data


def mock_fetch_daily_gainers(client=None):
    """
    Fetches the daily gainers data.

    The client parameter is included for interface compatibility.
    """
    return load_mock_json_data("mock_daily_gainers_data.json")


def mock_fetch_daily_losers(client=None):
    """
    Fetches the daily losers data.

    The client parameter is included for interface compatibility.
    """
    return load_mock_json_data("mock_daily_losers_data.json")


def mock_fetch_sector_performance(client=None):
    """
    Fetches the sector performance data.

    The client parameter is included for interface compatibility.
    """
    return load_mock_json_data("mock_sector_performance_data.json")


def mock_fetch_commodities_performance(client=None):
    """
    Fetches the commodities data.

    The client parameter is included for interface compatibility.
    """
    return load_mock_json_data("mock_commodities_data.json")
