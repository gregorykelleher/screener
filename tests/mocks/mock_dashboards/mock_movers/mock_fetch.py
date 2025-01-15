# mock_movers/mock_fetch.py

from tests.mocks.utils import load_mock_json_data


def mock_fetch_exchanges(client=None):
    """
    Fetches the mock exchanges data.

    Args:
        client: Included for interface compatibility (optional).

    Returns:
        The loaded JSON data for the exchanges mock file.
    """
    return load_mock_json_data("mock_movers_data.json")
