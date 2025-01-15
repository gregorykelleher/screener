# mock_overview/mock_fetch.py

import json
from pathlib import Path

# Define the directory containing JSON data files
MOCK_OVERVIEW_DIR = Path(__file__).resolve().parent


def _load_json_data(filename: str):
    """
    Load JSON data from a file in the MOCK_OVERVIEW_DIR.
    
    Args:
        filename (str): The name of the JSON file.
    
    Returns:
        The data loaded from the JSON file.
    
    Raises:
        FileNotFoundError: If the JSON file is not found.
        ValueError: If there is an error decoding the JSON.
    """
    file_path = MOCK_OVERVIEW_DIR / filename
    try:
        with file_path.open(encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {file_path}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Error decoding JSON from {file_path}: {e}") from e


def mock_fetch_daily_gainers(client=None):
    """
    Fetches the daily gainers data.
    
    The client parameter is included for interface compatibility.
    """
    return _load_json_data('mock_daily_gainers_data.json')


def mock_fetch_daily_losers(client=None):
    """
    Fetches the daily losers data.
    
    The client parameter is included for interface compatibility.
    """
    return _load_json_data('mock_daily_losers_data.json')


def mock_fetch_sector_performance(client=None):
    """
    Fetches the sector performance data.
    
    The client parameter is included for interface compatibility.
    """
    return _load_json_data('mock_sector_performance_data.json')


def mock_fetch_commodities_performance(client=None):
    """
    Fetches the commodities data.
    
    The client parameter is included for interface compatibility.
    """
    return _load_json_data('mock_commodities_data.json')