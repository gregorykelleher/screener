# utils.py

import json
from pathlib import Path
from typing import Any


def load_mock_json_data(filename: str) -> Any:
    """
    Load a specific JSON file from the 'mocks' directory or its subdirectories.

    Args:
        filename (str): The name of the JSON file to load (e.g., 'mock_movers_data.json').

    Returns:
        Any: The data loaded from the specified JSON file.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If there is an error decoding the JSON.
    """
    # Define the root directory for mocks
    MOCKS_DIR = Path(__file__).resolve().parent

    if not MOCKS_DIR.exists():
        raise FileNotFoundError(f"The mocks directory does not exist: {MOCKS_DIR}")

    # Search for the specified file in the directory and its subdirectories
    for json_file in MOCKS_DIR.rglob("*.json"):
        if json_file.name == filename:
            try:
                with json_file.open(encoding="utf-8") as f:
                    return json.load(f)
            except json.JSONDecodeError as e:
                raise ValueError(f"Error decoding JSON in file {json_file}: {e}")

    # Raise an error if the file is not found
    raise FileNotFoundError(f"File not found in the mocks directory: {filename}")
