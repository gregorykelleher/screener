# tests/live/conftest.py

import importlib
from typing import Callable, Awaitable, Any, Dict

import pytest

# === registry of data-vendor fetchers ======================================

# map <vendor name> -> coroutine  (import lazily to avoid heavy deps at import time)
_FETCHERS: Dict[str, Callable[[], Awaitable[Any]]] = {
    "euronext": lambda: importlib.import_module(
        "data_aggregator.data_vendors.euronext.fetch_equities_async"
    ).fetch_equities_async,
}


@pytest.fixture(params=list(_FETCHERS.keys()), ids=lambda k: k)
def vendor_name(request) -> str:
    return request.param


@pytest.fixture
def fetcher(vendor_name):
    """Return the vendor's async fetch function."""
    return _FETCHERS[vendor_name]()
