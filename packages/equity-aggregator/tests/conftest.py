# tests/conftest.py

import os
import shutil
import typing
from collections.abc import Iterator
from pathlib import Path

import pytest
import respx


@pytest.fixture
def respx_mock() -> typing.Generator[respx.MockRouter, None, None]:
    """
    Yields a respx.MockRouter instance for mocking HTTP requests in tests.

    Args:
        None

    Yields:
        respx.MockRouter: The mock router for HTTP request interception.

    This fixture avoids boilerplate in each test and automatically closes the router
    after the test completes.
    """
    with respx.mock(assert_all_called=False) as router:
        yield router


@pytest.fixture(autouse=True)
def clear_cache_dir() -> Iterator[None]:
    """
    Pytest fixture that resets the cache directory before each test.

    Deletes the cache directory specified by the CACHE_DIR environment variable,
    recreates it as an empty directory, and yields control to the test. This ensures
    a clean cache state for every test run.

    Args:
        None

    Yields:
        None: Control is yielded to the test after resetting the cache directory.
    """
    cache_dir_str = os.environ.get("CACHE_DIR")

    if not cache_dir_str:
        pytest.exit(
            "CACHE_DIR is not set: tests must be run via pytest-env so CACHE_DIR "
            "points at a test-only folder.",
        )

    cache_dir = Path(cache_dir_str)

    # Ensure the cache directory is empty before each test
    if cache_dir.exists():
        shutil.rmtree(cache_dir)

    cache_dir.mkdir(parents=True, exist_ok=True)
    yield
