# tests/conftest.py

import os
import typing
from pathlib import Path

import pytest
import respx


def pytest_configure(config: pytest.Config) -> None:
    """
    Configures pytest to create a temporary folder within .pytest_cache for test data.

    This function is automatically called by pytest before running tests. It creates a
    directory named 'data_sql_store' inside the .pytest_cache folder and sets the
    environment variable '_DATA_SQL_STORE_DIR' to the absolute path of this directory.
    This allows tests to access a temporary, isolated data store location.

    Args:
        config (pytest.Config): The pytest configuration object.

    Returns:
        None
    """
    root = Path(config.cache.makedir("data_sql_store").strpath)

    os.environ["_DATA_SQL_STORE_DIR"] = root.as_posix()


@pytest.fixture
def data_sql_store_dir() -> Path:
    """
    Fixture that provides the path to the temporary data sql store directory for test
    inspection.

    Returns:
        Path: The path to the temporary data sql store directory, as specified by the
            '_DATA_SQL_STORE_DIR' environment variable.
    """
    return Path(os.environ["_DATA_SQL_STORE_DIR"])


@pytest.fixture(autouse=True)
def fresh_data_store() -> None:
    """
    Ensures each test starts with a clean SQLite data store file.

    This fixture runs automatically before each test. It deletes the 'data_sql_store.db'
    file from the temporary data store directory if it exists, guaranteeing a pristine
    state for every test.

    Args:
        None

    Returns:
        None
    """
    db_file = Path(os.environ["_DATA_SQL_STORE_DIR"]) / "data_sql_store.db"
    if db_file.exists():
        db_file.unlink()


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
