# exchanges/conftest.py
import typing

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
