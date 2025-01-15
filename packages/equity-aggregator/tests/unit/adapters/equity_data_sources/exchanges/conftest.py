# exchanges/conftest.py
import pytest
import respx


@pytest.fixture
def respx_mock():
    """
    Yields a respx router → avoids boilerplate per test.
    Auto-closes after the test.
    """
    with respx.mock(assert_all_called=False) as router:
        yield router
