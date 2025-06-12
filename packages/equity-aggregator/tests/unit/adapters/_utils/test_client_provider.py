# _utils/test_client_provider.py

import pytest

from equity_aggregator.adapters.data_sources._utils import make_client_factory

pytestmark = pytest.mark.unit


def test_make_client_factory_returns_callable() -> None:
    """
    ARRANGE: valid base_url and timeout
    ACT:     call make_client_factory
    ASSERT:  returns a callable factory
    """
    factory = make_client_factory(base_url="https://api.test", timeout=5.5)

    assert callable(factory)


async def test_client_factory_applies_default_timeout() -> None:
    """
    ARRANGE: factory with custom timeout
    ACT:     create client from factory
    ASSERT:  client has expected timeout
    """
    expected_timeout = 2.5
    factory = make_client_factory(timeout=expected_timeout)
    client = factory()

    assert client.timeout.connect == expected_timeout

    await client.aclose()


async def test_client_factory_override_base_url() -> None:
    """
    ARRANGE: factory with default base_url, override at call
    ACT:     create client with override base_url
    ASSERT:  client uses override base_url
    """
    default_url = "https://api.test"
    override_url = "https://override.test"
    factory = make_client_factory(base_url=default_url)

    client = factory(base_url=override_url)

    assert str(client.base_url) == override_url
    await client.aclose()
