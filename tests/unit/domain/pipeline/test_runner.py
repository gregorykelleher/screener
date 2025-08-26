# pipeline/test_runner.py

import pytest

from equity_aggregator.domain.pipeline import aggregate_canonical_equities

pytestmark = pytest.mark.unit


async def test_smoke_returns_list_offline_safe() -> None:
    """
    ARRANGE: nothing
    ACT:     call aggregate_canonical_equities
    ASSERT:  returns a fully materialised list even when resolve() aborts
    """
    actual = await aggregate_canonical_equities()
    assert isinstance(actual, list)
