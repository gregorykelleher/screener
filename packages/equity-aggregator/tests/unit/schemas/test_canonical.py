# schemas/test_canonical.py

import pytest

from equity_aggregator.schemas.canonical import EquityIdentity
from equity_aggregator.schemas.raw import RawEquity

pytestmark = pytest.mark.unit


def test_equity_identity_from_raw_success() -> None:
    """
    ARRANGE: valid RawEquity instance and FIGI
    ACT:     build EquityIdentity via from_raw
    ASSERT:  share_class_figi equals supplied FIGI
    """
    raw = RawEquity(
        name="Foo Corp.",
        symbol="FOO",
        isin="US1234567890",
        cusip="123456789",
    )

    identity = EquityIdentity.from_raw(raw, "BBG000000001")

    assert identity.share_class_figi == "BBG000000001"


def test_equity_identity_from_raw_requires_figi() -> None:
    """
    ARRANGE: RawEquity instance with empty FIGI
    ACT:     call from_raw with blank FIGI
    ASSERT:  raises ValueError
    """
    raw = RawEquity(
        name="Bar Corp.",
        symbol="BAR",
        isin=None,
        cusip=None,
    )

    with pytest.raises(ValueError):
        EquityIdentity.from_raw(raw, "")
