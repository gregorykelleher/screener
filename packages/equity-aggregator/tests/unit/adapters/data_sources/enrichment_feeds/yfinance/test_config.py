# yfinance/test_config.py

import inspect

import pytest
from attr.exceptions import FrozenInstanceError

from equity_aggregator.adapters.data_sources.enrichment_feeds.yfinance.config import (
    FeedConfig,
)

pytestmark = pytest.mark.unit


def test_search_url_default() -> None:
    """
    ARRANGE: default-constructed FeedConfig
    ACT:     read `search_url`
    ASSERT:  hard-coded Yahoo endpoint is returned
    """
    config = FeedConfig()

    assert config.search_url == "https://query1.finance.yahoo.com/v1/finance/search"


def test_dataclass_is_frozen() -> None:
    """
    ARRANGE: obtain FeedConfig dataclass params
    ACT:     inspect `frozen` flag
    ASSERT:  dataclass is marked frozen (immutable)
    """
    assert FeedConfig.__setattr__.__name__ == "_frozen_setattrs"


def test_mutation_raises() -> None:
    """
    ARRANGE: default FeedConfig instance
    ACT:     attempt to mutate an attribute
    ASSERT:  FrozenInstanceError is raised
    """
    config = FeedConfig()

    with pytest.raises(FrozenInstanceError):
        config.search_url = "https://example.com"


def test_slots_exist() -> None:
    """
    ARRANGE: FeedConfig class object
    ACT:     check presence of __slots__
    ASSERT:  __slots__ attribute exists
    """
    has_slots = hasattr(FeedConfig, "__slots__")

    assert has_slots


def test_override_keeps_other_defaults() -> None:
    """
    ARRANGE: FeedConfig built with custom `modules`
    ACT:     inspect untouched `quote_base`
    ASSERT:  quote_base still equals Yahoo default
    """
    config = FeedConfig(modules=("price",))

    assert (
        config.quote_summary_url
        == "https://query2.finance.yahoo.com/v10/finance/quoteSummary/"
    )


def test_signature_fields_have_defaults() -> None:
    """
    ARRANGE: signature of FeedConfig
    ACT:     iterate parameters ensuring each has a default
    ASSERT:  every parameter comes with a default value
    """
    signature = inspect.signature(FeedConfig)

    assert all(p.default is not p.empty for p in signature.parameters.values())
