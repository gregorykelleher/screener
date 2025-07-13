# feeds/test_sec_feed_data.py

import pytest
from pydantic import ValidationError

from equity_aggregator.schemas import SecFeedData

pytestmark = pytest.mark.unit


def test_strips_extra_fields() -> None:
    """
    ARRANGE: input with unexpected extra field
    ACT:     construct SecFeedData
    ASSERT:  extra field is not present on the model
    """
    raw = {
        "name": "Foo Inc",
        "symbol": "FOO",
        "mics": ["XNYS"],
    }

    actual = SecFeedData(**raw, unexpected="FIELD")

    assert not hasattr(actual, "unexpected")


def test_accepts_empty_list_mics() -> None:
    """
    ARRANGE: mics provided as empty list
    ACT:     construct SecFeedData
    ASSERT:  mics is preserved as empty list
    """
    raw = {
        "name": "Foo Inc",
        "symbol": "FOO",
        "mics": [],
    }

    actual = SecFeedData(**raw)

    assert actual.mics == []


def test_missing_required_raises() -> None:
    """
    ARRANGE: omit required 'name'
    ACT:     construct SecFeedData
    ASSERT:  raises ValidationError
    """
    incomplete = {
        "symbol": "FOO",
        "mics": ["XNYS"],
    }

    with pytest.raises(ValidationError):
        SecFeedData(**incomplete)


def test_normalises_and_preserves_whitespace() -> None:
    """
    ARRANGE: raw fields include padding/whitespace
    ACT:     construct SecFeedData
    ASSERT:  whitespace in 'name' is retained (no trimming at this layer)
    """
    raw = {
        "name": "  Padded Name  ",
        "symbol": " PAD ",
        "mics": [" XNYS "],
    }

    actual = SecFeedData(**raw)

    assert actual.name == "  Padded Name  "


def test_cik_field_ignored() -> None:
    """
    ARRANGE: input contains 'cik' field
    ACT:     construct SecFeedData
    ASSERT:  'cik' field is ignored on the model
    """
    raw = {
        "name": "Foo Inc",
        "symbol": "FOO",
        "mics": ["XNYS"],
        "cik": "0000123456",
    }

    actual = SecFeedData(**raw)

    assert not hasattr(actual, "cik")
