# test_data_store.py

import os
from dataclasses import dataclass

import pytest

from equity_aggregator.data_store import (
    _CACHE_TABLE,
    _EQUITY_TABLE,
    _connect,
    _ttl_seconds,
    load_cache,
    load_cache_entry,
    save_cache,
    save_cache_entry,
    save_canonical_equities,
)

pytestmark = pytest.mark.unit


@dataclass(slots=True)
class _Identity:
    """
    Dataclass representing an identity with a share class FIGI.
    """

    share_class_figi: str


@dataclass(slots=True)
class DummyEquity:
    """
    A dummy data class representing an equity with a share class FIGI identifier.
    """

    share_class_figi: str

    @property
    def identity(self) -> _Identity:
        return _Identity(self.share_class_figi)


def _count_rows(table: str) -> int:
    """
    Counts the number of rows in the specified database table.

    Args:
        table (str): The name of the table to count rows from.

    Returns:
        int: The total number of rows present in the specified table.
    """
    with _connect() as conn:
        return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def test_save_and_load_cache_roundtrip() -> None:
    """
    ARRANGE: save_cache
    ACT:     load_cache
    ASSERT:  loaded equals saved
    """
    payload = {"x": 1}
    save_cache("rt", payload)

    assert load_cache("rt") == payload


def test_save_and_load_cache_entry_roundtrip() -> None:
    """
    ARRANGE: save_cache_entry
    ACT:     load_cache_entry
    ASSERT:  loaded equals saved
    """
    payload = [1, 2]
    save_cache_entry("rt2", "k", payload)

    assert load_cache_entry("rt2", "k") == payload


def test_load_cache_returns_none_when_expired() -> None:
    """
    ARRANGE: positive TTL and artificially age entry
    ACT:     load_cache
    ASSERT:  returns None
    """
    os.environ["CACHE_TTL_MINUTES"] = "1"
    save_cache("exp", value=True)

    ttl = _ttl_seconds()
    with _connect() as conn:
        conn.execute(
            f"UPDATE {_CACHE_TABLE} SET created_at = created_at - ?",
            (ttl + 1,),
        )

    assert load_cache("exp") is None


def test_ttl_seconds_negative_raises_value_error() -> None:
    """
    ARRANGE: CACHE_TTL_MINUTES = -5
    ACT:     _ttl_seconds
    ASSERT:  ValueError raised with correct message
    """
    os.environ["CACHE_TTL_MINUTES"] = "-5"
    with pytest.raises(ValueError, match="≥ 0"):
        _ttl_seconds()


def test_save_equities_inserts_rows() -> None:
    """
    ARRANGE: two DummyEquity objects
    ACT:     save_equities
    ASSERT:  row count == 2
    """
    expected_row_count = 2
    save_canonical_equities([DummyEquity("F1"), DummyEquity("F2")])

    assert _count_rows(_EQUITY_TABLE) == expected_row_count


def test_save_equities_upsert_single_row() -> None:
    """
    ARRANGE: same FIGI twice
    ACT:     save_equities twice
    ASSERT:  row count == 1
    """
    save_canonical_equities([DummyEquity("DUP")])
    save_canonical_equities([DummyEquity("DUP")])

    assert _count_rows(_EQUITY_TABLE) == 1


def test_save_equities_raises_for_missing_figi() -> None:
    """
    ARRANGE: DummyEquity with empty FIGI
    ACT:     save_equities
    ASSERT:  ValueError raised with correct message
    """
    with pytest.raises(ValueError, match="share_class_figi"):
        save_canonical_equities([DummyEquity("")])


def test_save_cache_entry_noop_when_cache_name_none() -> None:
    """
    ARRANGE: ensure cache table exists and capture row count
    ACT:     save_cache_entry with cache_name=None
    ASSERT:  row count unchanged
    """
    save_cache("warmup", value=True)
    before = _count_rows(_CACHE_TABLE)

    save_cache_entry(None, "ignored", {"x": 1})

    assert _count_rows(_CACHE_TABLE) == before


def test_load_cache_returns_none_when_cache_name_none() -> None:
    """
    ARRANGE: none
    ACT:     load_cache with cache_name=None
    ASSERT:  returns None
    """
    assert load_cache(None) is None
