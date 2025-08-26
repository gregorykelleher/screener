# test_data_store.py

import json
import os
from pathlib import Path

import pytest

from equity_aggregator.schemas import CanonicalEquity, EquityFinancials, EquityIdentity
from equity_aggregator.storage.data_store import (
    _CACHE_TABLE,
    _EQUITY_TABLE,
    _connect,
    _serialise_equity,
    _ttl_seconds,
    export_canonical_equities_to_json_file,
    load_cache,
    load_cache_entry,
    save_cache,
    save_cache_entry,
    save_canonical_equities,
)

pytestmark = pytest.mark.unit


def _create_canonical_equity(figi: str, name: str = "TEST EQUITY") -> CanonicalEquity:
    """
    Create a CanonicalEquity instance for testing purposes.

    Args:
        figi (str): The FIGI identifier for the equity.
        name (str): The name of the equity, defaults to "TEST EQUITY".

    Returns:
        CanonicalEquity: A properly constructed CanonicalEquity instance.
    """
    identity = EquityIdentity(
        name=name,
        symbol="TST",
        share_class_figi=figi,
    )
    financials = EquityFinancials()

    return CanonicalEquity(identity=identity, financials=financials)


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
    ARRANGE: two CanonicalEquity objects
    ACT:     save_canonical_equities
    ASSERT:  row count == 2
    """
    expected_row_count = 2
    equities = [
        _create_canonical_equity("BBG000B9XRY4", "EQUITY ONE"),
        _create_canonical_equity("BBG000BKQV61", "EQUITY TWO"),
    ]

    save_canonical_equities(equities)

    assert _count_rows(_EQUITY_TABLE) == expected_row_count


def test_save_equities_upsert_single_row() -> None:
    """
    ARRANGE: same FIGI twice
    ACT:     save_canonical_equities twice
    ASSERT:  row count == 1
    """
    equity = _create_canonical_equity("BBG000C6K6G9")

    save_canonical_equities([equity])
    save_canonical_equities([equity])

    assert _count_rows(_EQUITY_TABLE) == 1


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


def test_export_canonical_equities_writes_expected_json() -> None:
    """
    ARRANGE: two CanonicalEquity objects
    ACT:     export_canonical_equities_to_json_file
    ASSERT:  file JSON equals expected payloads
    """
    equities = [
        _create_canonical_equity("BBG000B9XRY4", "EQUITY ONE"),
        _create_canonical_equity("BBG000BKQV61", "EQUITY TWO"),
    ]
    data_path = Path(os.getenv("_DATA_STORE_DIR", "data"))

    export_canonical_equities_to_json_file(equities)

    assert json.loads(data_path.read_text(encoding="utf-8")) == [
        json.loads(equity.model_dump_json()) for equity in equities
    ]
