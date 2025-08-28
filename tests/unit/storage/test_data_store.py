# storage/test_data_store.py

import gzip
import json
import os
from pathlib import Path

import pytest

from equity_aggregator.schemas import CanonicalEquity, EquityFinancials, EquityIdentity
from equity_aggregator.storage.data_store import (
    _CACHE_TABLE,
    _CANONICAL_EQUITIES_TABLE,
    _CANONICAL_JSONL_ASSET,
    _DATA_STORE_PATH,
    _connect,
    _init_canonical_equities_table,
    _iter_canonical_equity_json_payloads,
    _rebuild_canonical_equity_rows,
    _ttl_seconds,
    export_canonical_equities,
    load_cache,
    load_cache_entry,
    load_canonical_equities,
    load_canonical_equity,
    rebuild_canonical_equities_from_jsonl_gz,
    save_cache,
    save_cache_entry,
    save_canonical_equities,
)

pytestmark = pytest.mark.unit


def test_load_canonical_equity_returns_none_when_not_found() -> None:
    """
    ARRANGE: no row for the FIGI
    ACT:     load_canonical_equity
    ASSERT:  returns None
    """
    assert load_canonical_equity("BBG000NOTFOUND") is None


def test_load_canonical_equity_returns_none_when_payload_empty() -> None:
    """
    ARRANGE: insert row with empty payload for a FIGI
    ACT:     load_canonical_equity
    ASSERT:  returns None
    """
    figi = "BBG000EMPTY1"

    with _connect() as conn:
        _init_canonical_equities_table(conn)
        conn.execute(
            f"INSERT OR REPLACE INTO {_CANONICAL_EQUITIES_TABLE} "
            "(share_class_figi, payload) VALUES (?, ?)",
            (figi, ""),
        )

    assert load_canonical_equity(figi) is None


def test_load_canonical_equity_returns_object_when_found() -> None:
    """
    ARRANGE: save a CanonicalEquity for a FIGI
    ACT:     load_canonical_equity
    ASSERT:  returns a CanonicalEquity with matching FIGI
    """
    figi = "BBG000FOUND1"
    equity = _create_canonical_equity(figi, "FOUND")

    save_canonical_equities([equity])

    loaded = load_canonical_equity(figi)
    assert loaded.identity.share_class_figi == figi


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


def _read_ndjson_gz(path: Path) -> list[dict]:
    """
    Reads a gzipped newline-delimited JSON (NDJSON) file and parses each line.

    Args:
        path (Path): Path to the gzipped NDJSON file.

    Returns:
        list[dict]: List of parsed JSON objects from each line in the file.
    """
    with gzip.open(path, mode="rt", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


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

    assert _count_rows(_CANONICAL_EQUITIES_TABLE) == expected_row_count


def test_save_equities_upsert_single_row() -> None:
    """
    ARRANGE: same FIGI twice
    ACT:     save_canonical_equities twice
    ASSERT:  row count == 1
    """
    equity = _create_canonical_equity("BBG000C6K6G9")

    save_canonical_equities([equity])
    save_canonical_equities([equity])

    assert _count_rows(_CANONICAL_EQUITIES_TABLE) == 1


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


def test_export_canonical_equities_sorted() -> None:
    """
    ARRANGE: save two equities with out-of-order FIGIs
    ACT:     export_canonical_equities
    ASSERT:  exported list is sorted by share_class_figi (deterministic)
    """
    equities = [
        _create_canonical_equity("BBG000BKQV61", "EQUITY TWO"),
        _create_canonical_equity("BBG000B9XRY4", "EQUITY ONE"),
    ]

    save_canonical_equities(equities)

    export_canonical_equities()

    out_path = _DATA_STORE_PATH / _CANONICAL_JSONL_ASSET

    exported = _read_ndjson_gz(out_path)
    assert [equity["identity"]["share_class_figi"] for equity in exported] == [
        "BBG000B9XRY4",
        "BBG000BKQV61",
    ]


def test_rebuild_canonical_equities_from_jsonl_gz_rebuilds_table() -> None:
    """
    ARRANGE: export three equities to the module's default JSONL.GZ path
    ACT:     rebuild_canonical_equities_from_jsonl_gz
    ASSERT:  row count equals number of exported equities
    """
    equities = [
        _create_canonical_equity("BBG000B9XRY4", "ONE"),
        _create_canonical_equity("BBG000BKQV61", "TWO"),
        _create_canonical_equity("BBG000C6K6G9", "THREE"),
    ]

    save_canonical_equities(equities)
    export_canonical_equities()

    rebuild_canonical_equities_from_jsonl_gz()

    assert _count_rows(_CANONICAL_EQUITIES_TABLE) == len(equities)


def test_load_canonical_equities_rehydrates_objects() -> None:
    """
    ARRANGE: save two CanonicalEquity objects
    ACT:     load_canonical_equities
    ASSERT:  loaded objects equal original identities
    """
    equities = [
        _create_canonical_equity("BBG000B9XRY4", "ONE"),
        _create_canonical_equity("BBG000BKQV61", "TWO"),
    ]

    save_canonical_equities(equities)

    loaded = load_canonical_equities()

    assert [equity.identity.share_class_figi for equity in loaded] == [
        "BBG000B9XRY4",
        "BBG000BKQV61",
    ]


def test__iter_canonical_equity_json_payloads_skips_empty_payloads() -> None:
    """
    ARRANGE: insert one row with empty-string payload into the table
    ACT:     iterate _iter_canonical_equity_json_payloads
    ASSERT:  yields no results
    """
    with _connect() as conn:
        _init_canonical_equities_table(conn)
        conn.execute(
            f"INSERT OR REPLACE INTO {_CANONICAL_EQUITIES_TABLE} "
            "(share_class_figi, payload) VALUES (?, ?)",
            ("BBG000EMPTY", ""),
        )

    assert list(_iter_canonical_equity_json_payloads()) == []


def test__rebuild_canonical_equity_rows_skips_blank_lines() -> None:
    """
    ARRANGE: iterable with blanks and two valid JSON lines
    ACT:     _rebuild_canonical_equity_rows
    ASSERT:  extracted (figi, payload) tuples match inputs and preserve payloads
    """
    first_equity = _create_canonical_equity("BBG000B9XRY4", "ONE").model_dump_json()
    second_equity = _create_canonical_equity("BBG000BKQV61", "TWO").model_dump_json()

    lines = ["\n", "   \n", f"{first_equity}\n", "\n", f"{second_equity}\n"]

    rows = list(_rebuild_canonical_equity_rows(lines))

    assert rows == [
        ("BBG000B9XRY4", json.loads(first_equity) and first_equity),
        ("BBG000BKQV61", json.loads(second_equity) and second_equity),
    ]


def test_export_then_read_back_jsonl_gz() -> None:
    """
    ARRANGE: two CanonicalEquity objects
    ACT:     export_canonical_equities
    ASSERT:  parsed JSON objects equal original payloads
    """
    equities = [
        _create_canonical_equity("BBG000B9XRY4", "EQUITY ONE"),
        _create_canonical_equity("BBG000BKQV61", "EQUITY TWO"),
    ]

    save_canonical_equities(equities)
    out_path = _DATA_STORE_PATH / _CANONICAL_JSONL_ASSET

    export_canonical_equities()

    parsed = _read_ndjson_gz(out_path)
    assert parsed == [json.loads(equity.model_dump_json()) for equity in equities]
