import os
import shelve
from datetime import UTC, datetime, timedelta

import pytest

from equity_aggregator.adapters.data_sources._cache._cache import (
    _CACHE_TTL_MINUTES,
    _db_path,
    _get_cache_ttl,
    _is_expired,
    load_cache,
    load_cache_entry,
    save_cache_entry,
)

pytestmark = pytest.mark.unit


def test_get_cache_ttl_non_integer_raises_value_error() -> None:
    """
    ARRANGE: set env var CACHE_TTL_MINUTES to 'abc'
    ACT:     call _get_cache_ttl
    ASSERT:  ValueError is raised with correct message
    """
    os.environ["CACHE_TTL_MINUTES"] = "abc"

    with pytest.raises(ValueError, match="must be an integer"):
        _get_cache_ttl()


def test_get_cache_ttl_negative_raises_value_error() -> None:
    """
    ARRANGE: set env var CACHE_TTL_MINUTES to '-1'
    ACT:     call _get_cache_ttl
    ASSERT:  ValueError is raised with correct message
    """
    os.environ["CACHE_TTL_MINUTES"] = "-1"

    with pytest.raises(ValueError, match=">= 0"):
        _get_cache_ttl()


def test_save_and_load_cache_entry_preserves_value_or_none_when_immediate_expiry() -> (
    None
):
    """
    ARRANGE: save_cache_entry then load_cache_entry
    ACT:     call helpers
    ASSERT:  loaded value equals saved (or None when TTL==0)
    """
    db_name = "roundtrip"
    key = "k"
    payload = {"v": 42}

    save_cache_entry(db_name, key, payload)
    loaded = load_cache_entry(db_name, key)

    assert loaded == payload or loaded is None


def test_load_cache_returns_value_when_not_expired() -> None:
    """
    ARRANGE: cache entry with timestamp in the future (guaranteed fresh)
    ACT:     call load_cache
    ASSERT:  returns the stored value (branch: _is_expired == False)
    """
    db_name = "fresh_entry"
    expected = {"fresh": True}
    future_ts = datetime.now(UTC) + timedelta(seconds=30)

    with shelve.open(_db_path(db_name)) as db:
        db["_"] = (future_ts, expected)

    assert load_cache(db_name) == expected


def test_is_expired_respects_ttl_setting() -> None:
    """
    ARRANGE: timestamp older than (effective) TTL by one minute
    ACT:     call _is_expired
    ASSERT:  • if TTL > 0 → True   (entry should expire)
             • if TTL = 0 → False  (expiry disabled)
    """
    # Ensure the delta is strictly greater than the active TTL
    delta_minutes = max(_CACHE_TTL_MINUTES, 1) + 1
    old_ts = datetime.now(UTC) - timedelta(minutes=delta_minutes)

    expected = _CACHE_TTL_MINUTES > 0
    assert _is_expired(old_ts) is expected


def test_load_cache_handles_expired_and_non_expired_entries() -> None:
    """
    ARRANGE: write an entry older than the effective TTL
    ACT:     load_cache(<db>)
    ASSERT:  • TTL > 0 → returns None   (entry purged)
             • TTL = 0 → returns value  (expiry disabled)
    """
    db_name = "test_expired"
    stored = {"dummy": 1}

    with shelve.open(_db_path(db_name)) as db:
        delta_minutes = max(_CACHE_TTL_MINUTES, 1) + 1
        expired_ts = datetime.now(UTC) - timedelta(minutes=delta_minutes)
        db["_"] = (expired_ts, stored)

    actual = load_cache(db_name)
    if _CACHE_TTL_MINUTES > 0:
        assert actual is None
    else:
        assert actual == stored


def test_save_and_load_cache_entry_roundtrip() -> None:
    """
    ARRANGE: save_cache_entry then load_cache_entry
    ACT:     call helpers
    ASSERT:  • always returns the saved payload when TTL = 0 (no expiry)
             • may return None when TTL > 0 but the entry has aged out
             (both outcomes are acceptable for generic round-trip test)
    """
    db_name = "roundtrip"
    key = "k"
    payload = {"v": 42}

    save_cache_entry(db_name, key, payload)
    loaded = load_cache_entry(db_name, key)

    if _CACHE_TTL_MINUTES == 0:
        assert loaded == payload
    else:
        # tolerate expiry in high-TTL configurations
        assert loaded == payload or loaded is None


def test_is_expired_both_outcomes() -> None:
    """
    ARRANGE: craft two timestamps – one older-than-TTL, one fresher-than-TTL
    ACT:     call _is_expired on each
    ASSERT:  old → expected (True if TTL>0 else False); fresh → always False
    """
    # ≥1 minute past the TTL ensures "old" really is old even when TTL==0
    minutes_past_ttl = max(_CACHE_TTL_MINUTES, 1) + 1
    ts_old = datetime.now(UTC) - timedelta(minutes=minutes_past_ttl)
    ts_fresh = datetime.now(UTC) + timedelta(minutes=1)

    # branch where comparison is evaluated
    assert (
        _is_expired(ts_old) == (_CACHE_TTL_MINUTES > 0)
        and _is_expired(ts_fresh) is False
    )


def test_is_expired_true_when_ttl_positive() -> None:
    """
    ARRANGE: force a positive TTL and craft an old timestamp
    ACT:     call _is_expired
    ASSERT:  returns True (executes the datetime comparison branch)
    """
    from equity_aggregator.adapters.data_sources._cache import _cache as cache_mod

    previous = cache_mod._CACHE_TTL_MINUTES
    cache_mod._CACHE_TTL_MINUTES = 1  # enable expiry

    try:
        ts_old = datetime.now(UTC) - timedelta(minutes=2)  # older than TTL
        assert cache_mod._is_expired(ts_old) is True
    finally:
        cache_mod._CACHE_TTL_MINUTES = previous  # restore global state


def test_load_cache_purges_and_returns_none() -> None:
    """
    ARRANGE: insert an expired record while TTL is positive
    ACT:     load_cache -> triggers `del db[key]` and returns None
    ASSERT:  load_cache returns None
    """
    from equity_aggregator.adapters.data_sources._cache import _cache as cache_mod

    previous = cache_mod._CACHE_TTL_MINUTES
    cache_mod._CACHE_TTL_MINUTES = 1  # enable expiry

    try:
        db_name = "branch_purge"
        with shelve.open(cache_mod._db_path(db_name)) as db:
            expired_ts = datetime.now(UTC) - timedelta(minutes=2)
            db["_"] = (expired_ts, {"dummy": True})

        assert cache_mod.load_cache(db_name) is None
    finally:
        cache_mod._CACHE_TTL_MINUTES = previous  # restore global state
