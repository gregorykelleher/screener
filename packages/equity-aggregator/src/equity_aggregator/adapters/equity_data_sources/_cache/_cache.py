# equity_data_sources/_cache.py

import shelve
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Optional, Tuple, Callable

logger = logging.getLogger(__name__)

_CACHE_DIR = Path.cwd() / "data"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _db_path(name: str) -> str:
    """
    Compute the filesystem path for a given cache database name.
    """
    return str(_CACHE_DIR / name)


def _is_expired(timestamp: datetime, ttl_minutes: int) -> bool:
    """
    Determine whether a given timestamp is older than `now - ttl_minutes`.
    This is used to check if a cache entry has expired.
    """
    return datetime.now(timezone.utc) > timestamp + timedelta(minutes=ttl_minutes)


def _with_db(name: str, fn: Callable[[shelve.DbfilenameShelf], Any]) -> Any:
    """
    Open a shelve database, execute a callback, and ensure the DB is closed.
    """
    path = _db_path(name)
    with shelve.open(path) as db:
        return fn(db)


def _load(
    db_name: str,
    key: str = "_",
    ttl_minutes: Optional[int] = None,
) -> Optional[Any]:
    """
    Core loader: retrieves a timestamped value from cache
    and checks if it is expired. If the entry is missing or expired,
    returns None; otherwise, returns the cached value.
    """

    def _fetch(db: shelve.DbfilenameShelf) -> Optional[Any]:
        entry: Optional[Tuple[datetime, Any]] = db.get(key)
        if entry is None:
            return None
        ts, value = entry
        if ttl_minutes is not None and _is_expired(ts, ttl_minutes):
            logger.info("Cache expired for %s[%s]", db_name, key)
            return None
        return value

    return _with_db(db_name, _fetch)


def load_cache(db_name: str, ttl_minutes: Optional[int] = None) -> Optional[Any]:
    """
    Load the whole-database cache entry from `<db_name>.db`.
    Returns the cached value, or None if missing or expired.
    """
    return _load(db_name, key="_", ttl_minutes=ttl_minutes)


def load_cache_entry(
    db_name: str,
    key: str,
    ttl_minutes: Optional[int] = None,
) -> Optional[Any]:
    """
    Load a single keyed entry from `<db_name>.db`.
    Returns the cached value, or None if missing or expired.
    """
    return _load(db_name, key=key, ttl_minutes=ttl_minutes)


def _save(
    db_name: str,
    key: str,
    value: Any,
) -> None:
    """
    Core saver: write a timestamped value under `key` into `<db_name>.db`.
    """

    def _write(db: shelve.DbfilenameShelf) -> None:
        db[key] = (datetime.now(timezone.utc), value)

    _with_db(db_name, _write)


def save_cache(db_name: str, value: Any) -> None:
    """
    Save a cache entry under `<db_name>.db`.
    """
    _save(db_name, "_", value)
    logger.info("Saved cache for %s.db", db_name)


def save_cache_entry(
    db_name: str,
    key: str,
    value: Any,
) -> None:
    """
    Save a single keyed cache entry into `<db_name>.db`.
    """
    _save(db_name, key, value)
    logger.info("Saved cache entry %r in %s.db", key, db_name)
