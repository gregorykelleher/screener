# equity_aggregator/data_store.py

import logging
import os
import pickle
import sqlite3
import time
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from pathlib import Path

from equity_aggregator.schemas import RawEquity

logger = logging.getLogger(__name__)

_DB_PATH: Path = Path(os.getenv("_DATA_STORE_DIR", "data/data_store")) / "data_store.db"
_DB_PATH.parent.mkdir(parents=True, exist_ok=True)

_EQUITY_TABLE = "equity_profiles"
_CACHE_TABLE = "object_cache"


@contextmanager
def _connect() -> Iterator[sqlite3.Connection]:
    """
    Context manager for establishing a SQLite database connection.

    Opens a connection to the database at the path specified by the module-level
    variable `_DB_PATH`. Enables foreign key support for the session. Ensures the
    connection is properly closed after use.

    Yields:
        sqlite3.Connection: An active SQLite database connection.

    Returns:
        Iterator[sqlite3.Connection]: An iterator yielding the database connection.
    """
    conn = sqlite3.connect(_DB_PATH, isolation_level=None, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
    finally:
        conn.close()


def _init_equity_table(conn: sqlite3.Connection) -> None:
    """
    Initialises the equity table in the provided SQLite database connection.

    Creates a table with the name specified by the module-level variable `_EQUITY_TABLE`
    if it does not already exist. The table contains two columns: 'share_class_figi' as
    the primary key and 'payload' as a blob field.

    Args:
        conn (sqlite3.Connection): The SQLite database connection to use for table
            creation.

    Returns:
        None
    """
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {_EQUITY_TABLE} (
            share_class_figi TEXT PRIMARY KEY,
            payload          BLOB NOT NULL
        );
        """,
    )


def _init_cache_table(conn: sqlite3.Connection) -> None:
    """
    Initialises the cache table in the provided SQLite database connection.

    Creates a table named as specified by the module-level variable `_CACHE_TABLE`
    if it does not already exist. The table includes columns for cache name, key,
    creation timestamp, and payload. The combination of cache name and key serves
    as the primary key.

    Args:
        conn (sqlite3.Connection): The SQLite database connection to use for
            table creation.

    Returns:
        None
    """
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {_CACHE_TABLE} (
            cache_name   TEXT NOT NULL,
            key          TEXT NOT NULL,
            created_at   INTEGER NOT NULL,
            payload      BLOB NOT NULL,
            PRIMARY KEY (cache_name, key)
        );
        """,
    )


def _serialise(raw_equity: RawEquity) -> tuple[str, bytes]:
    """
    Serialise a RawEquity object into a (figi, payload) tuple for database storage.

    Args:
        raw_equity (RawEquity): The RawEquity instance to serialise. Must have a
            non-empty 'share_class_figi' attribute.

    Returns:
        tuple[str, bytes]: A tuple containing the share class FIGI as a string and
            the pickled RawEquity object as bytes.

    Raises:
        ValueError: If 'share_class_figi' is missing or empty in the provided object.
    """
    figi = raw_equity.share_class_figi
    if not figi:
        raise ValueError("share_class_figi is required for equity persistence")

    return figi, pickle.dumps(raw_equity, protocol=4)


def save_equities(raw_equities: Iterable[RawEquity]) -> None:
    """
    Saves a collection of RawEquity objects to the database.

    Each equity is serialised and inserted or replaced in the database table. The
    function ensures the database connection is established and initialised before
    performing the operation.

    Args:
        equities (Iterable[RawEquity]): An iterable of RawEquity objects to be saved
            to the database.

    Returns:
        None
    """
    with _connect() as conn:
        _init_equity_table(conn)

        conn.executemany(
            f"INSERT OR REPLACE INTO {_EQUITY_TABLE} "
            "(share_class_figi, payload) VALUES (?, ?)",
            map(_serialise, raw_equities),
        )


def _ttl_seconds() -> int:
    """
    Calculates the cache time-to-live (TTL) in seconds based on the environment variable
    CACHE_TTL_MINUTES. If the variable is not set, defaults to 1440 minutes (24 hours).

    Args:
        None

    Returns:
        int: The TTL value in seconds.

    Raises:
        ValueError: If CACHE_TTL_MINUTES is set to a negative value.
    """
    ttl_min = int(os.getenv("CACHE_TTL_MINUTES", "1440"))

    if ttl_min < 0:
        raise ValueError("CACHE_TTL_MINUTES must be ≥ 0")
    return ttl_min * 60


def _purge_expired(conn: sqlite3.Connection, cache_name: str, key: str | None) -> None:
    """
    Remove expired cache entries from database for a given cache name and optional key.

    Entries are considered expired if the time elapsed since their creation exceeds the
    configured time-to-live (TTL) value. If TTL is set to 0, expiry is disabled and no
    entries are removed.

    Args:
        conn (sqlite3.Connection): The SQLite database connection to use for deletion.
        cache_name (str): The name of the cache to purge expired entries from.
        key (str | None): The specific cache key to purge. If None, purges entries with
            a NULL key.

    Returns:
        None
    """
    ttl = _ttl_seconds()

    if ttl == 0:
        return  # expiry disabled

    now = int(time.time())
    where_key = "key = ?" if key is not None else "key IS ?"

    conn.execute(
        f"DELETE FROM {_CACHE_TABLE} "
        f"WHERE cache_name = ? AND {where_key} AND ? - created_at > ?",
        (cache_name, key, now, ttl),
    )


def _cache_put(
    conn: sqlite3.Connection,
    cache_name: str,
    key: str,
    value: object,
) -> None:
    """
    Stores a value in the SQLite cache table with the specified cache name and key.

    If an entry with the same cache name and key already exists, it will be replaced.
    The value is serialised using pickle before storage, and the current timestamp is
    recorded as the creation time.

    Args:
        conn (sqlite3.Connection): The SQLite database connection object.
        cache_name (str): The name of the cache to store the value under.
        key (str): The key identifying the cached value.
        value (object): The Python object to cache; must be pickle-serialisable.

    Returns:
        None
    """
    _init_cache_table(conn)

    conn.execute(
        f"INSERT OR REPLACE INTO {_CACHE_TABLE} "
        "(cache_name, key, created_at, payload) "
        "VALUES (?, ?, ?, ?)",
        (cache_name, key, int(time.time()), pickle.dumps(value, protocol=4)),
    )


def _cache_get(conn: sqlite3.Connection, cache_name: str, key: str) -> object | None:
    """
    Retrieve a cached object from database for the specified cache name and key.

    This function initialises the cache table if it does not exist, purges any expired
    entries for the given cache name and key, and then attempts to fetch the cached
    payload. If a cached value is found, it is deserialised and returned; otherwise,
    None is returned.

    Args:
        conn (sqlite3.Connection): The SQLite database connection.
        cache_name (str): The name of the cache to query.
        key (str): The key identifying the cached object.

    Returns:
        object | None: The deserialised cached object if found, otherwise None.
    """
    _init_cache_table(conn)

    _purge_expired(conn, cache_name, key)

    row = conn.execute(
        f"SELECT payload FROM {_CACHE_TABLE} WHERE cache_name = ? AND key = ?",
        (cache_name, key),
    ).fetchone()

    return pickle.loads(row[0]) if row else None


def save_cache(cache_name: str, value: object) -> None:
    """
    Saves a value to the cache with the specified cache name.

    Args:
        cache_name (str): The unique identifier for the cache entry.
        value (object): The value to be stored in the cache.

    Returns:
        None
    """
    with _connect() as conn:
        _cache_put(conn, cache_name, "_", value)


def load_cache(cache_name: str) -> object | None:
    """
    Load a cached object from the cache store by its name.

    Args:
        cache_name (str): The unique name identifying the cached object.

    Returns:
        object | None: The cached object if found, otherwise None.
    """
    with _connect() as conn:
        return _cache_get(conn, cache_name, "_")


def save_cache_entry(cache_name: str, key: str, value: object) -> None:
    """
    Saves a value in the cache under the specified cache name and key.

    This function establishes a connection to the cache storage and stores the given
    value associated with the provided cache name and key.

    Args:
        cache_name (str): The name of the cache to store the entry in.
        key (str): The key under which the value will be stored.
        value (object): The value to be cached.

    Returns:
        None
    """
    with _connect() as conn:
        _cache_put(conn, cache_name, key, value)


def load_cache_entry(cache_name: str, key: str) -> object | None:
    """
    Retrieve a cached entry from the specified cache using the provided key.

    Args:
        cache_name (str): The name of the cache to retrieve the entry from.
        key (str): The key identifying the cached entry.

    Returns:
        object | None: The cached object if found, otherwise None.
    """
    with _connect() as conn:
        return _cache_get(conn, cache_name, key)
