# storage/data_store.py

import gzip
import json
import os
import pickle
import sqlite3
import time
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from pathlib import Path

from equity_aggregator.schemas import CanonicalEquity

_DATA_STORE_PATH: Path = Path(os.getenv("_DATA_STORE_DIR", "data/data_store/"))

_CANONICAL_EQUITIES_TABLE = "canonical_equities"
_CANONICAL_JSONL_ASSET = "canonical_equities.jsonl.gz"

_CACHE_TABLE = "object_cache"


@contextmanager
def _connect() -> Iterator[sqlite3.Connection]:
    """
    Context manager for establishing a SQLite database connection.

    Opens a connection to the database at the path specified by the module-level
    variable `_DATA_STORE_PATH`. Enables foreign key support for the session. Ensures
    the connection is properly closed after use.

    Yields:
        sqlite3.Connection: An active SQLite database connection.

    Returns:
        Iterator[sqlite3.Connection]: An iterator yielding the database connection.
    """
    _DATA_STORE_PATH.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(
        _DATA_STORE_PATH / "data_store.db",
        isolation_level=None,
        check_same_thread=False,
    )
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
    finally:
        conn.close()


def _init_canonical_equities_table(conn: sqlite3.Connection) -> None:
    """
    Initialises the canonical equities table in the provided SQLite database connection.

    Creates a table with the name specified by the variable `_CANONICAL_EQUITIES_TABLE`
    if it does not already exist. The table contains two columns: 'share_class_figi' as
    the primary key and 'payload' as a text field.

    Args:
        conn (sqlite3.Connection): The SQLite database connection to use for table
            creation.

    Returns:
        None
    """
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {_CANONICAL_EQUITIES_TABLE} (
            share_class_figi TEXT PRIMARY KEY,
            payload          TEXT NOT NULL
        ) WITHOUT ROWID;
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


def _serialise_equity(canonical_equity: CanonicalEquity) -> tuple[str, str]:
    """
    Serialise a CanonicalEquity object into (figi, payload) tuple for database
    storage.

    Args:
        canonical_equity (CanonicalEquity): The CanonicalEquity instance to serialise.

    Returns:
        tuple[str, str]: A tuple containing the share class FIGI as a string and
            the JSON-serialised CanonicalEquity object as a string.

    Raises:
        ValueError: If 'share_class_figi' is missing or empty in the provided object.
    """
    figi = canonical_equity.identity.share_class_figi

    json_data = canonical_equity.model_dump_json()
    return figi, json_data


def save_canonical_equities(canonical_equities: Iterable[CanonicalEquity]) -> None:
    """
    Saves a collection of CanonicalEquity objects to the database.

    Each equity is serialised and inserted or replaced in the database table. The
    function ensures the database connection is established and initialised before
    performing the operation.

    Args:
        equities (Iterable[CanonicalEquity]): An iterable of CanonicalEquity objects to
            be saved to the database.

    Returns:
        None
    """
    with _connect() as conn:
        _init_canonical_equities_table(conn)

        conn.executemany(
            f"INSERT OR REPLACE INTO {_CANONICAL_EQUITIES_TABLE} "
            "(share_class_figi, payload) VALUES (?, ?)",
            map(_serialise_equity, canonical_equities),
        )


def export_canonical_equities() -> None:
    """
    Export canonical equities as newline-delimited JSON (NDJSON), compressed with gzip.

    Each line contains one equity JSON object, matching the schema in the 'payload'
    column. Output is ordered by share_class_figi for deterministic results.

    Args:
        None

    Returns:
        None
    """
    # TODO: Add condition to only export equities if there's a pre-existing database
    with _connect() as conn:
        _init_canonical_equities_table(conn)

        cursor = conn.execute(
            (
                f"SELECT payload FROM {_CANONICAL_EQUITIES_TABLE} "
                "ORDER BY share_class_figi"
            ),
        )

        with gzip.open(
            _DATA_STORE_PATH / _CANONICAL_JSONL_ASSET,
            mode="wt",
            encoding="utf-8",
            compresslevel=9,
        ) as gz:
            for (payload_str,) in cursor:
                # payload is already JSON text; write as-is and terminate with newline
                gz.write(payload_str)
                gz.write("\n")


def rebuild_canonical_equities_from_jsonl_gz() -> None:
    """
    Rebuilds the canonical_equities table in the SQLite database from a gzip-compressed
    JSONL file. Drops and recreates the table, then populates it with canonical equity
    records from the source file. This operation is idempotent and optimises the
    database after completion.

    Args:
        None

    Returns:
        None
    """
    src_path = _DATA_STORE_PATH / _CANONICAL_JSONL_ASSET
    dest_path = _DATA_STORE_PATH / "data_store.db"

    with sqlite3.connect(dest_path, isolation_level=None) as conn:
        _rebuild_canonical_equities_schema(conn)
        _rebuild_canonical_equities_table(conn, src_path)
        conn.execute("VACUUM")  # Optimise database


def _rebuild_canonical_equities_schema(conn: sqlite3.Connection) -> None:
    """
    Drops and rebuilds the canonical_equities table with performance optimisations.

    This function disables SQLite journaling and synchronous writes for faster bulk
    operations, then drops the canonical_equities table if it exists and rebuilds it
    with the required schema.

    Args:
        conn (sqlite3.Connection): The SQLite database connection to use.

    Returns:
        None
    """
    conn.executescript(f"""
        PRAGMA journal_mode=OFF;
        PRAGMA synchronous=OFF;
        DROP TABLE IF EXISTS {_CANONICAL_EQUITIES_TABLE};
        CREATE TABLE {_CANONICAL_EQUITIES_TABLE}(
            share_class_figi TEXT PRIMARY KEY,
            payload          TEXT NOT NULL
        ) WITHOUT ROWID;
    """)


def _rebuild_canonical_equities_table(
    conn: sqlite3.Connection,
    src_path: Path,
) -> None:
    """
    Populates the canonical equities table in the database from a compressed JSONL file.

    Reads equity data from a gzip-compressed JSON Lines file, extracts relevant
    rows, and inserts or replaces them into the canonical equities table.

    Args:
        conn (sqlite3.Connection): SQLite database connection object.
        src_path (Path): Path to the gzip-compressed JSONL file containing equity data.

    Returns:
        None
    """
    with gzip.open(src_path, "rt", encoding="utf-8") as file_handler:
        equity_rows = _rebuild_canonical_equity_rows(file_handler)
        conn.executemany(
            (
                f"INSERT OR REPLACE INTO {_CANONICAL_EQUITIES_TABLE} "
                "(share_class_figi, payload) VALUES (?, ?)"
            ),
            equity_rows,
        )


def _rebuild_canonical_equity_rows(
    file_handler: Iterable[str],
) -> Iterator[tuple[str, str]]:
    """
    Extracts (figi, payload) pairs from a JSON Lines file handler and rebuilds
    each valid line into a tuple containing the share class FIGI and
    the original JSON payload string.

    Args:
        file_handler: An iterable file-like object yielding JSONL strings.

    Returns:
        Iterator[tuple[str, str]]: An iterator of (figi, payload) tuples.
    """
    loads = json.loads

    for line in file_handler:
        json_line = line.strip()  # Remove leading/trailing whitespace

        if not json_line:
            continue

        figi = loads(json_line)["identity"]["share_class_figi"]
        yield figi, json_line


def load_canonical_equity(share_class_figi: str) -> CanonicalEquity | None:
    """
    Retrieve a single CanonicalEquity by its exact share_class_figi value.

    Args:
        share_class_figi (str): The FIGI identifier of the equity to load.

    Returns:
        CanonicalEquity | None: The CanonicalEquity instance if found, else None.
    """
    with _connect() as conn:
        _init_canonical_equities_table(conn)
        row = conn.execute(
            (
                f"SELECT payload FROM {_CANONICAL_EQUITIES_TABLE} "
                "WHERE share_class_figi = ? LIMIT 1"
            ),
            (share_class_figi,),
        ).fetchone()
        return CanonicalEquity.model_validate_json(row[0]) if row and row[0] else None


def load_canonical_equities() -> list[CanonicalEquity]:
    """
    Loads and rehydrates all CanonicalEquity objects from the database.

    Iterates over all JSON payloads stored in the canonical_equities table,
    deserialises each payload using CanonicalEquity.model_validate_json, and
    returns a list of CanonicalEquity instances.

    Args:
        None

    Returns:
        list[CanonicalEquity]: List of all rehydrated CanonicalEquity objects.
    """
    return [
        CanonicalEquity.model_validate_json(payload)
        for payload in _iter_canonical_equity_json_payloads()
    ]


def _iter_canonical_equity_json_payloads() -> Iterator[str]:
    """
    Yields JSON payload strings from canonical_equities table in deterministic order.

    Args:
        None

    Returns:
        Iterator[str]: Iterator over JSON payload strings, ordered by share_class_figi.
    """
    with _connect() as conn:
        _init_canonical_equities_table(conn)
        cursor = conn.execute(
            (
                f"SELECT payload FROM {_CANONICAL_EQUITIES_TABLE} "
                "ORDER BY share_class_figi"
            ),
        )
        for (payload_str,) in cursor:
            if payload_str:
                yield payload_str


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
    Retrieve a cached object from the cache store using its cache name.

    Opens a database connection, purges expired entries, and fetches the cached
    value associated with the given cache name. Returns None if no entry is found
    or if cache_name is None.

    Args:
        cache_name (str): Unique identifier for the cached object.

    Returns:
        object | None: The cached object if present, otherwise None.
    """
    if cache_name is None:
        return None

    with _connect() as conn:
        return _cache_get(conn, cache_name, "_")


def save_cache_entry(
    cache_name: str,
    key: str,
    value: object,
) -> None:
    """
    Save a value in the cache under the given cache name and key.

    Opens a connection to the cache store and persists the value using the
    specified cache name and key. If cache_name is None, the function does
    nothing.

    Args:
        cache_name (str): Name of the cache to store the entry in.
        key (str): Key under which the value will be stored.
        value (object): The value to cache; must be pickle-serialisable.

    Returns:
        None
    """
    if cache_name is None:
        return

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
