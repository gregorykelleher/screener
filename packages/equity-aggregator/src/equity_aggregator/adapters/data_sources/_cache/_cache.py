# equity_data_sources/_cache.py

import logging
import os
import shelve
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TypeVar

logger = logging.getLogger(__name__)

# define a generic type for return values of db callbacks
T = TypeVar("T")


def _get_cache_dir() -> Path:
    """
    Get and prepare the cache directory for storing cache files.

    If the environment variable CACHE_DIR is set, use its value as the cache
    directory. Otherwise, default to a 'data/cache' directory in the current
    working directory. Ensures the directory exists by creating it if necessary.

    Args:
        None

    Returns:
        Path: The absolute path to the cache directory.
    """
    env = os.environ.get("CACHE_DIR")
    path = Path(env) if env else Path.cwd() / "data" / "cache"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _get_cache_ttl() -> int:
    """
    Retrieves the cache time-to-live (TTL) in minutes from the environment variable
    CACHE_TTL_MINUTES. If the variable is not set, defaults to 1440 minutes (24 hours).
    Validates that the value is a non-negative integer.

    Args:
        None

    Returns:
        int: The cache TTL in minutes.

    Raises:
        ValueError: If CACHE_TTL_MINUTES is not a valid non-negative integer.
    """
    raw = os.environ.get("CACHE_TTL_MINUTES", "1440")
    try:
        ttl = int(raw)
    except ValueError as err:
        raise ValueError("CACHE_TTL_MINUTES must be an integer") from err
    if ttl < 0:
        raise ValueError("CACHE_TTL_MINUTES must be >= 0")
    return ttl


# Set up the cache directory and TTL from environment variables or defaults
_CACHE_DIR = _get_cache_dir()
_CACHE_TTL_MINUTES = _get_cache_ttl()


def _db_path(name: str) -> str:
    """
    Get the filesystem path for a cache database with the given name.

    Args:
        name (str): The name of the cache database file.

    Returns:
        str: The absolute path to the cache database file in the cache directory.
    """
    return str(_CACHE_DIR / name)


def _is_expired(timestamp: datetime) -> bool:
    """
    Check if a timestamp is older than the allowed TTL (time-to-live) in minutes.

    Args:
        timestamp (datetime): The original timestamp of the cache entry.

    Returns:
        bool: True if the cache entry has expired, False otherwise.
    """
    return datetime.now(UTC) > timestamp + timedelta(minutes=_CACHE_TTL_MINUTES)


def _with_db(
    name: str,
    fn: Callable[[shelve.DbfilenameShelf], T],
) -> T:
    """
    Open a shelve database at the given name, execute a callback, and ensure the
    database is properly closed.

    Args:
        name (str): The name of the shelve database file (without extension).
        fn (Callable[[shelve.DbfilenameShelf], T]): A function that takes an open
            shelve database and performs operations, returning a value of type T.

    Returns:
        T: The result returned by the callback function.
    """
    path = _db_path(name)
    with shelve.open(path) as db:
        return fn(db)


def _load(db_name: str, key: str = "_") -> object | None:
    """
    Retrieve a cached value from the specified database and key, checking expiration.

    Looks up a timestamped value in the cache database. If the entry is missing or
    expired (based on the configured TTL in minutes), returns None. Otherwise,
    returns the cached value.

    Args:
        db_name (str): Name of the cache database file (without extension).
        key (str, optional): Cache key to retrieve. Defaults to "_".

    Returns:
        object | None: Cached value if present and not expired, otherwise None.
    """

    def _fetch(db: shelve.DbfilenameShelf) -> object | None:
        entry = db.get(key)
        if entry is None:
            return None

        # unpack into (timestamp, value)
        timestamp, value = entry

        if _is_expired(timestamp):
            del db[key]
            logger.debug(
                "Cache expired for %s%s",
                db_name,
                "" if key == "_" else f"[{key}]",
            )
            return None
        return value

    return _with_db(db_name, _fetch)


def load_cache(db_name: str) -> object | None:
    """
    Retrieve the default cache entry from the specified cache database.

    Args:
        db_name (str): Name of the cache database file (without extension).

    Returns:
        object | None: The cached value if present and not expired, else None.
    """
    return _load(db_name, key="_")


def load_cache_entry(db_name: str, key: str) -> object | None:
    """
    Retrieve a specific cache entry by key from the given cache database.

    Args:
        db_name (str): Name of the cache database file (without extension).
        key (str): Cache key to look up in the database.

    Returns:
        object | None: The cached value if present and not expired, else None.
    """
    return _load(db_name, key=key)


def _save(db_name: str, key: str, value: object) -> None:
    """
    Save a value with a timestamp under the given key in the specified cache database.

    Stores the value along with the current UTC timestamp, allowing for expiration
    checks based on the configured TTL. Overwrites any existing entry for the key.

    Args:
        db_name (str): Name of the cache database file (without extension).
        key (str): Cache key under which to store the value.
        value (object): The value to be cached.

    Returns:
        None
    """

    def _write(db: shelve.DbfilenameShelf) -> None:
        db[key] = (datetime.now(UTC), value)

    _with_db(db_name, _write)


def save_cache(db_name: str, value: object) -> None:
    """
    Save a value as the default cache entry in the specified cache database.

    Args:
        db_name (str): Name of the cache database file (without extension).
        value (object): The value to cache as the default entry.

    Returns:
        None: This function does not return a value.

    Side Effects:
        Stores the value in the cache database under the default key "_".
        Logs a message indicating the cache was saved.
    """
    _save(db_name, "_", value)
    logger.debug("Saved cache for %s.db", db_name)


def save_cache_entry(db_name: str, key: str, value: object) -> None:
    """
    Save a value as a keyed cache entry in the specified cache database.

    Args:
        db_name (str): Name of the cache database file (without extension).
        key (str): Cache key under which to store the value.
        value (object): The value to cache for the given key.

    Returns:
        None: This function does not return a value.

    Side Effects:
        Stores the value in the cache database under the specified key.
        Logs a message indicating the cache entry was saved.
    """
    _save(db_name, key, value)
    logger.debug("Saved cache entry %r in %s.db", key, db_name)
