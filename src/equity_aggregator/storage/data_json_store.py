# equity_aggregator/data_json_store.py

from collections.abc import Sequence
from pathlib import Path

from pydantic import TypeAdapter

from equity_aggregator.schemas import CanonicalEquity

_DATA_JSON_STORE_PATH: Path = Path("data/data_json_store.json")
_DATA_JSON_STORE_PATH.parent.mkdir(parents=True, exist_ok=True)


def _serialise(
    canonical_equities: Sequence[CanonicalEquity],
    *,
    pretty: bool,
) -> str:
    """
    Serialise a sequence of CanonicalEquity models into one JSON string.

    Uses Pydantic's TypeAdapter to serialise the entire list in one call. This
    applies Pydantic's encoders for types like Decimal, datetime, and UUID.

    Args:
        canonical_equities (Sequence[CanonicalEquity]):
            Sequence of CanonicalEquity models to serialise.
        pretty (bool):
            If True, apply indentation for readability. If False, produce a
            compact string.

    Returns:
        str: UTF-8 JSON text representing the list of canonical equities.
    """
    adapter = TypeAdapter(list[CanonicalEquity])
    payload = adapter.dump_json(
        canonical_equities,
        indent=2 if pretty else None,
    )
    return payload.decode("utf-8")


def save_canonical_equities(
    canonical_equities: Sequence[CanonicalEquity],
    filepath: str | Path = _DATA_JSON_STORE_PATH,
    *,
    pretty: bool = True,
) -> None:
    """
    Write canonical equities to a JSON file at the given path.

    Creates parent directories if needed, serialises the models using the
    Pydantic TypeAdapter, and writes the JSON text to disk.

    Args:
        canonical_equities (Sequence[CanonicalEquity]):
            Sequence of CanonicalEquity models to persist.
        filepath (str | Path):
            Target JSON file path. Defaults to data/data_json_store.json.
        pretty (bool, optional):
            Pretty-print output. Defaults to True.

    Returns:
        None
    """
    path = Path(filepath)
    path.parent.mkdir(parents=True, exist_ok=True)

    payload = _serialise(canonical_equities, pretty=pretty)
    path.write_text(payload, encoding="utf-8")
