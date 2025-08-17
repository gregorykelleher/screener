# reference_lookup/openfigi.py

import asyncio
import logging
import os
import re
from collections.abc import Callable, Sequence
from typing import Protocol

import pandas as pd
from openfigipy import OpenFigiClient

from equity_aggregator import load_cache, save_cache
from equity_aggregator.schemas import RawEquity

logger = logging.getLogger(__name__)

type IdentificationRecord = tuple[str | None, str | None, str | None]


class _OpenFigiClient(Protocol):
    """
    Protocol for OpenFIGI client implementations.

    Defines the expected interface for an OpenFIGI client.

    Methods
    -------
    connect() -> None
        Establishes a connection to the OpenFIGI service. May raise an exception
        if connection fails.

    map(df: pd.DataFrame) -> pd.DataFrame
        Maps reference data using the provided DataFrame and returns a new
        DataFrame containing the identification results.

        Parameters
        ----------
        df : pd.DataFrame
            The input DataFrame containing reference data queries (e.g. ISINs, tickers).

        Returns
        -------
        pd.DataFrame
            The mapped results returned from the OpenFIGI service.
    """

    def connect(self) -> object: ...
    def map(self, df: pd.DataFrame) -> pd.DataFrame: ...


async def fetch_equity_identification(
    raw_equities: Sequence[RawEquity],
    *,
    client_factory: Callable[[], _OpenFigiClient | None] = None,
    cache_key: str | None = "openfigi",
) -> list[IdentificationRecord]:
    """
    Fetches equity identification records using the OpenFIGI API, with caching support.

    This asynchronous function attempts to load identification records from cache
    first. If not available, it uses the provided client factory to create an
    OpenFIGI client and fetches identification records for the given raw equities.
    Results are cached for future calls.

    Args:
        raw_equities (Sequence[RawEquity]): List of raw equity objects to identify.
        client_factory (Callable[[], _OpenFigiClient | None], optional): Factory
            function to create an OpenFIGI client. If not provided, a default client
            is used.
        cache_key (str | None, optional): Key for cache lookup and storage. Defaults
            to "openfigi".

    Returns:
        list[IdentificationRecord]: List of identification records for the equities.
    """
    if not raw_equities:
        return []

    cached = load_cache(cache_key)
    if cached is not None:
        logger.debug("Loaded %d OpenFIGI records from cache.", len(cached))
        return cached

    # resolve identities using the provided client or a default one
    identities = await _resolve_identities(
        raw_equities,
        client_factory or _make_openfigi_client,
    )

    save_cache(cache_key, identities)
    logger.debug("Saved %d OpenFIGI identification records to cache.", len(identities))
    return identities


async def _resolve_identities(
    raw_equities: Sequence[RawEquity],
    client_factory: Callable[[], _OpenFigiClient | None],
) -> list[IdentificationRecord]:
    """
    Resolve identification records for a sequence of raw equities using OpenFIGI.

    This function attempts to create an OpenFIGI client using the provided factory.
    If the client is valid, it identifies each equity with a fallback mechanism.
    If the client cannot be created, it returns a list of default (None, None, None)
    records matching the input length.

    Args:
        raw_equities (Sequence[RawEquity]): Sequence of raw equity objects to resolve.
        client_factory (Callable[[], _OpenFigiClient | None]): Factory returning an
            OpenFIGI client instance or None.

    Returns:
        list[IdentificationRecord]: Identification records for each input equity.
            If client is invalid, returns a list of (None, None, None) records.
    """
    client = client_factory()

    if client is None:
        return [(None, None, None)] * len(raw_equities)

    return await _identify_with_fallback(raw_equities, client)


def _get_api_key(getenv: Callable[[str], str | None] = os.getenv) -> str | None:
    """
    Retrieve the OpenFIGI API key from environment variables.

    Args:
        getenv (Callable[[str], str | None], optional): Function to fetch environment
            variables. Defaults to os.getenv.

    Returns:
        str | None: The OpenFIGI API key if set, otherwise None.
    """
    api_key = getenv("OPENFIGI_API_KEY")

    if not api_key:
        logger.error(
            "OPENFIGI_API_KEY is not set; returning no identifications.",
            exc_info=False,
        )
        return None

    return api_key


def _make_openfigi_client(
    *,
    api_key_provider: Callable[[], str | None] = _get_api_key,
    incoming_client: Callable[[str], _OpenFigiClient] = OpenFigiClient,
) -> _OpenFigiClient | None:
    """
    Creates and connects an OpenFIGI client using the provided API key provider and
    client factory. If the API key is missing or the connection fails, returns None.

    Args:
        api_key_provider (Callable[[], str | None], optional): Function to retrieve
            the OpenFIGI API key. Defaults to _get_api_key.
        incoming_client (Callable[[str], _OpenFigiClient], optional): Factory function
            to instantiate OpenFIGI client with the API key. Defaults to OpenFigiClient.

    Returns:
        _OpenFigiClient | None: Connected OpenFIGI client instance, or None if the
            API key is missing or connection fails.
    """
    api_key = api_key_provider()

    if api_key is None:
        return None

    open_figi_client = incoming_client(api_key)

    try:
        open_figi_client.connect()
    except Exception as exc:
        logger.error("Failed to connect OpenFIGI client: %s", exc, exc_info=False)
        return None

    return open_figi_client


async def _identify_with_fallback(
    raw_equities: Sequence[RawEquity],
    client: _OpenFigiClient,
) -> list[IdentificationRecord]:
    """
    Attempts to identify a sequence of raw equities using the provided OpenFIGI client.
    Falls back to returning a list of (None, None, None) tuples if mapping fails.

    Args:
        raw_equities (Sequence[RawEquity]): Sequence of raw equity objects to identify.
        client (_OpenFigiClient): The OpenFIGI client instance.

    Returns:
        list[IdentificationRecord]: List of identification records for each equity.
            If mapping fails, returns list of (None, None, None) tuples of same length
            as raw_equities.
    """
    mapped_df = await _map_or_none(raw_equities, client)

    if mapped_df is None:
        return [(None, None, None)] * len(raw_equities)

    return extract_identified_records(mapped_df, expected=len(raw_equities))


async def _map_or_none(
    data: Sequence[RawEquity] | pd.DataFrame,
    client: _OpenFigiClient,
) -> pd.DataFrame | None:
    """
    Maps a sequence of RawEquity objects or a query DataFrame using OpenFIGI client.

    If data is a sequence of RawEquity, it will be converted to a DataFrame first.

    Args:
        data (Sequence[RawEquity] | pd.DataFrame): Raw equities or pre-constructed query
            DataFrame.
        client (_OpenFigiClient): The OpenFIGI client to use for mapping.

    Returns:
        pd.DataFrame | None: Mapped results or None on failure.
    """

    try:
        query_df = _build_query_dataframe(data) if isinstance(data, Sequence) else data
        return await asyncio.to_thread(client.map, query_df)
    except Exception as exc:
        logger.error("OpenFIGI mapping failed: %s", exc, exc_info=False)
        return None


def extract_identified_records(
    response: pd.DataFrame,
    *,
    expected: int,
) -> list[IdentificationRecord]:
    """
    Extract identification records from an OpenFIGI response DataFrame.

    Ensures a 1:1 mapping between input queries and output records. For each query
    index, selects the last valid match (last-wins) from the response. If no valid
    entry exists for an index, (None, None, None) is used.

    Args:
        response (pd.DataFrame): DataFrame containing OpenFIGI API response records.
        expected (int): Number of input queries; output list will match this length.

    Returns:
        list[IdentificationRecord]: List of identification records in input order.
            Each record is a tuple (name, symbol, figi), or (None, None, None) if
            missing.
    """
    response_entries = list(reversed(response.to_dict(orient="records")))

    # Filter only valid entries and extract (index, record) pairs
    valid_entries = filter(
        lambda entry: _is_valid_response_entry(entry, expected),
        response_entries,
    )

    indexed_records = map(_extract_indexed_record, valid_entries)

    records_by_query_index: dict[int, IdentificationRecord] = {}
    for index, record in indexed_records:
        records_by_query_index.setdefault(index, record)

    return [records_by_query_index.get(i, (None, None, None)) for i in range(expected)]


def _is_valid_response_entry(
    entry: dict,
    expected: int,
) -> bool:
    """
    Determine if a entry from the OpenFIGI response is valid.

    A valid entry must have a query index within the expected range, a valid FIGI,
    and a non-None identification record.

    Args:
        entry (dict): A single record from the OpenFIGI API response.
        expected (int): The total number of expected records.

    Returns:
        bool: True if the entry is valid and its index is within range, else False.
    """
    index = _get_query_index(entry)

    figi = entry.get("shareClassFIGI")

    return (
        index is not None
        and 0 <= index < expected
        and _is_valid_figi(figi)
        and _to_identification_record(entry) is not None
    )


def _extract_indexed_record(entry: dict) -> tuple[int, IdentificationRecord]:
    """
    Extracts the query index and associated identification record from a response entry.

    Args:
        entry (dict): A validated OpenFIGI response entry.

    Returns:
        tuple[int, IdentificationRecord]: The index and corresponding record.

    Raises:
        ValueError: If index or record is unexpectedly missing.
    """
    index = _get_query_index(entry)
    record = _to_identification_record(entry)

    if index is None or record is None:
        raise ValueError("Invalid response entry: missing index or record.")

    return index, record


def _to_identification_record(entry: dict) -> IdentificationRecord | None:
    """
    Extracts a structured identification record from a single OpenFIGI response entry.

    Attempts to construct a (name, symbol, figi) tuple from the given response dict.
    Returns None if the FIGI is missing or invalid.

    Args:
        entry (dict): A single response item from the OpenFIGI API. Expected to contain
            keys such as 'shareClassFIGI', 'ticker', 'name', or 'securityName'.

    Returns:
        IdentificationRecord | None: A (name, symbol, figi) tuple if valid, else None.
    """
    figi = entry.get("shareClassFIGI")

    if not isinstance(figi, str):
        return None

    # Extract ticker symbol only if it's a valid string
    symbol = entry.get("ticker") if isinstance(entry.get("ticker"), str) else None

    # Prefer 'name' field, fallback to 'securityName' if not present
    preferred_name_field = entry.get("name") or entry.get("securityName")

    # Use the preferred name if valid, else fallback to symbol
    name = (
        preferred_name_field
        if isinstance(preferred_name_field, str) and preferred_name_field
        else symbol
    )

    return (name, symbol, figi)


def _get_query_index(entry: dict) -> int | None:
    """
    Extracts the query index from an OpenFIGI response entry.

    Searches for known index keys ("query_number", "queryId", "request_index") in
    the entry and returns the first valid integer value found. Returns None if no
    valid index is present.

    Args:
        entry (dict): A single OpenFIGI API response record.

    Returns:
        int | None: The extracted query index, or None if not found or invalid.
    """
    return next(
        (
            int(value)
            for key in ("query_number", "queryId", "request_index")
            if isinstance((value := entry.get(key)), int | float)
        ),
        None,
    )


def _is_valid_figi(value: object) -> bool:
    """
    Validates if a value is a valid FIGI string.

    Args:
        value (object): Any input value.

    Returns:
        bool: True if value is a 12-character uppercase alphanumeric string.
    """
    return isinstance(value, str) and re.fullmatch(r"[A-Z0-9]{12}", value) is not None


def _build_query_dataframe(equities: Sequence[RawEquity]) -> pd.DataFrame:
    """
    Converts a list of RawEquity objects into an OpenFIGI query DataFrame.

    Args:
        equities (Sequence[RawEquity]): Input equity objects.

    Returns:
        pd.DataFrame: A DataFrame containing OpenFIGI-compatible queries.
    """
    return pd.DataFrame([_to_query_record(equity) for equity in equities])


def _to_query_record(equity: RawEquity) -> dict[str, str]:
    """
    Converts a RawEquity object into a dictionary for querying OpenFIGI.

    Args:
        equity (RawEquity): The equity containing ISIN, CUSIP, or symbol.

    Returns:
        dict[str, str]: A dict with idType, idValue, and marketSecDes.
    """
    if equity.isin:
        return {"idType": "ID_ISIN", "idValue": equity.isin, "marketSecDes": "Equity"}

    if equity.cusip:
        return {"idType": "ID_CUSIP", "idValue": equity.cusip, "marketSecDes": "Equity"}

    return {"idType": "TICKER", "idValue": equity.symbol, "marketSecDes": "Equity"}
