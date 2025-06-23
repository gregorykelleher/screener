# reference_lookup/openfigi.py

import asyncio
import logging
import os
import re
from collections.abc import Sequence

import pandas as pd
from openfigipy import OpenFigiClient

from equity_aggregator.adapters.data_sources._cache import load_cache, save_cache
from equity_aggregator.schemas import RawEquity

logger = logging.getLogger(__name__)


async def fetch_equity_identification(
    raw_equities: Sequence[RawEquity],
) -> list[tuple[str | None, str | None, str | None]]:
    """
    Retrieve (name, symbol, shareClassFIGI) for each RawEquity in input order.

    The function mirrors the caching, chunking and error-handling behaviour of
    `get_share_class_figi_for_raw_equities` but returns a triplet:

        (resolved_name | None, resolved_symbol | None, shareClassFIGI | None)

    Args:
        raw_equities (Sequence[RawEquity]): Raw equities to resolve.

    Returns:
        list[tuple[str | None, str | None, str | None]]:
            Triplet per input equity (same length, same order).
    """
    if not raw_equities:
        return []

    # load from cache if available
    cached = load_cache("openfigi_cache")
    if cached is not None:
        logger.debug("Loaded OpenFIGI identification metadata from cache.")
        _log_missing_figis(raw_equities, [figi for _, _, figi in cached])
        return cached

    # async resolution in 100-record chunks (10 requests in flight concurrently)
    identification = await _resolve_identification(raw_equities)

    _log_missing_figis(raw_equities, [figi for _, _, figi in identification])

    # TODO: add exception handling and exit strategy

    # persist retrieved shareClassFIGIs to cache
    save_cache("openfigi_cache", identification)
    logger.debug("Saved OpenFIGI identification metadata to cache.")

    return identification


async def _resolve_identification(
    raw_equities: Sequence[RawEquity],
    chunk_size: int = 100,
    # TODO: refactor out concurrency
    concurrency: int = 10,
) -> list[tuple[str | None, str | None, str | None]]:
    """
    Asynchronously resolve (name, symbol, shareClassFIGI) triplets for a sequence of
    RawEquity objects using the OpenFIGI API.

    The input sequence is split into batches of up to `chunk_size` equities. Each batch
    is processed concurrently, with a maximum of `concurrency` API requests in flight at
    once. Results from all batches are flattened into a single list, preserving the
    input order.

    Args:
        raw_equities (Sequence[RawEquity]): Sequence of RawEquity objects to resolve.
        chunk_size (int, optional): Max number of equities per batch. Defaults to 100.
        concurrency (int, optional): Max number of concurrent API requests.
            Defaults to 10.

    Returns:
        list[tuple[str | None, str | None, str | None]]:
            List of (name, symbol, shareClassFIGI) triplets (or None for missing values)
            for each input equity, preserving input order.
    """
    # slice into fixed‐size chunks
    chunks = _chunk_equities(raw_equities, chunk_size)

    # limit parallelism with a semaphore
    semaphore = asyncio.Semaphore(concurrency)

    # launch one task per chunk
    tasks = [
        asyncio.create_task(_fetch_identification_limited(batch, semaphore))
        for batch in chunks
    ]

    # await all results, then flatten
    batches: list[
        list[tuple[str | None, str | None, str | None]]
    ] = await asyncio.gather(*tasks)
    return [item for batch in batches for item in batch]


async def _fetch_identification_limited(
    batch: Sequence[RawEquity],
    semaphore: asyncio.Semaphore,
) -> list[tuple[str | None, str | None, str | None]]:
    """
    Fetch and extract (name, symbol, shareClassFIGI) triplets for a batch of
    RawEquity objects, using a semaphore to limit concurrency. If the API call
    fails, logs the error and returns a list of (None, None, None) triplets.

    Args:
        batch (Sequence[RawEquity]): Sequence of RawEquity objects to process.
        semaphore (asyncio.Semaphore): Semaphore to control concurrent API
            requests.

    Returns:
        list[tuple[str | None, str | None, str | None]]: List of (name, symbol,
            shareClassFIGI) triplets (or all None) for each input equity,
            preserving input order.
    """
    async with semaphore:
        try:
            return await _fetch_and_extract_identification_batch(batch)
        except Exception:
            logger.exception("OpenFIGI API call failed for a batch of %d", len(batch))
            return [(None, None, None)] * len(batch)


async def _fetch_and_extract_identification_batch(
    batch: Sequence[RawEquity],
) -> list[tuple[str | None, str | None, str | None]]:
    """
    Build the query DataFrame for a batch of RawEquity objects, perform a blocking
    OpenFIGI API call in a background thread, and extract (name, symbol, shareClassFIGI)
    triplets for each equity in the batch.

    Args:
        batch (Sequence[RawEquity]): Sequence of RawEquity objects to query.

    Returns:
        list[tuple[str | None, str | None, str | None]]:
            List of (name, symbol, shareClassFIGI) triplets (or None for missing values)
            for each input equity, preserving input order.
    """
    df = _build_query_dataframe(batch)

    # run blocking call off-thread
    raw_df: pd.DataFrame = await asyncio.to_thread(_fetch_map, df)
    return _retrieve_identification(raw_df, len(batch))


def _retrieve_identification(
    raw: pd.DataFrame,
    batch_size: int,
) -> list[tuple[str | None, str | None, str | None]]:
    """
    Extract a (name, symbol, shareClassFIGI) triplet for each query index in input
    order from the OpenFIGI API response DataFrame. If data is missing or invalid,
    returns (None, None, None) for that index.

    Args:
        raw (pd.DataFrame): DataFrame containing OpenFIGI API results, including
            'query_number', 'name' or 'securityName', 'ticker', and 'shareClassFIGI'.
        batch_size (int): Number of queries in the batch.

    Returns:
        list[tuple[str | None, str | None, str | None]]:
            List of (name, symbol, shareClassFIGI) triplets (or all None) for each
            query index, preserving input order.
    """
    valid_figi = re.compile(r"^[A-Z0-9]{12}$").fullmatch

    def _triplet(record: dict) -> tuple[str | None, str | None, str | None]:
        figi: str | None = record.get("shareClassFIGI")
        figi = figi if isinstance(figi, str) and valid_figi(figi) else None

        # OpenFIGI returns 'name' OR 'securityName' depending on mapping route.
        name: str | None = record.get("name") or record.get("securityName")
        name = name if isinstance(name, str) else None

        # OpenFIGI returns 'ticker' for symbol
        symbol: str | None = record.get("ticker")
        symbol = symbol if isinstance(symbol, str) else None

        return (name, symbol, figi)

    # Build {query_number: triplet} – later indices win (earlier API rows win)
    mapping: dict[int, tuple[str | None, str | None, str | None]] = {
        int(rec["query_number"]): _triplet(rec)
        for rec in reversed(raw.to_dict(orient="records"))
    }

    # Materialise list in original order, defaulting to all-None triplet
    default_triplet = (None, None, None)
    return [mapping.get(i, default_triplet) for i in range(batch_size)]


def _log_missing_figis(
    raw_equities: Sequence[RawEquity],
    share_class_figis: list[str | None],
) -> None:
    """
    Log a warning for each RawEquity without a corresponding shareClassFIGI.

    Args:
        raw_equities (Sequence[RawEquity]): Sequence of RawEquity objects queried.
        share_class_figis (list[str | None]): List of shareClassFIGI values (or None)
            for each equity, in the same order as raw_equities.

    Returns:
        None
    """
    for equity, figi in zip(raw_equities, share_class_figis, strict=False):
        if figi is None:
            logger.debug(
                "No share_class_figi for %s (isin=%s, cusip=%s)",
                equity.symbol,
                equity.isin or "None",
                equity.cusip or "None",
            )


def _chunk_equities(
    equities: Sequence[RawEquity],
    chunk_size: int = 100,
) -> list[Sequence[RawEquity]]:
    """
    Split a sequence of RawEquity objects into consecutive chunks of a given size.

    Args:
        equities (Sequence[RawEquity]): The sequence of RawEquity objects to split.
        chunk_size (int, optional): The maximum size of each chunk. Defaults to 100.

    Returns:
        list[Sequence[RawEquity]]: A list of slices, each containing up to chunk_size
            RawEquity objects, preserving input order.
    """
    return [
        equities[index : index + chunk_size]
        for index in range(0, len(equities), chunk_size)
    ]


def _build_query_dataframe(raw_equities: Sequence[RawEquity]) -> pd.DataFrame:
    """
    Convert sequence of RawEquity objects into DataFrame formatted for OpenFIGI queries.

    Each RawEquity is transformed into dictionary containing the most precise identifier
    available (ISIN, CUSIP, or symbol), and resulting list of dictionaries is converted
    into a pandas DataFrame suitable for use with OpenFigiClient.map.

    Args:
        raw_equities (Sequence[RawEquity]): Sequence of RawEquity objects
            for conversion.

    Returns:
        pd.DataFrame: DataFrame containing query records for OpenFIGI API mapping.
    """
    query_records = [
        _raw_equity_to_query_record(raw_equity) for raw_equity in raw_equities
    ]
    return pd.DataFrame(query_records)


def _raw_equity_to_query_record(equity: RawEquity) -> dict[str, str]:
    """
    Constructs query record dictionary for an equity, selecting most precise identifier.

    The function prioritises the use of ISIN, then CUSIP, and falls back to symbol as
    the identifier for the equity. The resulting dictionary is formatted for use with
    OpenFIGI queries.

    Args:
        equity (RawEquity): Equity object containing possible identifiers such as ISIN,
            CUSIP, and symbol.

    Returns:
        dict[str, str]: A dictionary with keys 'idType', 'idValue', and 'marketSecDes'
            representing the identifier type, its value, and the security description,
            respectively.
    """
    if equity.isin:
        id_type, id_value = "ID_ISIN", equity.isin
    elif equity.cusip:
        id_type, id_value = "ID_CUSIP", equity.cusip
    else:
        id_type, id_value = "TICKER", equity.symbol

    # build the query record and return
    return {
        "idType": id_type,
        "idValue": id_value,
        "marketSecDes": "Equity",
    }


def _fetch_map(df: pd.DataFrame) -> pd.DataFrame:
    """
    Perform a blocking OpenFIGI API map call using the provided DataFrame.

    Args:
        df (pd.DataFrame): DataFrame containing query records in the format expected by
            OpenFigiClient.map.

    Returns:
        pd.DataFrame: DataFrame with OpenFIGI API mapping results.
    """
    client = OpenFigiClient(api_key=os.getenv("OPENFIGI_API_KEY"))
    client.connect()
    return client.map(df)
