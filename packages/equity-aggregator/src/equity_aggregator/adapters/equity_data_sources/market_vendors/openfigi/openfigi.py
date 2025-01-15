# market_vendors/openfigi.py

import asyncio
import os
import re
import logging
import pandas as pd
from typing import Dict, List, Optional, Sequence

from openfigipy import OpenFigiClient
from equity_aggregator.schemas import RawEquity
from equity_aggregator.adapters.equity_data_sources._cache import load_cache, save_cache

logger = logging.getLogger(__name__)


async def get_share_class_figi_for_raw_equities(
    raw_equities: Sequence[RawEquity],
) -> List[Optional[str]]:
    """
    Return shareClassFIGI for each raw equity in order.
    Only shareClassFIGI or None if missing.
    """
    if not raw_equities:
        return []

    # load from cache if available
    cached = load_cache("figi_cache", ttl_minutes=1_440)
    if cached is not None:
        logger.info("Loaded OpenFIGI shareClassFIGIs from cache.")
        return cached

    logger.info("Fetching shareClassFIGIs from OpenFIGI API")

    # async resolution in 100-record chunks (10 requests in flight concurrently)
    share_class_figis = await _resolve_share_class_figis(raw_equities)

    # persist retrieved shareClassFIGIs to cache
    save_cache("figi_cache", share_class_figis)
    logger.info("Saved OpenFIGI shareClassFIGIs to cache.")

    return share_class_figis


async def _resolve_share_class_figis(
    raw_equities: Sequence[RawEquity],
    chunk_size: int = 100,
    concurrency: int = 10,
) -> List[Optional[str]]:
    """
    Split into batches of up to 100; fetch them in parallel (max concurrency
    in flight), then flatten the per-batch results into one list.
    """
    # slice into fixed‐size chunks
    chunks = _chunk_equities(raw_equities, chunk_size)

    # limit parallelism with a semaphore
    semaphore = asyncio.Semaphore(concurrency)

    # kick off one task per chunk
    tasks = [asyncio.create_task(_fetch_limited(batch, semaphore)) for batch in chunks]

    # await all results, then flatten
    batches: List[List[Optional[str]]] = await asyncio.gather(*tasks)
    return [figi for batch in batches for figi in batch]


def _chunk_equities(
    equities: Sequence[RawEquity],
    chunk_size: int = 100,
) -> List[Sequence[RawEquity]]:
    """
    Split the list into successive chunk_size slices.
    """
    return [
        equities[index : index + chunk_size]
        for index in range(0, len(equities), chunk_size)
    ]


async def _fetch_limited(
    batch: Sequence[RawEquity],
    semaphore: asyncio.Semaphore,
) -> List[Optional[str]]:
    """
    Fetch and extract one batch under semaphore control, with
    internal error handling that falls back to all None on failure.
    """
    async with semaphore:
        try:
            return await _fetch_and_extract_batch(batch)
        except Exception:
            logger.exception("OpenFIGI API call failed for a batch of %d", len(batch))
            return [None] * len(batch)


async def _fetch_and_extract_batch(
    batch: Sequence[RawEquity],
) -> List[Optional[str]]:
    """
    Build the query DataFrame, call OpenFIGI in a thread,
    then extract FIGIs for that batch.
    """
    df = _build_query_dataframe(batch)

    # run blocking call off-thread
    raw_df: pd.DataFrame = await asyncio.to_thread(_fetch_map, df)
    return _retrieve_share_class_figis(raw_df, len(batch))


def _build_query_dataframe(raw_equities: Sequence[RawEquity]) -> pd.DataFrame:
    """
    Convert RawEquity records to the dataframe format expected by
    `OpenFigiClient.map`.
    """
    query_records = [_raw_equity_to_query_record(eq) for eq in raw_equities]
    return pd.DataFrame(query_records)


def _raw_equity_to_query_record(equity: RawEquity) -> Dict[str, str]:
    """
    Pick the most precise ID and build one map-record.
    """
    if equity.isin:
        id_type, id_value = "ID_ISIN", equity.isin
    elif equity.cik:
        id_type, id_value = "ID_CIK", equity.cik
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
    Blocking OpenFIGI.map call (run inside a thread).
    """
    client = OpenFigiClient(api_key=os.getenv("OPENFIGI_API_KEY"))
    client.connect()
    return client.map(df)


def _retrieve_share_class_figis(
    raw: pd.DataFrame, batch_size: int
) -> List[Optional[str]]:
    """
    Build a list of length batch_size where each position index is the
    first valid shareClassFIGI seen for query_number == index, else None.
    """
    VALID_FIGI = re.compile(r"^[A-Z0-9]{12}$").fullmatch

    def _valid(record: dict) -> bool:
        figi = record.get("shareClassFIGI")
        return isinstance(figi, str) and VALID_FIGI(figi)

    # 1) Extract (index, figi) pairs only for syntactically valid FIGIs
    pairs = [
        # (query_number, shareClassFIGI)
        (int(record["query_number"]), record["shareClassFIGI"])
        # filter out invalid shareClassFIGIs
        for record in raw.to_dict(orient="records")
        if _valid(record)
    ]

    # build a dict via reversed order so that the earliest pair wins on duplicate keys
    figi_index_mapping = {index: figi for index, figi in reversed(pairs)}

    # materialise final list, defaulting to None
    return [figi_index_mapping.get(i) for i in range(batch_size)]
