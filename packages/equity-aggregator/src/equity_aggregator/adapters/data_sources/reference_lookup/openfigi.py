# reference_lookup/openfigi.py

import asyncio
import logging
import os
import re
from collections.abc import Awaitable, Callable, Sequence

import pandas as pd
from openfigipy import OpenFigiClient

from equity_aggregator.adapters.data_sources._cache import load_cache, save_cache
from equity_aggregator.schemas import RawEquity

logger = logging.getLogger(__name__)

# a triplet is a tuple of (name, symbol, shareClassFIGI)
Triplet = tuple[str | None, str | None, str | None]

# a function that fetches triplets for a sequence of RawEquity objects
FetchTriplets = Callable[[Sequence[RawEquity]], Awaitable[list[Triplet]]]

# a producer function that produces triplets for a chunk of RawEquity objects
Producer = Callable[
    [int, Sequence[RawEquity], asyncio.Queue["_INDEXED | None"]],
    Awaitable[None],
]

# an indexed triplet is a tuple of (index, triplet)
_INDEXED = tuple[int, Triplet]

# OpenFIGI limits
_REQUESTS_PER_WINDOW = 25
_WINDOW_SECONDS = 6.0
_CHUNK_SIZE = 100

_VALID_FIGI = re.compile(r"^[A-Z0-9]{12}$").fullmatch


async def fetch_equity_identification(
    raw_equities: Sequence[RawEquity],
    *,
    cache_key: str = "openfigi",
) -> list[Triplet]:
    """
    Fetches equity identification triplets (name, symbol, shareClassFIGI) for each
    RawEquity in the input sequence, preserving input order and length. Utilizes a
    cache to avoid redundant lookups. Missing FIGIs are represented as (None, None,
    None).

    Args:
        raw_equities (Sequence[RawEquity]): Sequence of RawEquity objects to resolve.
        cache_key (str, optional): Key for caching results. Defaults to
            "openfigi".

    Returns:
        list[Triplet]: List of (name, symbol, shareClassFIGI) triplets, aligned
            1-for-1 with input. If a FIGI cannot be resolved, the corresponding
            triplet is (None, None, None).
    """
    if not raw_equities:
        return []

    cached = load_cache(cache_key)

    if cached is not None:
        logger.debug("Loaded %d OpenFIGI triplets from cache.", len(cached))
        _log_missing_figis(raw_equities, [figi for _, _, figi in cached])
        return cached

    equity_triplets = await _identify_equity_triplets(raw_equities)

    _log_missing_figis(raw_equities, [figi for _, _, figi in equity_triplets])
    save_cache(cache_key, equity_triplets)

    logger.debug("Saved %d OpenFIGI triplets to cache.", len(equity_triplets))
    return equity_triplets


async def _identify_equity_triplets(equities: Sequence[RawEquity]) -> list[Triplet]:
    """
    Asynchronously resolves identification for a sequence of RawEquity objects.
    This function processes the given equities in chunks, distributing the workload
    across multiple asynchronous tasks. It uses a producer-consumer pattern with an
    asyncio queue to efficiently handle the identification resolution process.
    Args:
        equities (Sequence[RawEquity]): A sequence of RawEquity instances to resolve.
    Returns:
        list[Triplet]: A list of Triplet objects containing resolved identification
            information for each input equity.
    """

    queue: asyncio.Queue[_INDEXED | None] = asyncio.Queue()
    chunks = _enumerate_chunks(equities, _CHUNK_SIZE)

    async with asyncio.TaskGroup() as task_group:
        task_group.create_task(_fan_out(chunks, queue))  # producers
        return await _consume_queue(  # consumer
            queue,
            expected_items=len(equities),
            expected_sentinels=len(chunks),
        )


async def _fan_out(
    chunks: list[tuple[int, Sequence[RawEquity]]],
    queue: asyncio.Queue[_INDEXED | None],
    *,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    produce_chunk: Callable[
        [int, Sequence[RawEquity], asyncio.Queue[_INDEXED | None]],
        Awaitable[None],
    ] = None,
) -> None:
    """
    Launches producer tasks in waves, respecting the OpenFIGI rate limit of 25
    requests per 6 seconds. Each wave processes up to 25 chunks in parallel,
    then waits for the required window before launching the next wave.

    Args:
        chunks (list[tuple[int, Sequence[RawEquity]]]): List of (start_index,
            batch) tuples, where each batch is a chunk of RawEquity objects.
        queue (asyncio.Queue[_INDEXED | None]): Queue to which producer tasks
            will push their results and sentinels.
        sleep (Callable[[float], Awaitable[None]], optional): Async sleep
            function to pause between waves. Defaults to asyncio.sleep.
        produce_chunk (Callable[[int, Sequence[RawEquity],
            asyncio.Queue[_INDEXED | None]], Awaitable[None]], optional):
            Function to produce results for a chunk. Defaults to _produce_chunk.

    Returns:
        None
    """
    for offset in range(0, len(chunks), _REQUESTS_PER_WINDOW):
        wave = chunks[offset : offset + _REQUESTS_PER_WINDOW]

        if produce_chunk is None:
            produce_chunk = _produce_chunk

        tasks = [
            asyncio.create_task(produce_chunk(start, batch, queue))
            for start, batch in wave
        ]
        await asyncio.gather(*tasks)

        if offset + _REQUESTS_PER_WINDOW < len(chunks):
            await sleep(_WINDOW_SECONDS)


async def _produce_chunk(
    start_index: int,
    batch: Sequence[RawEquity],
    queue: asyncio.Queue[_INDEXED | None],
    *,
    fetch: FetchTriplets | None = None,
) -> None:
    """
    Producer coroutine that resolves a chunk of RawEquity objects and pushes
    (index, triplet) results to the queue. Always pushes a sentinel (None) at
    the end to mark completion, regardless of success or failure.

    Args:
        start_index (int): The starting index of this batch in the full sequence.
        batch (Sequence[RawEquity]): The chunk of RawEquity objects to resolve.
        queue (asyncio.Queue[_INDEXED | None]): Queue to push (index, triplet)
            results and a sentinel (None) when done.
        fetch (FetchTriplets, optional):
            Function to fetch triplets for the batch. If None, uses the default
            _fetch_and_extract function.

    Returns:
        None
    """
    if fetch is None:
        fetch = _fetch_and_extract

    try:
        triplets = await fetch(batch)
        for index, triplet in enumerate(triplets):
            await queue.put((start_index + index, triplet))

    except Exception:
        logger.exception("OpenFIGI batch starting at %d failed.", start_index)
        placeholder: Triplet = (None, None, None)
        for index in range(len(batch)):
            await queue.put((start_index + index, placeholder))

    finally:
        await queue.put(None)  # sentinel


async def _consume_queue(
    queue: asyncio.Queue[_INDEXED | None],
    *,
    expected_items: int,
    expected_sentinels: int,
) -> list[Triplet]:
    """
    Consumes items from the queue until all sentinels are received, reconstructing
    an ordered list of triplets matching the original input order.

    Args:
        queue (asyncio.Queue[_INDEXED | None]): Queue from which to consume
            (index, triplet) results and sentinel (None) values.
        expected_items (int): Total number of triplet items expected.
        expected_sentinels (int): Number of sentinel (None) values to wait for,
            corresponding to the number of producer tasks.

    Returns:
        list[Triplet]: List of triplets ordered by their original indices.
    """
    result: list[Triplet | None] = [None] * expected_items
    completed = items = 0

    while completed < expected_sentinels:
        item = await queue.get()
        if item is None:
            completed += 1
            continue
        index, triplet = item
        result[index] = triplet
        items += 1

    if items != expected_items:
        logger.error("Expected %d items, received %d", expected_items, items)

    return list(result)


async def _fetch_and_extract(batch: Sequence[RawEquity]) -> list[Triplet]:
    """
    Executes a blocking OpenFigiClient.map call in a thread to avoid event loop
    starvation. Converts the batch of RawEquity objects into a DataFrame, sends
    the mapping request, and extracts (name, symbol, shareClassFIGI) triplets
    for each input, preserving order.

    Args:
        batch (Sequence[RawEquity]): A batch of RawEquity objects to resolve.

    Returns:
        list[Triplet]: List of (name, symbol, shareClassFIGI) triplets, aligned
            1-for-1 with the input batch.
    """
    df = _build_query_dataframe(batch)
    raw: pd.DataFrame = await asyncio.to_thread(_blocking_map_call, df)
    return _extract_triplets(raw, batch_size=len(batch))


def _blocking_map_call(df: pd.DataFrame) -> pd.DataFrame:
    """
    Maps a DataFrame of identifiers to OpenFIGI results using a blocking API call.

    This function initializes an OpenFigiClient with the API key from the environment,
    establishes a connection, and submits the provided DataFrame for mapping.

    Args:
        df (pd.DataFrame): DataFrame with identifier data to be mapped via OpenFIGI.

    Returns:
        pd.DataFrame: DataFrame containing the mapping results from the OpenFIGI API.
    """
    client = OpenFigiClient(api_key=os.getenv("OPENFIGI_API_KEY"))
    client.connect()
    return client.map(df)


def _extract_triplets(
    response: pd.DataFrame,
    *,
    batch_size: int,
) -> list[Triplet]:
    """
    Extracts (name, symbol, shareClassFIGI) triplets from an OpenFIGI response
    DataFrame, preserving the input batch order.

    Args:
        response (pd.DataFrame): DataFrame returned by the OpenFIGI API, containing
            mapping results for a batch of queries.
        batch_size (int): The number of input records in the original batch,
            used to ensure output alignment and fill missing results.

    Returns:
        list[Triplet]: List of (name, symbol, shareClassFIGI) triplets, ordered
            to match the input batch. If a mapping is missing, (None, None, None)
            is returned for that position.
    """

    def _triplet(row: dict) -> Triplet:
        figi = row.get("shareClassFIGI")
        figi = figi if isinstance(figi, str) and _VALID_FIGI(figi) else None

        name = row.get("name") or row.get("securityName")
        name = name if isinstance(name, str) else None

        symbol = row.get("ticker")
        symbol = symbol if isinstance(symbol, str) else None

        return (name, symbol, figi)

    mapping: dict[int, Triplet] = {
        int(row["query_number"]): _triplet(row)
        for row in reversed(response.to_dict(orient="records"))
    }

    placeholder: Triplet = (None, None, None)
    return [mapping.get(index, placeholder) for index in range(batch_size)]


def _enumerate_chunks(
    equities: Sequence[RawEquity],
    chunk_size: int,
) -> list[tuple[int, Sequence[RawEquity]]]:
    """
    Splits the input sequence of equities into consecutive chunks of the given size,
    returning a list of (start_index, chunk) tuples for stable positional bookkeeping.

    Args:
        equities (Sequence[RawEquity]): The sequence of RawEquity objects to split.
        chunk_size (int): The maximum size of each chunk.

    Returns:
        list[tuple[int, Sequence[RawEquity]]]: List of (start_index, chunk) tuples,
            where start_index is the index of the first item in the chunk.
    """
    return [
        (index, equities[index : index + chunk_size])
        for index in range(0, len(equities), chunk_size)
    ]


def _build_query_dataframe(equities: Sequence[RawEquity]) -> pd.DataFrame:
    """
    Builds a DataFrame containing query records for a sequence of RawEquity objects.

    Args:
        equities (Sequence[RawEquity]): Sequence of RawEquity instances to be converted
            into query records.

    Returns:
        pd.DataFrame: A DataFrame where each row corresponds to a query record generated
            from a RawEquity object.
    """
    return pd.DataFrame([_to_query_record(equity) for equity in equities])


def _to_query_record(equity: RawEquity) -> dict[str, str]:
    """
    Builds a query record dictionary for a RawEquity object, selecting the best
    available identifier (ISIN, CUSIP, or symbol) for OpenFIGI lookup.

    Args:
        equity (RawEquity): The RawEquity instance to convert into a query record.

    Returns:
        dict[str, str]: Dictionary with keys 'idType', 'idValue', and
            'marketSecDes' for OpenFIGI API queries.
    """
    if equity.isin:
        id_type, id_value = "ID_ISIN", equity.isin
    elif equity.cusip:
        id_type, id_value = "ID_CUSIP", equity.cusip
    else:
        id_type, id_value = "TICKER", equity.symbol
    return {"idType": id_type, "idValue": id_value, "marketSecDes": "Equity"}


def _log_missing_figis(
    equities: Sequence[RawEquity],
    figis: Sequence[str | None],
) -> None:
    """
    Logs debug messages for equities that are missing a corresponding FIGI.

    Iterates over provided sequences of equities and their associated FIGIs. For each
    equity where the FIGI is None, logs a debug message including the equity's symbol,
    ISIN, and CUSIP.

    Args:
        equities (Sequence[RawEquity]): A sequence of RawEquity objects to check for
            missing FIGIs.
        figis (Sequence[str | None]): A sequence of FIGI strings or None, corresponding
            to each equity.

    Returns:
        None
    """
    for equity, figi in zip(equities, figis, strict=False):
        if figi is None:
            logger.debug(
                "No share_class_figi for %s (isin=%s, cusip=%s)",
                equity.symbol,
                equity.isin or "None",
                equity.cusip or "None",
            )
