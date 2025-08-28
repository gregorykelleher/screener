# pipeline/resolve.py

import asyncio
import logging
from collections.abc import AsyncIterator, Callable
from typing import NamedTuple

from equity_aggregator.adapters import (
    fetch_equity_records_euronext,
    fetch_equity_records_lse,
    fetch_equity_records_sec,
    fetch_equity_records_xetra,
)
from equity_aggregator.schemas import (
    EuronextFeedData,
    LseFeedData,
    SecFeedData,
    XetraFeedData,
)

logger = logging.getLogger(__name__)

FetchFunc = Callable[[], AsyncIterator[dict[str, object]]]
FeedPair = tuple[FetchFunc, type]


# Named tuple to hold the feed model and its raw data
class FeedRecord(NamedTuple):
    model: type
    raw_data: dict[str, object]


# List of authoritative feed fetchers and their corresponding data models
_AUTH_FEEDS: tuple[FeedPair] = [
    (fetch_equity_records_euronext, EuronextFeedData),
    (fetch_equity_records_xetra, XetraFeedData),
    (fetch_equity_records_lse, LseFeedData),
    (fetch_equity_records_sec, SecFeedData),
]


async def resolve(
    feeds: tuple[FeedPair, ...] | None = None,
) -> AsyncIterator[FeedRecord]:
    """
    Merge all authoritative feed streams into a single asynchronous output.

    Args:
        feeds

    Returns:
        AsyncIterator[FeedRecord]: Yields FeedRecord objects as soon as they are
        available from any feed. Stops when all feeds are exhausted.

    This function launches a producer task for each feed, enqueuing FeedRecord
    items into a shared queue. Records are yielded as they arrive, ensuring
    minimal latency and efficient merging of multiple asynchronous sources.
    """
    logger.info("Resolving raw equities from authoritative feeds...")

    feeds = feeds or _AUTH_FEEDS
    queue: asyncio.Queue[FeedRecord | None] = asyncio.Queue()

    async with asyncio.TaskGroup() as task_group:
        for fetcher, model in feeds:
            task_group.create_task(_produce(fetcher, model, queue))

        # consume the queue until all producers are exhausted
        async for record in _consume(queue, len(feeds)):
            yield record


async def _produce(
    fetcher: FetchFunc,
    model: type,
    queue: asyncio.Queue[FeedRecord | None],
) -> None:
    """
    Asynchronously fetches records from a data source and enqueues them for processing.

    Iterates over records from the given asynchronous fetcher, wraps each record in a
    FeedRecord using the specified model, and puts them onto the provided asyncio queue.
    After all records are enqueued, signals completion by putting None onto the queue.

    Args:
        fetcher (FetchFunc): An async function yielding records as dictionaries.
        model (type): The data model class to associate with each fetched record.
        queue (asyncio.Queue[FeedRecord | None]): Queue for FeedRecord instances and
            completion signals.

    Returns:
        None
    """
    try:
        async for record in fetcher():
            await queue.put(FeedRecord(model, record))
    except Exception as error:
        logger.warning("Producer for %s failed: %s", model.__name__, error)
    finally:
        await queue.put(None)


async def _consume(
    queue: asyncio.Queue[FeedRecord | None],
    total_producers: int,
) -> AsyncIterator[FeedRecord]:
    """
    Consumes items from the queue, yielding FeedRecord objects as they arrive.

    Tracks the number of completed producers by counting None signals. Yields each
    FeedRecord until all producers have finished.

    Args:
        queue (asyncio.Queue[FeedRecord | None]): The queue containing FeedRecord
            instances and completion signals (None).
        total_producers (int): The total number of producer tasks to wait for.

    Returns:
        AsyncIterator[FeedRecord]: An asynchronous iterator yielding FeedRecord
            objects as they are dequeued.
    """
    completed = 0
    while completed < total_producers:
        item = await queue.get()
        if item is None:
            completed += 1
        else:
            yield item
