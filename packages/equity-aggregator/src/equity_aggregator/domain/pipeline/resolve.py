# pipeline/resolve.py

import asyncio
import logging
from collections.abc import AsyncIterable, Awaitable, Callable
from typing import NamedTuple

from equity_aggregator.adapters import (
    fetch_equities_euronext,
    fetch_equities_lse,
    fetch_equities_xetra,
)
from equity_aggregator.schemas import (
    EuronextFeedData,
    LseFeedData,
    XetraFeedData,
)

logger = logging.getLogger(__name__)

type FeedPair = tuple[FetchFunc, type]
type FetchFunc = Callable[[], Awaitable[list[dict[str, object]]]]


# Named tuple to hold the feed model and its raw data
class FeedRecord(NamedTuple):
    model: type
    raw_data: dict[str, object]


# List of authoritative feed fetchers and their corresponding data models
_AUTH_FEEDS: list[FeedPair] = [
    (fetch_equities_euronext, EuronextFeedData),
    (fetch_equities_xetra, XetraFeedData),
    (fetch_equities_lse, LseFeedData),
]


async def resolve() -> AsyncIterable[FeedRecord]:
    """
    Concurrently fetch and yield raw feed records (unparsed) paired with
    their feed-model class.

    Launches fetches for Euronext, Xetra, and LSE in parallel. As soon as a feed's
    data is available, yields its equities one by one. This minimises memory usage
    and enables immediate downstream processing.

    Args:
        None

    Returns:
        AsyncIterable[FeedRecord]: An async iterable yielding
            tuples of feed-model classes and their raw record dicts.
    """
    logger.info("Resolving raw equities from authoritative feeds...")

    # create one task per feed coroutine
    tasks = [_resolve_feed(feed_pair) for feed_pair in _AUTH_FEEDS]

    logger.debug("Launched %d authoritative feed resolver tasks.", len(tasks))

    # total count of resolved equities across all feeds
    resolved_equities_total = 0

    for coroutine in asyncio.as_completed(tasks):
        records = await coroutine

        resolved_equities_total += _log_count(records)

        for feed_record in records:
            yield feed_record

    logger.info(
        "Resolved %d raw equities from all authoritative feeds.",
        resolved_equities_total,
    )


async def _resolve_feed(feed_pair: FeedPair) -> list[FeedRecord]:
    """
    Fetches raw equity records from a feed and returns them unparsed as dicts,
    tagged with their feed_model, for downstream parsing.

    Args:
        feed_pair (FeedPair): A tuple containing the fetcher function and feed model.

    Returns:
        list[FeedRecord]: A list of tuples, each containing the
            feed model and its corresponding raw record dict.
    """
    # unpack the feed pair into fetcher and model
    fetcher, feed_model = feed_pair

    # derive a concise feed name for logging (e.g. "Euronext" from "EuronextFeedData")
    feed_name = feed_model.__name__.removesuffix("FeedData")

    logger.info("Fetching raw equities from %s feed...", feed_name)

    # fetch the raw data, with timeout and exception handling
    fetched_raw_data = await _safe_fetch(fetcher, feed_name)

    if not fetched_raw_data:
        logger.warning("No raw equities for %s found.", feed_name)
        return []

    return [FeedRecord(feed_model, record) for record in fetched_raw_data]


async def _safe_fetch(
    fetcher: FetchFunc,
    feed_name: str,
    *,
    wait_timeout: float = 10.0,
) -> dict[str, object] | None:
    """
    Safely fetches raw data asynchronously using the provided fetcher function, with
    timeout and exception handling. If fetching fails due to a timeout or any other
    exception, logs the error and returns None.

    Args:
        fetcher (FetchFunc): An asynchronous function that fetches and returns a list
            of dictionaries containing raw data.
        feed_name (str): The name of the data feed, used for logging purposes.
        wait_timeout (float, optional): Maximum time in seconds to wait for the fetcher
            to complete. Defaults to 10.0 seconds.

    Returns:
        list[dict[str, object]]: The fetched raw data as a list of dictionaries, or an
            empty list if an error occurs.
    """

    try:
        return await asyncio.wait_for(fetcher(), timeout=wait_timeout)
    except TimeoutError:
        logger.error("Timed out fetching from %s.", feed_name)
        return None
    except Exception as exc:
        logger.error("Error fetching from %s: %s", feed_name, exc)
        return None


def _log_count(raw_equities: list[FeedRecord]) -> int:
    """
    Logs the number of equities returned by a feed.

    Args:
        raw_equities (list[FeedRecord]): The list of tuples returned
        by the feed, each containing the feed model and its corresponding raw record.

    Returns:
        int: The number of equities in the list.
    """
    count = len(raw_equities)
    logger.debug("Authoritative feed task completed, yielding %d equities.", count)
    return count
