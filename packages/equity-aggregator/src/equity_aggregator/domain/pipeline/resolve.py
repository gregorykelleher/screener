# pipeline/resolve.py

import asyncio
import logging
from collections.abc import AsyncIterable, Awaitable, Callable

from equity_aggregator.adapters import (
    fetch_equities_euronext,
    fetch_equities_lse,
    fetch_equities_xetra,
)
from equity_aggregator.schemas import (
    EuronextFeedData,
    LseFeedData,
    RawEquity,
    XetraFeedData,
)

logger = logging.getLogger(__name__)


async def resolve() -> AsyncIterable[RawEquity]:
    """
    Fetch and yield raw equities from multiple authoritative feeds concurrently.

    Launches Euronext, Xetra, and LSE fetches in parallel. As soon as one
    feed's dataset is ready, yields its equities one by one. This approach
    minimises memory usage and enables immediate downstream processing.

    Args:
        None

    Returns:
        AsyncIterable[RawEquity]: An async iterable of resolved raw equities from
        all feeds.
    """
    logger.info("Resolving raw equities from authoritative feeds...")

    # create one task per feed coroutine
    tasks = [asyncio.create_task(coroutine) for coroutine in _fetch_feed_coroutines()]

    logger.debug("Launched %d feed resolver tasks", len(tasks))

    # total count of resolved equities across all feeds
    resolved_equities_total = 0

    for task in asyncio.as_completed(tasks):
        # wait for the feed task to complete
        resolved_raw_equities = await task

        # assign number of resolved equities for this task to the task total
        resolved_equities_task_total = len(resolved_raw_equities)

        # add the task total to the overall total
        resolved_equities_total += resolved_equities_task_total

        logger.debug(
            "Authoritative feed task completed, yielding %d equities.",
            resolved_equities_task_total,
        )

        for raw_equity in resolved_raw_equities:
            yield raw_equity

    logger.info(
        "Resolved %d raw equities from all authoritative feeds.",
        resolved_equities_total,
    )


def _fetch_feed_coroutines() -> list[Awaitable[list[RawEquity]]]:
    """
    Prepare coroutines for resolving equities from all authoritative feeds.

    Returns a list of awaitable coroutines, each responsible for fetching and
    resolving raw equities from a specific feed (Euronext, Xetra, LSE).

    Args:
        None

    Returns:
        list[Awaitable[list[RawEquity]]]: Coroutines for resolving each feed's
        equities.
    """
    return [
        _resolve_feed(fetch_equities_euronext, EuronextFeedData),
        _resolve_feed(fetch_equities_xetra, XetraFeedData),
        _resolve_feed(fetch_equities_lse, LseFeedData),
    ]


async def _resolve_feed(
    fetcher: Callable[[], Awaitable[list[dict[str, object]]]],
    feed_model: type,
) -> list[RawEquity]:
    """
    Fetch a feed, coerce via a specific feed model, and validate into RawEquity objects.

    Args:
        fetcher: An async function that returns list[dict[str, object]], i.e.,
            the raw records for a single feed.
        feed_model: A model class that defines the expected structure of the feed.

    Returns:
        list[RawEquity]: Fully validated and normalised equity objects ready for
        downstream processing.
    """
    # derive a concise feed name for logging (e.g. "Euronext" from "EuronextFeedData")
    feed_name = feed_model.__name__.removesuffix("FeedData")

    # retrieve all raw records from the authoritative feed.
    logger.info("Fetching raw equities from %s feed...", feed_name)
    fetched_raw_data = await fetcher()

    if not fetched_raw_data:
        logger.warning("No raw equities for %s found.", feed_name)
        return []

    logger.info("Found %d raw equities for %s", len(fetched_raw_data), feed_name)

    def _map_feed_to_raw_equity(raw_data: dict[str, object]) -> RawEquity:
        # coerce the payload to the specific feed model to enforce field types
        coerced = feed_model.model_validate(raw_data)

        # dump to plain dict that RawEquity can accept
        normalised = coerced.model_dump()

        # hand off to RawEquity for full validation
        return RawEquity.model_validate(normalised)

    return [_map_feed_to_raw_equity(record) for record in fetched_raw_data]
