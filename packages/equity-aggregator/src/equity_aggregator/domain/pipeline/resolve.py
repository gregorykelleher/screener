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

type FetchFunc = Callable[[], Awaitable[list[dict[str, object]]]]
type FeedPair = tuple[FetchFunc, type]
type ValidatorFunc = Callable[[dict[str, object]], RawEquity | None]

# List of authoritative feed fetchers and their corresponding data models
_AUTH_FEEDS: list[FeedPair] = [
    (fetch_equities_euronext, EuronextFeedData),
    (fetch_equities_xetra, XetraFeedData),
    (fetch_equities_lse, LseFeedData),
]


async def resolve() -> AsyncIterable[RawEquity]:
    """
    Concurrently fetch and yield raw equities from all authoritative feeds.

    Launches fetches for Euronext, Xetra, and LSE in parallel. As soon as a feed's
    data is available, yields its equities one by one. This minimises memory usage
    and enables immediate downstream processing.

    Args:
        None

    Returns:
        AsyncIterable[RawEquity]: An async iterable yielding validated RawEquity
        objects from all authoritative feeds.
    """
    logger.info("Resolving raw equities from authoritative feeds...")

    # create one task per feed coroutine
    tasks = [_resolve_feed(feed_pair) for feed_pair in _AUTH_FEEDS]

    logger.debug("Launched %d authoritative feed resolver tasks.", len(tasks))

    # total count of resolved equities across all feeds
    resolved_equities_total = 0

    for coroutine in asyncio.as_completed(tasks):
        # wait for the feed task to complete
        resolved_raw_equities = await coroutine

        resolved_equities_total += _log_count(resolved_raw_equities)

        for raw_equity in resolved_raw_equities:
            yield raw_equity

    logger.info(
        "Resolved %d raw equities from all authoritative feeds.",
        resolved_equities_total,
    )


async def _resolve_feed(
    feed_pair: FeedPair,
) -> list[RawEquity]:
    """
    Fetches raw equity records from a feed, validates and coerces them using the
    specified feed model, and returns a list of validated RawEquity objects.

    Args:
        feed_pair (FeedPair): A tuple containing the fetcher function and feed model.

    Returns:
        list[RawEquity]: A list of validated and normalised RawEquity objects
            ready for downstream processing.
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

    validate_fn = _make_validator(feed_model)

    return [
        validated
        for validated in (validate_fn(record) for record in fetched_raw_data)
        if validated is not None
    ]


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


def _make_validator(
    feed_model: type,
) -> ValidatorFunc:
    """
    Creates a validator function for a given feed model to validate and coerce records.

    Args:
        feed_model (type): The Pydantic model class used to validate and coerce input
            records. The model should define the expected schema for the feed data.

    Returns:
        ValidatorFunc: A function that takes a record dictionary, validates and coerces
            it using the feed model, and returns a RawEquity instance if successful.
            Returns None if validation fails, logging a warning with the feed name and
            error details.
    """
    feed_name = feed_model.__name__.removesuffix("FeedData")

    def validate(record: dict[str, object]) -> RawEquity | None:
        try:
            # validate the record against the feed model, coercing types as needed
            coerced = feed_model.model_validate(record).model_dump()

            # convert the coerced data to a RawEquity instance
            return RawEquity.model_validate(coerced)

        except Exception as error:
            logger.warning("Skipping invalid record from %s: %s", feed_name, error)
            return None

    return validate


def _log_count(raw_equities: list[RawEquity]) -> int:
    """
    Logs the number of equities returned by a feed.

    Args:
        raw_equities (list[RawEquity]): The list of RawEquity objects returned by the
            feed.

    Returns:
        int: The number of equities in the list.
    """
    count = len(raw_equities)
    logger.debug("Authoritative feed task completed, yielding %d equities.", count)
    return count
