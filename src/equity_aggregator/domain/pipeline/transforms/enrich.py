# transforms/enrich.py

import asyncio
import logging
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable
from contextlib import AsyncExitStack
from typing import NamedTuple

from equity_aggregator.adapters import open_yfinance_feed
from equity_aggregator.domain._utils import get_usd_converter, merge
from equity_aggregator.schemas import RawEquity, YFinanceFeedData

logger = logging.getLogger(__name__)

# Type alias for an async function that fetches enrichment data for an equity
type FetchFunc = Callable[..., Awaitable[dict[str, object]]]

# Type alias for a factory that creates an async feed context manager
type FeedFactory = Callable[[], AsyncIterator[object]]

# Type alias for a tuple describing a feed: (factory, model, concurrency limit)
type FeedSpec = tuple[FeedFactory, type, int]

# Type alias for a function that validates and converts feed data to RawEquity
type ValidatorFunc = Callable[[dict[str, object], RawEquity], RawEquity]

# List of enrichment feeds to use, each with its factory, model, and concurrency limit
feed_specs: list[FeedSpec] = [
    (
        open_yfinance_feed,  # factory for creating YFinance feed context
        YFinanceFeedData,  # data model for YFinance feed data
        10,  # concurrency limit (max simultaneous YFinance requests)
    ),
]


class EnrichmentFeed(NamedTuple):
    """
    Represents a feed for enrichment in a data pipeline.

    Args:
        fetch (FetchFunc): A callable responsible for fetching enrichment data.
        model (type): The type of model to use for enrichment.
        semaphore (asyncio.Semaphore): Semaphore to control concurrency for fetch.

    Returns:
        EnrichmentFeed: A named tuple containing fetch, model, and semaphore.
    """

    fetch: FetchFunc
    model: type
    semaphore: asyncio.Semaphore


async def enrich(
    raw_equities: AsyncIterable[RawEquity],
) -> AsyncIterable[RawEquity]:
    """
    Enrich a stream of RawEquity objects concurrently using configured feeds.

    Each RawEquity is scheduled for enrichment and yielded as soon as its
    enrichment completes. Enrichment is performed concurrently, respecting
    per-feed concurrency limits.

    Args:
        raw_equities (AsyncIterable[RawEquity]):
            Async iterable stream of RawEquity objects to enrich.

    Returns:
        AsyncIterable[RawEquity]: Yields each enriched RawEquity as soon as
        enrichment finishes.
    """
    async with AsyncExitStack() as stack:
        feeds: list[EnrichmentFeed] = [
            EnrichmentFeed(
                fetch=(await stack.enter_async_context(factory())).fetch_equity,
                model=model,
                semaphore=asyncio.Semaphore(limit),
            )
            for factory, model, limit in feed_specs
        ]

        # launch enrichment tasks and yield results as they complete
        async with asyncio.TaskGroup() as enrich_tasks:
            tasks: list[asyncio.Task[RawEquity]] = []
            async for equity in raw_equities:
                tasks.append(
                    enrich_tasks.create_task(_enrich_equity(equity, feeds)),
                )

            for completed in asyncio.as_completed(tasks):
                enriched = await completed
                yield enriched

    logger.info(
        "Enrichment finished for %d equities using enrichment feeds: %s",
        len(tasks),
        ", ".join(feed.model.__name__.removesuffix("FeedData") for feed in feeds),
    )


async def _enrich_equity(
    source: RawEquity,
    feeds: list[EnrichmentFeed],
) -> RawEquity:
    """
    Enrich a RawEquity instance concurrently using all configured enrichment feeds.

    For each feed, fetch and validate data for the given equity. Merge results with
    the source, preferring non-None fields from the source.

    Args:
        source (RawEquity): The RawEquity object to enrich (assumed USD-denominated).
        feeds (list[EnrichmentFeed]): List of enrichment feeds to use.

    Returns:
        RawEquity: The enriched RawEquity with missing fields filled where possible.
    """

    async def run(feed: EnrichmentFeed) -> RawEquity:
        """
        Enrich a RawEquity using a single feed, respecting the feed's concurrency
        limit.

        Args:
            feed (EnrichmentFeed): The enrichment feed containing the fetch function,
                model, and semaphore for concurrency control.

        Returns:
            RawEquity: The enriched RawEquity instance, or the original if enrichment
                fails.
        """
        async with feed.semaphore:
            return await _enrich_with_feed(source, feed.fetch, feed.model)

    enriched_equities = await asyncio.gather(*(run(feed) for feed in feeds))

    # merge all feed‐enriched RawEquity instances into one
    merged_from_feeds = merge(enriched_equities)

    # replace only the none‐fields in source with values from merged_from_feeds
    return _replace_none_with_enriched(source, merged_from_feeds)


async def _enrich_with_feed(
    source: RawEquity,
    fetch_func: FetchFunc,
    feed_model: type,
) -> RawEquity:
    """
    Enrich a RawEquity using a feed: fetch, validate, and convert to USD.

    If the source has no missing fields, returns it unchanged. Otherwise, fetches
    data from the feed, validates it, and converts to USD. If any step fails,
    returns the original source.

    Args:
        source (RawEquity): The equity to enrich, possibly with missing fields.
        fetch_func (FetchFunc): Async function to fetch enrichment data.
        feed_model (type): Pydantic model class for validating feed data.

    Returns:
        RawEquity: The enriched RawEquity in USD, or the original source if
        enrichment fails or is unnecessary.
    """
    # if source has no missing fields, skip enrichment
    if not _has_missing_fields(source):
        return source

    # derive a concise feed name for logging (e.g. "YFinance" from "YFinanceFeedData")
    feed_name = feed_model.__name__.removesuffix("FeedData")

    # fetch the raw data, with timeout and exception handling
    fetched_raw_data = await _safe_fetch(source, fetch_func, feed_name)

    # if no data was fetched, fall back to source
    if not fetched_raw_data:
        return source

    # validate the fetched data against the feed model
    validated = _make_validator(feed_model)(fetched_raw_data, source)

    # always convert the validated feed‐record to USD or else fall back to source
    return await _convert_to_usd_or_fallback(validated, source, feed_name)


async def _safe_fetch(
    source: RawEquity,
    fetcher: FetchFunc,
    feed_name: str,
    *,
    wait_timeout: float = 180.0,
) -> dict[str, object] | None:
    """
    Safely fetch raw data for a RawEquity from an enrichment feed, handling
    timeouts and errors.

    Note:
        The CIK (Central Index Key) is intentionally omitted as an identifier
        for enrichment feeds, as it lacks broad support.

    Args:
        source (RawEquity): The RawEquity instance to fetch data for.
        fetcher (FetchFunc): The async fetch function for the enrichment feed.
        feed_name (str): The name of the enrichment feed for logging context.
        wait_timeout (float, optional): Maximum time to wait for the fetch, in
            seconds.

    Returns:
        dict[str, object] | None: The fetched data as a dictionary, or None if an
        exception occurs or the data is empty.
    """
    data: dict[str, object] | None = None

    try:
        data = await asyncio.wait_for(
            fetcher(
                symbol=source.symbol,
                name=source.name,
                isin=source.isin,
                cusip=source.cusip,
            ),
            timeout=wait_timeout,
        )

    except LookupError as error:
        _log_no_feed_data(
            feed_name,
            source,
            error,
        )

    except TimeoutError:
        logger.error(
            "Timed out after %.0f s while fetching from %s.",
            wait_timeout,
            feed_name,
        )

    except Exception as error:
        logger.error(
            "Error fetching from %s: %s",
            feed_name,
            error,
        )

    return data


def _make_validator(
    feed_model: type,
) -> ValidatorFunc:
    """
    Create a validator function for a given feed model to validate and coerce records.

    Args:
        feed_model (type): The Pydantic model class used to validate and coerce
            input records. The model should define the expected schema for the feed
            data.

    Returns:
        ValidatorFunc: A function that takes a record dictionary and a RawEquity
            source, validates and coerces the record using the feed model, and
            returns a RawEquity instance if successful. If validation fails, log
            and returns the original source.
    """
    feed_name = feed_model.__name__.removesuffix("FeedData")

    def validate(record: dict[str, object], source: RawEquity) -> RawEquity:
        """
        Validate and coerce a record using the feed model, returning a RawEquity.

        Args:
            record (dict[str, object]): The raw record to validate and coerce.
            source (RawEquity): The original RawEquity to return on failure.

        Returns:
            RawEquity: The validated RawEquity, or the original source if
                validation fails.
        """
        try:
            # validate the record against the feed model, coercing types as needed
            coerced = feed_model.model_validate(record).model_dump()

            # convert the coerced data to a RawEquity instance
            return RawEquity.model_validate(coerced)

        except Exception as error:
            if hasattr(error, "errors"):
                fields = {err["loc"][0] for err in error.errors()}
                summary = f"invalid {', '.join(sorted(fields))}"
            else:
                summary = str(error)

                _log_no_feed_data(
                    feed_name,
                    source,
                    summary,
                )
            return source

    return validate


def _has_missing_fields(equity: RawEquity) -> bool:
    """
    Checks if any field in a RawEquity instance is missing (i.e., set to None).

    Args:
        equity (RawEquity): The RawEquity instance to check for missing fields.

    Returns:
        bool: True if any field is None, indicating a missing value; False otherwise.
    """
    return any(value is None for value in equity.model_dump().values())


def _replace_none_with_enriched(
    source: RawEquity,
    enriched: RawEquity,
) -> RawEquity:
    """
    Return new RawEquity instance with missing fields from `source` filled in from
    `enriched`.

    For each field, if `source` has a non-None value, it is kept. If `source` has None,
    the value from `enriched` is used, but only if it is not None. None values in
    `enriched` never overwrite any value in `source`.

    Args:
        source (RawEquity): The original RawEquity instance, possibly with missing
            fields.
        enriched (RawEquity): The RawEquity instance to use for filling missing fields.

    Returns:
        RawEquity: A new RawEquity instance with missing fields filled from `enriched`.
    """
    # dump enriched, don’t include any None values
    enriched_data = enriched.model_dump(exclude_none=True)

    # pick only the keys where source is None
    to_update = {
        field: value
        for field, value in enriched_data.items()
        if getattr(source, field) is None
    }

    # return a copy of source with just those missing fields filled in
    return source.model_copy(update=to_update)


async def _convert_to_usd_or_fallback(
    validated: RawEquity,
    source: RawEquity,
    feed_name: str,
) -> RawEquity:
    """
    Attempt to convert a validated RawEquity instance to USD. If conversion fails
    due to a missing FX rate (ValueError), log and return the original
    source RawEquity.

    Args:
        validated (RawEquity): The RawEquity instance to convert to USD.
        source (RawEquity): The original RawEquity to return on conversion failure.
        feed_name (str): The name of the enrichment feed for logging context.

    Returns:
        RawEquity: The USD-converted RawEquity if successful, otherwise the original
        source RawEquity.
    """
    converter = await get_usd_converter()

    try:
        converted = converter(validated)

        if converted is None:
            raise ValueError("USD conversion failed")
        return converted

    except Exception as error:
        _log_no_feed_data(
            feed_name,
            source,
            error,
        )
        return source


def _log_no_feed_data(
    feed_name: str,
    source: RawEquity,
    error: object,
    *,
    level: int = logging.DEBUG,
) -> None:
    """
    Log a standardised message for missing or failed enrichment feed data.

    This logs details about the equity and the error context when no data is
    available from a feed, or when enrichment fails.

    Args:
        feed_name (str): Name of the enrichment feed.
        source (RawEquity): Equity instance with identifying fields.
        error (object): Error or context for the missing data.
        level (int, optional): Logging level (default: logging.DEBUG).

    Returns:
        None
    """
    logger.log(
        level,
        "No %s feed data for symbol=%s, name=%s "
        "(isin=%s, cusip=%s, cik=%s, share_class_figi=%s). %s",
        feed_name,
        source.symbol,
        source.name,
        source.isin or "<none>",
        source.cusip or "<none>",
        source.cik or "<none>",
        source.share_class_figi or "<none>",
        error,
    )
