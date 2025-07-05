# transforms/enrich.py

import asyncio
import logging
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable
from contextlib import AsyncExitStack

from equity_aggregator.adapters import open_yfinance_feed

# TODO: improve this import
from equity_aggregator.adapters.data_sources.enrichment_feeds.yfinance.errors import (
    FeedError,
)
from equity_aggregator.domain._utils import get_usd_converter, merge
from equity_aggregator.schemas import RawEquity, YFinanceFeedData

type FetchFunc = Callable[..., Awaitable[dict[str, object]]]
type FeedPair = tuple[FetchFunc, type]
type FeedFactory = Callable[[], AsyncIterator[object]]
type FeedRegistry = list[tuple[FeedFactory, type]]

feed_factories: FeedRegistry = [
    (open_yfinance_feed, YFinanceFeedData),
]

type ValidatorFunc = Callable[[dict[str, object], RawEquity], RawEquity]

logger = logging.getLogger(__name__)


async def enrich(raw_equities: AsyncIterable[RawEquity]) -> AsyncIterable[RawEquity]:
    """
    Enrich a stream of RawEquity objects concurrently using configured enrichment feeds.

    For each RawEquity, schedules an enrichment task and yields each enriched RawEquity
    as soon as its enrichment completes.

    Args:
        raw_equities (AsyncIterable[RawEquity]):
            An async iterable stream of RawEquity objects to enrich.

    Yields:
        RawEquity: The enriched RawEquity object as soon as enrichment finishes.
    """

    async with AsyncExitStack() as stack:
        feeds: list[FeedPair] = []

        # open every feed context and collect (fetch, model) pairs
        for factory, model in feed_factories:
            enrichment_feed = await stack.enter_async_context(factory())
            feeds.append((enrichment_feed.fetch_equity, model))

        # schedule one enrichment task per equity
        async with asyncio.TaskGroup() as task_group:
            enrich_tasks = [
                task_group.create_task(_enrich_equity(equity, feeds))
                async for equity in raw_equities
            ]

    for completed in asyncio.as_completed(enrich_tasks):
        yield await completed

    logger.info(
        "Enrichment finished for %d equities using enrichment feeds: %s",
        len(enrich_tasks),
        ", ".join(model.__name__.removesuffix("FeedData") for _, model in feeds),
    )


async def _enrich_equity(
    source: RawEquity,
    feeds: list[FeedPair],
) -> RawEquity:
    """
    Concurrently enrich a RawEquity instance using all configured enrichment feeds.

    Each feed fetches and validates data for the given equity. Results are merged
    with the source, preferring non-None fields from the source.

    Args:
        source (RawEquity): The RawEquity object to enrich (assumed USD-denominated).

    Returns:
        RawEquity: The enriched RawEquity with missing fields filled where possible.
    """
    # launch one task per enrich feed concurrently
    tasks = [
        asyncio.create_task(_enrich_with_feed(source, feed_pair)) for feed_pair in feeds
    ]
    enriched_equities = await asyncio.gather(*tasks)

    # merge all feed‐enriched RawEquity instances into one
    merged_from_feeds = merge(enriched_equities)

    # replace only the none‐fields in source with values from merged_from_feeds
    return _replace_none_with_enriched(source, merged_from_feeds)


async def _enrich_with_feed(
    source: RawEquity,
    feed_pair: FeedPair,
) -> RawEquity:
    """
    Fetch raw data from a feed, normalise it to RawEquity, and convert to USD.

    If the source has no missing fields, returns the source immediately. If fetching
    or validation fails, returns the source. Otherwise, returns a USD-denominated
    RawEquity containing only the fields provided by the feed.

    Args:
        source (RawEquity): The original equity, possibly with missing fields.
        feed_pair (FeedPair): Tuple of (fetcher function, feed model class).

    Returns:
        RawEquity: The original source (if skipped or error), or enriched RawEquity.
    """
    # if source has no missing fields, skip enrichment
    if not _has_missing_fields(source):
        return source

    # unpack the feed pair into fetcher and model
    fetcher, feed_model = feed_pair

    # derive a concise feed name for logging (e.g. "YFinance" from "YFinanceFeedData")
    feed_name = feed_model.__name__.removesuffix("FeedData")

    # fetch the raw data, with timeout and exception handling
    fetched_raw_data = await _safe_fetch(source, fetcher, feed_name)

    # if no data was fetched, fall back to source
    if not fetched_raw_data:
        return source

    # validate the fetched data against the feed model
    validate_fn = _make_validator(feed_model)
    validated = validate_fn(fetched_raw_data, source)

    # always convert the validated feed‐record to USD or else fall back to source
    return await _convert_to_usd_or_fallback(validated, source, feed_name)


async def _safe_fetch(
    source: RawEquity,
    fetcher: FetchFunc,
    feed_name: str,
    *,
    wait_timeout: float = 30.0,
) -> dict[str, object] | None:
    """
    Safely fetch raw data from the feed for the given RawEquity.

    Args:
        fetcher (FetchFunc): The async fetch function for the enrichment feed.
        source (RawEquity): The RawEquity instance to fetch data for.

    Returns:
        dict[str, object] | None: The fetched data as a dictionary, or None if an
        exception occurs or the data is empty.
    """
    try:
        return await asyncio.wait_for(
            fetcher(
                symbol=source.symbol,
                name=source.name,
                isin=source.isin,
                cusip=source.cusip,
            ),
            timeout=wait_timeout,
        )

    except FeedError as error:
        logger.warning(
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
        return None

    except TimeoutError:
        logger.error("Timed out fetching from %s.", feed_name)
        return None

    except Exception as error:
        logger.error(
            "Error fetching from %s: %s",
            feed_name,
            _describe_httpx_error(error),
        )
        return None


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
            returns a RawEquity instance if successful. If validation fails, logs
            a warning and returns the original source.
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

            logger.warning(
                "No %s feed data for %s (symbol=%s): %s",
                feed_name,
                source.name,
                source.symbol,
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
    due to a missing FX rate (ValueError), log a warning and return the original
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
    except Exception as exception:
        logger.warning(
            "No %s feed data for %s (symbol=%s): %s",
            feed_name,
            source.name,
            source.symbol,
            exception,
        )
        return source


# TODO: make this a proper util for everything to use
def _describe_httpx_error(error: Exception) -> str:
    """
    Converts common httpx and httpcore exceptions into concise, human-readable messages.

    This function is intended to simplify error reporting for HTTP requests by mapping
    specific exceptions from the httpx and httpcore libraries to clear, user-friendly
    descriptions. It handles HTTP status errors and transport protocol errors, and falls
    back to the string representation for other exceptions.

    Args:
        error (Exception): The exception instance to describe. Typically an exception
            raised by httpx or httpcore during HTTP requests.

    Returns:
        str: A concise, human-friendly description of the exception.
    """
    import httpcore
    import httpx

    if isinstance(error, httpx.HTTPStatusError):
        code = error.response.status_code
        phrase = error.response.reason_phrase
        target = error.request.url.path
        return f"Received HTTP {code} {phrase} when requesting {target or '/'}"

    if isinstance(error, httpcore.LocalProtocolError):
        return f"Transport error: {error}"

    return str(error)
