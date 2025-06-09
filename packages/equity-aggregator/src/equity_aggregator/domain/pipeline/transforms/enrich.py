# transforms/enrich.py

import asyncio
import logging
from collections.abc import AsyncIterable, Awaitable, Callable

from equity_aggregator.adapters import fetch_equity_yfinance
from equity_aggregator.domain._utils import get_usd_converter, merge
from equity_aggregator.schemas import RawEquity, YFinanceFeedData

type FetchFunc = Callable[..., Awaitable[dict[str, object]]]
type FeedPair = tuple[FetchFunc, type]
ValidatorFunc = Callable[[dict[str, object]], RawEquity | None]

logger = logging.getLogger(__name__)

# List of enrichment feed fetchers and their corresponding data models
_ENRICH_FEEDS: list[FeedPair] = [
    (fetch_equity_yfinance, YFinanceFeedData),
]


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
    async with asyncio.TaskGroup() as task_group:
        tasks = [
            task_group.create_task(_enrich_equity(equity))
            async for equity in raw_equities
        ]

    for completed in asyncio.as_completed(tasks):
        yield await completed

    logger.debug(
        "Enrichment finished for %d equities using enrichment feeds: %s",
        len(tasks),
        ", ".join(
            model.__name__.removesuffix("FeedData") for _, model in _ENRICH_FEEDS
        ),
    )


async def _enrich_equity(source: RawEquity) -> RawEquity:
    """
    Concurrently enrich a RawEquity instance using all configured enrichment feeds.

    Each feed fetches and validates data for the given equity. Results are merged
    with the original, preferring non-None fields from the source.

    Args:
        source (RawEquity): The RawEquity object to enrich (assumed USD-denominated).

    Returns:
        RawEquity: The enriched RawEquity with missing fields filled where possible.
    """
    # launch one task per enrich feed concurrently
    tasks = [
        asyncio.create_task(_enrich_with_feeds(source, feed_pair))
        for feed_pair in _ENRICH_FEEDS
    ]
    enriched_equities = await asyncio.gather(*tasks)

    # merge all feed‐enriched RawEquity instances into one
    merged_from_feeds = merge(enriched_equities)

    # replace only the none‐fields in source with values from merged_from_feeds
    return _replace_none_with_enriched(source, merged_from_feeds)


async def _enrich_with_feeds(
    source: RawEquity,
    feed_pair: FeedPair,
) -> RawEquity:
    """
    Fetch raw JSON from a feed, normalise it to RawEquity, and convert to USD.

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

    logger.info("Fetching raw equities from %s feed...", feed_name)

    # fetch the raw data, with timeout and exception handling
    fetched_raw_data = await _safe_fetch(source, fetcher, feed_name)

    if not fetched_raw_data:
        logger.warning("No raw equities for %s found.", feed_name)

    validate_fn = _make_validator(feed_model)

    validated = validate_fn(fetched_raw_data)
    if validated is None:
        return source

    # always convert enriched equities to USD before returning
    convert_to_usd = await get_usd_converter()
    return convert_to_usd(validated)


async def _safe_fetch(
    source: RawEquity,
    fetcher: FetchFunc,
    feed_name: str,
    *,
    wait_timeout: float = 10.0,
) -> dict[str, object] | None:
    """
    Safely fetch raw JSON data from the feed for the given RawEquity.

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
