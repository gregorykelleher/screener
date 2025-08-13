# transforms/parse.py

import logging
from collections.abc import AsyncIterable, Callable

from equity_aggregator.domain.pipeline.resolve import FeedRecord
from equity_aggregator.schemas import RawEquity

logger = logging.getLogger(__name__)

type ValidatorFunc = Callable[[dict[str, object]], RawEquity | None]


async def parse(
    raw_feed_data: AsyncIterable[FeedRecord],
) -> AsyncIterable[RawEquity]:
    """
    Asynchronously validate and coerce raw feed records into RawEquity instances.

    Args:
        raw_feed_data (AsyncIterable[FeedRecord]):
            An async iterable yielding tuples of (feed model class, raw record dict).

    Returns:
        AsyncIterable[RawEquity]:
            Validated and coerced RawEquity instances. Invalid records are skipped.
    """
    async for feed_record in raw_feed_data:
        raw_equity = _validator(feed_record.model)(feed_record.raw_data)

        if raw_equity:
            yield raw_equity
            continue

        logger.warning(
            "Skipping invalid record from %s feed",
            feed_record.model.__name__.removesuffix("FeedData"),
        )


def _validator(
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
