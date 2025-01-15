# transforms/enrich.py

import asyncio
from collections.abc import AsyncIterable, Awaitable, Callable

from equity_aggregator.domain._utils._merge import merge
from equity_aggregator.domain.enrichers import enrich_equity_with_yfinance
from equity_aggregator.schemas import RawEquity

FeedEnricher = Callable[[RawEquity], Awaitable[RawEquity]]


async def enrich(
    raw_equities: AsyncIterable[RawEquity],
    *,
    concurrency: int = 32,
) -> AsyncIterable[RawEquity]:
    """
    Enriches an asynchronous stream of raw equities using feed enrichers.

    This function consumes an async iterable of RawEquity objects, enriches each
    equity concurrently using one or more feed enrichers, and yields enriched
    results as soon as they are available. Concurrency is controlled by the
    'concurrency' parameter, which limits the number of simultaneous enrichment
    tasks. The order of the output equities is not guaranteed to match the input
    order.

    Args:
        raw_equities (AsyncIterable[RawEquity]): An async iterable of raw equities to
            enrich.
        concurrency (int, optional): Maximum number of concurrent enrichment tasks.
            Defaults to 32.

    Returns:
        AsyncIterable[RawEquity]: An async iterable yielding enriched raw equities.
    """
    enrichers = [enrich_equity_with_yfinance]

    # limit the number of concurrent feed requests using a semaphore
    semaphore = asyncio.Semaphore(concurrency)

    # schedule enrichment tasks for all equities
    tasks = []
    async for raw_equity in raw_equities:
        tasks.append(
            asyncio.create_task(
                _enrich_and_fill_equity(raw_equity, enrichers, semaphore),
            ),
        )

    # yield results as they become available
    for task in asyncio.as_completed(tasks):
        yield await task


async def _enrich_and_fill_equity(
    raw_equity: RawEquity,
    enrichers: list[FeedEnricher],
    semaphore: asyncio.Semaphore,
) -> RawEquity:
    """
    Enrich a single raw equity by merging data from multiple feed enrichers.

    Runs all feed enricher calls concurrently under a semaphore to limit
    concurrency. Merges their retrieved data and finally fills missing fields in the
    original raw equity with acquired enriched data.

    Args:
        raw_equity (RawEquity): The equity to enrich.
        enrichers (list[FeedEnricher]): List of async feed enrichers.
        semaphore (asyncio.Semaphore): Semaphore to control concurrency.

    Returns:
        RawEquity: The enriched equity with missing fields filled.
    """
    async with semaphore:
        # run all feed enrichers concurrently
        enriched_raw_equities = await asyncio.gather(
            *(enricher(raw_equity) for enricher in enrichers),
        )

    # merge the enriched raw equities into a single enriched raw equity
    return merge(enriched_raw_equities)
