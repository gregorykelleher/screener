# transforms/identify.py

import logging
from collections.abc import AsyncIterable, AsyncIterator, Sequence

from equity_aggregator.adapters import fetch_equity_identification
from equity_aggregator.schemas import RawEquity

logger = logging.getLogger(__name__)


async def identify(
    raw_equities_stream: AsyncIterable[RawEquity],
) -> AsyncIterator[RawEquity]:
    """
    Associates raw equities with identification metadata sourced from OpenFigi.

    This function collects all RawEquity records from the input async stream,
    resolves their identification metadata (name, symbol, ShareClassFIGI) in batch using
    OpenFIGI, and yields only those records for which a valid FIGI is found.

    Note:
        The original RawEquity record's name and symbol fields are overwritten with new
        resolved values from OpenFIGI.

    Args:
        raw_equities_stream (AsyncIterable[RawEquity]):
            Async stream of RawEquity records to process.

    Yields:
        RawEquity: Updated RawEquity records with identification fields set.
    """
    # materialise incoming records for batch resolution
    raw_equities = [equity async for equity in raw_equities_stream]
    if not raw_equities:
        return

    # resolve (name, symbol, figi) for each record
    identification_metadata: Sequence[
        tuple[str | None, str | None, str | None]
    ] = await fetch_equity_identification(raw_equities)

    def _update(
        equity: RawEquity,
        metadata: tuple[str | None, str | None, str | None],
    ) -> RawEquity | None:
        name, symbol, figi = metadata
        if figi is None:
            return None

        return equity.model_copy(
            update={
                "share_class_figi": figi,
                "name": name or equity.name,
                "symbol": symbol or equity.symbol,
            },
        )

    # apply update in a lazy pipeline, filter out unmapped records
    updated_iter = (
        update
        for update in (
            _update(equity, metadata)
            for equity, metadata in zip(
                raw_equities,
                identification_metadata,
                strict=False,
            )
        )
        if update is not None
    )

    # yield each updated record, count successes for logging
    identified_count = 0
    for equity in updated_iter:
        identified_count += 1
        yield equity

    logger.info(
        "Identified %d raw equities (failed for %d)",
        identified_count,
        len(raw_equities) - identified_count,
    )
