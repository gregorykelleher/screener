# transforms/identify.py

import logging
from collections.abc import AsyncIterable, AsyncIterator, Iterator, Sequence

from equity_aggregator.adapters import fetch_equity_identification
from equity_aggregator.schemas import RawEquity

logger = logging.getLogger(__name__)

type IdentificationMetadata = tuple[str | None, str | None, str | None]


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
    raw_equities = [equity async for equity in raw_equities_stream]
    if not raw_equities:
        return

    identification_metadata = await fetch_equity_identification(raw_equities)
    updated_iter = _generate_updates(raw_equities, identification_metadata)

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


def _update(
    equity: RawEquity,
    id_metadata: IdentificationMetadata,
) -> RawEquity | None:
    """
    Updates a RawEquity instance with new metadata if a FIGI is provided.

    Args:
        equity (RawEquity): The equity object to update.
        metadata (IdentificationMetadata): A tuple containing
            (name, symbol, figi) values. Each value may be None.

    Returns:
        RawEquity | None: A new RawEquity instance with updated fields if FIGI is
            provided; otherwise, returns None.
    """
    name, symbol, figi = id_metadata

    if figi is None:
        return None

    return equity.model_copy(
        update={
            "share_class_figi": figi,
            "name": name or equity.name,
            "symbol": symbol or equity.symbol,
        },
    )


def _generate_updates(
    raw_equities: Sequence[RawEquity],
    id_metadata: Sequence[IdentificationMetadata],
) -> Iterator[RawEquity]:
    """
    Generates updated RawEquity objects by applying identification metadata.

    Iterates over pairs of raw equities and their corresponding identification metadata,
    updating each equity with the provided metadata using the _update function. Only
    non-None updated equities are yielded.

    Args:
        raw_equities (Sequence[RawEquity]): A sequence of RawEquity objects to update.
        id_metadata (Sequence[IdentificationMetadata]):
            A sequence of metadata tuples, each containing up to three optional string
            values, to be applied to the corresponding RawEquity.

    Yields:
        RawEquity: The updated RawEquity objects, excluding any that are None after
        updating.
    """
    for equity, metadata in zip(raw_equities, id_metadata, strict=False):
        updated = _update(equity, metadata)
        if updated is not None:
            yield updated
