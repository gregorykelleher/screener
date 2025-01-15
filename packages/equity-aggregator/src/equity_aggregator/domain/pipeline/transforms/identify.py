# transforms/identify.py

from typing import AsyncIterable
from equity_aggregator.schemas import RawEquity
from equity_aggregator.adapters import get_share_class_figi_for_raw_equities


async def identify(
    raw_equities_stream: AsyncIterable[RawEquity],
) -> AsyncIterable[RawEquity]:
    """
    Asynchronously associate a share_class_figi to each RawEquity record,
    intentionally dropping any records for which no FIGI could be resolved.
    """

    # materialise incoming async items into a batch
    batch = [equity async for equity in raw_equities_stream]
    if not batch:
        return

    # fetch shareClassFIGIs for each raw equity in the batch
    share_class_figis = await get_share_class_figi_for_raw_equities(batch)

    for raw_equity, share_class_figi in zip(batch, share_class_figis):
        # if no FIGI was found, skip this record
        if share_class_figi is None:
            continue
        yield raw_equity.model_copy(update={"share_class_figi": share_class_figi})
