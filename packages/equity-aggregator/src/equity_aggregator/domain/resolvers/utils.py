# exchanges/_utils.py

from typing import List, Optional

from equity_aggregator.schemas import RawEquity


def build_raw_equity(
    raw: dict,
    *,
    name_key: str = "name",
    symbol_key: str = "symbol",
    isin_key: Optional[str] = "isin",
    cik_key: Optional[str] = "cik",
    mics_key: str = "mics",
    currency_key: str = "currency",
    last_price_key: str = "last_price",
    default_mics: Optional[List[str]] = None,
) -> RawEquity:
    """
    Build a validated and normalised RawEquity object
    from extracted data vendor equity data.
    """
    payload = {
        "name": raw.get(name_key),
        "symbol": raw.get(symbol_key),
        "isin": raw.get(isin_key) if isin_key else None,
        "cik": raw.get(cik_key) if cik_key else None,
        "mics": raw.get(mics_key) or default_mics or [],
        "currency": raw.get(currency_key),
        "last_price": raw.get(last_price_key),
    }

    # will raise ValidationError if any required field is missing/invalid
    return RawEquity(**payload)


def replace_none_with_enriched(
    source: RawEquity,
    enriched: RawEquity,
) -> RawEquity:
    """
    Return a new RawEquity that:

      - keeps every field from source if it's non-None
      - otherwise takes the field from enriched (if enriched has it)

    None-values in enriched never overwrite anything.
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


def has_missing_fields(equity: RawEquity) -> bool:
    """
    Return True if any field on this Pydantic model is None.
    """
    # Dump all the fields, then check for any None
    return any(value is None for value in equity.model_dump().values())
