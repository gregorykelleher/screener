# resolvers/_utils.py

import logging

# TODO: can be removed after build_raw_equity is removed
from equity_aggregator.schemas import RawEquity

logger = logging.getLogger(__name__)


# TODO: refactor to switch to schema validation approach
def build_raw_equity(  # noqa: PLR0913 (too-many-arguments)
    raw: dict,
    *,
    name_key: str = "name",
    symbol_key: str = "symbol",
    isin_key: str | None = "isin",
    cusip_key: str | None = "cusip",
    mics_key: str = "mics",
    currency_key: str = "currency",
    last_price_key: str = "last_price",
    market_cap_key: str | None = "market_cap",
    default_mics: list[str] | None = None,
) -> RawEquity:
    """
    Builds and validates a RawEquity object from a dictionary of feed equity data.

    This function extracts and normalises equity attributes from the provided `raw`
    dict, mapping keys as specified by the keyword arguments. It supports optional
    remapping of field names and provides defaults for missing or optional fields.
    Raises a ValidationError if required fields are missing or invalid.

    Args:
        raw (dict): Source dictionary containing raw equity data.
        name_key (str, optional): Key for name in `raw`. Defaults to "name".
        symbol_key (str, optional): Key for symbol in `raw`. Defaults to "symbol".
        isin_key (str | None, optional): Key for the ISIN in `raw`. Defaults to "isin".
        cusip_key (str | None, optional): Key for CUSIP in `raw`. Defaults to "cusip".
        mics_key (str, optional): Key for the list of MICs in `raw`. Defaults to "mics".
        currency_key (str, optional): Key for currency in `raw`. Defaults to "currency".
        last_price_key (str, optional): Key for last price in `raw`.
            Defaults to "last_price".
        market_cap_key (str | None, optional): Key for the market cap in `raw`.
            Defaults to "market_cap".
        default_mics (list[str] | None, optional): Default MICs if not present in `raw`.
            Defaults to None.

    Returns:
        RawEquity: A validated and normalised RawEquity object.

    Raises:
        ValidationError: If any required field is missing or invalid.
    """
    payload = {
        "name": raw.get(name_key),
        "symbol": raw.get(symbol_key),
        "isin": raw.get(isin_key) if isin_key else None,
        "cusip": raw.get(cusip_key) if cusip_key else None,
        "mics": raw.get(mics_key) or default_mics or [],
        "currency": raw.get(currency_key),
        "last_price": raw.get(last_price_key),
        "market_cap": raw.get(market_cap_key),
    }

    return RawEquity(**payload)
