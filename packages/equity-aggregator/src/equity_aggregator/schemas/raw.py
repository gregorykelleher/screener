# schemas/raw.py

from typing import Annotated

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
)

from .types import (
    CIKStr,
    CurrencyStr,
    CUSIPStr,
    FIGIStr,
    ISINStr,
    MICStr,
    NonEmptyStr,
    NonNegDecimal,
)
from .validators import (
    validate_currency,
    validate_id,
    validate_last_price,
    validate_market_cap,
    validate_mics,
    validate_name,
    validate_symbol,
)


# ──────────────────────────── Raw equity data ────────────────────────────
class RawEquity(BaseModel):
    """Raw equity data fetched from data feeds. Fields undergo validation
    and normalisation to ensure consistency and correctness.

    Fields:
      - name: name of the equity
      - symbol: equity symbol
      - isin, cusip, cik, share_class_figi: equity identifiers
      - mics: list of Market Identifier Codes (MICs)
      - currency: currency code (ISO-4217)
      - last_price: last known price of the equity
      - market_cap: latest market capitalisation
    """

    model_config = ConfigDict(strict=True, frozen=True)

    # raw metadata, required
    name: Annotated[NonEmptyStr, BeforeValidator(validate_name)] = Field(
        ...,
        description="Equity name, required.",
    )
    symbol: Annotated[NonEmptyStr, BeforeValidator(validate_symbol)] = Field(
        ...,
        description="Equity symbol, required.",
    )

    # identifiers, optional
    isin: Annotated[ISINStr | None, BeforeValidator(validate_id)] = None
    cusip: Annotated[CUSIPStr | None, BeforeValidator(validate_id)] = None
    cik: Annotated[CIKStr | None, BeforeValidator(validate_id)] = None
    share_class_figi: Annotated[FIGIStr | None, BeforeValidator(validate_id)] = None

    # financial data, optional
    mics: Annotated[list[MICStr] | None, BeforeValidator(validate_mics)] = None
    currency: Annotated[CurrencyStr | None, BeforeValidator(validate_currency)] = None
    last_price: Annotated[
        NonNegDecimal | None,
        BeforeValidator(validate_last_price),
    ] = None
    market_cap: Annotated[
        NonNegDecimal | None,
        BeforeValidator(validate_market_cap),
    ] = None

    # fiftyTwoWeeksMin
    # fiftyTwoWeeksMax
    # industry
    # sector
    # p/e
    # eps
    # dividend yield/rate
    # p/b
    # market volume
    # held % insiders/institutions
    # short interest
    # share float
    # shares outstanding
    # revenue per share
    # margins (profit/gross/operating)
    # analyst ratings
    # free cash flow
    # operating cash flow
    # EBITDA
    # return on equity
    # return on assets
    # performance 1 years (% change)
    # revenue
    # total debt
