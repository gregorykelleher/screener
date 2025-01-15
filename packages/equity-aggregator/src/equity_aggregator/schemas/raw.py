# schemas/raw.py

from typing import Annotated, List

from pydantic import (
    BaseModel,
    BeforeValidator,
    Field,
    ConfigDict,
)

from .types import (
    NonEmptyStr,
    ISINStr,
    CIKStr,
    FIGIStr,
    MICStr,
    CurrencyStr,
    NonNegDecimal,
)

from .validators import (
    validate_name,
    validate_symbol,
    validate_id,
    validate_mics,
    validate_currency,
    validate_last_price,
)


# ──────────────────────────── Raw equity data ────────────────────────────
class RawEquity(BaseModel):
    """
    Raw equity data fetched from data vendors. Fields undergo validation
    and normalisation to ensure consistency and correctness.

    Fields:
      - name: name of the equity
      - symbol: equity symbol
      - isin, cik, share_class_figi: equity identifiers
      - mics: list of Market Identifier Codes (MICs)
      - currency: currency code (ISO-4217)
      - last_price: last known price of the equity
    """

    model_config = ConfigDict(strict=True, frozen=True)

    # raw metadata, required
    name: Annotated[NonEmptyStr, BeforeValidator(validate_name)] = Field(
        ..., description="Equity name, required."
    )
    symbol: Annotated[NonEmptyStr, BeforeValidator(validate_symbol)] = Field(
        ..., description="Equity symbol, required."
    )

    # identifiers, optional
    isin: Annotated[ISINStr | None, BeforeValidator(validate_id)] = None
    cik: Annotated[CIKStr | None, BeforeValidator(validate_id)] = None
    share_class_figi: Annotated[FIGIStr | None, BeforeValidator(validate_id)] = None

    # financial data, optional
    mics: Annotated[List[MICStr] | None, BeforeValidator(validate_mics)] = None
    currency: Annotated[CurrencyStr | None, BeforeValidator(validate_currency)] = None
    last_price: Annotated[
        NonNegDecimal | None, BeforeValidator(validate_last_price)
    ] = None
