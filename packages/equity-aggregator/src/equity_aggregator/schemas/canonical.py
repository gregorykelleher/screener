# schemas/normalised.py

from equity_aggregator.schemas.raw import RawEquity
from pydantic import (
    BaseModel,
    ConfigDict,
)
from typing import List

from .types import (
    NonEmptyStr,
    ISINStr,
    CIKStr,
    FIGIStr,
    MICStr,
    CurrencyStr,
    NonNegDecimal,
)


# ──────────────────────────── Equity Identity metadata ───────────────────────────
class EquityIdentity(BaseModel):
    """
    Globally unique metadata for a single equity record.
    The authoritative identifier is share_class_figi.
    """

    model_config = ConfigDict(strict=True, frozen=True)

    @classmethod
    def from_raw(cls, raw: RawEquity, figi: str) -> "EquityIdentity":
        """
        Factory: build an equity identity record from a RawEquity object plus a FIGI.
        """
        if not figi:
            raise ValueError("Cannot build equity identity without FIGI")
        return cls(
            name=raw.name,
            symbol=raw.symbol,
            share_class_figi=figi,
            isin=raw.isin,
            cik=raw.cik,
        )

    name: NonEmptyStr
    symbol: NonEmptyStr

    # share_class_figi is the authoritative id that uniquely identifies the equity.
    share_class_figi: FIGIStr

    # optional local IDs
    isin: ISINStr | None = None
    cik: CIKStr | None = None


# ─────────────────────── Supplementary financial data ──────────────────────
class EquityFinancials(BaseModel):
    """
    Contains supplementary financial data for an equity.

    Fields:
      - mics: A list of Market Identifier Codes for exchanges where the equity is traded.
      - currency: The currency code (e.g., USD, EUR) in which the equity's price is denominated.
      - last_price: The most recent trade price.
    """

    model_config = ConfigDict(strict=True, frozen=True)

    mics: List[MICStr]
    currency: CurrencyStr
    last_price: NonNegDecimal


# ────────────────────────────── Composite model ─────────────────────────────
class EquityProfile(BaseModel):
    """
    A unified view of an equity, aggregating both core (EquityIdentity) metadata
    and supplementary financial data.

    The model combines the following:
      - Equity identity data (provided by EquityIdentity):
          * name (equity name)
          * symbol (equity symbol)
          * share_class_figi (unique OpenFIGI identifier for the equity)
          * Local IDs (optional):
              * isin (International Securities Identification Number)
              * cik (SEC Central Index Key)
      - Financial data (provided by EquityFinancials):
          * mics (list of Market Identifier Codes)
          * currency (price currency code, e.g., USD)
          * last_price (the last trade price, as a Decimal)

    This composite model represents a normalised view of an equity, ensuring unique
    identification and including available financial details.
    """

    model_config = ConfigDict(strict=True, frozen=True)

    equity_identity: EquityIdentity
    financial: EquityFinancials
