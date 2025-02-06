# equities_aggregator/schemas.py

from decimal import Decimal
from pydantic import BaseModel, Field, StringConstraints, Strict, ConfigDict
from typing import Optional, List, Annotated

# A non-empty string that is stripped of whitespace.
NonEmptyStr = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]

# A valid ISIN must be exactly 12 characters, start with two letters,
# followed by nine alphanumeric chars, and end with a digit.
ISINStr = Annotated[
    str,
    Strict(),
    StringConstraints(
        strip_whitespace=True,
        min_length=12,
        max_length=12,
        strict=True,
        to_upper=True,
        pattern=r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$",
    ),
]

# A valid MIC must be exactly 4 characters and consist of uppercase letters and digits.
MICStr = Annotated[
    str,
    Strict(),
    StringConstraints(
        strip_whitespace=True,
        min_length=4,
        max_length=4,
        strict=True,
        to_upper=True,
        pattern=r"^[A-Z0-9]{4}$",
    ),
]


class CanonicalEquityData(BaseModel):
    """
    Contains the core metadata for an equity.

    Fields:
      - isin: The unique 12-character International Securities Identification Number.
      - name: The equity's name.
      - symbol: The ticker symbol.
    """

    model_config = ConfigDict(strict=True, frozen=True)

    name: NonEmptyStr = Field(..., description="Equity name, required.")
    isin: ISINStr = Field(..., description="Unique ISIN, required.")
    symbol: str = Field(..., description="Ticker symbol, required.")


class FinancialEquityData(BaseModel):
    """
    Contains supplementary financial data for an equity.

    Fields:
      - mics: A list of Market Identifier Codes for exchanges where the equity is traded.
      - currency: The currency code (e.g., USD, EUR) in which the equity's price is denominated.
      - last_price: The most recent trade price.
    """

    model_config = ConfigDict(strict=True, frozen=True)

    mics: Optional[List[MICStr]] = Field(
        None, description="List of Market Identifier Codes."
    )
    currency: Optional[str] = Field(
        None,
        description="Currency code in which the equity is denominated (e.g., USD).",
    )
    last_price: Optional[Decimal] = Field(
        None, description="The most recent trade price."
    )


class EquityData(BaseModel):
    """
    A unified view of an equity, aggregating both core (canonical) metadata
    and supplementary financial data.

    The model combines the following:
      - Canonical data (provided by CanonicalEquityData):
          * isin (unique International Securities Identification Number)
          * name (equity name)
          * symbol (ticker symbol)
      - Financial data (provided by FinancialEquityData):
          * mics (list of Market Identifier Codes)
          * currency (price currency code, e.g., USD)
          * last_price (the last trade price, as a Decimal)

    This composite model represents a normalized view of an equity, ensuring unique
    identification and including available financial details.
    """

    model_config = ConfigDict(strict=True, frozen=True)

    canonical: CanonicalEquityData
    financial: Optional[FinancialEquityData] = Field(
        None,
        description="Normalized financial data; may be absent or partially populated.",
    )
