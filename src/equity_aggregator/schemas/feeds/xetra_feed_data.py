# feeds/xetra_feed_data.py

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, model_validator

from .feed_validators import required


@required("name", "symbol")
class XetraFeedData(BaseModel):
    """
    Represents a single Xetra feed record, transforming and normalising incoming fields
    to match the RawEquity model's expected attributes.

    Args:
        self (dict[str, object]): Raw payload containing Xetra feed data.

    Returns:
        XetraFeedData: An instance with fields normalised for RawEquity validation.
    """

    # Fields exactly match RawEquity's signature
    name: str
    symbol: str
    isin: str | None
    mics: list[str]
    currency: str | None
    last_price: str | float | int | Decimal | None
    market_cap: str | float | int | Decimal | None
    fifty_two_week_min: str | float | int | Decimal | None = None
    fifty_two_week_max: str | float | int | Decimal | None = None
    performance_1_year: str | float | int | Decimal | None = None
    dividend_yield: str | float | int | Decimal | None = None
    price_to_book: str | float | int | Decimal | None = None
    trailing_eps: str | float | int | Decimal | None = None

    @model_validator(mode="before")
    def _normalise_fields(self: dict[str, object]) -> dict[str, object]:
        """
        Normalise a raw Xetra feed record into the flat schema expected by RawEquity.

        Extracts and renames nested fields to match the RawEquity signature.

        Args:
            self (dict[str, object]): Raw payload containing raw Xetra feed data.

        Returns:
            dict[str, object]: A new dictionary with flattened and renamed keys suitable
                for the RawEquity schema.
        """
        return {
            "name": self.get("name"),
            # wkn → maps to RawEquity.symbol
            "symbol": self.get("wkn"),
            "isin": self.get("isin"),
            # no CUSIP, CIK or FIGI in Xetra feed, so omitting from model
            "mics": [self.get("mic")] if self.get("mic") else ["XETR"],
            # default to XETR if mic not provided
            "currency": self.get("currency"),
            # nested fields are flattened
            "last_price": (self.get("overview") or {}).get("lastPrice"),
            "market_cap": (self.get("key_data") or {}).get("marketCapitalisation"),
            "fifty_two_week_min": (self.get("performance") or {}).get("weeks52Low"),
            "fifty_two_week_max": (self.get("performance") or {}).get("weeks52High"),
            "performance_1_year": (self.get("performance") or {}).get(
                "performance1Year",
            ),
            "dividend_yield": (self.get("key_data") or {}).get("dividendYield"),
            "price_to_book": (self.get("key_data") or {}).get("priceBookRatio"),
            "trailing_eps": (self.get("key_data") or {}).get("earningsPerShareBasic"),
            # no additional fields in Xetra feed, so omitting from model
        }

    model_config = ConfigDict(
        # ignore extra fields in incoming Xetra raw data feed
        extra="ignore",
        # defer strict type validation to RawEquity
        strict=False,
    )
