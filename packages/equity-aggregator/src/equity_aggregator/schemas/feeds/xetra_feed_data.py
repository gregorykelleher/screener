# feeds/xetra_feed_data.py

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, model_validator


class XetraFeedData(BaseModel):
    """
    Represents a single Xetra feed record, transforming and normalising incoming fields
    to match the RawEquity model's expected attributes.

    Args:
        name (str): The security's full name.
        symbol (str): The security symbol, mapped from WKN.
        isin (str | None): The ISIN identifier, if available.
        mics (list[str]): List of MIC codes for trading venues.
        currency (str | None): The trading currency code.
        last_price (str | float | int | Decimal | None): Last traded price.
        market_cap (str | float | int | Decimal | None): Market capitalization.

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
            # default to XETR if mic not provided
            "mics": [self.get("mic")] if self.get("mic") else ["XETR"],
            "currency": self.get("currency"),
            # nested fields are flattened
            "last_price": (self.get("overview") or {}).get("lastPrice"),
            "market_cap": (self.get("key_data") or {}).get("marketCapitalisation"),
        }

    model_config = ConfigDict(
        # ignore extra fields in incoming Xetra raw data feed
        extra="ignore",
        # defer strict type validation to RawEquity
        strict=False,
    )
