# feeds/euronext_feed_data.py

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, model_validator


class EuronextFeedData(BaseModel):
    """
    Represents a single Euronext feed record, transforming and normalising incoming
    fields to match the RawEquity model's expected attributes.

    Args:
        name (str): Company name, mapped from "name".
        symbol (str): Equity symbol, mapped from "symbol".
        isin (str | None): ISIN code, if available.
        mics (list[str]): List of MIC codes; defaults to an empty list if missing.
        currency (str | None): Trading currency code.
        last_price (str | float | int | Decimal | None): Last traded price.

    Returns:
        EuronextFeedData: An instance with fields normalised for RawEquity validation.
    """

    # Fields exactly match RawEquity’s signature
    name: str
    symbol: str
    isin: str | None
    mics: list[str]
    currency: str | None
    last_price: str | float | int | Decimal | None

    @model_validator(mode="before")
    def _normalise_fields(self: dict[str, object]) -> dict[str, object]:
        """
        Normalise a raw Euronext feed record into the flat schema expected by RawEquity.

        Args:
            self (dict[str, object]): Raw payload containing Euronext feed data.

        Returns:
            dict[str, object]: A new dictionary with renamed keys suitable for the
                RawEquity schema.
        """
        return {
            "name": self.get("name"),
            "symbol": self.get("symbol"),
            "isin": self.get("isin"),
            # no CUSIP, CIK or FIGI in Euronext feed, so omitting from model
            "mics": self.get("mics"),
            "currency": self.get("currency"),
            "last_price": self.get("last_price"),
        }

    model_config = ConfigDict(
        # ignore extra fields in incoming Euronext raw data feed
        extra="ignore",
        # defer strict type validation to RawEquity
        strict=False,
    )
