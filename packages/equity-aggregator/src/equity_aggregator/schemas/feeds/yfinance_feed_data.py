# feeds/yfinance_feed_data.py

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, model_validator


class YFinanceFeedData(BaseModel):
    """
    Represents a single YFinance feed record, transforming and normalising incoming
    fields to match the RawEquity model's expected attributes.

    Args:
        name (str): Company name, mapped from "longName".
        symbol (str): Equity symbol, mapped from "symbol".
        currency (str | None): Trading currency code.
        last_price (str | float | int | Decimal | None): Last traded price,
            mapped from "currentPrice".
        market_cap (str | float | int | Decimal | None): Market capitalisation,
            mapped from "marketCap".

    Returns:
        YFinanceFeedData: An instance with fields normalised for RawEquity validation.
    """

    # Fields exactly match RawEquity’s signature
    name: str
    symbol: str
    currency: str | None
    last_price: str | float | int | Decimal | None
    market_cap: str | float | int | Decimal | None

    @model_validator(mode="before")
    def _normalise_fields(self: dict[str, object]) -> dict[str, object]:
        """
        Normalise a raw YFinance feed record into the flat schema expected by RawEquity.

        Args:
            self (dict[str, object]): Raw payload containing YFinance feed data.

        Returns:
            dict[str, object]: A new dictionary with renamed keys suitable for the
                RawEquity schema.
        """
        return {
            # longName → maps to RawEquity.name
            "name": self.get("longName"),
            # underlyingSymbol → maps to RawEquity.symbol
            "symbol": self.get("underlyingSymbol"),
            # no ISIN, CUSIP, FIGI or MICS in YFinance feed, so intentionally omitted
            "currency": self.get("currency"),
            # currentPrice → maps to RawEquity.last_price
            "last_price": self.get("currentPrice"),
            # marketCap → maps to RawEquity.market_cap
            "market_cap": self.get("marketCap"),
        }

    model_config = ConfigDict(
        # ignore extra fields in incoming YFinance raw data feed
        extra="ignore",
        # defer strict type validation to RawEquity
        strict=False,
    )
