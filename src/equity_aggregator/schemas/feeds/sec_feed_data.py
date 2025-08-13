# feeds/sec_feed_data.py

from pydantic import BaseModel, ConfigDict, model_validator

from .feed_validators import required


@required("name", "symbol")
class SecFeedData(BaseModel):
    """
    Represents a single SEC feed record, transforming and normalising incoming
    fields to match the RawEquity model's expected attributes.

    Args:
        name (str): Company name, mapped from "name".
        symbol (str): Equity symbol, mapped from "symbol".
        mics (list[str]): List of MIC codes; defaults to an empty list if missing.

    Returns:
        EuronextFeedData: An instance with fields normalised for RawEquity validation.
    """

    # Fields exactly match RawEquity’s signature
    name: str
    symbol: str
    mics: list[str]

    @model_validator(mode="before")
    def _normalise_fields(self: dict[str, object]) -> dict[str, object]:
        """
        Normalise a raw SEC feed record into the flat schema expected by RawEquity.

        Args:
            self (dict[str, object]): Raw payload containing SEC feed data.

        Returns:
            dict[str, object]: A new dictionary with renamed keys suitable for the
                RawEquity schema.
        """
        return {
            "name": self.get("name"),
            "symbol": self.get("symbol"),
            "cik": self.get("cik"),
            # no CUSIP, ISIN or FIGI in SEC feed, so omitting from model
            "mics": self.get("mics"),
            # no currency or last_price in SEC feed, so omitting from model
            # no more additional fields in SEC feed, so omitting from model
        }

    model_config = ConfigDict(
        # ignore extra fields in incoming SEC raw data feed
        extra="ignore",
        # defer strict type validation to RawEquity
        strict=False,
    )
