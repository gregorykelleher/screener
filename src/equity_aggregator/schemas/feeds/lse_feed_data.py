# feeds/lse_feed_data.py

import re
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, model_validator

from .feed_validators import required


@required("name", "symbol")
class LseFeedData(BaseModel):
    """
    Represents a single LSE feed record, transforming and normalising incoming fields
    to match the RawEquity model's expected attributes. If the currency is "GBX",
    price fields such as "last_price" are automatically converted from pence to
    pounds (GBP) for consistency.

    Args:
        name (str): The issuer's full name, mapped from "issuername".
        symbol (str): The tradable instrument mnemonic, mapped from "tidm".
        isin (str | None): The ISIN identifier, if available.
        mics (list[str]): List of MIC codes for trading venues; defaults to ["XLON"].
        currency (str | None): The trading currency code, with "GBX" converted to
            "GBP" if applicable.
        last_price (str | float | int | Decimal | None): Last traded price, mapped
            from "lastprice" and converted from pence to pounds if currency is "GBX".
        market_cap (str | float | int | Decimal | None): Market capitalisation, mapped
            from "marketcapitalization".

    Returns:
        LseFeedData: An instance with fields normalised for RawEquity validation,
            including automatic GBX to GBP conversion where relevant.
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

    @model_validator(mode="before")
    def _normalise_fields(self: dict[str, object]) -> dict[str, object]:
        """
        Normalise a raw LSE feed record into the flat schema expected by RawEquity.

        Extracts and renames nested fields to match the RawEquity signature. If the
        currency is "GBX", automatically converts price fields from pence to pounds
        (GBP) using the convert_gbx_to_gbp helper.

        Args:
            self (dict[str, object]): Raw payload containing raw LSE feed data.

        Returns:
            dict[str, object]: A new dictionary with renamed keys and, if applicable,
            price and currency fields converted from GBX to GBP, suitable for the
            RawEquity schema.
        """
        # convert GBX to GBP
        raw = convert_gbx_to_gbp(self)
        return {
            # issuername → maps to RawEquity.name
            "name": raw.get("issuername"),
            # tidm → maps to RawEquity.symbol
            "symbol": raw.get("tidm"),
            "isin": raw.get("isin"),
            # no CUSIP, CIK or FIGI in LSE feed, so omitting from model
            # default to XLON if mic not provided
            "mics": raw.get("mics") or ["XLON"],
            "currency": raw.get("currency"),
            # lastprice → maps to RawEquity.last_price
            "last_price": raw.get("lastprice"),
            # marketcapitalization → maps to RawEquity.market_cap
            "market_cap": raw.get("marketcapitalization"),
            # fiftyTwoWeeksMin → maps to RawEquity.fifty_two_week_min
            "fifty_two_week_min": raw.get("fiftyTwoWeeksMin"),
            # fiftyTwoWeeksMax → maps to RawEquity.fifty_two_week_max
            "fifty_two_week_max": raw.get("fiftyTwoWeeksMax"),
            # no additional fields in LSE feed, so omitting from model
        }

    model_config = ConfigDict(
        # ignore extra fields in incoming LSE raw data feed
        extra="ignore",
        # defer strict type validation to RawEquity
        strict=False,
    )


def _gbx_to_decimal(pence: str | None) -> Decimal | None:
    """
    Convert a pence string (e.g., "150", "1,50") to a Decimal value.

    Accepts strings representing pence values, optionally using a comma as a decimal
    separator (e.g., "1,23" is treated as "1.23"). Returns None if the input is None or
    does not match a positive number format.

    Args:
        pence (str | None): The pence value as a string, possibly with a comma decimal
            separator, or None.

    Returns:
        Decimal | None: The parsed Decimal value, or None if input is invalid.
    """
    if pence is None:
        return None

    s = str(pence).strip()
    # allow "1,23" → "1.23"
    if "," in s and "." not in s:
        s = s.replace(",", ".")

    # only digits with optional single decimal point
    if not re.fullmatch(r"\d+(?:\.\d+)?", s):
        return None

    return Decimal(s)


def convert_gbx_to_gbp(raw: dict) -> dict:
    """
    Converts price and currency fields from GBX (pence) to GBP (pounds) if applicable.

    If the input dictionary has a "currency" field set to "GBX", this function divides
    the "lastprice" value by 100 to convert from pence to pounds, sets the "currency"
    field to "GBP", and returns a new dictionary with these updates. All other fields
    remain unchanged. If the currency is not "GBX", the original dictionary is returned
    unmodified.

    Args:
        raw (dict): A dictionary containing at least a "currency" field, and optionally
            a "lastprice" field representing the price in pence.

    Returns:
        dict: A new dictionary with "lastprice" converted to pounds and "currency" set
        to "GBP" if original currency was "GBX". Otherwise, returns original dict.
    """
    if raw.get("currency") != "GBX":
        return raw

    pence = raw.get("lastprice")
    amount = _gbx_to_decimal(pence)

    updates = {"currency": "GBP"}
    if amount is None:
        updates["lastprice"] = None
    else:
        # convert pence to pounds
        updates["lastprice"] = amount / Decimal("100")

    # return a new dict rather than mutating in place
    return {**raw, **updates}
