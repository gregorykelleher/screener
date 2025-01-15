# schemas/canonical.py

from pydantic import (
    BaseModel,
    ConfigDict,
)

from equity_aggregator.schemas.raw import RawEquity

from .types import (
    CurrencyStr,
    CUSIPStr,
    FIGIStr,
    ISINStr,
    MICStr,
    NonEmptyStr,
    NonNegDecimal,
)


# ──────────────────────────── Equity Identity metadata ───────────────────────────
class EquityIdentity(BaseModel):
    """
    Globally unique identity metadata for a single equity record.

    The authoritative identifier is `share_class_figi`, which uniquely distinguishes
    the equity. Optional local identifiers such as ISIN and CUSIP may also be present.

    Attributes:
        name (NonEmptyStr): Full name of the equity.
        symbol (NonEmptyStr): Trading symbol for the equity.
        share_class_figi (FIGIStr): Unique OpenFIGI identifier for the share class.
        isin (ISINStr | None): Optional International Securities Identification Number.
        cusip (CUSIPStr | None): Optional CUSIP identifier.

    Methods:
        from_raw(raw: RawEquity, figi: str) -> EquityIdentity:
            Construct an EquityIdentity from a RawEquity instance and a FIGI.

    Args:
        name (NonEmptyStr): Full name of the equity.
        symbol (NonEmptyStr): Trading symbol.
        share_class_figi (FIGIStr): Authoritative OpenFIGI identifier.
        isin (ISINStr | None, optional): ISIN code, if available.
        cusip (CUSIPStr | None, optional): CUSIP code, if available.

    Returns:
        EquityIdentity: Immutable identity record for the equity.
    """

    model_config = ConfigDict(strict=True, frozen=True)

    @classmethod
    def from_raw(cls, raw: RawEquity, figi: str) -> "EquityIdentity":
        """
        Build an EquityIdentity from a RawEquity object and a FIGI.

        Args:
            raw (RawEquity): The raw equity data source.
            figi (str): The authoritative share class FIGI identifier.

        Returns:
            EquityIdentity: An immutable identity record for the equity.

        Raises:
            ValueError: If the provided FIGI is empty or None.
        """
        if not figi:
            raise ValueError("Cannot build equity identity without FIGI")
        return cls(
            name=raw.name,
            symbol=raw.symbol,
            share_class_figi=figi,
            isin=raw.isin,
            cusip=raw.cusip,
        )

    name: NonEmptyStr
    symbol: NonEmptyStr

    # share_class_figi is the authoritative id that uniquely identifies the equity.
    share_class_figi: FIGIStr

    # optional local IDs
    isin: ISINStr | None = None
    cusip: CUSIPStr | None = None


# ─────────────────────── Supplementary financial data ──────────────────────
class EquityFinancials(BaseModel):
    """
    Supplementary financial data for an equity.

    Attributes:
        mics (list[MICStr]): Market Identifier Codes where the equity is traded.
        currency (CurrencyStr): Currency code (e.g., USD, EUR) for the equity's price.
        last_price (NonNegDecimal): Most recent trade price.
        market_cap (NonNegDecimal): Latest market capitalisation.

    Args:
        mics (list[MICStr]): List of MICs for trading venues.
        currency (CurrencyStr): Price currency code.
        last_price (NonNegDecimal): Last trade price.
        market_cap (NonNegDecimal): Market capitalisation.

    Returns:
        EquityFinancials: An instance containing supplementary financial data.
    """

    model_config = ConfigDict(strict=True, frozen=True)

    mics: list[MICStr]
    currency: CurrencyStr
    last_price: NonNegDecimal
    market_cap: NonNegDecimal


# ────────────────────────────── Composite model ─────────────────────────────
class EquityProfile(BaseModel):
    """
    Represents a unified, immutable view of an equity, aggregating identity metadata
    and financial attributes into a single profile.

    This composite model ensures unique identification of an equity and provides a
    normalised structure for both descriptive and financial attributes.

    Args:
        equity_identity (EquityIdentity): Core identity information for the equity,
            including:
                - name: The full name of the equity.
                - symbol: The trading symbol.
                - share_class_figi: Unique OpenFIGI identifier.
                - isin (optional): International Securities Identification Number.
                - cusip (optional): Committee on Uniform Securities Identification
                    Procedures identifier.
        financial (EquityFinancials): Supplementary financial data, including:
                - mics: List of Market Identifier Codes.
                - currency: Price currency code (e.g., "USD").
                - last_price: Last trade price as a Decimal.
                - market_cap: Latest market capitalisation as a Decimal.

    Returns:
        EquityProfile: An immutable, normalised representation of an equity with both
            identity and financial details.
    """

    model_config = ConfigDict(strict=True, frozen=True)

    equity_identity: EquityIdentity
    financial: EquityFinancials
