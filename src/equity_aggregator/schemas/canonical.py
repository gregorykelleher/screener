# schemas/canonical.py

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
)

from .raw import RawEquity
from .types import (
    AnalystRatingStrOpt,
    CIKStrOpt,
    CurrencyStrOpt,
    CUSIPStrOpt,
    FIGIStrReq,
    ISINStrOpt,
    MICListOpt,
    SignedDecOpt,
    UnsignedDecOpt,
    UpperStrOpt,
    UpperStrReq,
)


# ──────────────────────────── Equity Identity metadata ───────────────────────────
class EquityIdentity(BaseModel):
    """
    Globally unique identity metadata for a single equity record.

    The authoritative identifier is `share_class_figi`, which uniquely distinguishes
    the equity. Other local identifiers such as ISIN, CUSIP or CIK may also be present.

    Attributes:
        name (UpperStrReq): Full name of the equity.
        symbol (UpperStrReq): Trading symbol for the equity.
        share_class_figi (FIGIStrReq): Unique OpenFIGI identifier for the share class.
        isin (ISINStrOpt): Optional International Securities Identification Number.
        cusip (CUSIPStrOpt): Optional CUSIP identifier.
        cik (CIKStrOpt): Optional Central Index Key for SEC filings.

    Args:
        name (UpperStrReq): Full name of the equity, required.
        symbol (UpperStrReq): Trading symbol, required.
        share_class_figi (FIGIStrReq): Authoritative OpenFIGI identifier, required.
        isin (ISINStrOpt): ISIN code, if available.
        cusip (CUSIPStrOpt): CUSIP code, if available.
        cik (CIKStrOpt): CIK code, if available.

    Returns:
        EquityIdentity: Immutable identity record for the equity.
    """

    model_config = ConfigDict(strict=True, frozen=True)

    # required name and symbol
    name: UpperStrReq = Field(..., description="Equity name, required.")
    symbol: UpperStrReq = Field(..., description="Equity symbol, required.")

    # required share_class_figi is the authoritative id uniquely identifying an equity.
    share_class_figi: FIGIStrReq = Field(
        ...,
        description="Equity share class FIGI, required.",
    )

    # optional local IDs
    isin: ISINStrOpt = None
    cusip: CUSIPStrOpt = None
    cik: CIKStrOpt = None


# ─────────────────────── Supplementary financial data ──────────────────────
class EquityFinancials(BaseModel):
    """
    Supplementary financial data for an equity, including market, price, and
    fundamental metrics.

    This model aggregates trading venue codes, currency, price, market cap, ratios,
    and other financial attributes relevant to an equity. All financial fields are
    optional and immutable.

    Args:
        mics: List of MICs for trading venues (optional).
        currency: Price currency code (optional).
        ...: Additional financial metrics such as last_price, market_cap, margins,
            ratios, and more, all as optional fields.

    Returns:
        EquityFinancials: Immutable supplementary financial data for an equity.
    """

    model_config = ConfigDict(strict=True, frozen=True)

    mics: MICListOpt = None
    currency: CurrencyStrOpt = None

    last_price: UnsignedDecOpt = None
    market_cap: UnsignedDecOpt = None
    fifty_two_week_min: UnsignedDecOpt = None
    fifty_two_week_max: UnsignedDecOpt = None
    dividend_yield: UnsignedDecOpt = None
    market_volume: UnsignedDecOpt = None
    held_insiders: UnsignedDecOpt = None
    held_institutions: UnsignedDecOpt = None
    short_interest: UnsignedDecOpt = None
    share_float: UnsignedDecOpt = None
    shares_outstanding: UnsignedDecOpt = None
    revenue_per_share: UnsignedDecOpt = None
    profit_margin: SignedDecOpt = None
    gross_margin: SignedDecOpt = None
    operating_margin: SignedDecOpt = None
    free_cash_flow: SignedDecOpt = None
    operating_cash_flow: SignedDecOpt = None
    return_on_equity: SignedDecOpt = None
    return_on_assets: SignedDecOpt = None
    performance_1_year: SignedDecOpt = None
    total_debt: UnsignedDecOpt = None
    revenue: UnsignedDecOpt = None
    ebitda: SignedDecOpt = None
    trailing_pe: SignedDecOpt = None
    price_to_book: SignedDecOpt = None
    trailing_eps: SignedDecOpt = None
    analyst_rating: AnalystRatingStrOpt = None
    industry: UpperStrOpt = None
    sector: UpperStrOpt = None


# ────────────────────────────── Composite model ─────────────────────────────
class CanonicalEquity(BaseModel):
    """
    Immutable, unified model aggregating identity and financial data for an equity.

    Combines globally unique identity metadata (name, symbol, share_class_figi, etc.)
    with supplementary financial attributes (market data, ratios, industry, etc.)
    into a single, normalised structure.

    Args:
        identity (EquityIdentity): Immutable identity metadata for the equity,
            including name, symbol, share_class_figi, and optional local IDs.
        financials (EquityFinancials): Supplementary financial data such as MICs,
            currency, price, market cap, ratios, and sector/industry.

    Returns:
        CanonicalEquity: Immutable, normalised representation of an equity with both
            identity and financial details.
    """

    model_config = ConfigDict(strict=True, frozen=True)

    identity: EquityIdentity = Field(
        ...,
        description="Equity identity metadata, required.",
    )
    financials: EquityFinancials = Field(
        ...,
        description="Equity financial data, required.",
    )

    @classmethod
    def from_raw(cls, raw: RawEquity) -> "CanonicalEquity":
        """
        Create a CanonicalEquity instance from a RawEquity object.

        This method extracts the relevant fields from the provided RawEquity instance,
        splitting them into identity and financials components, and constructs a new
        CanonicalEquity object using these components.

        Args:
            raw (RawEquity): The raw equity data to be transformed.

        Returns:
            CanonicalEquity: An instance of CanonicalEquity with identity and financials
                populated from the raw data.
        """

        # dump raw data from incoming RawEquity model
        raw_data = raw.model_dump()

        # extract identity and financials fields
        id_keys = EquityIdentity.model_fields.keys()
        fin_keys = EquityFinancials.model_fields.keys()

        # create identity and financials models from raw data
        identity = EquityIdentity(**{key: raw_data.get(key) for key in id_keys})
        financials = EquityFinancials(**{key: raw_data.get(key) for key in fin_keys})

        return cls(identity=identity, financials=financials)
