# schemas/raw.py

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
)

from .types import (
    AnalystRatingStrOpt,
    CIKStrOpt,
    CurrencyStrOpt,
    CUSIPStrOpt,
    FIGIStrOpt,
    ISINStrOpt,
    MICListOpt,
    SignedDecOpt,
    UnsignedDecOpt,
    UpperStrOpt,
    UpperStrReq,
)


# ──────────────────────────── Raw equity data ────────────────────────────
class RawEquity(BaseModel):
    """
    Raw equity data fetched from data feeds. Fields undergo validation
    and normalisation to ensure consistency and correctness.

    Fields:
      - name: name of the equity
      - symbol: equity symbol
      - isin, cusip, cik, share_class_figi: equity identifiers
      - mics: list of Market Identifier Codes (MICs)
      - currency: currency code (ISO-4217)
      - last_price: last known price of the equity
      - market_cap: latest market capitalisation
      - fifty_two_week_min: 52-week low price
      - fifty_two_week_max: 52-week high price
      - dividend_yield: annual dividend yield
      - market_volume: latest trading volume
      - held_insiders: % of shares held by insiders
      - held_institutions: % of shares held by institutions
      - short_interest: % of float sold short
      - share_float: shares available for trading
      - shares_outstanding: total shares outstanding
      - revenue_per_share: revenue per share
      - profit_margin: net profit margin
      - gross_margin: gross profit margin
      - operating_margin: operating profit margin
      - free_cash_flow: free cash flow
      - operating_cash_flow: operating cash flow
      - return_on_equity: return on equity
      - return_on_assets: return on assets
      - performance_1_year: total 1-year return
      - total_debt: total debt
      - revenue: total revenue
      - ebitda: EBITDA
      - trailing_pe: price-to-earnings ratio
      - price_to_book: price-to-book ratio
      - trailing_eps: earnings per share
      - analyst_rating: consensus analyst rating
      - industry: industry classification
      - sector: sector classification
    """

    model_config = ConfigDict(strict=True, frozen=True)

    # raw metadata, required
    name: UpperStrReq = Field(..., description="Equity name, required.")
    symbol: UpperStrReq = Field(..., description="Equity symbol, required.")

    # identifiers, optional
    isin: ISINStrOpt = None
    cusip: CUSIPStrOpt = None
    cik: CIKStrOpt = None
    share_class_figi: FIGIStrOpt = None

    # financial data, optional
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
