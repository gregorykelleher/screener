# yfinance/config.py


from attr import dataclass


@dataclass(frozen=True, slots=True)
class FeedConfig:
    """
    Immutable configuration for Yahoo Finance endpoints and default modules.

    This dataclass centralises the URL patterns and default modules used to fetch
    equity data from Yahoo Finance. It ensures a single source of truth for all
    endpoints and module lists required by the enrichment feed.

    Args:
        search_url (str): Endpoint for searching equities by symbol, name, ISIN, or
            CUSIP.
        quote_base (str): Base URL for retrieving detailed quote summary data.
        crumb_url (str): Endpoint for obtaining the anti-CSRF crumb token.
        modules (tuple[str, ...]): Default modules to request for equity data.

    Returns:
        FeedConfig: Immutable configuration object with Yahoo Finance endpoints and
            module defaults.
    """

    # search URL for searching equities by symbol, name, ISIN, or CUSIP
    search_url: str = "https://query1.finance.yahoo.com/v1/finance/search"

    # quote summary URL for fetching equity data
    quote_summary_url: str = (
        "https://query2.finance.yahoo.com/v10/finance/quoteSummary/"
    )

    # quote summary URL fallback endpoint for fetching equity data
    quote_summary_fallback_url: str = (
        "https://query1.finance.yahoo.com/v7/finance/quote"
    )

    # crumb URL for session validation
    crumb_url: str = "https://query1.finance.yahoo.com/v1/test/getcrumb"

    # all modules to fetch from Yahoo Finance for equity data
    modules: tuple[str, ...] = (
        "price",
        "quoteType",
        "summaryProfile",
        "summaryDetail",
        "defaultKeyStatistics",
        "financialData",
        "assetProfile",
        "esgScores",
        "incomeStatementHistory",
        "incomeStatementHistoryQuarterly",
        "balanceSheetHistory",
        "balanceSheetHistoryQuarterly",
        "cashFlowStatementHistory",
        "cashFlowStatementHistoryQuarterly",
        "calendarEvents",
        "secFilings",
        "recommendationTrend",
        "upgradeDowngradeHistory",
        "institutionOwnership",
        "fundOwnership",
        "majorDirectHolders",
        "majorHoldersBreakdown",
        "insiderTransactions",
        "insiderHolders",
        "earnings",
        "earningsHistory",
        "earningsTrend",
        "industryTrend",
        "indexTrend",
        "sectorTrend",
    )
