# yfinance/errors.py


class FeedError(Exception):
    """Base class for all controlled feed failures."""

    def __init__(self, value: str) -> None:
        self.value = value
        super().__init__(value)

    def __str__(self) -> str:
        return self.__doc__


class NoQuotesError(FeedError):
    """Search returned zero quotes."""


class NoEquityDataError(FeedError):
    """No quote had both name and symbol."""


class LowFuzzyScoreError(FeedError):
    """Best candidate below min-score threshold."""


class EmptySummaryError(FeedError):
    """QuoteSummary endpoint returned no payload."""
