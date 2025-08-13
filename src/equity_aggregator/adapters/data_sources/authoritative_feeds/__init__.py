# authoritative_feeds/__init__.py

from .euronext import fetch_equity_records as fetch_equity_records_euronext
from .lse import fetch_equity_records as fetch_equity_records_lse
from .sec import fetch_equity_records as fetch_equity_records_sec
from .xetra import fetch_equity_records as fetch_equity_records_xetra

__all__ = [
    "fetch_equity_records_euronext",
    "fetch_equity_records_lse",
    "fetch_equity_records_xetra",
    "fetch_equity_records_sec",
]
