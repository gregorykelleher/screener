# authoritative_feeds/__init__.py

from .euronext import fetch_equities as fetch_equities_euronext
from .lse import fetch_equities as fetch_equities_lse
from .xetra import fetch_equities as fetch_equities_xetra

__all__ = [
    "fetch_equities_euronext",
    "fetch_equities_lse",
    "fetch_equities_xetra",
]
