# resolvers/utils.py

import os
import json
from decimal import Decimal, InvalidOperation
from typing import Awaitable, Callable


from data_aggregator.equities_aggregator.schemas import (
    CanonicalEquityData,
    FinancialEquityData,
    EquityData,
)


async def fetch_or_load_equities_async(
    fetch_coroutine: Callable[[], Awaitable[list[dict]]],
    json_filename: str,
) -> list[dict]:
    """
    1) Build path from "data/" + json_filename
    2) If the file exists, load & return it
    3) Otherwise, call fetch_func() asynchronously, save to disk, then return
    """
    path = os.path.join("data", json_filename)
    if os.path.isfile(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    data = await fetch_coroutine()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)
    return data


def load_json_if_exists(path):
    """
    Return JSON data from 'path' if the file exists,
    otherwise raise FileNotFoundError.
    """
    if not os.path.isfile(path):
        raise FileNotFoundError(f"File not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path, data):
    """
    Write 'data' to 'path' as JSON, creating directories as needed.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)


def safe_parse_decimal(value):
    """
    Convert 'value' to Decimal if possible, or return None otherwise.
    """
    if not value and value != 0:
        return None
    try:
        # Convert to string, replace comma with dot, then parse as Decimal
        return Decimal(str(value).replace(",", "."))
    except (ValueError, InvalidOperation):
        return None


def normalise_isin(raw_isin: str) -> str:
    """
    Normalise an ISIN by:
      1) Stripping leading/trailing whitespace
      2) Converting to uppercase
    """
    return raw_isin.strip().upper()


def normalise_equity_name(raw_name: str) -> str:
    """
    Normalise an equity name by:
      1) Stripping leading/trailing whitespace
      2) Collapsing consecutive spaces
      3) Converting to uppercase
    """
    s = raw_name.strip()
    s = " ".join(s.split())  # collapses any run of whitespace to one space
    s = s.upper()
    return s


def normalise_symbol(raw_symbol: str) -> str:
    """
    Normalise a symbol by:
      1) Stripping leading/trailing whitespace
      2) Converting to uppercase
    """
    return raw_symbol.strip().upper()


def build_canonical_equity(
    raw: dict,
    isin_key: str = "isin",
    name_key: str = "name",
    symbol_key: str = "symbol",
) -> CanonicalEquityData:
    """
    Create a CanonicalEquityData object by extracting and normalizing
    the ISIN, name, and symbol from 'raw'.
    """
    raw_isin = raw.get(isin_key, "")
    raw_name = raw.get(name_key, "")
    raw_symbol = raw.get(symbol_key, "")

    return CanonicalEquityData(
        isin=normalise_isin(raw_isin),
        name=normalise_equity_name(raw_name),
        symbol=normalise_symbol(raw_symbol),
    )


def build_equity_data(
    raw: dict,
    isin_key: str = "isin",
    name_key: str = "name",
    symbol_key: str = "symbol",
    currency_key: str = "currency",
    last_price_key: str = "last_price",
    default_mics: list[str] | None = None,
) -> EquityData:
    """
    Build a full EquityData (canonical + financial) from a raw dict,
    using the provided key names.
    """
    canonical = build_canonical_equity(raw, isin_key, name_key, symbol_key)

    raw_currency = raw.get(currency_key, "")
    raw_last_price = raw.get(last_price_key, None)
    mics = default_mics or raw.get("mics") or None

    financial = FinancialEquityData(
        mics=mics,
        currency=raw_currency,
        last_price=safe_parse_decimal(raw_last_price),
    )

    return EquityData(canonical=canonical, financial=financial)
