# eodhd/fetch_equities.py

import os
import requests

from .exchanges import exchanges


def fetch_equities(country):
    """
    Fetch all EODHD equities for the given 'country' from all exchanges
    found in 'exchanges', then merge duplicates (same ISIN) by
    concatenating their 'mic' fields into one comma-separated string.

    Example return structure for an equity on multiple MICs:
        {
          "symbol": "2FB",
          "name": "Fortune Brands Home & Security Inc",
          "isin": "US34964C1062",
          "mic": "XHAN, XBER"
        }
    """
    exchange_entries = _get_exchanges_by_country(country)
    merged_equities = {}

    for exchange_entry in exchange_entries:
        raw_data = _fetch_eodhd_equities_data(exchange_entry["Code"])
        parsed_equities = _parse_equities(raw_data, exchange_entry)
        _merge_equities(merged_equities, parsed_equities)

    return _finalize_equities(merged_equities)


def _get_exchanges_by_country(country):
    """
    Returns all exchange dicts from 'exchanges' whose 'Country' == country.
    Raises ValueError if none found. (No error if multiple are found.)
    """
    found = [ex for ex in exchanges if ex["Country"] == country]
    if not found:
        raise ValueError(f"No exchange entry found for country: {country}")
    return found


def _fetch_eodhd_equities_data(exchange_code):
    """
    Fetch raw data from EODHD for the specified exchange code.
    """
    api_token = os.environ.get("EODHD_API_KEY")
    if not api_token:
        raise EnvironmentError("EODHD_API_KEY environment variable is not set.")

    base_url = "https://eodhd.com/api/exchange-symbol-list"
    url = f"{base_url}/{exchange_code}?api_token={api_token}&fmt=json"
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.json()


def _parse_equities(raw_data, exchange_entry):
    """
    Filter out 'Type' != 'Common Stock' and build standard records.
    'mic' starts as just the OperatingMIC from the exchange_entry dict.
    """
    operating_mic = exchange_entry.get("OperatingMIC") or ""

    parsed = []
    for item in raw_data:
        if item.get("Type") != "Common Stock":
            continue

        equity = {
            "symbol": item.get("Code", ""),
            "name": item.get("Name", ""),
            "isin": item.get("Isin"),
            "mic": operating_mic,
        }
        parsed.append(equity)

    return parsed


def _merge_equities(merged_equities, new_equities):
    """
    Accumulates new_equities into merged_equities.
    If 'isin' is present, we use it to de-duplicate.
    If 'isin' is missing (None/empty), we treat that equity as always unique.
    """
    for eq in new_equities:
        eq_isin = eq["isin"]

        if eq_isin:
            # Deduplicate by ISIN
            if eq_isin not in merged_equities:
                # First time seeing this ISIN -> store it
                merged_equities[eq_isin] = {
                    "symbol": eq["symbol"],
                    "name": eq["name"],
                    "isin": eq_isin,
                    "mic": [eq["mic"]] if eq["mic"] else [],
                }
            else:
                # Already have a record for this ISIN
                existing = merged_equities[eq_isin]
                if eq["mic"] and eq["mic"] not in existing["mic"]:
                    existing["mic"].append(eq["mic"])
        else:
            # No ISIN => always keep as a unique record
            # Use a unique key (e.g. 'id(eq)') to avoid collisions
            merged_equities[id(eq)] = {
                "symbol": eq["symbol"],
                "name": eq["name"],
                "isin": None,
                "mic": [eq["mic"]] if eq["mic"] else [],
            }


def _finalize_equities(merged_equities):
    """
    Converts the merged_equities dict to a list, joining the list of MICs
    into a comma-separated string. Returns the final list of equity dicts.
    """
    final_list = []
    for record in merged_equities.values():
        record["mic"] = ", ".join(record["mic"])
        final_list.append(record)
    return final_list
