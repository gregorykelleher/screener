# schemas/validators.py

from decimal import Decimal
import re


# ───────── name: presence, strip spaces, upper ────────
def validate_name(v: str) -> str:
    """
    Validate a name string by stripping whitespace,
    collapsing internal spaces, and upper-casing.
    """
    if v is None:
        raise ValueError("name is mandatory")
    v = re.sub(r"[^\w]+", " ", v)  # strip punctuation to spaces
    v = re.sub(r"\s+", " ", v)  # collapse whitespace
    return v.strip().upper()


# ───────── symbol: strip + upper ──────────────────────
def validate_symbol(v: str) -> str:
    """
    Validate an equity symbol by stripping whitespace and
    upper-casing.
    """
    if v is None:
        raise ValueError("symbol is mandatory")
    return v.strip().upper()


# ───────── IDs: strip + upper ──────────────────────
def validate_id(v):
    """
    Validate an identifier by stripping whitespace and
    upper-casing if a string.
    """
    return v.strip().upper() if isinstance(v, str) else v


# ───────── mics: presence, strip, upper‑case ──────────────────
def validate_mics(v):
    """
    Normalise MIC lists.

    * None or empty list → None
    * Otherwise: strip / upper-case / deduplicate / validate 4-char MIC codes
    """
    if v is None or len(v) == 0:
        return None

    seen: set[str] = set()
    out: list[str] = []

    for m in v:
        if m is None:  # ignore literal None
            continue

        norm = str(m).strip().upper()
        if not norm:
            continue  # blank after stripping

        if len(norm) != 4:
            raise ValueError(f"invalid MIC code: {m!r}")

        if norm not in seen:
            seen.add(norm)
            out.append(norm)

    return out or None


# ───────── currency: presence, strip, upper‑case ──────────────
def validate_currency(v: str) -> str:
    """
    Validate a currency code string.
    - Coerce None or empty string to None.
    - Strip whitespace and upper-case.
    """
    if v is None:
        return None
    s = str(v).strip()
    if s == "":
        return None
    return s.upper()


# ───────── last_price: presence, comma→dot, Decimal ───────────
def validate_last_price(v):
    """
    Validate a price value to Decimal.
    - Coerce None or empty string to None.
    - Strip leading plus sign, reject negatives.
    - Handle European and US formatting (commas/dots).
    - Validate numeric format before conversion.
    """
    if v is None:
        return None
    s0 = str(v).strip()
    if s0 == "":
        return None

    if s0.startswith("+"):
        s0 = s0[1:]
    if s0.startswith("-"):
        raise ValueError(f"negative last_price not allowed: {v!r}")

    has_dot = "." in s0
    has_comma = "," in s0

    if has_comma and has_dot:
        # US style: comma before dot → remove commas
        if s0.rfind(",") < s0.rfind("."):
            s = s0.replace(",", "")
        # European: comma after dot → remove dots, comma→dot
        else:
            s = s0.replace(".", "").replace(",", ".", 1)
    elif has_comma:
        s = s0.replace(",", ".", 1)
    else:
        s = s0

    if re.fullmatch(r"\d+(?:\.\d+)?", s) is None:
        raise ValueError(f"invalid last_price: {v!r}")

    return Decimal(s)
