# schemas/validators.py

import re
from decimal import Decimal, InvalidOperation

from pydantic_core import core_schema as cs

_WORDS = re.compile(r"[^\w]+")
_SPACES = re.compile(r"\s+")


def require_non_empty(value: str | None, info: cs.ValidationInfo) -> str:
    """
    Validates that the provided string value is not None or empty.

    Args:
        value (str | None): The value to validate. Can be a string or None.
        info (cs.ValidationInfo): Validation context containing field metadata.

    Returns:
        str: The validated, non-empty string value.

    Raises:
        ValueError: If the value is None or an empty string after stripping whitespace.
    """
    if value is None or (isinstance(value, str) and value.strip() == ""):
        raise ValueError(f"{info.field_name} is mandatory")
    return value


def to_upper(value: str | float | Decimal | None) -> str | None:
    """
    Normalises text by removing punctuation, collapsing spaces, and converting to
    uppercase.

    - Returns None for None or blank input.
    - Use a separate presence check (e.g., AfterValidator(require_non_empty)) on
      required fields to enforce non-blank values.

    Args:
        value: The input value to normalise, expected as a string or None.

    Returns:
        str | None: The normalised uppercase string, or None if input is blank.
    """
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    text = _WORDS.sub(" ", text)
    text = _SPACES.sub(" ", text)
    return text.strip().upper()


def to_signed_decimal(
    value: str | float | Decimal | None,
    info: cs.ValidationInfo,
) -> Decimal | None:
    """
    Converts a numeric string to a Decimal, accepting both EU and US formats.
    Rejects negative values and returns None for invalid input. Raises a ValueError
    with a field-specific message if conversion fails.

    Args:
        value: The input value to convert, expected as a string or number.
        info (cs.ValidationInfo): Validation context containing field metadata.

    Returns:
        Decimal or None: The converted Decimal value, or None if input is invalid.

    Raises:
        ValueError: If the input cannot be converted to Decimal, with field name in
            the error message.
    """
    text = _parse_numeric_text(value)
    if text is None:
        return None
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        raise ValueError(f"invalid {info.field_name}: {value!r}") from None


def to_unsigned_decimal(
    value: str | float | Decimal | None,
    info: cs.ValidationInfo,
) -> Decimal | None:
    """
    Converts the input value to an unsigned decimal, raising an error for negatives.

    Args:
        value: The value to be converted to an unsigned decimal.
        info: Additional context or metadata used during conversion.

    Returns:
        Decimal or None: The converted unsigned decimal value, or None if conversion
        fails.

    Raises:
        ValueError: If the converted value is negative.
    """
    num = to_signed_decimal(value, info)
    if num is not None and num < 0:
        raise ValueError(f"negative value not allowed: {value!r}")
    return num


def to_isin(value: str | float | Decimal | None) -> str | None:
    """
    Normalises and validates an ISIN (ISO-6166) code.

    - Accepts None or blank input and returns None.
    - Normalises input using to_upper (removes punctuation, collapses spaces,
      converts to uppercase).
    - Validates that the result matches the ISO-6166 12-character pattern:
      two uppercase letters, nine alphanumeric characters, and a final digit.

    Args:
        value (str | float | Decimal | None): The input ISIN code.
        info (cs.ValidationInfo): Validation context with field metadata.

    Returns:
        str | None: The normalised ISIN code, or None if input is blank.

    Raises:
        ValueError: If the value does not match the ISIN format.
    """
    isin = to_upper(value)

    isin_pattern = r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$"

    if isin is None:
        return None

    if not re.fullmatch(isin_pattern, isin):
        raise ValueError(f"invalid ISIN code: {value!r}")

    return isin


def to_cusip(value: str | float | Decimal | None) -> str | None:
    """
    Normalises and validates a CUSIP code (9 alphanumeric characters).

    - Converts input to uppercase and trims using to_upper().
    - Accepts None or blank input as None.
    - Ensures the result is exactly 9 uppercase letters or digits.

    Args:
        value (str | float | Decimal | None): The input CUSIP code.
        info (cs.ValidationInfo): Validation context with field metadata.

    Returns:
        str | None: The normalised 9-character CUSIP code, or None if input is blank.

    Raises:
        ValueError: If the value does not match the CUSIP format.
    """
    cusip = to_upper(value)

    cusip_pattern = r"^[0-9A-Z]{9}$"

    if cusip is None:
        return None

    if not re.fullmatch(cusip_pattern, cusip):
        raise ValueError(f"invalid CUSIP code: {value!r}")

    return cusip


def to_cik(value: str | float | Decimal | None) -> str | None:
    """
    Normalises and validates a CIK (Central Index Key) to exactly 10 digits.

    - Accepts None or blank input and returns None.
    - Strips, collapses whitespace, and uppercases using to_upper (safe for digits).
    - Ensures the result is exactly 10 digits.

    Args:
        value (str | float | Decimal | None): The input CIK value.
        info (cs.ValidationInfo): Validation context with field metadata.

    Returns:
        str | None: The normalised 10-digit CIK, or None if input is blank.

    Raises:
        ValueError: If the value does not match the CIK specification.
    """
    cik = to_upper(value)

    cik_pattern = re.compile(r"^[0-9]{10}$")

    if cik is None:
        return None

    if not cik_pattern.fullmatch(cik):
        raise ValueError(f"invalid CIK code: {value!r}")

    return cik


def to_figi(value: str | float | Decimal | None) -> str | None:
    """
    Normalises and validates a FIGI (Financial Instrument Global Identifier).

    - Uses to_upper() to remove punctuation, collapse spaces, and convert to uppercase.
    - Accepts None or blank input and returns None.
    - Ensures the result is exactly 12 uppercase letters or digits.

    Args:
        value (str | float | Decimal | None): The input FIGI code.
        info (cs.ValidationInfo): Validation context with field metadata.

    Returns:
        str | None: The normalised 12-character FIGI code, or None if input is blank.

    Raises:
        ValueError: If the value does not match the FIGI format.
    """
    figi = to_upper(value)

    figi_pattern = r"^[A-Z0-9]{12}$"

    if figi is None:
        return None

    if not re.fullmatch(figi_pattern, figi):
        raise ValueError(f"invalid FIGI code: {value!r}")

    return figi


def to_mic(value: str | float | Decimal | None) -> str | None:
    """
    Normalises and validates a MIC (Market Identifier Code, ISO 10383).

    - Uses to_upper() to trim whitespace, remove punctuation, and convert to uppercase.
    - Accepts None or blank input and returns None.
    - Ensures the result is exactly 4 uppercase alphanumeric characters.

    Args:
        value (str | float | Decimal | None): The input MIC code.
        info (cs.ValidationInfo): Validation context with field metadata.

    Returns:
        str | None: The normalised 4-character MIC code, or None if input is blank.

    Raises:
        ValueError: If the code does not match the MIC format (^[A-Z0-9]{4}$).
    """
    mic = to_upper(value)

    mic_pattern = r"^[A-Z0-9]{4}$"

    if mic is None:
        return None

    if not re.fullmatch(mic_pattern, mic):
        raise ValueError(f"invalid MIC code: {value!r}")

    return mic


def to_currency(value: str | float | Decimal | None) -> str | None:
    """
    Normalises and validates a currency code to ISO-4217 format (AAA).

    - Converts input to uppercase and trims using to_upper().
    - Accepts None or blank input as None.
    - Ensures the result is exactly 3 uppercase A-Z letters.

    Args:
        value (str | float | Decimal | None): The input currency code.
        info (cs.ValidationInfo): Validation context with field metadata.

    Returns:
        str | None: The normalised 3-letter currency code, or None if input is blank.

    Raises:
        ValueError: If the code is not exactly 3 A-Z letters.
    """
    currency = to_upper(value)

    currency_code_length = 3

    if currency is None:
        return None

    if len(currency) != currency_code_length or not currency.isalpha():
        raise ValueError(f"invalid currency code: {value!r}")

    return currency


def to_analyst_rating(value: str | float | Decimal | None) -> str | None:
    """
    Normalises analyst rating input to one of the canonical tokens: "BUY", "SELL",
    or "HOLD".

    - Accepts blank or unknown input and returns None.
    - Invalid strings are silently discarded (returned as None) to avoid polluting
      downstream aggregates.
    - Input is normalised using to_upper (removes punctuation, collapses spaces,
      converts to uppercase).

    Args:
        value (str | float | Decimal | None): The analyst rating input.
        info (cs.ValidationInfo): Validation context with field metadata.

    Returns:
        str | None: The canonical rating ("BUY", "SELL", or "HOLD"), or None if
        input is blank or unrecognised.
    """
    text = to_upper(value)
    if text in {"BUY", "SELL", "HOLD"}:
        return text
    return None


def _parse_numeric_text(value: str | float | Decimal | None) -> str | None:
    """
    Normalises numeric text.

    Args:
        value (str | float | Decimal): The value to normalise. Can be a string,
            float, Decimal, or None.

    Returns:
        str | None: The normalised numeric string, or None if input is None or blank.

    - Returns None for None or blank input.
    - Removes leading '+'.
    - Delegates separator handling to _convert_separators.
    """
    text = str(value).strip() if value is not None else ""
    if not text:
        return None

    text = text.lstrip("+")
    return _convert_separators(text)


def _convert_separators(text: str) -> str:
    """
    Converts numeric strings with mixed European/US separators to dot-decimal format.

    Handles numbers with both commas and dots (e.g., "1,234.56" or "1.234,56"), as well
    as numbers with only commas (e.g., "1234,56"). Removes thousands separators and
    ensures the decimal separator is a dot.

    Args:
        text (str): The numeric string to normalise.

    Returns:
        str: The normalised numeric string with a dot as the decimal separator.
    """
    has_comma = "," in text
    has_dot = "." in text

    result = text
    if has_comma and has_dot:
        # US style (1,234.56) vs EU style (1.234,56)
        if text.rfind(",") < text.rfind("."):
            result = text.replace(",", "")
        else:
            result = text.replace(".", "").replace(",", ".", 1)
    elif has_comma:
        result = text.replace(",", ".", 1)

    return result
