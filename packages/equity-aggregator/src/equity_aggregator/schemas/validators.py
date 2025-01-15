# schemas/validators.py

import re
from collections.abc import Iterable
from decimal import Decimal


def validate_name(value: str) -> str:
    """
    Validates a name by removing punctuation, collapsing whitespace, and uppercasing.

    Args:
        value (str): The name string to validate.

    Returns:
        str: The cleaned and uppercased name string.

    Raises:
        ValueError: If the input name is None.
    """
    if value is None:
        raise ValueError("name is mandatory")

    value = re.sub(r"[^\w]+", " ", value)  # strip punctuation to spaces
    value = re.sub(r"\s+", " ", value)  # collapse whitespace

    return value.strip().upper()


def validate_symbol(value: str) -> str:
    """
    Validates an equity symbol by removing leading and trailing whitespace
    and converting the symbol to uppercase. Raises a ValueError if the input is None.

    Args:
        value (str): The equity symbol to validate.

    Returns:
        str: The cleaned and uppercased equity symbol.

    Raises:
        ValueError: If the input symbol is None.
    """
    if value is None:
        raise ValueError("symbol is mandatory")
    return value.strip().upper()


def validate_id(value: str) -> str:
    """
    Normalise an identifier value.

    Args:
        value (str): The identifier to validate. If not a string, returns as-is.

    Returns:
        str: The identifier, stripped of whitespace and upper-cased if a string.
        Otherwise, returns the original value.
    """
    return value.strip().upper() if isinstance(value, str) else value


def validate_mics(value: Iterable[str] | None) -> list[str] | None:
    """
    Normalise a list of MIC (Market Identifier Code) values.

    Args:
        value (Iterable[str] or None): A list or iterable of MIC codes to validate.
            Each MIC should be a 4-character string. None or empty values are ignored.

    Returns:
        list[str] or None: A list of unique, uppercased, and validated 4-character MIC
            codes, or None if the input is None or empty.

    Raises:
        ValueError: If any MIC code is not exactly 4 characters after normalisation.
    """
    if not value:  # handles None, empty list, empty generator
        return None

    seen: set[str] = set()
    out: list[str] = []

    # MIC codes are always 4 characters long, uppercased, and stripped of whitespace
    mic_length = 4

    for mic in filter(None, map(_normalise_mic, value)):
        if len(mic) != mic_length:
            raise ValueError(f"invalid MIC code: {mic!r}")
        if mic not in seen:  # preserves first-seen order
            seen.add(mic)
            out.append(mic)

    return out or None


def validate_currency(value: str) -> str:
    """
    Validates a currency code string.

    Args:
        value (str): The currency code to validate. Can be None or a string.

    Returns:
        str or None: The uppercased currency code, or None if input is None or empty.
    """
    if value is None:
        return None

    currency = str(value).strip()
    if currency == "":
        return None
    return currency.upper()


def validate_last_price(value: str) -> Decimal | None:
    """
    Validates and normalises a price value to a Decimal.

    - Accepts numeric strings, handling both US and European formats (commas/dots).
    - Strips leading plus sign, rejects negative values.
    - Converts commas to dots as needed.
    - Returns None for None or empty input.

    Args:
        value (str or numeric): The price value to validate and normalise.

    Returns:
        Decimal or None: The normalised price as a Decimal, or None if input is empty.

    Raises:
        ValueError: If the value is negative or not a valid numeric format.
    """
    text = _normalise_numeric_text(value)
    if text is None:
        return None
    if re.compile(r"^\d+(?:\.\d+)?$").fullmatch(text) is None:
        raise ValueError(f"invalid last_price: {value!r}")
    return Decimal(text)


def validate_market_cap(value: str) -> Decimal | None:
    """
    Validates and normalises a market capitalisation value to a Decimal.

    - Accepts numeric strings in US or European formats (commas/dots).
    - Strips leading plus sign, rejects negative values.
    - Converts commas to dots as needed.
    - Returns None for None or empty input.

    Args:
        value (str): The market capitalisation value to validate and normalise.

    Returns:
        Decimal or None: The normalised market cap as a Decimal, or None if empty.

    Raises:
        ValueError: If the value is negative or not a valid numeric format.
    """
    text = _normalise_numeric_text(value)
    if text is None:
        return None
    if re.compile(r"^\d+(?:\.\d+)?$").fullmatch(text) is None:
        raise ValueError(f"invalid market_cap: {value!r}")
    return Decimal(text)


def _normalise_mic(value: str | None) -> str | None:
    """
    Normalise a MIC code string.

    Args:
        value (str | None): The MIC code to normalise.
            May be None.

    Returns:
        Optional[str]: The stripped and uppercased MIC code,
        or None if input is None or blank.
    """
    if value is None:
        return None
    text = str(value).strip().upper()
    return text or None


def _normalise_numeric_text(value: str | float | Decimal) -> str | None:
    """
    Normalises numeric text for price or market-cap values.

    Args:
        value (str | float | Decimal): The value to normalise. Can be a string,
            float, Decimal, or None.

    Returns:
        str | None: The normalised numeric string, or None if input is None or blank.

    - Returns None for None or blank input.
    - Removes leading '+'; raises ValueError for negative values.
    - Delegates separator handling to _convert_separators.
    """
    text = str(value).strip() if value is not None else ""
    if not text:
        return None

    text = text.lstrip("+")
    if text.startswith("-"):
        raise ValueError(f"negative value not allowed: {value!r}")

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
