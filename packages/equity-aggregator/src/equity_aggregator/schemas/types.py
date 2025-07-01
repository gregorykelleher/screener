# schemas/types.py

from decimal import Decimal
from typing import Annotated

from pydantic import Field, Strict, StringConstraints

# A non-empty string that is stripped of whitespace.
NonEmptyStr = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]

# Valid ISIN must be exactly 12 characters, start with two letters,
# followed by nine alphanumeric chars, and end with a digit.
ISINStr = Annotated[
    str,
    Strict(),
    StringConstraints(
        strip_whitespace=True,
        min_length=12,
        max_length=12,
        strict=True,
        to_upper=True,
        pattern=r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$",
    ),
]

# Valid CUSIP must be exactly 9 characters, consisting of digits and uppercase letters.
# Doesn't strictly enforce the CUSIP checksum.
CUSIPStr = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=9,
        max_length=9,
        strict=True,
        to_upper=True,
        pattern=r"^[0-9A-Z]{9}$",
    ),
]

# Valid CIK must be exactly 10 digits.
# Only digits allowed; no letters.
CIKStr = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=10,
        max_length=10,
        strict=True,
        pattern=r"^[0-9]{10}$",
    ),
]

# Valid FIGI must be exactly 12 characters and consist of uppercase letters and digits.
FIGIStr = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=12,
        max_length=12,
        pattern=r"^[A-Z0-9]{12}$",
        to_upper=True,
        strict=True,
    ),
]

# Valid MIC must be exactly 4 characters and consist of uppercase letters and digits.
MICStr = Annotated[
    str,
    Strict(),
    StringConstraints(
        strip_whitespace=True,
        min_length=4,
        max_length=4,
        strict=True,
        to_upper=True,
        pattern=r"^[A-Z0-9]{4}$",
    ),
]

# Valid currency code must be exactly 3 uppercase letters (ISO-4217).
CurrencyStr = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=3,
        max_length=3,
        pattern=r"^[A-Z]{3}$",
        to_upper=True,
        strict=True,
    ),
]

# A non-negative decimal value.
NonNegDecimal = Annotated[Decimal, Field(ge=0)]
