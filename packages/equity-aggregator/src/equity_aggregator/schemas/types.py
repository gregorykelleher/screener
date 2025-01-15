# schemas/types.py

from decimal import Decimal
from pydantic import Field, StringConstraints, Strict
from typing import Annotated

# A non-empty string that is stripped of whitespace.
NonEmptyStr = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]

# A valid ISIN must be exactly 12 characters, start with two letters,
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

CIKStr = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=10,
        max_length=10,
        pattern=r"^\d{10}$",
        strict=True,
    ),
]

# A valid FIGI must be exactly 12 characters and consist of uppercase letters and digits.
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

# A valid MIC must be exactly 4 characters and consist of uppercase letters and digits.
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

# A valid currency code must be exactly 3 uppercase letters (ISO-4217).
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
