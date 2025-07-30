# schemas/types.py

from decimal import Decimal
from typing import Annotated

from pydantic import BeforeValidator, StringConstraints

from .validators import (
    to_analyst_rating,
    to_cik,
    to_currency,
    to_cusip,
    to_figi,
    to_isin,
    to_mic,
    to_signed_decimal,
    to_unsigned_decimal,
    to_upper,
)

# Non-empty string with whitespace stripped.
NonEmptyStr = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]

# Required upper-cased string.
UpperStrReq = Annotated[
    str,
    BeforeValidator(lambda v, i: to_upper(v, i, required=True)),
]

# Optional upper-cased string.
UpperStrOpt = Annotated[str | None, BeforeValidator(to_upper)]

# Signed decimal - can be positive or negative (±).
SignedDecOpt = Annotated[Decimal | None, BeforeValidator(to_signed_decimal)]

# Unsigned decimal - can be positive (or zero).
UnsignedDecOpt = Annotated[Decimal | None, BeforeValidator(to_unsigned_decimal)]

# Valid ISIN must be exactly 12 characters, start with two letters,
# followed by nine alphanumeric chars, and end with a digit.
ISINStr = Annotated[
    str | None,
    BeforeValidator(to_isin),
]

# Valid CUSIP must be exactly 9 characters, consisting of digits and uppercase letters.
# Doesn't strictly enforce the CUSIP checksum.
CUSIPStr = Annotated[
    str | None,
    BeforeValidator(to_cusip),
]

# Valid CIK must be exactly 10 digits.
# Only digits allowed; no letters.
CIKStr = Annotated[
    str | None,
    BeforeValidator(to_cik),
]

# Valid FIGI must be exactly 12 characters and consist of uppercase letters and digits.
FIGIStr = Annotated[
    str | None,
    BeforeValidator(to_figi),
]

# Valid MIC must be exactly 4 characters and consist of uppercase letters and digits.
MICStr = Annotated[
    str | None,
    BeforeValidator(to_mic),
]

# List of MICs, which are non-empty strings.
MICListOpt = Annotated[
    list[MICStr] | None,
    BeforeValidator(lambda v, _: v if v else None),
]

# Valid currency code must be exactly 3 uppercase letters (ISO-4217).
CurrencyStr = Annotated[str | None, BeforeValidator(to_currency)]

# Analyst rating must be a distinct value, either "BUY", "SELL", or "HOLD".
AnalystRatingStr = Annotated[str | None, BeforeValidator(to_analyst_rating)]
