# schemas/types.py

from decimal import Decimal
from typing import Annotated

from pydantic import AfterValidator, BeforeValidator

from .validators import (
    require_non_empty,
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

# Optional upper-cased string.
UpperStrOpt = Annotated[str | None, BeforeValidator(to_upper)]

# Required upper-cased string must be non-empty.
UpperStrReq = Annotated[
    str | None,
    BeforeValidator(to_upper),
    AfterValidator(require_non_empty),
]

# Signed decimal - can be positive or negative (±).
SignedDecOpt = Annotated[Decimal | None, BeforeValidator(to_signed_decimal)]

# Unsigned decimal - can be positive (or zero).
UnsignedDecOpt = Annotated[Decimal | None, BeforeValidator(to_unsigned_decimal)]

# Valid ISIN must be exactly 12 characters, start with two letters,
# followed by nine alphanumeric chars, and end with a digit.
ISINStrOpt = Annotated[
    str | None,
    BeforeValidator(to_isin),
]

# Valid CUSIP must be exactly 9 characters, consisting of digits and uppercase letters.
# Doesn't strictly enforce the CUSIP checksum.
CUSIPStrOpt = Annotated[
    str | None,
    BeforeValidator(to_cusip),
]

# Valid CIK must be exactly 10 digits.
# Only digits allowed; no letters.
CIKStrOpt = Annotated[
    str | None,
    BeforeValidator(to_cik),
]

# Valid FIGI must be exactly 12 characters and consist of uppercase letters and digits.
FIGIStrOpt = Annotated[
    str | None,
    BeforeValidator(to_figi),
]

# Required FIGI must be non-empty.
FIGIStrReq = Annotated[
    str | None,
    BeforeValidator(to_figi),
    AfterValidator(require_non_empty),
]

# Valid MIC must be exactly 4 characters and consist of uppercase letters and digits.
MICStrOpt = Annotated[
    str | None,
    BeforeValidator(to_mic),
]

# List of MICs.
MICListOpt = Annotated[
    list[MICStrOpt] | None,
    BeforeValidator(lambda v, _: v if v else None),
]

# Valid currency code must be exactly 3 uppercase letters (ISO-4217).
CurrencyStrOpt = Annotated[str | None, BeforeValidator(to_currency)]

# Analyst rating must be a distinct value, either "BUY", "SELL", or "HOLD".
AnalystRatingStrOpt = Annotated[str | None, BeforeValidator(to_analyst_rating)]
