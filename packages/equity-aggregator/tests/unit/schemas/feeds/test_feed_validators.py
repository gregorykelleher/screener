# feeds/test_feed_validators.py

import pytest
from pydantic import BaseModel, ValidationError

from equity_aggregator.schemas.feeds.feed_validators import required

pytestmark = pytest.mark.unit


def test_valid_single_required_field() -> None:
    """
    ARRANGE: Model with a non-empty 'name'
    ACT:     construct Model
    ASSERT:  name equals provided value
    """

    @required("name")
    class Model(BaseModel):
        name: str

    payload = {"name": "Alice"}

    actual = Model(**payload)

    assert actual.name == "Alice"


def test_missing_required_field_none_raises() -> None:
    """
    ARRANGE: Model with 'name' set to None
    ACT:     construct Model
    ASSERT:  raises ValidationError with correct message
    """

    @required("name")
    class Model(BaseModel):
        name: str | None

    payload = {"name": None}

    with pytest.raises(ValidationError) as excinfo:
        Model(**payload)

    assert "name is required" in str(excinfo.value.errors()[0]["msg"])


def test_missing_required_field_empty_string_raises() -> None:
    """
    ARRANGE: Model with 'symbol' set to empty string
    ACT:     construct Model
    ASSERT:  raises ValidationError with correct message
    """

    @required("symbol")
    class Model(BaseModel):
        symbol: str

    payload = {"symbol": ""}

    with pytest.raises(ValidationError) as excinfo:
        Model(**payload)

    assert "symbol is required" in str(excinfo.value.errors()[0]["msg"])


def test_missing_required_field_whitespace_string_raises() -> None:
    """
    ARRANGE: Model with 'code' set to whitespace
    ACT:     construct Model
    ASSERT:  raises ValidationError with correct message
    """

    @required("code")
    class Model(BaseModel):
        code: str

    payload = {"code": "   "}

    with pytest.raises(ValidationError) as excinfo:
        Model(**payload)

    assert "code is required" in str(excinfo.value.errors()[0]["msg"])


def test_valid_multiple_required_fields() -> None:
    """
    ARRANGE: Model with both 'name' and 'symbol' non-empty
    ACT:     construct Model
    ASSERT:  both fields equal provided values
    """

    @required("name", "symbol")
    class Model(BaseModel):
        name: str
        symbol: str

    payload = {"name": "Acme", "symbol": "ACM"}

    actual = Model(**payload)

    assert (actual.name, actual.symbol) == ("Acme", "ACM")


def test_missing_one_of_multiple_required_fields_raises() -> None:
    """
    ARRANGE: Model with 'name' valid and 'symbol' empty
    ACT:     construct Model
    ASSERT:  raises ValidationError for 'symbol'
    """

    @required("name", "symbol")
    class Model(BaseModel):
        name: str
        symbol: str

    payload = {"name": "Acme", "symbol": ""}

    with pytest.raises(ValidationError) as excinfo:
        Model(**payload)

    errors = excinfo.value.errors()
    assert any("symbol is required" in err["msg"] for err in errors)
