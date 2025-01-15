# market_vendors/test_openfigi.py

import pytest
import pandas as pd

from equity_aggregator.schemas import RawEquity
from equity_aggregator.adapters.equity_data_sources.market_vendors.openfigi.openfigi import (
    _chunk_equities,
    _build_query_dataframe,
    _retrieve_share_class_figis,
    _raw_equity_to_query_record,
    get_share_class_figi_for_raw_equities,
)

pytestmark = pytest.mark.unit


def test_idtype_prefers_isin() -> None:
    """
    ARRANGE: RawEquity with name, symbol, valid ISIN and CIK
    ACT:     call _raw_equity_to_query_record
    ASSERT:  idType == 'ID_ISIN'
    """
    equity = RawEquity(
        name="TestCo",
        symbol="SYM",
        isin="US1234567890",
        cik="0001234567",
        currency="USD",
        mics=["XNYS"],
    )

    actual = _raw_equity_to_query_record(equity)

    assert actual["idType"] == "ID_ISIN"


def test_idvalue_prefers_isin() -> None:
    """
    ARRANGE: RawEquity with valid ISIN
    ACT:     call _raw_equity_to_query_record
    ASSERT:  idValue == "US1234567890"
    """
    raw_equity = RawEquity(name="TestCo", symbol="SYM", isin="US1234567890")

    actual = _raw_equity_to_query_record(raw_equity)

    assert actual["idValue"] == "US1234567890"


def test_no_currency_for_isin() -> None:
    """
    ARRANGE: RawEquity.ISIN path with currency set
    ACT:     call _raw_equity_to_query_record
    ASSERT:  'currency' not in actual
    """
    raw_equity = RawEquity(
        name="TestCo", symbol="SYM", isin="US1234567890", currency="EUR"
    )

    actual = _raw_equity_to_query_record(raw_equity)

    assert "currency" not in actual


def test_no_exchcode_for_isin() -> None:
    """
    ARRANGE: RawEquity.ISIN path with mics set
    ACT:     call _raw_equity_to_query_record
    ASSERT:  'exchCode' not in actual
    """
    raw_equity = RawEquity(
        name="TestCo", symbol="SYM", isin="US1234567890", mics=["XNYS"]
    )

    actual = _raw_equity_to_query_record(raw_equity)

    assert "exchCode" not in actual


def test_marketsecdes_always_equity() -> None:
    """
    ARRANGE: RawEquity with any ID
    ACT:     call _raw_equity_to_query_record
    ASSERT:  marketSecDes == 'Equity'
    """
    raw_equity = RawEquity(name="TestCo", symbol="SYM")

    actual = _raw_equity_to_query_record(raw_equity)

    assert actual["marketSecDes"] == "Equity"


def test_idtype_fallback_to_cik() -> None:
    """
    ARRANGE: RawEquity with no ISIN but valid CIK
    ACT:     call _raw_equity_to_query_record
    ASSERT:  idType == 'ID_CIK'
    """
    raw_equity = RawEquity(name="TestCo", symbol="SYM", cik="0001234567")

    actual = _raw_equity_to_query_record(raw_equity)

    assert actual["idType"] == "ID_CIK"


def test_idvalue_fallback_to_cik() -> None:
    """
    ARRANGE: RawEquity with valid CIK
    ACT:     call _raw_equity_to_query_record
    ASSERT:  idValue == "0001234567"
    """
    raw_equity = RawEquity(name="TestCo", symbol="SYM", cik="0001234567")

    actual = _raw_equity_to_query_record(raw_equity)

    assert actual["idValue"] == "0001234567"


def test_idtype_ticker_when_no_isin_or_cik() -> None:
    """
    ARRANGE: RawEquity with only symbol
    ACT:     call _raw_equity_to_query_record
    ASSERT:  idType == 'TICKER'
    """
    raw_equity = RawEquity(name="TestCo", symbol="SYM")

    actual = _raw_equity_to_query_record(raw_equity)

    assert actual["idType"] == "TICKER"


def test_idvalue_ticker_when_no_isin_or_cik() -> None:
    """
    ARRANGE: RawEquity with only symbol
    ACT:     call _raw_equity_to_query_record
    ASSERT:  idValue == symbol
    """
    raw_equity = RawEquity(name="TestCo", symbol="SYM")

    actual = _raw_equity_to_query_record(raw_equity)

    assert actual["idValue"] == "SYM"


def test_build_query_dataframe_type_and_length() -> None:
    """
    ARRANGE: three RawEquity items
    ACT:     call _build_query_dataframe
    ASSERT:  DataFrame is pandas.DataFrame of length 3
    """
    inputs = [
        RawEquity(name="A", symbol="S1", isin="US1234567890"),
        RawEquity(name="B", symbol="S2", cik="0001234567"),
        RawEquity(name="C", symbol="S3", currency="USD", mics=["XNYS"]),
    ]

    df = _build_query_dataframe(inputs)

    assert isinstance(df, pd.DataFrame) and len(df) == 3


def test_build_query_dataframe_idtypes() -> None:
    """
    ARRANGE: mixed ID types in inputs
    ACT:     call _build_query_dataframe
    ASSERT:  idType column matches expected list
    """
    inputs = [
        RawEquity(name="A", symbol="S1", isin="US1234567890"),
        RawEquity(name="B", symbol="S2", cik="0001234567"),
        RawEquity(name="C", symbol="S3", currency="USD", mics=["XNYS"]),
    ]

    df = _build_query_dataframe(inputs)

    assert list(df["idType"]) == ["ID_ISIN", "ID_CIK", "TICKER"]


def test_build_query_dataframe_idvalues() -> None:
    """
    ARRANGE: mixed ID types in inputs
    ACT:     call _build_query_dataframe
    ASSERT:  idValue column matches expected list
    """
    inputs = [
        RawEquity(name="A", symbol="S1", isin="US1234567890"),
        RawEquity(name="B", symbol="S2", cik="0001234567"),
        RawEquity(name="C", symbol="S3", currency="USD", mics=["XNYS"]),
    ]

    df = _build_query_dataframe(inputs)

    assert list(df["idValue"]) == ["US1234567890", "0001234567", "S3"]


def test_retrieve_share_class_figis_first_hit() -> None:
    """
    ARRANGE: DataFrame with duplicate query_number 0
    ACT:     call _retrieve_share_class_figis
    ASSERT:  first element is first shareClassFIGI
    """
    # both entries have valid 12-char FIGIs; we should pick the first
    raw = pd.DataFrame(
        [
            {"query_number": 0, "shareClassFIGI": "FIGI00000001"},
            {"query_number": 0, "shareClassFIGI": "FIGI00000002"},
        ]
    )

    actual = _retrieve_share_class_figis(raw, batch_size=1)

    assert actual[0] == "FIGI00000001"


def test_retrieve_share_class_figis_none_for_explicit_none() -> None:
    """
    ARRANGE: DataFrame entry with shareClassFIGI None
    ACT:     call _retrieve_share_class_figis
    ASSERT:  actual element is None
    """
    raw = pd.DataFrame([{"query_number": 0, "shareClassFIGI": None}])

    actual = _retrieve_share_class_figis(raw, batch_size=1)

    assert actual[0] is None


def test_retrieve_share_class_figis_nan_for_missing_column() -> None:
    """
    ARRANGE: DataFrame missing shareClassFIGI key
    ACT:     call _retrieve_share_class_figis
    ASSERT:  actual element is NaN
    """
    raw = pd.DataFrame([{"query_number": 0}])

    actual = _retrieve_share_class_figis(raw, batch_size=1)

    # should be None (pandas.isna(None) is True)
    assert actual[0] is None


def test_retrieve_share_class_figis_none_for_unqueried_index() -> None:
    """
    ARRANGE: n > max query_number
    ACT:     call _retrieve_share_class_figis
    ASSERT:  second element is None
    """
    raw = pd.DataFrame([{"query_number": 0, "shareClassFIGI": "FIGI0"}])

    actual = _retrieve_share_class_figis(raw, batch_size=2)

    assert actual[1] is None


def test_retrieve_share_class_figis_invalid_format_dropped() -> None:
    """
    ARRANGE: one short/invalid FIGI, one valid FIGI
    ACT:     call _retrieve_share_class_figis
    ASSERT:  invalid at idx 0 → None; valid at idx 1 → captured
    """
    raw = pd.DataFrame(
        [
            {"query_number": 0, "shareClassFIGI": "SHORT"},  # too short
            {"query_number": 1, "shareClassFIGI": "FIGI00000003"},
            {"query_number": 1, "shareClassFIGI": "FIGI00000004"},
        ]
    )

    actual = _retrieve_share_class_figis(raw, batch_size=2)
    assert actual[0] is None
    assert actual[1] == "FIGI00000003"


def test_retrieve_share_class_figis_empty_list() -> None:
    """
    ARRANGE: empty DataFrame
    ACT:     call _retrieve_share_class_figis
    ASSERT:  returns empty list
    """
    raw = pd.DataFrame([])
    actual = _retrieve_share_class_figis(raw, batch_size=0)
    assert actual == []


@pytest.mark.asyncio
async def test_empty_input_returns_empty() -> None:
    """
    ARRANGE: no raw_equities
    ACT:     call get_share_class_figi_for_raw_equities([])
    ASSERT:  returns []
    """
    actual = await get_share_class_figi_for_raw_equities([])
    assert actual == []


def test_chunk_equities_splits_correctly() -> None:
    """
    ARRANGE: 250 dummy RawEquity objects
    ACT:     chunk_size=100
    ASSERT:  produces 3 chunks of lengths 100,100,50
    """
    dummy = [RawEquity(name=str(i), symbol=str(i)) for i in range(250)]
    chunks = _chunk_equities(dummy, chunk_size=100)
    assert len(chunks) == 3
    assert [len(c) for c in chunks] == [100, 100, 50]
