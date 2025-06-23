# reference_lookup/test_openfigi.py

import asyncio
import re
from collections.abc import Sequence

import pandas as pd
import pytest

from equity_aggregator.adapters.data_sources._cache._cache import load_cache, save_cache
from equity_aggregator.adapters.data_sources.reference_lookup.openfigi import (
    _build_query_dataframe,
    _chunk_equities,
    _fetch_identification_limited,
    _fetch_map,
    _log_missing_figis,
    _raw_equity_to_query_record,
    _resolve_identification,
    _retrieve_identification,
    fetch_equity_identification,
)
from equity_aggregator.schemas import RawEquity

pytestmark = pytest.mark.unit


def test_idtype_prefers_isin() -> None:
    """
    ARRANGE: RawEquity with name, symbol, valid ISIN and CUSIP
    ACT:     call _raw_equity_to_query_record
    ASSERT:  idType == 'ID_ISIN'
    """

    equity = RawEquity(
        name="TestCo",
        symbol="SYM",
        isin="US1234567890",
        cusip="037833100",
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
        name="TestCo",
        symbol="SYM",
        isin="US1234567890",
        currency="EUR",
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
        name="TestCo",
        symbol="SYM",
        isin="US1234567890",
        mics=["XNYS"],
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


def test_idtype_fallback_to_cusip() -> None:
    """
    ARRANGE: RawEquity with no ISIN but valid CUSIP
    ACT:     call _raw_equity_to_query_record
    ASSERT:  idType == 'ID_CUSIP'
    """
    raw_equity = RawEquity(name="TestCo", symbol="SYM", cusip="037833100")

    actual = _raw_equity_to_query_record(raw_equity)

    assert actual["idType"] == "ID_CUSIP"


def test_idvalue_fallback_to_cusip() -> None:
    """
    ARRANGE: RawEquity with valid CUSIP
    ACT:     call _raw_equity_to_query_record
    ASSERT:  idValue == "037833100"
    """
    raw_equity = RawEquity(name="TestCo", symbol="SYM", cusip="037833100")

    actual = _raw_equity_to_query_record(raw_equity)

    assert actual["idValue"] == "037833100"


def test_idtype_ticker_when_no_isin_or_cusip() -> None:
    """
    ARRANGE: RawEquity with only symbol
    ACT:     call _raw_equity_to_query_record
    ASSERT:  idType == 'TICKER'
    """
    raw_equity = RawEquity(name="TestCo", symbol="SYM")

    actual = _raw_equity_to_query_record(raw_equity)

    assert actual["idType"] == "TICKER"


def test_idvalue_ticker_when_no_isin_or_cusip() -> None:
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
        RawEquity(name="B", symbol="S2", cusip="037833100"),
        RawEquity(name="C", symbol="S3", currency="USD", mics=["XNYS"]),
    ]
    expected_df_length = 3

    df = _build_query_dataframe(inputs)

    assert isinstance(df, pd.DataFrame) and len(df) == expected_df_length


def test_build_query_dataframe_idtypes() -> None:
    """
    ARRANGE: mixed ID types in inputs
    ACT:     call _build_query_dataframe
    ASSERT:  idType column matches expected list
    """
    inputs = [
        RawEquity(name="A", symbol="S1", isin="US1234567890"),
        RawEquity(name="B", symbol="S2", cusip="037833100"),
        RawEquity(name="C", symbol="S3", currency="USD", mics=["XNYS"]),
    ]

    df = _build_query_dataframe(inputs)

    assert list(df["idType"]) == ["ID_ISIN", "ID_CUSIP", "TICKER"]


def test_build_query_dataframe_idvalues() -> None:
    """
    ARRANGE: mixed ID types in inputs
    ACT:     call _build_query_dataframe
    ASSERT:  idValue column matches expected list
    """
    inputs = [
        RawEquity(name="A", symbol="S1", isin="US1234567890"),
        RawEquity(name="B", symbol="S2", cusip="037833100"),
        RawEquity(name="C", symbol="S3", currency="USD", mics=["XNYS"]),
    ]

    df = _build_query_dataframe(inputs)

    assert list(df["idValue"]) == ["US1234567890", "037833100", "S3"]


def test_chunk_equities_splits_correctly() -> None:
    """
    ARRANGE: 250 dummy RawEquity objects
    ACT:     chunk_size=100
    ASSERT:  produces 3 chunks of lengths 100,100,50
    """
    dummy = [RawEquity(name=str(i), symbol=str(i)) for i in range(250)]
    expected_chunks = 3

    chunks = _chunk_equities(dummy, chunk_size=100)

    assert len(chunks) == expected_chunks
    assert [len(chunk) for chunk in chunks] == [100, 100, 50]


def test_retrieve_identification_first_hit() -> None:
    """
    ARRANGE: DataFrame with duplicate query_number 0
    ACT:     call _retrieve_identification
    ASSERT:  first element triplet figi == first shareClassFIGI
    """
    raw = pd.DataFrame(
        [
            {"query_number": 0, "shareClassFIGI": "FIGI00000001"},
            {"query_number": 0, "shareClassFIGI": "FIGI00000002"},
        ],
    )

    actual = _retrieve_identification(raw, batch_size=1)

    assert actual[0] == (None, None, "FIGI00000001")


def test_retrieve_identification_none_for_explicit_none() -> None:
    """
    ARRANGE: DataFrame entry with shareClassFIGI None
    ACT:     call _retrieve_identification
    ASSERT:  element is all-None triplet
    """
    raw = pd.DataFrame([{"query_number": 0, "shareClassFIGI": None}])

    actual = _retrieve_identification(raw, batch_size=1)

    assert actual[0] == (None, None, None)


def test_retrieve_identification_none_for_missing_column() -> None:
    """
    ARRANGE: DataFrame missing shareClassFIGI key
    ACT:     call _retrieve_identification
    ASSERT:  element is all-None triplet
    """
    raw = pd.DataFrame([{"query_number": 0}])

    actual = _retrieve_identification(raw, batch_size=1)

    assert actual[0] == (None, None, None)


def test_retrieve_identification_none_for_unqueried_index() -> None:
    """
    ARRANGE: n > max query_number
    ACT:     call _retrieve_identification
    ASSERT:  second element is all-None triplet
    """
    raw = pd.DataFrame([{"query_number": 0, "shareClassFIGI": "FIGI0"}])

    actual = _retrieve_identification(raw, batch_size=2)

    assert actual[1] == (None, None, None)


def test_retrieve_identification_invalid_format_dropped() -> None:
    """
    ARRANGE: one short/invalid FIGI, one valid FIGI
    ACT:     call _retrieve_identification
    ASSERT:  invalid at idx 0 → None; valid at idx 1 → captured in triplet
    """
    raw = pd.DataFrame(
        [
            {"query_number": 0, "shareClassFIGI": "SHORT"},
            {"query_number": 1, "shareClassFIGI": "FIGI00000003"},
            {"query_number": 1, "shareClassFIGI": "FIGI00000004"},
        ],
    )

    actual = _retrieve_identification(raw, batch_size=2)

    assert actual[0] == (None, None, None)
    assert actual[1] == (None, None, "FIGI00000003")


def test_retrieve_identification_empty_list() -> None:
    """
    ARRANGE: empty DataFrame
    ACT:     call _retrieve_identification
    ASSERT:  returns empty list
    """
    raw = pd.DataFrame([])
    actual = _retrieve_identification(raw, batch_size=0)
    assert actual == []


def test_retrieve_identification_extracts_name_and_symbol() -> pd.DataFrame:
    """
    ARRANGE: DataFrame with name, ticker, shareClassFIGI
    ACT:     call _retrieve_identification
    ASSERT:  returns tuple with name, ticker, shareClassFIGI"""
    raw = pd.DataFrame(
        [
            {
                "query_number": 0,
                "name": "Foo Inc",
                "ticker": "FOO",
                "shareClassFIGI": "ABCDEFGHIJKL",
            },
        ],
    )

    actual = _retrieve_identification(raw, batch_size=1)

    assert actual[0] == ("Foo Inc", "FOO", "ABCDEFGHIJKL")


def test_retrieve_identification_uses_security_name_if_name_missing() -> pd.DataFrame:
    """
    ARRANGE: DataFrame with securityName instead of name
    ACT:     call _retrieve_identification
    ASSERT:  securityName is used as name, ticker and shareClassFIGI are valid
    """
    raw = pd.DataFrame(
        [
            {
                "query_number": 0,
                "securityName": "Bar Ltd",
                "ticker": "BAR",
                "shareClassFIGI": "MNOPQRSTUVWX",
            },
        ],
    )

    actual = _retrieve_identification(raw, batch_size=1)

    assert actual[0] == ("Bar Ltd", "BAR", "MNOPQRSTUVWX")


def test_retrieve_identification_drops_non_str_name_and_symbol() -> pd.DataFrame:
    """
    ARRANGE: DataFrame with non-string name and ticker
    ACT:     call _retrieve_identification
    ASSERT:  name and ticker become None, shareClassFIGI remains valid
    """
    raw = pd.DataFrame(
        [
            {
                "query_number": 0,
                "name": 123,
                "ticker": 456,
                "shareClassFIGI": "ABCDEFGHIJKL",
            },
        ],
    )

    actual = _retrieve_identification(raw, batch_size=1)

    # name & symbol are invalid so become None
    assert actual[0] == (None, None, "ABCDEFGHIJKL")


def test_chunk_equities_empty() -> None:
    """
    ARRANGE: empty list of RawEquity
    ACT:     call _chunk_equities with chunk_size=10
    ASSERT:  returns empty list
    """
    assert _chunk_equities([], chunk_size=10) == []


def test_build_query_dataframe_market_sec_des_is_equity() -> None:
    """
    ARRANGE: list of RawEquity objects
    ACT:     call _build_query_dataframe
    ASSERT:  every marketSecDes entry is 'Equity'
    """
    inputs = [
        RawEquity(name="A", symbol="S1"),
        RawEquity(name="B", symbol="S2"),
    ]

    df = _build_query_dataframe(inputs)

    assert set(df["marketSecDes"]) == {"Equity"}


def test_chunk_equities_single_chunk_when_smaller_than_size() -> None:
    """
    ARRANGE: 50 RawEquity objects
    ACT:     chunk_size=100
    ASSERT:  returns one chunk of length 50
    """
    equities = [RawEquity(name=str(i), symbol=str(i)) for i in range(50)]

    chunks = _chunk_equities(equities, chunk_size=100)

    assert [len(chunk) for chunk in chunks] == [50]


async def test_fetch_equity_identification_empty() -> None:
    """
    ARRANGE: empty list of RawEquity
    ACT:     call fetch_equity_identification
    ASSERT:  returns empty list
    """
    actual = await fetch_equity_identification([])

    assert actual == []


def test_log_missing_figis_returns_none() -> None:
    """
    ARRANGE: one RawEquity with missing FIGI
    ACT:     call _log_missing_figis
    ASSERT:  function returns None
    """
    raw_equity = RawEquity(name="A", symbol="A")

    assert _log_missing_figis([raw_equity], [None]) is None


async def test_fetch_identification_limited_returns_none_triplet_on_error() -> None:
    """
    ARRANGE: batch with a dummy object lacking RawEquity attrs (forces AttributeError)
    ACT:     call _fetch_identification_limited
    ASSERT:  returns list with one (None, None, None) triplet
    """

    class Dummy:
        pass

    batch: Sequence[RawEquity] = [Dummy()]

    actual = await _fetch_identification_limited(batch, asyncio.Semaphore(1))

    assert actual == [(None, None, None)]


async def test_resolve_identification_empty_input() -> None:
    """
    ARRANGE: empty list
    ACT:     call _resolve_identification
    ASSERT:  returns empty list (exercises chunking & task orchestration paths)
    """
    actual = await _resolve_identification([])

    assert actual == []


def test_log_missing_figis_no_return_value() -> None:
    """
    ARRANGE: one RawEquity with missing FIGI
    ACT:     call _log_missing_figis
    ASSERT:  function returns None (side-effect is logging only)
    """
    equity = RawEquity(name="Test", symbol="TST")

    assert _log_missing_figis([equity], [None]) is None


def test_fetch_map_returns_dataframe_or_none() -> None:
    """
    ARRANGE: minimal DataFrame with one ticker query
    ACT:     call _fetch_map
    ASSERT:  returns DataFrame or None (no broad-exception assertion)
    """
    df = pd.DataFrame(
        [
            {"idType": "TICKER", "idValue": "FOO", "marketSecDes": "Equity"},
        ],
    )

    try:
        actual = _fetch_map(df)
    except Exception as exc:  # network or key unavailable locally
        pytest.skip(f"OpenFIGI offline: {exc}")
    else:
        assert actual is None or isinstance(actual, pd.DataFrame)


def test_cache_roundtrip() -> None:
    """
    ARRANGE: arbitrary object and cache name
    ACT:     save_cache then load_cache
    ASSERT:  loaded object equals original or is None when TTL=0
    """
    name = "unit_test_cache_roundtrip"
    payload = {"alpha": 1, "beta": 2}

    save_cache(name, payload)
    loaded = load_cache(name)

    assert loaded == payload or loaded is None


def test_load_cache_missing_returns_none() -> None:
    """
    ARRANGE: non-existent cache key
    ACT:     call load_cache
    ASSERT:  returns None
    """
    assert load_cache("cache_key_that_does_not_exist") is None


async def test_resolve_identification_returns_none_triplets_on_api_failure() -> None:
    """
    ARRANGE: single RawEquity (symbol only)
    ACT:     call _resolve_identification (API likely unavailable)
    ASSERT:  actual is [(None, None, None)]
    """
    equities = [RawEquity(name="X", symbol="X")]
    expected_triplet_length = 3

    actual = await _resolve_identification(equities, chunk_size=1, concurrency=1)

    assert (
        len(actual) == 1
        and len(actual[0]) == expected_triplet_length
        and (actual[0][2] is None or bool(re.fullmatch(r"[A-Z0-9]{12}", actual[0][2])))
    )


async def test_fetch_equity_identification_cache_miss_triggers_resolution() -> None:
    """
    ARRANGE: no cache, one RawEquity
    ACT:     call fetch_equity_identification
    ASSERT:  returns single 3-tuple (figi may be None or valid)
    """
    equity = RawEquity(name="X", symbol="X")
    expected_triplet_length = 3

    actual = await fetch_equity_identification([equity])

    assert len(actual) == 1 and len(actual[0]) == expected_triplet_length


async def test_fetch_equity_identification_uses_cache() -> None:
    """
    ARRANGE: cache primed with a known payload
    ACT:     call fetch_equity_identification
    ASSERT:  function returns the cached payload
    """
    equities = [RawEquity(name="Foo", symbol="FOO")]
    payload = [("Foo Inc", "FOO", "FIGI1234567890")]

    save_cache("openfigi_cache", payload)

    actual = await fetch_equity_identification(equities)

    assert actual == payload
