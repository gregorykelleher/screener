# reference_lookup/test_openfigi.py

import os

import pandas as pd
import pytest

from equity_aggregator.adapters.data_sources.reference_lookup.openfigi import (
    _build_query_dataframe,
    _extract_indexed_record,
    _get_api_key,
    _get_query_index,
    _is_valid_figi,
    _make_openfigi_client,
    _map_or_none,
    _OpenFigiClient,
    _to_identification_record,
    _to_query_record,
    extract_identified_records,
    fetch_equity_identification,
)
from equity_aggregator.schemas import RawEquity
from equity_aggregator.storage import load_cache, save_cache

pytestmark = pytest.mark.unit


def _records_df(entrys: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(entrys)


class _DummyClient:
    """
    Single configurable OpenFIGI dummy.

    - toggles 'connected' on connect()
    - map() can return a preset frame, echo input, or raise
    """

    def __init__(
        self,
        *,
        frame: pd.DataFrame | None = None,
        connect_error: BaseException | None = None,
        map_error: BaseException | None = None,
        echo_input: bool = False,
    ) -> None:
        self._frame = frame
        self._connect_error = connect_error
        self._map_error = map_error
        self._echo_input = echo_input
        self.connected = False

    def connect(self) -> object:
        if self._connect_error:
            raise self._connect_error
        self.connected = True
        return object()

    def map(self, df: pd.DataFrame) -> pd.DataFrame:
        if self._map_error:
            raise self._map_error
        if self._frame is not None:
            return self._frame
        if self._echo_input:
            return df
        return df


def test_to_query_record_prefers_isin_type() -> None:
    """
    ARRANGE: equity with ISIN;
    ACT: to_query_record;
    ASSERT: idType ID_ISIN
    """
    equity = RawEquity(name="T", symbol="SYM", isin="US1234567890", cusip="037833100")

    assert _to_query_record(equity)["idType"] == "ID_ISIN"


def test_to_query_record_uses_isin_value() -> None:
    """
    ARRANGE: equity with ISIN;
    ACT: to_query_record;
    ASSERT: idValue equals ISIN
    """
    equity = RawEquity(name="T", symbol="SYM", isin="US1234567890")

    assert _to_query_record(equity)["idValue"] == "US1234567890"


def test_to_query_record_uses_cusip_when_no_isin() -> None:
    """
    ARRANGE: equity with CUSIP only;
    ACT: to_query_record;
    ASSERT: idType ID_CUSIP
    """
    equity = RawEquity(name="T", symbol="SYM", cusip="037833100")

    assert _to_query_record(equity)["idType"] == "ID_CUSIP"


def test_to_query_record_falls_back_to_ticker() -> None:
    """
    ARRANGE: equity with no ISIN/CUSIP;
    ACT: to_query_record;
    ASSERT: idType TICKER
    """
    equity = RawEquity(name="T", symbol="SYM")

    assert _to_query_record(equity)["idType"] == "TICKER"


def test_to_query_record_sets_market_sec_des_equity() -> None:
    """
    ARRANGE: any equity;
    ACT: to_query_record;
    ASSERT: marketSecDes Equity
    """
    equity = RawEquity(name="T", symbol="SYM")

    assert _to_query_record(equity)["marketSecDes"] == "Equity"


def test_build_query_dataframe_preserves_length() -> None:
    """
    ARRANGE: two equities;
    ACT: build_query_dataframe;
    ASSERT: len 2
    """
    expected_dataframe_length = 2

    equities = [RawEquity(name="A", symbol="A"), RawEquity(name="B", symbol="B")]

    assert len(_build_query_dataframe(equities)) == expected_dataframe_length


def test_is_valid_figi_true_for_upper_alnum_12_chars() -> None:
    """
    ARRANGE: proper FIGI;
    ACT: is_valid_figi;
    ASSERT: True
    """
    assert _is_valid_figi("BBG000BLNNH6") is True


def test_is_valid_figi_false_for_wrong_length() -> None:
    """
    ARRANGE: bad FIGI;
    ACT: is_valid_figi;
    ASSERT: False
    """
    assert _is_valid_figi("SHORT") is False


def test_get_query_index_from_query_number_int() -> None:
    """
    ARRANGE: entry with query_number;
    ACT: get_query_index;
    ASSERT: index value
    """
    expected_query_index_value = 3

    assert _get_query_index({"query_number": 3}) == expected_query_index_value


def test_get_query_index_from_query_id_float() -> None:
    """
    ARRANGE: entry with queryId float;
    ACT: get_query_index;
    ASSERT: coerced int
    """
    expected_query_index_value = 2

    assert _get_query_index({"queryId": 2.0}) == expected_query_index_value


def test_get_query_index_from_request_index() -> None:
    """
    ARRANGE: entry with request_index;
    ACT: get_query_index;
    ASSERT: index value
    """
    expected_query_request_index_value = 7

    assert _get_query_index({"request_index": 7}) == expected_query_request_index_value


def test_get_query_index_returns_none_when_missing() -> None:
    """
    ARRANGE: entry without index keys;
    ACT: get_query_index;
    ASSERT: None
    """
    assert _get_query_index({"x": 1}) is None


def test_extract_identified_records_last_wins() -> None:
    """
    ARRANGE: two entrys same index;
    ACT: extract_identified_records;
    ASSERT: last FIGI kept
    """
    df = _records_df(
        [
            {"query_number": 0, "shareClassFIGI": "OLDOLDOLDOLD", "ticker": "AAA"},
            {"query_number": 0, "shareClassFIGI": "NEWNEWNEWNEW", "ticker": "BBB"},
        ],
    )
    assert extract_identified_records(df, expected=1)[0][2] == "NEWNEWNEWNEW"


def test_extract_identified_records_ignores_invalid_figi() -> None:
    """
    ARRANGE: invalid FIGI;
    ACT: extract_identified_records;
    ASSERT: placeholder
    """
    df = _records_df([{"query_number": 0, "shareClassFIGI": "invalid", "ticker": "T"}])

    assert extract_identified_records(df, expected=1)[0] == (None, None, None)


def test_extract_identified_records_ignores_out_of_range_index() -> None:
    """
    ARRANGE: index >= expected;
    ACT: extract_identified_records;
    ASSERT: placeholder
    """
    df = _records_df(
        [{"query_number": 5, "shareClassFIGI": "BBG000BLNNH6", "ticker": "T"}],
    )

    assert extract_identified_records(df, expected=1)[0] == (None, None, None)


def test_extract_identified_records_uses_security_name_when_name_missing() -> None:
    """
    ARRANGE: only securityName present;
    ACT: extract_identified_records;
    ASSERT: name from securityName
    """
    df = _records_df(
        [
            {
                "query_number": 0,
                "shareClassFIGI": "BBG000BLNNH6",
                "securityName": "Sec Name",
                "ticker": "T",
            },
        ],
    )
    assert extract_identified_records(df, expected=1)[0][0] == "Sec Name"


def test_extract_identified_records_name_falls_back_to_ticker() -> None:
    """
    ARRANGE: no name fields;
    ACT: extract_identified_records;
    ASSERT: name == ticker
    """
    df = _records_df(
        [{"query_number": 0, "shareClassFIGI": "BBG000BLNNH6", "ticker": "T"}],
    )
    assert extract_identified_records(df, expected=1)[0][0] == "T"


def test_extract_identified_records_symbol_none_when_ticker_not_string() -> None:
    """
    ARRANGE: non-string ticker;
    ACT: extract_identified_records;
    ASSERT: symbol None
    """
    df = _records_df(
        [
            {
                "query_number": 0,
                "shareClassFIGI": "BBG000BLNNH6",
                "name": "Name",
                "ticker": 123,
            },
        ],
    )
    assert extract_identified_records(df, expected=1)[0][1] is None


def test_extract_indexed_record_raises_on_none_index() -> None:
    """
    ARRANGE: entry without index;
    ACT: extract_indexed_record;
    ASSERT: raises
    """
    with pytest.raises(ValueError, match="missing index or record"):
        _extract_indexed_record({"shareClassFIGI": "BBG000BLNNH6", "ticker": "T"})


def test_extract_indexed_record_raises_on_none_record() -> None:
    """
    ARRANGE: entry with invalid FIGI;
    ACT: extract_indexed_record;
    ASSERT: raises
    """
    with pytest.raises(ValueError, match="missing index or record"):
        _extract_indexed_record(
            {"query_number": 0, "shareClassFIGI": None, "ticker": "T"},
        )


def test_to_identification_record_returns_none_when_figi_not_string() -> None:
    """
    ARRANGE: entry with non-string FIGI;
    ACT: to_identification_record;
    ASSERT: None
    """
    entry = {"query_number": 0, "shareClassFIGI": 123456, "ticker": "T"}

    assert _to_identification_record(entry) is None


def test_get_api_key_returns_none_when_missing_or_empty() -> None:
    """
    ARRANGE: ensure env empty;
    ACT: get_api_key;
    ASSERT: None
    """
    prev = os.environ.get("OPENFIGI_API_KEY")

    try:
        if "OPENFIGI_API_KEY" in os.environ:
            del os.environ["OPENFIGI_API_KEY"]
        assert _get_api_key() is None

    finally:
        if prev is not None:
            os.environ["OPENFIGI_API_KEY"] = prev


def test_get_api_key_returns_value_when_provider_supplies() -> None:
    """
    ARRANGE: provider returns KEY123;
    ACT: get_api_key(provider);
    ASSERT: KEY123
    """
    assert _get_api_key(lambda _: "KEY123") == "KEY123"


def test_make_openfigi_client_returns_none_when_no_key() -> None:
    """
    ARRANGE: provider returns None;
    ACT: make_openfigi_client;
    ASSERT: None
    """
    assert _make_openfigi_client(api_key_provider=lambda: None) is None


def test_make_openfigi_client_returns_none_on_connect_error() -> None:
    """
    ARRANGE: client connect raises;
    ACT: make_openfigi_client;
    ASSERT: None
    """
    client = _make_openfigi_client(
        api_key_provider=lambda: "KEY",
        incoming_client=lambda _: _DummyClient(connect_error=RuntimeError("boom")),
    )
    assert client is None


def test_make_openfigi_client_executes_connect() -> None:
    """
    ARRANGE: connectable client;
    ACT: make_openfigi_client;
    ASSERT: connected flag True
    """
    dummy = _DummyClient()

    _make_openfigi_client(
        api_key_provider=lambda: "KEY",
        incoming_client=lambda _: dummy,
    )

    assert dummy.connected is True


def test_make_openfigi_client_returns_same_instance() -> None:
    """
    ARRANGE: client factory returns object;
    ACT: make_openfigi_client;
    ASSERT: identity preserved
    """
    dummy = _DummyClient()
    client_instance = _make_openfigi_client(
        api_key_provider=lambda: "KEY",
        incoming_client=lambda _: dummy,
    )
    assert client_instance is dummy


async def test_map_or_none_returns_none_if_client_is_none() -> None:
    """
    ARRANGE: client None;
    ACT: _map_or_none;
    ASSERT: None
    """
    assert await _map_or_none([RawEquity(name="A", symbol="A")], None) is None


async def test_map_or_none_calls_map_and_returns_df_from_sequence() -> None:
    """
    ARRANGE: list input;
    ACT: _map_or_none;
    ASSERT: returns DataFrame
    """

    class _Echo:
        def connect(self) -> object:
            return object()

        def map(self, df: pd.DataFrame) -> pd.DataFrame:
            return pd.DataFrame(
                [{"query_number": 0, "shareClassFIGI": "BBG000BLNNH6", "ticker": "A"}],
            )

    result = await _map_or_none([RawEquity(name="A", symbol="A")], _Echo())

    assert isinstance(result, pd.DataFrame)


async def test_map_or_none_accepts_dataframe_input() -> None:
    """
    ARRANGE: DataFrame input;
    ACT: _map_or_none;
    ASSERT: preserves DataFrame shape
    """
    df = _records_df([{"query_number": 0, "shareClassFIGI": "BBG000BLNNH6"}])

    class _Echo:
        def connect(self) -> object:
            return object()

        def map(self, frame: pd.DataFrame) -> pd.DataFrame:
            return frame

    mapped_dataframe = await _map_or_none(df, _Echo())

    assert list(mapped_dataframe.columns) == ["query_number", "shareClassFIGI"]


async def test_map_or_none_returns_none_on_map_exception() -> None:
    """
    ARRANGE: client map raises;
    ACT: _map_or_none;
    ASSERT: None
    """
    err_client = _DummyClient(map_error=RuntimeError("boom"))

    assert await _map_or_none([RawEquity(name="A", symbol="A")], err_client) is None


async def test_fetch_equity_identification_returns_empty_on_no_inputs() -> None:
    """
    ARRANGE: empty input;
    ACT: fetch_equity_identification;
    ASSERT: []
    """
    assert await fetch_equity_identification([], cache_key="openfigi_empty") == []


async def test_fetch_equity_identification_returns_placeholders_on_client_error() -> (
    None
):
    """
    ARRANGE: map error;
    ACT: fetch_equity_identification;
    ASSERT: placeholders
    """
    inputs = [RawEquity(name="X", symbol="X"), RawEquity(name="Y", symbol="Y")]

    fetched_equity_identification = await fetch_equity_identification(
        inputs,
        cache_key="openfigi_error",
        client_factory=lambda: _DummyClient(map_error=RuntimeError("boom")),
    )

    assert fetched_equity_identification == [(None, None, None), (None, None, None)]


async def test_fetch_equity_identification_with_empty_api_key() -> None:
    """
    ARRANGE: empty OPENFIGI_API_KEY;
    ACT: fetch_equity_identification;
    ASSERT: placeholders
    """
    prev = os.environ.get("OPENFIGI_API_KEY")

    try:
        os.environ["OPENFIGI_API_KEY"] = ""  # forces _get_api_key -> None

        fetched_equity_identification = await fetch_equity_identification(
            [RawEquity(name="Fallback", symbol="F")],
            cache_key="openfigi_default_factory_and_none_client",
        )

        assert fetched_equity_identification == [(None, None, None)]

    finally:
        if prev is None:
            del os.environ["OPENFIGI_API_KEY"]
        else:
            os.environ["OPENFIGI_API_KEY"] = prev


async def test_fetch_equity_identification_returns_records_on_success() -> None:
    """
    ARRANGE: valid mapped frame;
    ACT: fetch_equity_identification;
    ASSERT: expected tuple values
    """
    inputs = [RawEquity(name="A", symbol="A"), RawEquity(name="B", symbol="B")]

    frame = _records_df(
        [
            {
                "query_number": 0,
                "shareClassFIGI": "BBG000BLNNH6",
                "ticker": "A",
                "name": "A plc",
            },
            {
                "query_number": 1,
                "shareClassFIGI": "BBG000C3Q7N2",
                "ticker": "B",
                "name": "B plc",
            },
        ],
    )

    fetched_equity_identification = await fetch_equity_identification(
        inputs,
        cache_key="openfigi_success_results",
        client_factory=lambda: _DummyClient(frame=frame),
    )

    assert fetched_equity_identification == [
        ("A plc", "A", "BBG000BLNNH6"),
        ("B plc", "B", "BBG000C3Q7N2"),
    ]


async def test_fetch_equity_identification_saves_to_cache_on_success() -> None:
    """
    ARRANGE: success path;
    ACT: fetch_equity_identification;
    ASSERT: cache contains records
    """
    cache_key = "openfigi_success_cache"

    inputs = [RawEquity(name="A", symbol="A")]

    frame = _records_df(
        [
            {
                "query_number": 0,
                "shareClassFIGI": "BBG000BLNNH6",
                "ticker": "A",
                "name": "A plc",
            },
        ],
    )

    await fetch_equity_identification(
        inputs,
        cache_key=cache_key,
        client_factory=lambda: _DummyClient(frame=frame),
    )

    assert load_cache(cache_key) == [("A plc", "A", "BBG000BLNNH6")]


async def test_fetch_equity_identification_uses_cache_when_present() -> None:
    """
    ARRANGE: pre-seeded cache;
    ACT: fetch_equity_identification;
    ASSERT: returns cached
    """
    cache_key = "openfigi_cached_path"
    cached = [("N", "S", "BBG000BLNNH6")]
    save_cache(cache_key, cached)

    def raising_factory() -> _OpenFigiClient:
        raise AssertionError("client should not be called")

    fetched_equity_identification = await fetch_equity_identification(
        [RawEquity(name="N", symbol="S")],
        cache_key=cache_key,
        client_factory=raising_factory,
    )
    assert fetched_equity_identification == cached
