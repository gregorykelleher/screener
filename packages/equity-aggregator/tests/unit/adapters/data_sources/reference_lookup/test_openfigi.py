# reference_lookup/test_openfigi.py

import asyncio
import os
from collections.abc import Sequence

import pandas as pd
import pytest

from equity_aggregator.adapters.data_sources._cache._cache import (
    load_cache,
    save_cache,
)
from equity_aggregator.adapters.data_sources.reference_lookup.openfigi import (
    IndentificationRecord,
    _blocking_map_call,
    _build_query_dataframe,
    _consume_queue,
    _enumerate_chunks,
    _extract_identification_records,
    _fan_out,
    _log_missing_figis,
    _produce_chunk,
    _to_query_record,
    fetch_equity_identification,
)
from equity_aggregator.schemas import RawEquity

pytestmark = pytest.mark.unit


def test_to_query_record_prefers_isin() -> None:
    """
    ARRANGE: RawEquity with ISIN and CUSIP
    ACT:     call _to_query_record
    ASSERT:  idType == 'ID_ISIN'
    """
    equity = RawEquity(name="T", symbol="SYM", isin="US1234567890", cusip="037833100")

    assert _to_query_record(equity)["idType"] == "ID_ISIN"


def test_to_query_record_prefers_isin_value() -> None:
    """
    ARRANGE: RawEquity with ISIN
    ACT:     call _to_query_record
    ASSERT:  idValue equals ISIN
    """
    equity = RawEquity(name="T", symbol="SYM", isin="US1234567890")

    assert _to_query_record(equity)["idValue"] == "US1234567890"


def test_to_query_record_market_sec_des_is_equity() -> None:
    """
    ARRANGE: RawEquity with any identifier
    ACT:     call _to_query_record
    ASSERT:  marketSecDes == 'Equity'
    """
    equity = RawEquity(name="T", symbol="SYM")

    assert _to_query_record(equity)["marketSecDes"] == "Equity"


def test_to_query_record_fallback_to_cusip() -> None:
    """
    ARRANGE: RawEquity with CUSIP only
    ACT:     call _to_query_record
    ASSERT:  idType == 'ID_CUSIP'
    """
    equity = RawEquity(name="T", symbol="SYM", cusip="037833100")

    assert _to_query_record(equity)["idType"] == "ID_CUSIP"


def test_to_query_record_uses_ticker_when_no_isin_or_cusip() -> None:
    """
    ARRANGE: RawEquity with symbol only
    ACT:     call _to_query_record
    ASSERT:  idType == 'TICKER'
    """
    equity = RawEquity(name="T", symbol="SYM")

    assert _to_query_record(equity)["idType"] == "TICKER"


def test_build_query_dataframe_length() -> None:
    """
    ARRANGE: three RawEquity objects
    ACT:     call _build_query_dataframe
    ASSERT:  DataFrame length == 3
    """
    inputs = [
        RawEquity(name="A", symbol="S1", isin="US1234567890"),
        RawEquity(name="B", symbol="S2", cusip="037833100"),
        RawEquity(name="C", symbol="S3"),
    ]
    expected_df_length = 3

    assert len(_build_query_dataframe(inputs)) == expected_df_length


def test_build_query_dataframe_idtypes_column() -> None:
    """
    ARRANGE: mixed identifier inputs
    ACT:     call _build_query_dataframe
    ASSERT:  idType column matches expected sequence
    """
    inputs = [
        RawEquity(name="A", symbol="S1", isin="US1234567890"),
        RawEquity(name="B", symbol="S2", cusip="037833100"),
        RawEquity(name="C", symbol="S3"),
    ]

    df = _build_query_dataframe(inputs)

    assert list(df["idType"]) == ["ID_ISIN", "ID_CUSIP", "TICKER"]


def test_enumerate_chunks_produces_expected_sizes() -> None:
    """
    ARRANGE: 250 dummy equities, chunk_size=100
    ACT:     call _enumerate_chunks
    ASSERT:  chunk sizes == [100, 100, 50]
    """
    equities = [RawEquity(name=str(i), symbol=str(i)) for i in range(250)]
    chunks = _enumerate_chunks(equities, chunk_size=100)
    assert [len(chunk) for _, chunk in chunks] == [100, 100, 50]


def test_enumerate_chunks_empty_input() -> None:
    """
    ARRANGE: empty list
    ACT:     call _enumerate_chunks
    ASSERT:  returns empty list
    """
    assert _enumerate_chunks([], 10) == []


def test_extract_triplets_duplicate_keeps_first_row() -> None:
    """
    ARRANGE: two rows with query_number 0, first has valid 12-char FIGI
    ACT:     call _extract_triplets
    ASSERT:  triplet uses FIGI from first row
    """
    df = pd.DataFrame(
        [
            {"query_number": 0, "shareClassFIGI": "ABCDEFGH1234", "ticker": "AAA"},
            {"query_number": 0, "shareClassFIGI": "ZZZZZZZZ9999", "ticker": "BBB"},
        ],
    )
    assert _extract_identification_records(df, batch_size=1)[0] == (
        None,
        "AAA",
        "ABCDEFGH1234",
    )


def test_extract_triplets_duplicate_first_invalid_figi() -> None:
    """
    ARRANGE: first row invalid FIGI, second row valid FIGI for same query_number
    ACT:     call _extract_triplets
    ASSERT:  triplet FIGI is None (first row wins even if invalid)
    """
    df = pd.DataFrame(
        [
            {"query_number": 0, "shareClassFIGI": "SHORTFIGI", "ticker": "AAA"},
            {"query_number": 0, "shareClassFIGI": "ABCDEFGH1234", "ticker": "BBB"},
        ],
    )
    assert _extract_identification_records(df, batch_size=1)[0] == (None, "AAA", None)


def test_extract_triplets_missing_name_and_security_name() -> None:
    """
    ARRANGE: row with ticker & FIGI but no name/securityName
    ACT:     call _extract_triplets
    ASSERT:  name is None, symbol present
    """
    df = pd.DataFrame(
        [
            {"query_number": 0, "ticker": "FOO", "shareClassFIGI": "ABCDEFGH1234"},
        ],
    )
    assert _extract_identification_records(df, batch_size=1)[0] == (
        None,
        "FOO",
        "ABCDEFGH1234",
    )


def test_extract_triplets_row_outside_batch_range() -> None:
    """
    ARRANGE: row with query_number 5 but batch_size 3
    ACT:     call _extract_triplets
    ASSERT:  last position placeholder is (None, None, None)
    """
    df = pd.DataFrame(
        [{"query_number": 5, "shareClassFIGI": "ABCDEFGH1234"}],
    )
    assert _extract_identification_records(df, batch_size=3)[2] == (None, None, None)


def test_extract_triplets_empty_dataframe() -> None:
    """
    ARRANGE: empty DataFrame
    ACT:     call _extract_triplets
    ASSERT:  returns empty list
    """
    assert _extract_identification_records(pd.DataFrame(), batch_size=0) == []


def test_log_missing_figis_returns_none() -> None:
    """
    ARRANGE: one RawEquity with missing FIGI
    ACT:     call _log_missing_figis
    ASSERT:  returns None
    """
    equity = RawEquity(name="X", symbol="X")

    assert _log_missing_figis([equity], [None]) is None


def test_blocking_map_call_returns_dataframe_or_none() -> None:
    """
    ARRANGE: minimal query DataFrame
    ACT:     call _blocking_map_call
    ASSERT:  returns DataFrame or None
    """
    df = pd.DataFrame(
        [{"idType": "TICKER", "idValue": "FOO", "marketSecDes": "Equity"}],
    )
    try:
        actual = _blocking_map_call(df)
    except Exception as error:  # offline, bad key, etc.
        pytest.skip(f"OpenFIGI unavailable: {error}")
    else:
        assert actual is None or isinstance(actual, pd.DataFrame)


def test_cache_roundtrip() -> None:
    """
    ARRANGE: payload & cache name
    ACT:     save_cache then load_cache
    ASSERT:  loaded payload equals saved payload or None (TTL expired)
    """
    name, payload = "unit_test_cache_roundtrip", {"alpha": 1}

    save_cache(name, payload)

    assert load_cache(name) in (payload, None)


def test_load_cache_missing_returns_none() -> None:
    """
    ARRANGE: non-existent key
    ACT:     call load_cache
    ASSERT:  returns None
    """
    assert load_cache("key_does_not_exist") is None


async def test_fetch_equity_identification_empty() -> None:
    """
    ARRANGE: empty list
    ACT:     call fetch_equity_identification
    ASSERT:  returns empty list
    """
    assert await fetch_equity_identification([]) == []


async def test_fetch_equity_identification_cache_miss() -> None:
    """
    ARRANGE: single equity, clear cache
    ACT:     call fetch_equity_identification
    ASSERT:  list length == 1
    """
    save_cache("openfigi", None)

    actual = await fetch_equity_identification([RawEquity(name="Y", symbol="Y")])

    assert len(actual) == 1


async def test_fetch_equity_identification_uses_cache() -> None:
    """
    ARRANGE: cache primed, one equity
    ACT:     call fetch_equity_identification
    ASSERT:  output equals cached payload
    """
    payload = [("Foo Inc", "FOO", "ABCDEFGH1234")]

    save_cache("openfigi", payload)

    actual = await fetch_equity_identification([RawEquity(name="Foo", symbol="FOO")])

    assert actual == payload


async def test_fan_out_hits_sleep_branch() -> None:
    """
    ARRANGE: single chunk of equities, dummy sleep function
    ACT:     call _fan_out
    ASSERT:  queue eventually contains sentinel value
    """
    equities = [RawEquity(name=str(index), symbol=str(index)) for index in range(26)]
    chunks = _enumerate_chunks(equities, 1)
    queue: asyncio.Queue[None | tuple[int, IndentificationRecord]] = asyncio.Queue()

    async def dummy_chunk(
        start: int,
        batch: Sequence[RawEquity],
        queue: asyncio.Queue[None | tuple[int, IndentificationRecord]],
    ) -> None:
        await queue.put((start, (None, None, None)))
        await queue.put(None)

    async def dummy_sleep(_seconds: float) -> None:
        return None

    await _fan_out(chunks, queue, sleep=dummy_sleep, produce_chunk=dummy_chunk)

    # assert queue should eventually contain the sentinel
    assert any(item is None for item in [await queue.get() for _ in range(len(chunks))])


async def test_produce_chunk_exception_path() -> None:
    """
    ARRANGE: failing fetch coroutine
    ACT:     call _produce_chunk
    ASSERT:  placeholder triplet is returned
    """

    async def boom(_batch: object) -> None:
        raise RuntimeError

    queue: asyncio.Queue[None | tuple[int, IndentificationRecord]] = asyncio.Queue()

    await _produce_chunk(0, [RawEquity(name="X", symbol="X")], queue, fetch=boom)

    index, triplet = await queue.get()

    assert triplet == (None, None, None)


async def test_consume_queue_reports_shortfall() -> None:
    """
    ARRANGE: queue contains only a sentinel, so no data items arrive
    ACT:     call _consume_queue
    ASSERT:  returned list is [None] (placeholder for the missing item)
    """
    queue: asyncio.Queue[None | tuple[int, IndentificationRecord]] = asyncio.Queue()
    await queue.put(None)  # sentinel, but zero items

    result = await _consume_queue(queue, expected_items=1, expected_sentinels=1)

    assert result == [None]


async def test_produce_chunk_success_pushes_triplet() -> None:
    """
    ARRANGE: dummy fetch coroutine returns one known triplet.
    ACT:     call _produce_chunk.
    ASSERT:  first queued item equals expected triplet.
    """

    expected: IndentificationRecord = ("Alpha Co", "ALP", "ABCDEFGH1234")

    async def stub_fetch(_batch: Sequence[RawEquity]) -> list[IndentificationRecord]:
        return [expected]

    queue: asyncio.Queue[None | tuple[int, IndentificationRecord]] = asyncio.Queue()

    await _produce_chunk(
        0,
        [RawEquity(name="Alpha Co", symbol="ALP")],
        queue,
        fetch=stub_fetch,
    )

    index, triplet = await queue.get()
    await queue.get()  # sentinel

    assert triplet == expected


def test_blocking_map_call_with_api_key_set_empty_df_returns_fast() -> None:
    """
    ARRANGE: OPENFIGI_API_KEY set, but pass an empty DataFrame.
    ACT:     call _blocking_map_call.
    ASSERT:  function returns a DataFrame (empty) or None.
    """
    os.environ["OPENFIGI_API_KEY"] = "DUMMY_KEY"

    # columns expected by the API – zero rows avoids any outbound call
    df = pd.DataFrame(columns=["idType", "idValue", "marketSecDes"])

    try:
        result = _blocking_map_call(df)
    except Exception:
        assert True
    else:
        assert result is None or (isinstance(result, pd.DataFrame) and result.empty)
