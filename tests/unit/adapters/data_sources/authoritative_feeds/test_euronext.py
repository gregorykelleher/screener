# authoritative_feeds/test_euronext.py

import asyncio
from collections.abc import AsyncGenerator

import httpx
import pytest

from equity_aggregator.adapters.data_sources.authoritative_feeds.euronext import (
    _PAGE_SIZE,
    _build_payload,
    _consume_queue,
    _deduplicate_records,
    _parse_row,
    _produce_mic,
    _safe_cell,
    _stream_mic_records,
    fetch_equity_records,
)
from equity_aggregator.storage import load_cache, save_cache

pytestmark = pytest.mark.unit


def test_payload_zero_and_negative() -> None:
    """
    ARRANGE: zero and negative parameters
    ACT: build payload
    ASSERT: maps values literally
    """
    payload = _build_payload(0, 0)

    assert payload["start"] == 0


def test_parse_row_name_extraction() -> None:
    """
    ARRANGE: aaData row with HTML in name column
    ACT: parse row
    ASSERT: name is extracted without tags
    """
    row = [
        "",
        '<a href="#">Example Co.</a>',
        "ISIN1234",
        "EXM",
        "<div>XPAR</div>",
        "EUR <span>99.99</span>",
    ]

    record = _parse_row(row)

    assert record["name"] == "Example Co."


def test_parse_row_mics_extraction() -> None:
    """
    ARRANGE: aaData row with multiple MICs
    ACT: parse row
    ASSERT: mics list is correct
    """
    row = [
        "",
        "Name",
        "ISIN5678",
        "SYM",
        "<div>XAMS, XBRU</div>",
        "EUR <span>10.00</span>",
    ]

    record = _parse_row(row)

    assert record["mics"] == ["XAMS", "XBRU"]


def test_parse_row_currency_extraction() -> None:
    """
    ARRANGE: aaData row with currency HTML
    ACT: parse row
    ASSERT: currency is parsed correctly
    """
    row = [
        "",
        "Name",
        "ISIN9101",
        "SYM1",
        "<div>XCORP</div>",
        "<div>USD <span>1234.56</span></div>",
    ]

    record = _parse_row(row)

    assert record["currency"] == "USD"


def test_parse_row_last_price_extraction() -> None:
    """
    ARRANGE: aaData row with price HTML
    ACT: parse row
    ASSERT: last_price is parsed correctly
    """
    row = [
        "",
        "Name",
        "ISIN9101",
        "SYM1",
        "<div>XCORP</div>",
        "<div>USD <span>1234.56</span></div>",
    ]

    record = _parse_row(row)

    assert record["last_price"] == "1234.56"


def test_parse_row_fallbacks() -> None:
    """
    ARRANGE: aaData row without optional HTML
    ACT: parse row
    ASSERT: fallback values are empty strings or lists
    """
    row = ["", "PlainName", "", "  SYM  ", "", ""]

    record = _parse_row(row)

    assert (
        record["name"],
        record["mics"],
        record["currency"],
        record["last_price"],
        record["symbol"],
    ) == ("PlainName", [], "", "", "SYM")


def test_parse_row_single_mic_no_comma() -> None:
    """
    ARRANGE: aaData row with one MIC and no delimiter
    ACT: parse row
    ASSERT: mics list contains single element
    """
    row = ["", "Sample Co.", "ISIN1111", "SAM", "<div>XPAR</div>", ""]

    record = _parse_row(row)

    assert record["mics"] == ["XPAR"]


def test_parse_row_price_with_commas() -> None:
    """
    ARRANGE: aaData row with price containing commas
    ACT: parse row
    ASSERT: last_price includes commas
    """
    row = [
        "",
        "Widgets Ltd",
        "ISIN2222",
        "WID",
        "",
        "<div>EUR <span>1,234.56</span></div>",
    ]

    record = _parse_row(row)

    assert record["last_price"] == "1,234.56"


def test_parse_row_malformed_html() -> None:
    """
    ARRANGE: aaData row with broken tags
    ACT: parse row
    ASSERT: fallback to raw text
    """
    row = ["", "<a>Broken", "", "BROK", "NoDiv", "NoPrice"]

    record = _parse_row(row)

    assert record["name"] == "<a>Broken"


def test_parse_row_integer_price() -> None:
    """
    ARRANGE: aaData row with integer price HTML
    ACT: parse row
    ASSERT: last_price equals "100"
    """
    row = ["", "Integer Inc", "ISIN3333", "S", "", "<div>EUR <span>100</span></div>"]

    record = _parse_row(row)

    assert record["last_price"] == "100"


def test_parse_row_short_row_returns_none() -> None:
    """
    ARRANGE: aaData row with too few columns
    ACT: parse row
    ASSERT: function returns None (insufficient mandatory fields)
    """
    record = _parse_row(["only", "three", "cols"])
    assert record is None


async def test_deduplicate_records_filters_duplicates() -> None:
    """
    ARRANGE: async iterable with duplicate key values
    ACT: apply deduplicator to filter duplicates
    ASSERT: only first instance of each key is yielded
    """

    async def source() -> AsyncGenerator[dict[str, int], None]:
        yield {"key": 1}
        yield {"key": 2}
        yield {"key": 1}

    dedup = _deduplicate_records(lambda record: record["key"])
    actual = [record async for record in dedup(source())]

    assert [record["key"] for record in actual] == [1, 2]


async def test_deduplicate_records_empty_iterable() -> None:
    """
    ARRANGE: empty async iterator
    ACT: apply deduplicator
    ASSERT: yields empty list
    """

    async def nothing() -> AsyncGenerator[dict, None]:
        if False:
            yield

    dedup = _deduplicate_records(lambda record: record)
    actual = [record async for record in dedup(nothing())]

    assert actual == []


async def test_deduplicate_records_all_duplicates() -> None:
    """
    ARRANGE: async iterable where every item has the same key
    ACT: apply deduplicator
    ASSERT: only the first item is retained
    """

    async def dupes() -> AsyncGenerator[dict[str, int], None]:
        for _ in range(3):
            yield {"key": 1}

    dedup = _deduplicate_records(lambda record: record["key"])
    records = [rec async for rec in dedup(dupes())]

    assert len(records) == 1


async def test_deduplicate_records_none_keys() -> None:
    """
    ARRANGE: async iterable with None keys
    ACT: apply deduplicator
    ASSERT: only first None key retained
    """

    async def source() -> AsyncGenerator[dict[str, None], None]:
        yield {"key": None}
        yield {"key": None}

    dedup = _deduplicate_records(lambda record: record["key"])
    actual = [record async for record in dedup(source())]

    assert len(actual) == 1


async def test_deduplicate_records_across_mics() -> None:
    """
    ARRANGE: queue emits two records with identical ISIN from different MICs
    ACT: consume queue via _deduplicate_records
    ASSERT: only first record is yielded
    """
    first_record = {"isin": "DUP", "mics": ["XPAR"]}
    second_record = {"isin": "DUP", "mics": ["XAMS"]}

    queue: asyncio.Queue[dict | None] = asyncio.Queue()

    await queue.put(first_record)  # producer-A
    await queue.put(second_record)  # producer-B

    await queue.put(None)  # sentinel-A
    await queue.put(None)  # sentinel-B

    stream = _consume_queue(queue, expected_sentinels=2)
    dedup = _deduplicate_records(lambda record: record["isin"])

    records = [record async for record in dedup(stream)]

    assert records == [first_record]


async def test_consume_queue_yields_until_sentinels() -> None:
    """
    ARRANGE: queue with two records followed by one sentinel
    ACT: consume with _consume_queue
    ASSERT: both records are yielded in order
    """
    queue: asyncio.Queue[int | None] = asyncio.Queue()
    await queue.put(1)
    await queue.put(2)

    # sentinel to stop consumption
    await queue.put(None)

    records = [record async for record in _consume_queue(queue, expected_sentinels=1)]

    assert records == [1, 2]


def test_parse_row_symbol_trimming() -> None:
    """
    ARRANGE: aaData with padded symbol
    ACT: parse row
    ASSERT: symbol is stripped of whitespace
    """
    row = ["", "Pad Co", "ISIN4444", "  PAD  ", "", ""]

    record = _parse_row(row)

    assert record["symbol"] == "PAD"


async def test_consume_queue_two_producers() -> None:
    """
    ARRANGE: queue with interleaved records and two sentinels
    ACT: consume with _consume_queue
    ASSERT: all records returned in insertion order
    """
    queue: asyncio.Queue[int | None] = asyncio.Queue()

    # producer #1 pushes 1 then sentinel
    await queue.put(1)
    await queue.put(None)

    # producer #2 pushes 2 then sentinel
    await queue.put(2)
    await queue.put(None)

    records = [record async for record in _consume_queue(queue, expected_sentinels=2)]

    assert records == [1, 2]


async def test_produce_mic_places_sentinel_on_success() -> None:
    """
    ARRANGE: mock transport returns one aaData row then normal loop ends
    ACT: run _produce_mic
    ASSERT: record followed by sentinel is enqueued
    """
    row = [
        "",
        "<a>One</a>",
        "ISIN",
        "SYM",
        "<div>XPAR</div>",
        "<div>EUR <span>1.00</span></div>",
    ]
    payload = {"aaData": [row], "iTotalRecords": 1}

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    queue: asyncio.Queue[dict | None] = asyncio.Queue()

    await _produce_mic(client, "XPAR", queue)

    assert (await queue.get(), await queue.get()) == (
        {
            "name": "One",
            "symbol": "SYM",
            "isin": "ISIN",
            "mics": ["XPAR"],
            "currency": "EUR",
            "last_price": "1.00",
        },
        None,
    )


async def test_fetch_equity_records_exits_on_http_error() -> None:
    """
    ARRANGE: mock transport returns 500 for every request
    ACT: iterate fetch_equity_records
    ASSERT: httpx.HTTPStatusError is raised
    """

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    async def consume() -> None:
        async for _ in fetch_equity_records(client):
            pass

    with pytest.raises(httpx.HTTPStatusError):
        await consume()


async def test_fetch_equity_records_exits_on_json_error() -> None:
    """
    ARRANGE: mock transport returns 200 with invalid JSON
    ACT: iterate fetch_equity_records
    ASSERT: ValueError is raised
    """

    async def handler(request: httpx.Request) -> httpx.Response:
        # valid status but body is not JSON -> json() raises JSONDecodeError
        return httpx.Response(200, content=b"not-json")

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    async def consume() -> None:
        async for _ in fetch_equity_records(client):
            pass

    with pytest.raises(ValueError):
        await consume()


async def test_fetch_equity_records_exits_on_read_error() -> None:
    """
    ARRANGE: mock transport raises httpx.ReadError while reading
    ACT: iterate fetch_equity_records
    ASSERT: httpx.ReadError is raised
    """

    async def handler(request: httpx.Request) -> httpx.Response:
        # Simulate a socket/read failure during response creation.
        raise httpx.ReadError("stream interrupted", request=request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    async def consume() -> None:
        async for _ in fetch_equity_records(client):
            pass

    with pytest.raises(httpx.ReadError):
        await consume()


def test_payload_large_numbers() -> None:
    """
    ARRANGE: very large pagination values
    ACT: build payload
    ASSERT: fields mirror inputs exactly
    """
    expected_start = 1_000_000
    start, draw = 1_000_000, 42

    payload = _build_payload(start, draw)

    assert payload["start"] == expected_start


def test_parse_row_missing_span() -> None:
    """
    ARRANGE: price HTML without <span>
    ACT: parse row
    ASSERT: currency and last_price fall back to empty strings
    """
    row = ["", "NoPrice PLC", "ISIN5555", "NOP", "", "<div>EUR </div>"]
    record = _parse_row(row)

    assert (record["currency"], record["last_price"]) == ("", "")


def test_parse_row_mixed_case_mics() -> None:
    """
    ARRANGE: MIC div with mixed casing and spaces
    ACT: parse row
    ASSERT: mics are trimmed but case preserved
    """
    row = ["", "MixedCase", "ISIN6666", "MIX", "<div>xPar , xBru</div>", ""]
    record = _parse_row(row)

    assert record["mics"] == ["xPar", "xBru"]


async def test_produce_mic_zero_rows_places_only_sentinel() -> None:
    """
    ARRANGE: mock transport returns aaData [] and iTotalRecords 0
    ACT: run _produce_mic
    ASSERT: queue contains only the sentinel (None)
    """
    payload = {"aaData": [], "iTotalRecords": 0}

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    queue: asyncio.Queue[dict | None] = asyncio.Queue()

    await _produce_mic(client, "XPAR", queue)

    assert await queue.get() is None


def test_fetch_equity_records_uses_cache() -> None:
    """
    ARRANGE: cache primed with two known records
    ACT:     collect via fetch_equity_records
    ASSERT:  yielded records equal the cached payload
    """
    payload = [
        {"isin": "CACHED1", "mics": ["XPAR"]},
        {"isin": "CACHED2", "mics": ["XAMS"]},
    ]
    save_cache("euronext_records", payload)

    async def collect() -> list[dict]:
        return [rec async for rec in fetch_equity_records()]

    actual = asyncio.run(collect())

    assert actual == payload


async def test_fetch_equity_records_saves_to_cache() -> None:
    """
    ARRANGE: empty cache and mock transport returning two rows
    ACT:     iterate fetch_equity_records once
    ASSERT:  cache now identical to streamed payload
    """
    save_cache("euronext_records", [])  # start with an empty cache

    row_a = [
        "",
        "<a>A</a>",
        "ISIN_A",
        "SYMA",
        "<div>XPAR</div>",
        "<div>EUR <span>1.00</span></div>",
    ]
    row_b = [
        "",
        "<a>B</a>",
        "ISIN_B",
        "SYMB",
        "<div>XPAR</div>",
        "<div>EUR <span>2.00</span></div>",
    ]
    payload = {"aaData": [row_a, row_b], "iTotalRecords": 2}

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    async def consume() -> None:
        async for _ in fetch_equity_records(client):
            pass

    await consume()

    assert load_cache("euronext_records") == [
        {
            "name": "A",
            "symbol": "SYMA",
            "isin": "ISIN_A",
            "mics": ["XPAR"],
            "currency": "EUR",
            "last_price": "1.00",
        },
        {
            "name": "B",
            "symbol": "SYMB",
            "isin": "ISIN_B",
            "mics": ["XPAR"],
            "currency": "EUR",
            "last_price": "2.00",
        },
    ]


async def test_stream_mic_records_paginates() -> None:
    """
    ARRANGE: mock transport forces two-page fetch (iTotalRecords > _PAGE_SIZE)
    ACT:     exhaust _stream_mic_records
    ASSERT:  second request's payload 'start' equals _PAGE_SIZE
    """
    starts: list[int] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        # form-encoded body: b'draw=1&start=0&length=100&...'
        form = dict(pair.split("=", 1) for pair in request.content.decode().split("&"))
        starts.append(int(form["start"]))

        # always return a single dummy row; iTotalRecords makes caller paginate
        row = [
            "",
            "<a>X</a>",
            "ISIN_X",
            "SYMX",
            "<div>XPAR</div>",
            "<div>EUR <span>9.99</span></div>",
        ]
        body = {"aaData": [row], "iTotalRecords": _PAGE_SIZE + 1}
        return httpx.Response(200, json=body)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    async def drain() -> None:
        async for _ in _stream_mic_records(client, "XPAR"):
            pass

    await drain()

    assert starts[-1] == _PAGE_SIZE


def test_safe_cell_out_of_range_returns_empty() -> None:
    """
    ARRANGE: cells list shorter than requested index
    ACT:     call _safe_cell with an out-of-range index
    ASSERT:  empty string is returned
    """
    cells = ["A", "B"]
    actual = _safe_cell(cells, 5)

    assert actual == ""


def test_safe_cell_non_string_returns_empty() -> None:
    """
    ARRANGE: cells list where targeted element is not a string
    ACT:     call _safe_cell with index pointing at a non-string value
    ASSERT:  empty string is returned
    """
    cells = ["A", None, "C"]
    actual = _safe_cell(cells, 1)

    assert actual == ""
