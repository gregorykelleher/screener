# authoritative_feeds/test_euronext.py

from collections.abc import AsyncGenerator

import httpx
import pytest

from equity_aggregator.adapters.data_sources.authoritative_feeds.euronext import (
    _build_payload,
    _country_pages,
    _fetch_equity_records_from_mics,
    _fetch_first_page,
    _parse_equities,
    _unique_by_key,
)

pytestmark = pytest.mark.unit


def test_build_payload_correct_values() -> None:
    """
    ARRANGE: define pagination parameters
    ACT: build payload dictionary
    ASSERT: payload contains expected keys and values
    """
    start = 10
    length = 50
    draw = 3

    actual = _build_payload(start, length, draw)

    expected = {
        "draw": 3,
        "start": 10,
        "length": 50,
        "iDisplayLength": 50,
        "iDisplayStart": 10,
    }

    assert actual == expected


def test_parse_equities_name_extraction() -> None:
    """
    ARRANGE: aaData row with HTML in name column
    ACT: parse equities
    ASSERT: name is extracted without tags
    """
    expected_aa_data = [
        [
            "",
            '<a href="#">Example Co.</a>',
            "ISIN1234",
            "EXM",
            "<div>XPAR</div>",
            "EUR <span>99.99</span>",
        ],
    ]

    records = list(_parse_equities(expected_aa_data))

    assert records[0]["name"] == "Example Co."


def test_parse_equities_mics_extraction() -> None:
    """
    ARRANGE: aaData row with multiple MICs
    ACT: parse equities
    ASSERT: mics list is correct
    """
    expected_aa_data = [
        [
            "",
            "Name",
            "ISIN5678",
            "SYM",
            "<div>XAMS, XBRU</div>",
            "EUR <span>10.00</span>",
        ],
    ]

    records = list(_parse_equities(expected_aa_data))

    assert records[0]["mics"] == ["XAMS", "XBRU"]


def test_parse_equities_currency_extraction() -> None:
    """
    ARRANGE: aaData row with currency HTML
    ACT: parse equities
    ASSERT: currency is parsed correctly
    """
    expected_aa_data = [
        [
            "",
            "Name",
            "ISIN9101",
            "SYM1",
            "<div>XCORP</div>",
            "<div>USD <span>1234.56</span></div>",
        ],
    ]

    records = list(_parse_equities(expected_aa_data))

    assert records[0]["currency"] == "USD"


def test_parse_equities_last_price_extraction() -> None:
    """
    ARRANGE: aaData row with price HTML
    ACT: parse equities
    ASSERT: last_price is parsed correctly
    """
    expected_aa_data = [
        [
            "",
            "Name",
            "ISIN9101",
            "SYM1",
            "<div>XCORP</div>",
            "<div>USD <span>1234.56</span></div>",
        ],
    ]

    records = list(_parse_equities(expected_aa_data))

    assert records[0]["last_price"] == "1234.56"


def test_parse_equities_fallback_name() -> None:
    """
    ARRANGE: aaData row without HTML tags
    ACT: parse equities
    ASSERT: fallback name is used
    """
    expected_aa_data = [
        [
            "",
            "PlainName",
            "",
            "",
            "",
            "",
        ],
    ]

    records = list(_parse_equities(expected_aa_data))

    assert records[0]["name"] == "PlainName"


def test_parse_equities_fallback_mics() -> None:
    """
    ARRANGE: aaData row without HTML tags
    ACT: parse equities
    ASSERT: fallback mics list is empty
    """
    expected_aa_data = [
        [
            "",
            "",
            "",
            "",
            "",
            "",
        ],
    ]

    records = list(_parse_equities(expected_aa_data))

    assert records[0]["mics"] == []


def test_parse_equities_fallback_currency() -> None:
    """
    ARRANGE: aaData row without HTML tags
    ACT: parse equities
    ASSERT: fallback currency is empty string
    """
    expected_aa_data = [
        [
            "",
            "",
            "",
            "",
            "",
            "",
        ],
    ]

    records = list(_parse_equities(expected_aa_data))

    assert records[0]["currency"] == ""


def test_parse_equities_fallback_last_price() -> None:
    """
    ARRANGE: aaData row without HTML tags
    ACT: parse equities
    ASSERT: fallback last_price is empty string
    """
    expected_aa_data = [
        [
            "",
            "",
            "",
            "",
            "",
            "",
        ],
    ]

    records = list(_parse_equities(expected_aa_data))

    assert records[0]["last_price"] == ""


async def test_unique_by_key_filters_duplicates() -> None:
    """
    ARRANGE: async iterable with duplicate key values
    ACT: apply unique_by_key to filter duplicates
    ASSERT: only first instance of each key is yielded
    """

    async def sample_iterable() -> AsyncGenerator[dict, None]:
        yield {"key": 1}
        yield {"key": 2}
        yield {"key": 1}

    result = [
        record
        async for record in _unique_by_key(
            sample_iterable(),
            lambda record: record["key"],
        )
    ]

    assert [record["key"] for record in result] == [1, 2]


def test_parse_equities_empty_list() -> None:
    """
    ARRANGE: empty aaData
    ACT: parse equities
    ASSERT: yields nothing
    """
    records = list(_parse_equities([]))

    assert records == []


def test_parse_equities_symbol_stripped() -> None:
    """
    ARRANGE: aaData with padded symbol
    ACT: parse equities
    ASSERT: symbol is stripped of whitespace
    """
    expected_aa_data = [["", "N", "I", "  SYM  ", "", "", ""]]

    records = list(_parse_equities(expected_aa_data))

    assert records[0]["symbol"] == "SYM"


def test_parse_equities_single_mic_no_comma() -> None:
    """
    ARRANGE: aaData row with one MIC and no delimiter
    ACT: parse equities
    ASSERT: mics list contains single element
    """
    aa_data = [["", "", "", "", "<div>XPAR</div>", "", ""]]

    records = list(_parse_equities(aa_data))

    assert records[0]["mics"] == ["XPAR"]


def test_parse_equities_price_with_commas() -> None:
    """
    ARRANGE: aaData row with price containing commas
    ACT: parse equities
    ASSERT: last_price includes commas
    """
    aa_data = [["", "", "", "", "", "<div>EUR <span>1,234.56</span></div>"]]

    records = list(_parse_equities(aa_data))

    assert records[0]["last_price"] == "1,234.56"


def test_parse_equities_malformed_html() -> None:
    """
    ARRANGE: aaData row with broken tags
    ACT: parse equities
    ASSERT: fallback to raw text
    """
    aa_data = [["", "<a>Broken", "", "", "NoDiv", "NoPrice"]]

    records = list(_parse_equities(aa_data))

    assert records[0]["name"] == "<a>Broken"


def test_build_payload_zero_and_negative() -> None:
    """
    ARRANGE: zero and negative parameters
    ACT: build payload
    ASSERT: maps values literally
    """
    payload = _build_payload(0, -1, 0)

    assert payload["length"] == -1


async def test_unique_by_key_empty() -> None:
    """
    ARRANGE: empty async iterator
    ACT: apply unique_by_key
    ASSERT: yields empty list
    """

    async def nothing() -> AsyncGenerator[dict, None]:
        if False:
            yield

    result = [record async for record in _unique_by_key(nothing(), lambda x: x)]
    assert result == []


async def test_unique_by_key_all_duplicates() -> None:
    """
    ARRANGE: async iterable where every item has the same key
    ACT: apply unique_by_key
    ASSERT: only the first item is retained
    """

    async def dupes() -> AsyncGenerator[dict[str, int], None]:
        for _ in range(3):
            yield {"key": 1}

    result = [
        record async for record in _unique_by_key(dupes(), lambda record: record["key"])
    ]
    assert len(result) == 1


async def test__fetch_first_page_returns_none_on_500() -> None:
    """
    ARRANGE: handler returns 500
    ACT: call _fetch_page
    ASSERT: returns None
    """

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    actual = await _fetch_first_page(client, "XPAR")

    assert actual is None


async def test__country_pages_empty_if_first_page_none() -> None:
    """
    ARRANGE: first page returns 500
    ACT: iterate _country_pages
    ASSERT: yields no records
    """

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    records = [record async for record in _country_pages(client, "XPAR")]
    assert records == []


async def test_fetch_equity_records_from_mics_skips_failed_mic() -> None:
    """
    ARRANGE: XPAR succeeds, others error
    ACT: iterate _fetch_equity_records_from_mics
    ASSERT: yields only successful records
    """

    def handler(request: httpx.Request) -> httpx.Response:
        row = [
            "",
            "<a>Foo</a>",
            "ISIN",
            "SYM",
            "<div>XPAR</div>",
            "<div>EUR <span>1.23</span></div>",
        ]
        payload = {"aaData": [row]}
        if b"XPAR" in request.url.query:
            return httpx.Response(200, json=payload)
        error_response = httpx.Response(500)
        raise httpx.HTTPStatusError(
            "err",
            request=request,
            response=error_response,
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    async for record in _fetch_equity_records_from_mics(client):
        assert record["symbol"] == "SYM"


async def test_unique_by_key_none_keys() -> None:
    """
    ARRANGE: async iterable with None keys
    ACT: apply unique_by_key
    ASSERT: only first None key retained
    """

    async def source() -> AsyncGenerator[dict[str, None], None]:
        yield {"key": None}
        yield {"key": None}

    actual = [
        record
        async for record in _unique_by_key(source(), lambda record: record["key"])
    ]
    assert len(actual) == 1


def test_parse_equities_integer_price() -> None:
    """
    ARRANGE: aaData row with integer price HTML
    ACT: parse equities
    ASSERT: last_price equals "100"
    """
    expected_aa_data = [["", "", "I", "S", "", "<div>EUR <span>100</span></div>"]]

    record = next(_parse_equities(expected_aa_data))

    assert record["last_price"] == "100"


def test_parse_equities_raises_on_short_row() -> None:
    """
    ARRANGE: aaData row with too few columns
    ACT: parse equities
    ASSERT: IndexError is raised
    """
    with pytest.raises(IndexError):
        next(_parse_equities([["only", "three", "cols"]]))


async def test_country_pages_two_pages() -> None:
    """
    ARRANGE: handler returns two pages of data
    ACT: collect with _country_pages
    ASSERT: names from both pages are returned
    """
    calls = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["count"] += 1
        name = f"#{calls['count']}"
        row = [
            "",
            f"<a>{name}</a>",
            "I",
            "SYM",
            "<div>XPAR</div>",
            "<div>EUR <span>1</span></div>",
        ]
        payload = {"aaData": [row], "iTotalRecords": 101}
        return httpx.Response(200, json=payload)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    names = {record["name"] async for record in _country_pages(client, "XPAR")}

    assert names == {"#1", "#2"}
