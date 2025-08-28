# tests/unit/retrieval/test_retrieval.py

from collections.abc import AsyncGenerator

import httpx
import pytest

from equity_aggregator.domain.retrieval.retrieval import (
    _DATA_STORE_PATH,
    _asset_browser_url,
    _download_to_temp,
    _finalise_download,
    _get_release_by_tag,
    _open_client,
    _stream_download,
    _write_chunks_to_file,
)

pytestmark = pytest.mark.unit


class _Stream(httpx.AsyncByteStream):
    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = chunks

    async def aiter_bytes(self) -> "AsyncGenerator[bytes, None]":
        for chunk in self._chunks:
            yield chunk

    async def aclose(self) -> None:
        return None

    def __aiter__(self) -> None:
        return self.aiter_bytes()


async def test_write_chunks_to_file_writes_all_bytes() -> None:
    """
    ARRANGE: Response with two byte chunks
    ACT:     _write_chunks_to_file
    ASSERT:  file content equals concatenated chunks
    """
    response = httpx.Response(200, stream=_Stream([b"ab", b"cd"]))
    out_path = _DATA_STORE_PATH / "out.gz"

    await _write_chunks_to_file(response, out_path)

    assert out_path.read_bytes() == b"abcd"


async def test_download_to_temp_returns_counts_and_writes() -> None:
    """
    ARRANGE: MockTransport serves 4 bytes with Content-Length header
    ACT:     _download_to_temp
    ASSERT:  returns (4, 4)
    """
    payload = b"ABCD"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, headers={"Content-Length": "4"}, content=payload)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    dest = _DATA_STORE_PATH / "file.tmp"

    written, expected = await _download_to_temp(
        client,
        "https://example/file",
        dest,
    )

    assert (written, expected) == (4, 4)


async def test_stream_download_creates_final_file() -> None:
    """
    ARRANGE: MockTransport serves bytes with matching length
    ACT:     _stream_download
    ASSERT:  final file exists with expected content
    """
    body = b"hello"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"Content-Length": str(len(body))},
            content=body,
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    dest = _DATA_STORE_PATH / "final.gz"

    returned = await _stream_download(client, "https://example/a", dest)

    assert returned.read_bytes() == body


def test_finalise_download_raises_on_mismatch() -> None:
    """
    ARRANGE: tmp contains 2 bytes but expected=3
    ACT:     _finalise_download
    ASSERT:  OSError raised
    """
    tmp = _DATA_STORE_PATH / "y.tmp"
    dest = _DATA_STORE_PATH / "y.bin"
    tmp.write_bytes(b"ab")

    with pytest.raises(OSError):
        _finalise_download(tmp, dest, (2, 3))


async def test_open_client_yields_supplied_instance() -> None:
    """
    ARRANGE: AsyncClient instance
    ACT:     _open_client(client)
    ASSERT:  yielded object is the same
    """
    client = httpx.AsyncClient()
    async with _open_client(client) as yielded:
        assert yielded is client
    await client.aclose()


async def test_open_client_creates_when_none() -> None:
    """
    ARRANGE: None client
    ACT:     _open_client(None)
    ASSERT:  yielded is an AsyncClient
    """
    async with _open_client(None) as yielded:
        assert isinstance(yielded, httpx.AsyncClient)


async def test_get_release_by_tag_404_raises_file_not_found() -> None:
    """
    ARRANGE: MockTransport returns 404
    ACT:     _get_release_by_tag
    ASSERT:  FileNotFoundError raised
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"message": "Not Found"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    with pytest.raises(FileNotFoundError):
        await _get_release_by_tag(client, "o", "r", "t")


async def test_get_release_by_tag_success() -> None:
    """
    ARRANGE: MockTransport returns 200 with empty assets
    ACT:     _get_release_by_tag
    ASSERT:  returns expected release dict
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"assets": []})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    release = await _get_release_by_tag(client, "o", "r", "t")
    assert release == {"assets": []}


async def test_get_release_by_tag_5xx_raises_httpstatus() -> None:
    """
    ARRANGE: MockTransport returns 503 with JSON error message
    ACT:     Call _get_release_by_tag with mocked client
    ASSERT:  httpx.HTTPStatusError is raised
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"message": "unavailable"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    with pytest.raises(httpx.HTTPStatusError):
        await _get_release_by_tag(client, "o", "r", "t")


def test_asset_browser_url_returns_expected() -> None:
    """
    ARRANGE: release dict with matching asset
    ACT:     _asset_browser_url
    ASSERT:  returns expected URL
    """
    release = {
        "assets": [
            {"name": "a.gz", "browser_download_url": "https://example/a.gz"},
        ],
    }

    url = _asset_browser_url(release, "a.gz")

    assert url == "https://example/a.gz"


def test_asset_browser_url_raises_when_missing() -> None:
    """
    ARRANGE: release dict without target asset
    ACT:     _asset_browser_url
    ASSERT:  FileNotFoundError raised
    """
    release = {"assets": [{"name": "b.gz", "browser_download_url": "x"}]}

    with pytest.raises(FileNotFoundError):
        _asset_browser_url(release, "a.gz")
