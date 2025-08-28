# retrieval/retrieval.py

import asyncio
import os
from collections.abc import AsyncIterable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from httpx import AsyncClient, Response, codes

from equity_aggregator.adapters.data_sources._utils import make_client
from equity_aggregator.schemas import CanonicalEquity
from equity_aggregator.storage import (
    load_canonical_equities,
    load_canonical_equity,
    rebuild_canonical_equities_from_jsonl_gz,
)

_DATA_STORE_PATH: Path = Path(os.getenv("_DATA_STORE_DIR", "data/data_store/"))
_DATA_STORE_PATH.mkdir(parents=True, exist_ok=True)

_OWNER = "gregorykelleher"
_REPO = "equity-aggregator"
_TAG = "latest-build-release"

_GITHUB_RELEASE_URL = "https://api.github.com/repos/{owner}/{repo}/releases/tags/{tag}"

_HEADERS = {
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}


def retrieve_canonical_equity(share_class_figi: str) -> CanonicalEquity:
    """
    Retrieves a single CanonicalEquity instance by its share class FIGI.

    If the local database does not exist, it will be created and populated using
    `retrieve_canonical_equities()`. The function then attempts to load the equity
    corresponding to the provided FIGI. If no matching equity is found, a LookupError
    is raised.

    Args:
        share_class_figi (str): The FIGI identifier for the share class to retrieve.

    Returns:
        CanonicalEquity: The CanonicalEquity object associated with the given FIGI.

    Raises:
        LookupError: If no equity is found for the specified share_class_figi.
    """
    db_path = _DATA_STORE_PATH / "data_store.db"

    if not db_path.exists():
        retrieve_canonical_equities()

    equity = load_canonical_equity(share_class_figi)

    if equity is None:
        raise LookupError(f"No equity found for share_class_figi={share_class_figi!r}")

    return equity


def retrieve_canonical_equities() -> list[CanonicalEquity]:
    """
    Retrieves the canonical equities by downloading the latest JSONL file from GitHub,
    rebuilding the canonical equities table, and loading the equities from the table.

    Args:
        None

    Returns:
        list[CanonicalEquity]: A list of CanonicalEquity objects loaded from the
        rebuilt table.
    """
    # Download the canonical equities JSONL file from GitHub
    asyncio.run(download_canonical_equities())

    # Rebuild the canonical equities table from the downloaded JSONL file
    rebuild_canonical_equities_from_jsonl_gz()

    return load_canonical_equities()


async def download_canonical_equities(
    client: AsyncClient | None = None,
) -> None:
    """
    Download the 'data_store.db' release asset from a release tag URL.

    Args:
        release_tag_url (str): GitHub release tag URL (e.g.,
            'https://github.com/{owner}/{repo}/releases/tag/{tag}').
        client (AsyncClient | None, optional): Optional HTTP client. If None, creates
            a new client session.

    Returns:
        None
    """
    dest_path = _DATA_STORE_PATH / "canonical_equities.jsonl.gz"

    async with _open_client(client) as session:
        release = await _get_release_by_tag(session, _OWNER, _REPO, _TAG)

        url = _asset_browser_url(release, "canonical_equities.jsonl.gz")

        await _stream_download(session, url, dest_path)


@asynccontextmanager
async def _open_client(client: AsyncClient | None) -> AsyncIterable[AsyncClient]:
    """
    Async context manager to provide an AsyncClient instance.

    If an AsyncClient is provided via the `client` argument, it yields that client.
    Otherwise, it creates a new AsyncClient using `make_client()` and yields it.

    Args:
        client (AsyncClient | None): An optional AsyncClient instance to use.

    Yields:
        AsyncClient: An active AsyncClient instance for use within the context.
    """
    if client is not None:
        yield client
    else:
        async with make_client() as new_client:
            yield new_client


async def _get_release_by_tag(
    client: AsyncClient,
    owner: str,
    repo: str,
    tag: str,
) -> dict[str, Any]:
    """
    Retrieve a GitHub release by tag using the GitHub REST API.

    Args:
        client (AsyncClient): HTTP client for making requests.
        owner (str): GitHub repository owner.
        repo (str): GitHub repository name.
        tag (str): Release tag to fetch.

    Returns:
        dict[str, Any]: Parsed JSON response containing release details.

    Raises:
        FileNotFoundError: If the release tag does not exist.
        httpx.HTTPStatusError: On other HTTP errors.
    """
    api = _GITHUB_RELEASE_URL.format(owner=owner, repo=repo, tag=tag)

    response = await client.get(
        api,
        headers=_HEADERS,
        follow_redirects=True,
    )

    if response.status_code == codes.NOT_FOUND:
        raise FileNotFoundError(f"Release tag not found: {owner}/{repo}@{tag}")

    response.raise_for_status()

    return response.json()


def _asset_browser_url(release: dict[str, Any], asset_name: str) -> str:
    """
    Retrieves the browser download URL for a specific asset from a release dictionary.

    Args:
        release (dict[str, Any]): A dictionary representing the release, expected to
            contain an "assets" key with a list of asset dictionaries.
        asset_name (str): The exact name of the asset to search for.

    Returns:
        str: The browser download URL of the asset with the specified name.

    Raises:
        FileNotFoundError: If the asset with the given name is not found or does not
            have a browser download URL.
    """
    assets = release.get("assets") or []

    url = next(
        (
            asset.get("browser_download_url")
            for asset in assets
            if asset.get("name") == asset_name and asset.get("browser_download_url")
        ),
        None,
    )

    if not url:
        raise FileNotFoundError(f"Asset not found: {asset_name}")

    return url


async def _stream_download(
    client: AsyncClient,
    url: str,
    dest_path: Path,
) -> Path:
    """
    Download a file from a URL to dest_path robustly and atomically.

    Streams response bytes and follows redirects (needed for GitHub asset URLs).
    Writes to dest_path.tmp, then atomically replaces dest_path on success.
    If 'Content-Length' exists and the byte count mismatches, deletes the temp file
    and raises an error. Creates parent directories if missing.

    Guarantees:
      * dest_path is either the previous complete file or a new complete file.
      * Never leaves a partial file visible.
      * Raises on HTTP errors and incomplete transfers.

    Args:
        client (AsyncClient): HTTP client for requests.
        url (str): The URL to download from.
        dest_path (Path): Destination file path.

    Returns:
        Path: The path to the downloaded file.

    Raises:
        httpx.HTTPStatusError: On HTTP errors.
        IOError: On incomplete downloads.
        FileNotFoundError: If the asset is missing.
    """
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    tmp_path = dest_path.with_suffix(dest_path.suffix + ".tmp")

    # stream download to temporary file
    written_bytes = await _download_to_temp(client, url, tmp_path)

    # validate download completeness and finalise
    return _finalise_download(tmp_path, dest_path, written_bytes)


async def _download_to_temp(
    client: AsyncClient,
    url: str,
    tmp_path: Path,
) -> tuple[int, int]:
    """
    Download content from a URL to a temporary file.

    Streams the response from the given URL and writes it to tmp_path. Returns a tuple
    containing the number of bytes written and the expected content length from the
    response headers.

    Args:
        client (AsyncClient): HTTP client for making requests.
        url (str): The URL to download from.
        tmp_path (Path): Path to the temporary file for writing content.

    Returns:
        tuple[int, int]: (written_bytes, expected_bytes) where written_bytes is the
            number of bytes written to tmp_path, and expected_bytes is the value from
            the Content-Length header (or 0 if missing).
    """
    async with client.stream("GET", url, follow_redirects=True) as response:
        response.raise_for_status()

        # Extract expected content length
        expected_bytes = int(response.headers.get("Content-Length", "0") or 0)

        # Stream content to temporary file
        written_bytes = await _write_chunks_to_file(response, tmp_path)

        return written_bytes, expected_bytes


async def _write_chunks_to_file(
    response: Response,
    tmp_path: Path,
) -> int:
    """
    Write streamed response chunks to a file.

    Args:
        response (Response): The HTTP response object to stream bytes from.
        tmp_path (Path): Path to the temporary file for writing content.

    Returns:
        int: Total number of bytes written to the file.
    """
    written = 0
    with tmp_path.open("wb") as file_handler:
        async for chunk in response.aiter_bytes():
            file_handler.write(chunk)
            written += len(chunk)
    return written


def _finalise_download(
    tmp_path: Path,
    dest_path: Path,
    download_result: tuple[int, int],
) -> Path:
    """
    Validate the downloaded file and atomically move it to its final destination.

    Ensures the downloaded file matches the expected byte count. If the download is
    incomplete, deletes the temporary file and raises an error. On success, moves
    the temporary file to the destination path atomically.

    Args:
        tmp_path (Path): Path to the temporary file containing the downloaded data.
        dest_path (Path): Final destination path for the completed file.
        download_result (tuple[int, int]): Tuple of (written_bytes, expected_bytes).

    Returns:
        Path: The final destination path of the downloaded file.

    Raises:
        OSError: If the downloaded file is incomplete.
    """
    written_bytes, expected_bytes = download_result

    # Validate download completeness
    if expected_bytes and written_bytes != expected_bytes:
        tmp_path.unlink(missing_ok=True)
        raise OSError(
            (
                f"Incomplete download: expected {expected_bytes} bytes, "
                f"got {written_bytes}"
            ),
        )

    # Atomic move to final destination
    os.replace(tmp_path, dest_path)
    return dest_path
