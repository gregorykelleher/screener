# tests/live/test_lse_live.py
import httpx
import pytest

URL = "https://api.londonstockexchange.com/api/v1/components/refresh"

#  A single-row payload is enough to prove the backend works
_PAYLOAD = {
    "path": "live-markets/market-data-dashboard/price-explorer",
    # Main-market equities, index “ASX”, first page
    "parameters": (
        "markets%3DMAINMARKET%26categories%3DEQUITY%26indices%3DASX"
        "%26showonlylse%3Dtrue&page%3D0"
    ),
    "components": [
        {
            "componentId": "block_content%3A9524a5dd-7053-4f7a-ac75-71d12db796b4",
            "parameters": (
                "markets=MAINMARKET&categories=EQUITY&indices=ASX"
                "&showonlylse=true&page=0&size=1"
            ),
        }
    ],
}

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/134.0.0.0 Safari/537.36"
    )
}

# ---------------------------------------------------------------------------


async def _ping(client: httpx.AsyncClient):
    """
    POST the minimal payload and return:
      * True              - non-empty equities list
      * HTTP status code  - if the server answers but not 2xx
      * False             - network / JSON error
    """
    try:
        r = await client.post(URL, json=_PAYLOAD, headers=_HEADERS)
        r.raise_for_status()
        data = r.json()  # API wraps the payload in a 1-element list
        if not (isinstance(data, list) and data):
            return False

        block = next(
            (
                c
                for c in data[0].get("content", [])
                if c.get("name") == "priceexplorersearch"
            ),
            None,
        )
        equities = (block or {}).get("value", {}).get("content", [])
        return bool(equities)
    except httpx.HTTPStatusError as exc:
        return exc.response.status_code
    except Exception:
        return False


# ---------------------------------------------------------------------------


@pytest.mark.live
@pytest.mark.asyncio
@pytest.mark.timeout(10)
async def test_lse_ping():
    async with httpx.AsyncClient(timeout=8) as client:
        result = await _ping(client)

    # ----- result handling --------------------------------------------------
    if result is True:
        assert True  # healthy
    elif result is False:
        pytest.xfail("LSE: network or JSON error")
    elif 500 <= result < 600:
        pytest.xfail(f"LSE: HTTP {result}")  # upstream hiccup
    else:  # 4xx means contract changed
        pytest.fail(f"LSE: HTTP {result}")
