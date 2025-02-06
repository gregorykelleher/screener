# tests/live/test_xetra_live.py
import httpx
import pytest

URL = "https://api.boerse-frankfurt.de/v1/search/equity_search"

# minimal, first-page payload (one row is enough to prove the API is up)
_PAYLOAD = {
    "indices": [],
    "regions": [],
    "countries": [],
    "sectors": [],
    "types": [],
    "forms": [],
    "segments": [],
    "markets": [],
    "stockExchanges": ["XETR"],
    "lang": "en",
    "offset": 0,
    "limit": 1,
    "sorting": "TURNOVER",
    "sortOrder": "DESC",
}

_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "User-Agent": "Mozilla/5.0",
    "Content-Type": "application/json; charset=UTF-8",
    "Referer": "https://www.boerse-frankfurt.de/",
    "Origin": "https://www.boerse-frankfurt.de",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

# ---------------------------------------------------------------------------


async def _ping(client: httpx.AsyncClient):
    """
    POST once and classify the outcome:
      * True   → data list non-empty
      * int    → HTTP status code (error)
      * False  → network / JSON error
    """
    try:
        r = await client.post(URL, json=_PAYLOAD, headers=_HEADERS)
        r.raise_for_status()
        data = r.json()
        equities = data.get("data", [])
        return bool(equities)
    except httpx.HTTPStatusError as exc:
        return exc.response.status_code
    except Exception:
        return False


# ---------------------------------------------------------------------------


@pytest.mark.live
@pytest.mark.asyncio
@pytest.mark.timeout(10)
async def test_xetra_ping():
    async with httpx.AsyncClient(timeout=8) as client:
        result = await _ping(client)

    # ----- result handling --------------------------------------------------
    if result is True:
        assert True  # healthy
    elif result is False:
        pytest.xfail("Xetra: network or JSON error")
    elif 500 <= result < 600:
        pytest.xfail(f"Xetra: HTTP {result}")  # upstream hiccup
    else:  # 4xx means contract changed
        pytest.fail(f"Xetra: HTTP {result}")
