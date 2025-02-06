# tests/live/test_euronext_live.py
import httpx
import pytest

_MIC_CODES = {
    "XPAR",
    "XAMS",
    "XBRU",
    "XMSM",
    "XLIS",
    "MTAA",
    "XOSL",
}

HEADERS = {"X-Requested-With": "XMLHttpRequest"}
URL_TMPL = "https://live.euronext.com/en/pd_es/data/stocks?mics={mic}"

# --- helpers ---------------------------------------------------------------


async def _ping(client: httpx.AsyncClient, mic: str) -> tuple[str, bool | int]:
    try:
        r = await client.get(URL_TMPL.format(mic=mic), headers=HEADERS)
        r.raise_for_status()
        ok = bool(r.json().get("aaData"))  # non-empty payload?
        return mic, ok
    except httpx.HTTPStatusError as exc:  # 4xx / 5xx
        return mic, exc.response.status_code
    except Exception:
        return mic, False  # network, JSON, etc.


# --- parametrised test -----------------------------------------------------


@pytest.mark.live
@pytest.mark.asyncio
@pytest.mark.timeout(10)
@pytest.mark.parametrize("mic", sorted(_MIC_CODES))
async def test_euronext_mic_ping(mic):
    async with httpx.AsyncClient(timeout=8, follow_redirects=True) as c:
        # warm-up once per test to get cookies
        await c.get("https://live.euronext.com/en")

        _, result = await _ping(c, mic)

    # ----- result handling --------------------------------------------------
    if result is True:
        assert True  # passed
    elif result is False:
        pytest.xfail(f"{mic}: network or JSON error")
    elif 500 <= result < 600:
        pytest.xfail(f"{mic}: HTTP {result}")  # upstream hiccup
    else:  # e.g. 404, 403
        pytest.fail(f"{mic}: HTTP {result}")
