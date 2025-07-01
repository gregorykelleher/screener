# _utils/_client.py

from httpx import AsyncClient, AsyncHTTPTransport, Limits, Timeout


def make_client(**overrides: object) -> AsyncClient:
    """
    Create a client using default settings and optional overrides.

    Args:
        **overrides: Arbitrary keyword arguments to override default AsyncClient
            parameters such as base_url, headers, timeout, etc.

    Returns:
        httpx.AsyncClient: A configured AsyncClient instance.

    Example:
        client = make_client(
            base_url="https://api.xetra.de",
            headers={"X-API-Key": "..."},
        )
        async with client as session:
            response = await session.get("/path")
    """

    # Set default limits for connections and keepalive
    limits = Limits(
        max_connections=100,
        max_keepalive_connections=0,
        keepalive_expiry=1.5,
    )

    # Set default timeouts for connections, reading, and writing
    timeout = Timeout(
        connect=3.0,  # 3s to establish TLS
        read=None,  # no read timeout
        write=5.0,  # up to 5s to send a body
        pool=None,  # no pool timeout
    )

    # Use HTTP/2 transport with retries enabled
    transport = AsyncHTTPTransport(
        http2=True,
        retries=1,
        limits=limits,
    )

    # Set default generic headers
    headers = {
        # accept anything
        "Accept": "*/*",
        # accept compressed responses
        "Accept-Encoding": "gzip",
        # generic language hint
        "Accept-Language": "en-US,en;q=0.9",
        # user agent to identify the client
        "User-Agent": "Mozilla/5.0",
    }

    # Combine base parameters with any overrides provided
    base_params: dict[str, object] = {
        "http2": True,
        "transport": transport,
        "timeout": timeout,
        "headers": headers,
        **overrides,
    }

    return AsyncClient(**base_params)
