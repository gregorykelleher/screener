import os
import logging
import typing
import requests
import csv
import json
import io
from .settings import BASE_URL_STABLE, CONNECT_TIMEOUT, READ_TIMEOUT

# Type alias for the return type
DataResponse = typing.Optional[typing.List[typing.Any]]


def _request(
    url: str,
    params: typing.Dict[str, typing.Any],
) -> typing.Optional[requests.Response]:
    """
    Perform an HTTP GET request with the given URL and parameters.
    Return a requests.Response if successful, or None if any exception occurs.
    """
    try:
        # Perform the request with a timeout for both the connection and read
        response = requests.get(
            url, params=params, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
        )

        # If the response is a 400 with an empty list, return it as-is
        if response.status_code == 400 and response.text.strip() == "[]":
            return response

        # Otherwise, raise_for_status() as normal
        response.raise_for_status()
        return response

    except requests.ConnectionError as exc:
        logging.error("Network connection issue: %s", exc)
    except requests.TooManyRedirects as exc:
        logging.error("Too many redirects: %s", exc)
    except requests.Timeout as exc:
        logging.error("Request timed out: %s", exc)
    except requests.HTTPError as exc:
        logging.error("Request to FMP failed: %s", exc)
    except requests.RequestException as exc:
        logging.error("Request to FMP failed: %s", exc)

    return None


def _parse_response(response: requests.Response) -> DataResponse:
    """
    Parse the response content. If empty, return None.
    If it's CSV, parse with csv.DictReader. Otherwise, attempt JSON parsing.
    """
    raw_text = response.text.strip()
    if not raw_text:
        logging.warning("Response was empty.")
        return None

    content_type = response.headers.get("Content-Type", "").lower()
    content_disposition = response.headers.get("Content-Disposition", "").lower()

    # Check if CSV via headers
    if "csv" in content_type or (".csv" in content_disposition):
        logging.info("Response appears to be CSV; parsing accordingly.")
        reader = csv.DictReader(io.StringIO(raw_text))
        return list(reader)

    # Otherwise, assume JSON
    try:
        return response.json()
    except json.JSONDecodeError as exc:
        logging.error("Could not parse JSON: %s", exc)
        return None


def _query_api(
    path: str, query_vars: typing.Optional[typing.Dict[str, typing.Any]] = None
) -> DataResponse:
    """
    Fetch data from the FMP API (CSV or JSON) using the given path and query parameters.
    Automatically appends the FMP_API_KEY, returning a list of rows or JSON objects.
    """
    query_vars = query_vars or {}

    # Retrieve the API key
    api_key = os.getenv("FMP_API_KEY")
    if not api_key:
        logging.error("FMP_API_KEY not found in environment.")
        return None

    # Append the API key to the query parameters
    query_vars["apikey"] = api_key

    # Construct the full URL
    url = f"{BASE_URL_STABLE}{path}"

    # Fetch the response
    response = _request(url, params=query_vars)
    if not response:
        return None

    # Parse and return the data
    return _parse_response(response)
