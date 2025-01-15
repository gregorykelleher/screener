from .response_retrieval import _query_api, DataResponse


def bulk_profiles(part: str) -> DataResponse:
    """
    Retrieve a bulk profile for the specified part.

    Endpoint: https://site.financialmodelingprep.com/developer/docs/bulk-profiles
    """
    return _query_api("profile-bulk", query_vars={"part": part})


def biggest_gainers() -> DataResponse:
    """
    Retrieve the biggest gainers.

    Endpoint: https://site.financialmodelingprep.com/developer/docs/stable/biggest-gainers
    """
    return _query_api("biggest-gainers")
