# utils/fuzzy.py

from rapidfuzz import fuzz, utils


def pick_best_symbol(
    quotes: list[dict],
    *,
    name_key: str,
    expected_name: str,
    expected_symbol: str,
    min_score: int = 0,
) -> str | None:
    """
    Select the best-matching symbol from a list of Yahoo Finance quotes using
    fuzzy matching.

    For each quote, this function computes a combined fuzzy score based on the
    similarity between the quote's symbol and the expected symbol, and between the
    quote's name (using `name_key`) and the expected name. Quote with the highest
    combined score is selected if its score meets or exceeds `min_score`. If no
    quote meets the threshold, None is returned.

    Args:
        quotes (list[dict]): List of quote dictionaries, each with at least a
            "symbol" key and a name field specified by `name_key`.
        name_key (str): The key in each quote dict for equity name
            (e.g., "longname").
        expected_name (str): The expected equity name to match against.
        expected_symbol (str): The expected ticker symbol to match against.
        min_score (int, optional): Minimum combined fuzzy score required to accept a
            match. Defaults to 0.

    Returns:
        str | None: Best-matching symbol if a suitable match is found, else None.
    """

    if not quotes:
        return None

    # compute fuzzy scores for each quote
    scored = [
        _score_quote(
            quote,
            name_key=name_key,
            expected_symbol=expected_symbol,
            expected_name=expected_name,
        )
        for quote in quotes
    ]

    # compute the best score and symbol from the scored list
    best_score, best_symbol, best_name = max(scored, key=lambda t: t[0])

    # if the best score is below the minimum threshold, return None
    if best_score < min_score:
        return None

    # otherwise, return the best symbol found
    return best_symbol


def _score_quote(
    quote: dict,
    *,
    name_key: str,
    expected_symbol: str,
    expected_name: str,
) -> tuple[int, str, str]:
    """
    Compute a combined fuzzy score for a Yahoo Finance quote.

    This function calculates the sum of the fuzzy string similarity between the
    quote's symbol and the expected symbol, and between the quote's name (using
    `name_key`) and the expected name. The result is a tuple containing the total
    score, the actual symbol, and the actual name.

    Args:
        quote (dict): The quote dictionary containing at least a "symbol" key and
            a name field specified by `name_key`.
        name_key (str): The key in the quote dict for the equity name.
        expected_symbol (str): The expected ticker symbol to match against.
        expected_name (str): The expected equity name to match against.

    Returns:
        tuple[int, str, str]: A tuple of (total_score, actual_symbol, actual_name),
            where total_score is the sum of the symbol and name fuzzy scores.
    """
    actual_symbol = quote["symbol"]
    actual_name = quote.get(name_key, "<no-name>")

    symbol_score = fuzz.ratio(
        actual_symbol,
        expected_symbol,
        processor=utils.default_process,
    )
    name_score = fuzz.WRatio(
        actual_name,
        expected_name,
        processor=utils.default_process,
    )

    total_score = symbol_score + name_score
    return total_score, actual_symbol, actual_name
