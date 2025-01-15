# _utils/_merge.py


from decimal import Decimal
from functools import cache
from itertools import chain
from statistics import median
from typing import List, Sequence
from collections import Counter

from rapidfuzz import fuzz

from equity_aggregator.schemas.raw import RawEquity


def merge(group: Sequence[RawEquity]) -> RawEquity:
    """
    Merge group of duplicate `RawEquity` records into a single record by
    computing representative values per field.

    Currently handled fields:
      - name: most common
      - symbol: most common
      - isin/cik/share_class_figi: most-common non-null (tie: first seen)
      - mics: union of all non-null lists (order-preserving, duplicates removed)
      - currency: most-common non-null (tie: first seen)
      - last_price: median
    """
    return RawEquity(
        **{
            "name": _merge_name(group),
            "symbol": _merge_symbol(group),
            "isin": _merge_id(group, "isin"),
            "cik": _merge_id(group, "cik"),
            "share_class_figi": _merge_id(group, "share_class_figi"),
            "mics": _merge_mics(group),
            "currency": _merge_currency(group),
            "last_price": _merge_last_price(group),
        }
    )


def _merge_name(duplicate_group: Sequence[RawEquity], *, threshold: int = 90) -> str:
    """
    Pick a representative equity name.
    - cluster: group near-identical forms
    - select: choose cluster with the highest total count and return
              the earliest original spelling in that cluster
    """

    names = [equity.name for equity in duplicate_group]

    # cluster names by fuzzy similarity
    clusters = _cluster(names, threshold=threshold)

    # weight clusters and keep earliest spelling
    weight = Counter(names)  # how many times each form occurs

    def _cluster_weight(cluster: list[str]) -> int:
        return sum(weight[token] for token in cluster)

    # choose cluster with the most occurrences (i.e. highest weight)
    best_cluster = max(clusters, key=_cluster_weight)

    for equity in duplicate_group:
        if equity.name in best_cluster:  # first in order wins
            return equity.name

    # should never fall through; if it does, return the first name
    return duplicate_group[0].name


def _merge_symbol(duplicate_group: Sequence[RawEquity]) -> str:
    """
    Choose the mode of symbols. If a tie occurs, the first occurrence wins.
    """
    symbols = [equity.symbol for equity in duplicate_group]
    return Counter(symbols).most_common(1)[0][0]


def _merge_last_price(duplicate_group: Sequence[RawEquity]) -> Decimal | None:
    """
    Return the median of available `last_price` values; ignore nulls.
    """
    last_prices: list[Decimal] = [
        equity.last_price for equity in duplicate_group if equity.last_price is not None
    ]
    return median(last_prices) if last_prices else None


def _merge_id(duplicate_group: Sequence[RawEquity], field: str) -> str | None:
    """
    Return the most frequent non-null value for the given field
    ("isin" or "cik" or "share_class_figi").

    If there is a tie, the earliest occurrence in duplicate group wins.

    If all values are null: None.
    """

    # get the values for the given field
    values = [
        getattr(equity, field)
        for equity in duplicate_group
        if getattr(equity, field) is not None
    ]

    if not values:
        return None

    counts = Counter(values)

    # max(counts.values()) guaranteed ≥ 1
    best_freq = max(counts.values())

    # keep insertion order in case of tie
    for equity in duplicate_group:
        value = getattr(equity, field)
        if value is not None and counts[value] == best_freq:
            return value

    # defensive fallback – shouldn’t happen
    return values[0]


def _merge_mics(duplicate_group: Sequence[RawEquity]) -> List[str]:
    """
    Combine all non-null MIC lists from the duplicate group,
    preserving first-seen order and dropping duplicates.
    Returns None if no MICs are found.
    """

    all_mics = chain.from_iterable(equity.mics or [] for equity in duplicate_group)

    unique_mics = list(dict.fromkeys(all_mics))

    return unique_mics or None


def _merge_currency(duplicate_group: Sequence[RawEquity]) -> str | None:
    """
    Pick the most frequent non-null currency code (ISO-4217).

    Tie: first seen. All null: None.
    """

    # get the currency codes from the duplicate group
    currency_codes = [
        equity.currency for equity in duplicate_group if equity.currency is not None
    ]

    if not currency_codes:
        return None

    freq = Counter(currency_codes)
    best_freq = max(freq.values())

    for equity in duplicate_group:  # keep duplicate group order for tie‑break
        currency = equity.currency
        if currency is not None and freq[currency] == best_freq:
            return currency

    return currency_codes[0]  # defensive fallback


@cache
def _token_ratio(a: str, b: str) -> int:
    """
    token-set ratio.
    """
    return fuzz.token_set_ratio(a, b)


def _cluster(names: List[str], threshold: int = 90) -> List[List[str]]:
    """
    Single-link clustering: names with token-set ratio ≥ threshold
    join the same cluster.
    """
    clusters: List[List[str]] = []

    for name in names:
        # find the first cluster whose representative (first item)
        # is similar enough to this name
        target: List[str] = next(
            (
                cluster
                for cluster in clusters
                if _token_ratio(name, cluster[0]) >= threshold
            ),
            None,
        )

        if target:
            target.append(name)
        else:
            clusters.append([name])

    return clusters
