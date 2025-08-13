# _utils/_merge.py


from collections import Counter
from collections.abc import Sequence
from decimal import Decimal
from functools import cache
from itertools import chain
from statistics import median

from rapidfuzz import fuzz

from equity_aggregator.schemas.raw import RawEquity


def merge(group: Sequence[RawEquity]) -> RawEquity:
    """
    Merges a group of duplicate RawEquity records into a single representative record.

    For each field, a representative value is computed as follows:
      - name: Clustered by fuzzy similarity; selecting most frequent, earliest spelling.
      - symbol: Most frequent symbol; ties broken by first occurrence.
      - isin, cusip, cik, share_class_figi: Most frequent value; ties broken by order.
      - mics: Union of all non-null lists, order-preserving and duplicates removed.
      - currency: Most frequent non-null value; ties broken by first occurrence.
      - last_price: Median of all non-null values.
      - market_cap: Median of all non-null values.

    Note:
        This function requires that the input group is non-empty and that all RawEquity
        objects in the group share the same identical share_class_figi. If these
        conditions are not met, a ValueError will be raised. This is an enforced
        constraint to prevent merging heterogeneous equity records.

    Args:
        group (Sequence[RawEquity]): A sequence of RawEquity objects considered
            duplicates to be merged.

    Returns:
        RawEquity: A new RawEquity instance with merged field values.
    """

    # validate share_class_figi consistency first
    share_class_figi_value = _validate_share_class_figi(group)

    return RawEquity(
        name=_merge_name(group),
        symbol=_merge_symbol(group),
        isin=_merge_id(group, "isin"),
        cusip=_merge_id(group, "cusip"),
        cik=_merge_id(group, "cik"),
        share_class_figi=share_class_figi_value,
        mics=_merge_mics(group),
        currency=_merge_currency(group),
        last_price=_merge_decimal_field(group, "last_price"),
        market_cap=_merge_decimal_field(group, "market_cap"),
        fifty_two_week_min=_merge_decimal_field(group, "fifty_two_week_min"),
        fifty_two_week_max=_merge_decimal_field(group, "fifty_two_week_max"),
        dividend_yield=_merge_decimal_field(group, "dividend_yield"),
        market_volume=_merge_decimal_field(group, "market_volume"),
        held_insiders=_merge_decimal_field(group, "held_insiders"),
        held_institutions=_merge_decimal_field(group, "held_institutions"),
        short_interest=_merge_decimal_field(group, "short_interest"),
        share_float=_merge_decimal_field(group, "share_float"),
        shares_outstanding=_merge_decimal_field(group, "shares_outstanding"),
        revenue_per_share=_merge_decimal_field(group, "revenue_per_share"),
        profit_margin=_merge_decimal_field(group, "profit_margin"),
        gross_margin=_merge_decimal_field(group, "gross_margin"),
        operating_margin=_merge_decimal_field(group, "operating_margin"),
        free_cash_flow=_merge_decimal_field(group, "free_cash_flow"),
        operating_cash_flow=_merge_decimal_field(group, "operating_cash_flow"),
        return_on_equity=_merge_decimal_field(group, "return_on_equity"),
        return_on_assets=_merge_decimal_field(group, "return_on_assets"),
        performance_1_year=_merge_decimal_field(group, "performance_1_year"),
        total_debt=_merge_decimal_field(group, "total_debt"),
        revenue=_merge_decimal_field(group, "revenue"),
        ebitda=_merge_decimal_field(group, "ebitda"),
        trailing_pe=_merge_decimal_field(group, "trailing_pe"),
        price_to_book=_merge_decimal_field(group, "price_to_book"),
        trailing_eps=_merge_decimal_field(group, "trailing_eps"),
        analyst_rating=_merge_analyst_rating(group),
        industry=_merge_industry(group),
        sector=_merge_sector(group),
    )


def _validate_share_class_figi(group: Sequence[RawEquity]) -> str:
    """
    Validates that all RawEquity objects in the group share the same
    share_class_figi value.

    Args:
        group (Sequence[RawEquity]): A non-empty sequence of RawEquity objects to
            validate.

    Raises:
        ValueError: If the group is empty or contains multiple distinct
            share_class_figi values.

    Returns:
        str: The single shared share_class_figi value present in the group.
    """
    if not group:
        raise ValueError("Cannot merge an empty group of equities")

    figis = {raw_equity.share_class_figi for raw_equity in group}
    if len(figis) != 1:
        raise ValueError(
            "All raw equities in the group must have identical share_class_figi values "
            f"(found: {sorted(figis)})",
        )
    return figis.pop()


def _merge_name(duplicate_group: Sequence[RawEquity], *, threshold: int = 90) -> str:
    """
    Selects a representative equity name from a group of near-duplicate equities.

    This function clusters similar equity names using fuzzy matching, then selects the
    cluster with the highest total occurrence count. Within the chosen cluster, it
    returns the earliest original spelling found in the input sequence.

    Args:
        duplicate_group (Sequence[RawEquity]): A sequence of RawEquity objects
            considered near-duplicates, each with a 'name' attribute.
        threshold (int, optional): Similarity threshold (0-100) for clustering names.
            Defaults to 90.

    Returns:
        str: The selected representative equity name from the group.
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

    return next(name for name in names if name in best_cluster)


def _merge_symbol(duplicate_group: Sequence[RawEquity]) -> str:
    """
    Selects the most frequently occurring symbol from a group of RawEquity objects.

    If multiple symbols share the highest frequency (a tie), the symbol that appears
    first in the group is returned.

    Args:
        duplicate_group (Sequence[RawEquity]): A sequence of RawEquity objects that are
            considered duplicates and need to be merged based on their symbol.

    Returns:
        str: The symbol that is the mode of the group, with ties broken by first
            occurrence.
    """
    symbols = [equity.symbol for equity in duplicate_group]
    return Counter(symbols).most_common(1)[0][0]


def _merge_decimal_field(
    duplicate_group: Sequence[RawEquity],
    field: str,
) -> Decimal | None:
    """
    Calculates the median value for a specified Decimal field from a group of
    RawEquity objects, ignoring any entries where the field value is None.

    Args:
        duplicate_group (Sequence[RawEquity]): A sequence of RawEquity instances,
            each potentially containing the specified field.
        field (str): The name of the Decimal field to merge.

    Returns:
        Decimal | None: The median of the non-null field values as a Decimal,
            or None if no valid values are present.
    """
    values: list[Decimal] = [
        getattr(equity, field)
        for equity in duplicate_group
        if getattr(equity, field) is not None
    ]
    return median(values) if values else None


def _merge_id(duplicate_group: Sequence[RawEquity], field: str) -> str | None:
    """
    Selects the most frequent non-null value for a specified identifier field
    ("isin", "cusip", "cik" or "share_class_figi") from a group of RawEquity objects.

    In case of a tie, returns the earliest occurrence in the original group order.
    Returns None if all values are null.

    Args:
        duplicate_group (Sequence[RawEquity]): A sequence of RawEquity objects to merge.
        field (str): The name of the identifier field to merge ("isin", "cusip", "cik"
        or "share_class_figi").

    Returns:
        str | None: Most frequent non-null identifier value, or None if all are null.
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

    # max(counts.values()) guaranteed ≥ 1
    best_freq = max(counts.values())

    # earliest among the non-null values with best frequency
    return next(value for value in values if counts[value] == best_freq)


def _merge_mics(duplicate_group: Sequence[RawEquity]) -> list[str] | None:
    """
    Merges all non-null MIC lists from a group of RawEquity objects, preserving the
    order of first occurrence and removing duplicates.

    Args:
        duplicate_group (Sequence[RawEquity]): A sequence of RawEquity objects, each
            possibly containing a list of MICs.

    Returns:
        list[str] | None: A list of unique MICs in order of first appearance, or None
            if no MICs are found.
    """
    # combine all (possibly None) MIC lists
    combined_mics = chain.from_iterable(e.mics or [] for e in duplicate_group)

    # Keep only truthy, non-blank strings
    cleaned = (mic for mic in combined_mics if mic and str(mic).strip())

    # Order-preserving de-duplication
    unique_mics = list(dict.fromkeys(cleaned))

    return unique_mics or None


def _merge_currency(duplicate_group: Sequence[RawEquity]) -> str | None:
    """
    Selects the most frequent non-null currency code (ISO-4217) from a group of
    duplicate RawEquity objects. In case of a tie, returns the first encountered
    currency code in the original group order. If all currency codes are null,
    returns None.

    Args:
        duplicate_group (Sequence[RawEquity]): A sequence of RawEquity objects, each
            possibly containing a currency attribute.

    Returns:
        str | None: The most frequent non-null currency code, or None if all are null.
    """
    # get the currency codes from the duplicate group
    currency_codes = [
        equity.currency for equity in duplicate_group if equity.currency is not None
    ]

    if not currency_codes:
        return None

    freq = Counter(currency_codes)
    best_freq = max(freq.values())

    # earliest among the non-null currencies with best frequency
    return next(currency for currency in currency_codes if freq[currency] == best_freq)


def _merge_analyst_rating(duplicate_group: Sequence[RawEquity]) -> str | None:
    """
    Selects the most frequent non-null analyst rating ("BUY", "SELL", or "HOLD")
    from a group of RawEquity objects. If there is a tie, the rating that appears
    first in the input sequence is returned. Returns None if all ratings are missing.

    Args:
        duplicate_group (Sequence[RawEquity]): A sequence of RawEquity objects,
            each potentially containing an analyst_rating attribute.

    Returns:
        str | None: The most frequent non-null analyst rating, or None if all are
            missing.
    """
    ratings = [
        equity.analyst_rating
        for equity in duplicate_group
        if equity.analyst_rating is not None
    ]

    if not ratings:
        return None

    freq = Counter(ratings)
    best = max(freq.values())

    # earliest among the non-null ratings with best frequency
    return next(rating for rating in ratings if freq[rating] == best)


def _merge_industry(
    duplicate_group: Sequence[RawEquity],
    *,
    threshold: int = 90,
) -> str | None:
    """
    Selects a representative industry from a group of RawEquity objects.

    - Ignores blank or missing industry values.
    - Clusters similar industry names with single-link fuzzy matching (token-set ratio).
    - The cluster with the highest total frequency is chosen (majority rule).
    - Within the winning cluster, the earliest spelling in the original sequence is
        returned, preserving original capitalisation.

    Args:
        duplicate_group (Sequence[RawEquity]): Sequence of RawEquity objects, each
            possibly containing an industry attribute.
        threshold (int, optional): Similarity threshold (0-100) for clustering
            industry names. Defaults to 90.

    Returns:
        str | None: The selected representative industry string, or None if all values
            are missing or blank.
    """
    # skip if every record is null or blank
    industries = [
        equity.industry for equity in duplicate_group if equity.industry is not None
    ]

    if not industries:
        return None

    # cluster names by fuzzy similarity
    clusters = _cluster(industries, threshold=threshold)

    # weight clusters and keep earliest spelling
    weight = Counter(industries)

    def _cluster_weight(cluster: list[str]) -> int:
        return sum(weight[token] for token in cluster)

    # choose cluster with the most occurrences (i.e. highest weight)
    best_cluster = max(clusters, key=_cluster_weight)

    # earliest spelling in original order among non-nulls
    return next(industry for industry in industries if industry in best_cluster)


def _merge_sector(
    duplicate_group: Sequence[RawEquity],
    *,
    threshold: int = 90,
) -> str | None:
    """
    Selects a representative sector from a group of RawEquity objects.

    This function clusters similar sector names using fuzzy matching (token-set ratio,
    single-link) and a configurable threshold. The cluster with the highest total
    frequency is chosen. Within the winning cluster, the earliest spelling in the
    original sequence is returned, preserving original capitalisation.

    Args:
        duplicate_group (Sequence[RawEquity]): Sequence of RawEquity records, each
            possibly containing a sector attribute.
        threshold (int, optional): Similarity threshold (0-100) for clustering sector
            names. Defaults to 90.

    Returns:
        str | None: The selected representative sector string, or None if all values
            are missing or blank.
    """
    # skip if every record is null or blank
    sectors = [equity.sector for equity in duplicate_group if equity.sector is not None]

    if not sectors:
        return None

    # cluster names by fuzzy similarity
    clusters = _cluster(sectors, threshold=threshold)

    # weight clusters and keep earliest spelling
    weights = Counter(sectors)

    def _cluster_weight(cluster: list[str]) -> int:
        return sum(weights[token] for token in cluster)

    # choose cluster with the most occurrences (i.e. highest weight)
    best_cluster = max(clusters, key=_cluster_weight)

    # earliest spelling in original order among non-nulls
    return next(sector for sector in sectors if sector in best_cluster)


@cache
def _token_ratio(a: str, b: str) -> int:
    """
    Compute the token-set ratio between two strings using fuzzy matching.

    Args:
        a (str): The first string to compare.
        b (str): The second string to compare.

    Returns:
        int: The token-set similarity ratio (0-100) between the two strings.
    """
    return fuzz.token_set_ratio(a, b)


def _cluster(names: list[str], threshold: int = 90) -> list[list[str]]:
    """
    Groups similar strings into clusters using single-link clustering based on token-set
    ratio.

    Each name is compared to the representative (first item) of each existing cluster.
    If the token-set ratio between the name and a cluster's representative is greater
    than or equal to the specified threshold, the name is added to that cluster.

    Otherwise, a new cluster is created for the name.

    Args:
        names (list[str]): List of strings to be clustered.
        threshold (int, optional): Minimum token-set ratio (0-100) required to join an
            existing cluster. Defaults to 90.

    Returns:
        list[list[str]]: A list of clusters, where each cluster is a list of similar
            strings.
    """
    clusters: list[list[str]] = []

    for name in names:
        # find the first cluster whose representative (first item)
        # is similar enough to this name
        target: list[str] = next(
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
