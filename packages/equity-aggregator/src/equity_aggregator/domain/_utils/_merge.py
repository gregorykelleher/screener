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
        last_price=_merge_last_price(group),
        market_cap=_merge_market_cap(group),
        fifty_two_week_min=_merge_fifty_two_week_min(group),
        fifty_two_week_max=_merge_fifty_two_week_max(group),
        dividend_yield=_merge_dividend_yield(group),
        market_volume=_merge_market_volume(group),
        held_insiders=_merge_insiders(group),
        held_institutions=_merge_institutions(group),
        short_interest=_merge_short_interest(group),
        share_float=_merge_share_float(group),
        shares_outstanding=_merge_shares_outstanding(group),
        revenue_per_share=_merge_revenue_per_share(group),
        profit_margin=_merge_profit_margin(group),
        gross_margin=_merge_gross_margin(group),
        operating_margin=_merge_operating_margin(group),
        free_cash_flow=_merge_free_cash_flow(group),
        operating_cash_flow=_merge_operating_cash_flow(group),
        return_on_equity=_merge_return_on_equity(group),
        return_on_assets=_merge_return_on_assets(group),
        performance_1_year=_merge_performance_1_year(group),
        total_debt=_merge_total_debt(group),
        revenue=_merge_revenue(group),
        ebitda=_merge_ebitda(group),
        trailing_pe=_merge_trailing_pe(group),
        price_to_book=_merge_price_to_book(group),
        trailing_eps=_merge_trailing_eps(group),
        analyst_rating=_merge_analyst_rating(group),
        industry=_merge_industry(group),
        sector=_merge_sector(group),
        # TODO: add sector
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

    for equity in duplicate_group:
        if equity.name in best_cluster:  # first in order wins
            return equity.name

    # Unreachable by construction; defensive only
    return duplicate_group[0].name  # pragma: no cover


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


def _merge_last_price(duplicate_group: Sequence[RawEquity]) -> Decimal | None:
    """
    Calculates the median last traded price from a group of RawEquity objects,
    ignoring any entries with a null last_price value.

    Args:
        duplicate_group (Sequence[RawEquity]): A sequence of RawEquity instances,
            each potentially containing a last_price value.

    Returns:
        Decimal | None: The median of the non-null last_price values as a Decimal,
            or None if no valid last_price values are present.
    """
    last_prices: list[Decimal] = [
        equity.last_price for equity in duplicate_group if equity.last_price is not None
    ]
    return median(last_prices) if last_prices else None


def _merge_market_cap(duplicate_group: Sequence[RawEquity]) -> Decimal | None:
    """
    Calculates the median market capitalisation from a group of RawEquity objects,
    ignoring any entries with a null market_cap value.

    Args:
        duplicate_group (Sequence[RawEquity]): A sequence of RawEquity instances,
            each potentially containing a market_cap value.

    Returns:
        Decimal | None: The median of the non-null market_cap values as a Decimal,
            or None if no valid market_cap values are present.
    """
    market_caps: list[Decimal] = [
        equity.market_cap for equity in duplicate_group if equity.market_cap is not None
    ]
    return median(market_caps) if market_caps else None


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

    # max(counts.values()) guaranteed ≥ 1
    best_freq = max(counts.values())

    # keep insertion order in case of tie
    for equity in duplicate_group:
        value = getattr(equity, field)
        if value is not None and counts[value] == best_freq:
            return value

    # Unreachable by construction; defensive only
    return values[0]  # pragma: no cover


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

    for equity in duplicate_group:  # keep duplicate group order for tie‑break
        currency = equity.currency
        if currency is not None and freq[currency] == best_freq:
            return currency

    # Unreachable by construction; defensive only
    return currency_codes[0]  # pragma: no cover


def _merge_fifty_two_week_min(
    duplicate_group: Sequence[RawEquity],
) -> Decimal | None:
    """
    Computes the median 52-week minimum price from a group of RawEquity objects,
    ignoring any entries with a null fifty_two_week_min value.

    Args:
        duplicate_group (Sequence[RawEquity]): A sequence of RawEquity instances,
            each potentially containing a fifty_two_week_min value.

    Returns:
        Decimal | None: The median of the non-null fifty_two_week_min values as a
            Decimal, or None if no valid values are present.
    """
    fifty_two_week_mins: list[Decimal] = [
        equity.fifty_two_week_min
        for equity in duplicate_group
        if equity.fifty_two_week_min is not None
    ]
    return median(fifty_two_week_mins) if fifty_two_week_mins else None


def _merge_fifty_two_week_max(
    duplicate_group: Sequence[RawEquity],
) -> Decimal | None:
    """
    Computes the median 52-week maximum price from a group of RawEquity objects,
    ignoring any entries with a null fifty_two_week_max value.

    Args:
        duplicate_group (Sequence[RawEquity]): A sequence of RawEquity instances,
            each potentially containing a fifty_two_week_max value.

    Returns:
        Decimal | None: The median of the non-null fifty_two_week_max values,
        or None if no valid values are present.
    """
    fifty_two_week_maxs: list[Decimal] = [
        equity.fifty_two_week_max
        for equity in duplicate_group
        if equity.fifty_two_week_max is not None
    ]
    return median(fifty_two_week_maxs) if fifty_two_week_maxs else None


def _merge_dividend_yield(duplicate_group: Sequence[RawEquity]) -> Decimal | None:
    """
    Calculates the median dividend yield from a group of RawEquity objects,
    ignoring any entries with a null dividend_yield value.

    Args:
        duplicate_group (Sequence[RawEquity]): A sequence of RawEquity instances,
            each potentially containing a dividend_yield value.

    Returns:
        Decimal | None: The median of the non-null dividend_yield values as a
            Decimal, or None if no valid values are present.
    """
    dividend_yields: list[Decimal] = [
        equity.dividend_yield
        for equity in duplicate_group
        if equity.dividend_yield is not None
    ]
    return median(dividend_yields) if dividend_yields else None


def _merge_market_volume(duplicate_group: Sequence[RawEquity]) -> Decimal | None:
    """
    Calculates the median market trading volume from a group of RawEquity objects,
    ignoring any entries with a null market_volume value.

    Args:
        duplicate_group (Sequence[RawEquity]): A sequence of RawEquity instances,
            each potentially containing a market_volume value.

    Returns:
        Decimal | None: The median of the non-null market_volume values as a
            Decimal, or None if no valid values are present.
    """
    volumes: list[Decimal] = [
        equity.market_volume
        for equity in duplicate_group
        if equity.market_volume is not None
    ]
    return median(volumes) if volumes else None


def _merge_insiders(duplicate_group: Sequence[RawEquity]) -> Decimal | None:
    """
    Calculates the median percentage of shares held by insiders from a group of
    RawEquity objects, ignoring any entries where held_insiders is None.

    Args:
        duplicate_group (Sequence[RawEquity]): A sequence of RawEquity instances,
            each potentially containing held_insiders.

    Returns:
        Decimal | None: The median of the non-null held_insiders values as a Decimal,
            or None if no valid values are present.
    """
    insiders: list[Decimal] = [
        equity.held_insiders
        for equity in duplicate_group
        if equity.held_insiders is not None
    ]
    return median(insiders) if insiders else None


def _merge_institutions(duplicate_group: Sequence[RawEquity]) -> Decimal | None:
    """
    Calculates the median percentage of shares held by institutions from a group of
    RawEquity objects, ignoring any entries where held_institutions is None.

    Args:
        duplicate_group (Sequence[RawEquity]): A sequence of RawEquity instances,
            each potentially containing held_institutions.

    Returns:
        Decimal | None: The median of the non-null held_institutions values as a Decimal,
            or None if no valid values are present.
    """
    institutions: list[Decimal] = [
        equity.held_institutions
        for equity in duplicate_group
        if equity.held_institutions is not None
    ]
    return median(institutions) if institutions else None


def _merge_short_interest(duplicate_group: Sequence[RawEquity]) -> Decimal | None:
    """
    Calculates the median short interest percentage from a group of RawEquity objects,
    ignoring any entries where short_interest is None.

    Args:
        duplicate_group (Sequence[RawEquity]): A sequence of RawEquity instances,
            each potentially containing short_interest.

    Returns:
        Decimal | None: The median of the non-null short_interest values as a Decimal,
            or None if no valid values are present.
    """
    shorts: list[Decimal] = [
        equity.short_interest
        for equity in duplicate_group
        if equity.short_interest is not None
    ]
    return median(shorts) if shorts else None


def _merge_share_float(duplicate_group: Sequence[RawEquity]) -> Decimal | None:
    """
    Calculates the median share float from a group of RawEquity objects,
    ignoring any entries where share_float is None.

    Args:
        duplicate_group (Sequence[RawEquity]): A sequence of RawEquity instances,
            each potentially containing share_float.

    Returns:
        Decimal | None: The median of the non-null share_float values as a Decimal,
            or None if no valid values are present.
    """
    floats: list[Decimal] = [
        equity.share_float
        for equity in duplicate_group
        if equity.share_float is not None
    ]
    return median(floats) if floats else None


def _merge_shares_outstanding(duplicate_group: Sequence[RawEquity]) -> Decimal | None:
    """
    Calculates the median shares outstanding from a group of RawEquity objects,
    ignoring any entries where shares_outstanding is None.

    Args:
        duplicate_group (Sequence[RawEquity]): A sequence of RawEquity instances,
            each potentially containing shares_outstanding.

    Returns:
        Decimal | None: The median of the non-null shares_outstanding values as a Decimal,
            or None if no valid values are present.
    """
    outstanding: list[Decimal] = [
        equity.shares_outstanding
        for equity in duplicate_group
        if equity.shares_outstanding is not None
    ]
    return median(outstanding) if outstanding else None


def _merge_revenue_per_share(duplicate_group: Sequence[RawEquity]) -> Decimal | None:
    """
    Calculates the median revenue per share from a group of RawEquity objects,
    ignoring any entries where revenue_per_share is None.

    Args:
        duplicate_group (Sequence[RawEquity]): A sequence of RawEquity instances,
            each potentially containing revenue_per_share.

    Returns:
        Decimal | None: The median of the non-null revenue_per_share values as a Decimal,
            or None if no valid values are present.
    """
    rps: list[Decimal] = [
        equity.revenue_per_share
        for equity in duplicate_group
        if equity.revenue_per_share is not None
    ]
    return median(rps) if rps else None


def _merge_profit_margin(duplicate_group: Sequence[RawEquity]) -> Decimal | None:
    """
    Calculates the median net profit margin from a group of RawEquity objects,
    ignoring any entries where profit_margin is None.

    Args:
        duplicate_group (Sequence[RawEquity]): A sequence of RawEquity instances,
            each potentially containing profit_margin.

    Returns:
        Decimal | None: The median of the non-null profit_margin values as a Decimal,
            or None if no valid values are present.
    """
    margins: list[Decimal] = [
        equity.profit_margin
        for equity in duplicate_group
        if equity.profit_margin is not None
    ]
    return median(margins) if margins else None


def _merge_gross_margin(duplicate_group: Sequence[RawEquity]) -> Decimal | None:
    """
    Calculates the median gross margin from a group of RawEquity objects,
    ignoring any entries where gross_margin is None.

    Args:
        duplicate_group (Sequence[RawEquity]): A sequence of RawEquity instances,
            each potentially containing gross_margin.

    Returns:
        Decimal | None: The median of the non-null gross_margin values as a Decimal,
            or None if no valid values are present.
    """
    gross: list[Decimal] = [
        equity.gross_margin
        for equity in duplicate_group
        if equity.gross_margin is not None
    ]
    return median(gross) if gross else None


def _merge_operating_margin(duplicate_group: Sequence[RawEquity]) -> Decimal | None:
    """
    Calculates the median operating margin from a group of RawEquity objects,
    ignoring any entries where operating_margin is None.

    Args:
        duplicate_group (Sequence[RawEquity]): A sequence of RawEquity instances,
            each potentially containing operating_margin.

    Returns:
        Decimal | None: The median of the non-null operating_margin values as a Decimal,
            or None if no valid values are present.
    """
    opm: list[Decimal] = [
        equity.operating_margin
        for equity in duplicate_group
        if equity.operating_margin is not None
    ]
    return median(opm) if opm else None


def _merge_free_cash_flow(duplicate_group: Sequence[RawEquity]) -> Decimal | None:
    """
    Calculates the median free cash flow from a group of RawEquity objects,
    ignoring any entries where free_cash_flow is None.

    Args:
        duplicate_group (Sequence[RawEquity]): A sequence of RawEquity instances,
            each potentially containing free_cash_flow.

    Returns:
        Decimal | None: The median of the non-null free_cash_flow values as a Decimal,
            or None if no valid values are present.
    """
    fcf: list[Decimal] = [
        equity.free_cash_flow
        for equity in duplicate_group
        if equity.free_cash_flow is not None
    ]
    return median(fcf) if fcf else None


def _merge_operating_cash_flow(duplicate_group: Sequence[RawEquity]) -> Decimal | None:
    """
    Calculates the median operating cash flow from a group of RawEquity objects,
    ignoring any entries where operating_cash_flow is None.

    Args:
        duplicate_group (Sequence[RawEquity]): A sequence of RawEquity instances,
            each potentially containing operating_cash_flow.

    Returns:
        Decimal | None: The median of the non-null operating_cash_flow values as a Decimal,
            or None if no valid values are present.
    """
    ocf: list[Decimal] = [
        equity.operating_cash_flow
        for equity in duplicate_group
        if equity.operating_cash_flow is not None
    ]
    return median(ocf) if ocf else None


def _merge_return_on_equity(duplicate_group: Sequence[RawEquity]) -> Decimal | None:
    """
    Calculates the median return on equity from a group of RawEquity objects,
    ignoring any entries where return_on_equity is None.

    Args:
        duplicate_group (Sequence[RawEquity]): A sequence of RawEquity instances,
            each potentially containing return_on_equity.

    Returns:
        Decimal | None: The median of the non-null return_on_equity values as a Decimal,
            or None if no valid values are present.
    """
    roe: list[Decimal] = [
        equity.return_on_equity
        for equity in duplicate_group
        if equity.return_on_equity is not None
    ]
    return median(roe) if roe else None


def _merge_return_on_assets(duplicate_group: Sequence[RawEquity]) -> Decimal | None:
    """
    Calculates the median return on assets from a group of RawEquity objects,
    ignoring any entries where return_on_assets is None.

    Args:
        duplicate_group (Sequence[RawEquity]): A sequence of RawEquity instances,
            each potentially containing return_on_assets.

    Returns:
        Decimal | None: The median of the non-null return_on_assets values as a Decimal,
            or None if no valid values are present.
    """
    roa: list[Decimal] = [
        equity.return_on_assets
        for equity in duplicate_group
        if equity.return_on_assets is not None
    ]
    return median(roa) if roa else None


def _merge_performance_1_year(duplicate_group: Sequence[RawEquity]) -> Decimal | None:
    """
    Calculates the median one-year performance from a group of RawEquity objects,
    ignoring any entries where performance_1_year is None.

    Args:
        duplicate_group (Sequence[RawEquity]): A sequence of RawEquity instances,
            each potentially containing performance_1_year.

    Returns:
        Decimal | None: The median of the non-null performance_1_year values as a Decimal,
            or None if no valid values are present.
    """
    perf: list[Decimal] = [
        equity.performance_1_year
        for equity in duplicate_group
        if equity.performance_1_year is not None
    ]
    return median(perf) if perf else None


def _merge_total_debt(duplicate_group: Sequence[RawEquity]) -> Decimal | None:
    """
    Calculates the median total debt from a group of RawEquity objects,
    ignoring any entries where total_debt is None.

    Args:
        duplicate_group (Sequence[RawEquity]): A sequence of RawEquity instances,
            each potentially containing total_debt.

    Returns:
        Decimal | None: The median of the non-null total_debt values as a Decimal,
            or None if no valid values are present.
    """
    debt: list[Decimal] = [
        equity.total_debt for equity in duplicate_group if equity.total_debt is not None
    ]
    return median(debt) if debt else None


def _merge_revenue(duplicate_group: Sequence[RawEquity]) -> Decimal | None:
    """
    Calculates the median revenue from a group of RawEquity objects,
    ignoring any entries where revenue is None.

    Args:
        duplicate_group (Sequence[RawEquity]): A sequence of RawEquity instances,
            each potentially containing revenue.

    Returns:
        Decimal | None: The median of the non-null revenue values as a Decimal,
            or None if no valid values are present.
    """
    rev: list[Decimal] = [
        equity.revenue for equity in duplicate_group if equity.revenue is not None
    ]
    return median(rev) if rev else None


def _merge_ebitda(duplicate_group: Sequence[RawEquity]) -> Decimal | None:
    """
    Calculates the median EBITDA from a group of RawEquity objects,
    ignoring any entries where ebitda is None.

    Args:
        duplicate_group (Sequence[RawEquity]): A sequence of RawEquity instances,
            each potentially containing ebitda.

    Returns:
        Decimal | None: The median of the non-null ebitda values as a Decimal,
            or None if no valid values are present.
    """
    ebitdas: list[Decimal] = [
        equity.ebitda for equity in duplicate_group if equity.ebitda is not None
    ]
    return median(ebitdas) if ebitdas else None


def _merge_trailing_pe(duplicate_group: Sequence[RawEquity]) -> Decimal | None:
    """
    Calculates the median trailing P/E ratio from a group of RawEquity objects,
    ignoring any entries where trailing_pe is None.

    Args:
        duplicate_group (Sequence[RawEquity]): A sequence of RawEquity instances,
            each potentially containing trailing_pe.

    Returns:
        Decimal | None: The median of the non-null trailing_pe values as a Decimal,
            or None if no valid values are present.
    """
    pes: list[Decimal] = [
        equity.trailing_pe
        for equity in duplicate_group
        if equity.trailing_pe is not None
    ]
    return median(pes) if pes else None


def _merge_price_to_book(duplicate_group: Sequence[RawEquity]) -> Decimal | None:
    """
    Calculates the median price-to-book ratio from a group of RawEquity objects,
    ignoring any entries where price_to_book is None.

    Args:
        duplicate_group (Sequence[RawEquity]): A sequence of RawEquity instances,
            each potentially containing price_to_book.

    Returns:
        Decimal | None: The median of the non-null price_to_book values as a Decimal,
            or None if no valid values are present.
    """
    p2b: list[Decimal] = [
        equity.price_to_book
        for equity in duplicate_group
        if equity.price_to_book is not None
    ]
    return median(p2b) if p2b else None


def _merge_trailing_eps(duplicate_group: Sequence[RawEquity]) -> Decimal | None:
    """
    Calculates the median trailing EPS from a group of RawEquity objects,
    ignoring any entries where trailing_eps is None.

    Args:
        duplicate_group (Sequence[RawEquity]): A sequence of RawEquity instances,
            each potentially containing trailing_eps.

    Returns:
        Decimal | None: The median of the non-null trailing_eps values as a Decimal,
            or None if no valid values are present.
    """
    eps: list[Decimal] = [
        equity.trailing_eps
        for equity in duplicate_group
        if equity.trailing_eps is not None
    ]
    return median(eps) if eps else None


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

    for equity in duplicate_group:
        rating = equity.analyst_rating
        if rating is not None and freq[rating] == best:
            return rating

    # Unreachable by construction; defensive only
    return ratings[0]  # pragma: no cover


def _merge_industry(
    duplicate_group: Sequence[RawEquity],
    *,
    threshold: int = 90,
) -> str | None:
    """
    Selects a representative industry from a group of RawEquity objects.

    - Ignores blank or missing industry values.
    - Clusters similar industry names using single-link fuzzy matching (token-set ratio).
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

    for equity in duplicate_group:
        if equity.industry in best_cluster:  # first in order wins
            return equity.industry

    # Unreachable by construction; defensive only
    return industries[0]  # pragma: no cover


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

    for equity in duplicate_group:
        if equity.sector in best_cluster:  # first in order wins
            return equity.sector

    # Unreachable by construction; defensive only
    return sectors[0]  # pragma: no cover


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
