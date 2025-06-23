# pipeline/test_merge.py

from decimal import Decimal

import pytest

from equity_aggregator.domain._utils._merge import merge
from equity_aggregator.schemas import RawEquity

pytestmark = pytest.mark.unit


def test_merge_empty_group_raises() -> None:
    """
    ARRANGE: no equities
    ACT:     merge
    ASSERT:  merge([]) raises ValueError
    """
    with pytest.raises(ValueError):
        merge([])


def test_merge_single_equity_round_trips() -> None:
    """
    ARRANGE: one equity, one FIGI
    ACT:     merge
    ASSERT:  same object returned
    """
    raw_equities = [
        RawEquity(
            name="SOLO CORP",
            symbol="S",
            share_class_figi="FIGI00000001",
        ),
    ]

    actual = merge(raw_equities)

    assert actual == raw_equities[0]


def test_merge_all_prices_none_propagates_none() -> None:
    """
    ARRANGE: two duplicates, both last_price None
    ACT:     merge
    ASSERT:  merged.last_price is None
    """
    raw_equities = [
        RawEquity(
            name="NIL",
            symbol="N",
            share_class_figi="FIGI00000001",
            last_price=None,
        ),
        RawEquity(
            name="NIL",
            symbol="N",
            share_class_figi="FIGI00000001",
            last_price=None,
        ),
    ]

    actual = merge(raw_equities)

    assert actual.last_price is None


def test_merge_symbol_tie_first_occurrence_wins() -> None:
    """
    ARRANGE: symbols AAA and ZZZ appear once each
    ACT:     merge
    ASSERT:  chosen symbol is first in duplicate group (AAA)
    """
    raw_equities = [
        RawEquity(
            name="T",
            symbol="AAA",
            share_class_figi="FIGI00000001",
        ),
        RawEquity(
            name="T",
            symbol="ZZZ",
            share_class_figi="FIGI00000001",
        ),
    ]

    actual = merge(raw_equities)

    assert actual.symbol == "AAA"


def test_merge_symbol_mode_with_mixed_case_variants() -> None:
    """
    ARRANGE: same ticker in different capitalisation + one rival ticker.
    ACT:     merge
    ASSERT:  validator normalises to upper-case, so ALL count toward mode.
    """
    raw_equities = [
        RawEquity(
            name="X",
            symbol="mSft",
            share_class_figi="FIGI00000001",
        ),
        RawEquity(
            name="X",
            symbol="MSFT",
            share_class_figi="FIGI00000001",
        ),
        RawEquity(
            name="X",
            symbol="AMZN",
            share_class_figi="FIGI00000001",
        ),
    ]

    actual = merge(raw_equities)

    assert actual.symbol == "MSFT"


def test_merge_cluster_weight_tie_keeps_earliest_name() -> None:
    """
    ARRANGE: two distinct names that don't fuzzy-match
    ACT:     merge
    ASSERT:  first name retained
    """
    raw_equities = [
        RawEquity(
            name="X CORP",
            symbol="X",
            share_class_figi="FIGI00000001",
        ),
        RawEquity(
            name="Y CORP",
            symbol="Y",
            share_class_figi="FIGI00000001",
        ),
    ]

    actual = merge(raw_equities)

    assert actual.name == "X CORP"


def test_merge_name_cluster_weight_vs_frequency() -> None:
    """
    ARRANGE: Two clusters. One has 3 occurrences, the other 2,
             but the 2-cluster appears earlier in duplicate group order.
    ASSERT:  majority weight still wins (3-cluster's earliest form kept)
    """
    raw_equities = [
        RawEquity(
            name="FOO INC",
            symbol="F",
            share_class_figi="FIGI00000001",
        ),
        RawEquity(
            name="FOO INC.",
            symbol="F",
            share_class_figi="FIGI00000001",
        ),
        RawEquity(
            name="FOO INCORPORATED",
            symbol="F",
            share_class_figi="FIGI00000001",
        ),
        RawEquity(
            name="BAR CORP",
            symbol="F",
            share_class_figi="FIGI00000001",
        ),
        RawEquity(
            name="BAR CORPORATION",
            symbol="F",
            share_class_figi="FIGI00000001",
        ),
    ]

    actual = merge(raw_equities)

    assert actual.name == "FOO INC"


def test_merge_even_number_of_prices_median_midpoint() -> None:
    """
    ARRANGE: two duplicates, last_price 1 and 9
    ACT:     merge
    ASSERT:  median == 5
    """
    raw_equities = [
        RawEquity(
            name="E",
            symbol="E",
            share_class_figi="FIGI00000001",
            last_price=Decimal("1"),
        ),
        RawEquity(
            name="E",
            symbol="E",
            share_class_figi="FIGI00000001",
            last_price=Decimal("9"),
        ),
    ]

    actual = merge(raw_equities)

    assert actual.last_price == Decimal("5")


def test_merge_large_duplicate_group_outlier_ignored() -> None:
    """
    ARRANGE: prices [0, 4.32, 4.51, 443, 0.11]
    ACT:     merge
    ASSERT:  actual last_price == 4.32
    """
    last_prices = ["0", "4.32", "4.51", "443", "0.11"]

    raw_equities = [
        RawEquity(
            name="BIG",
            symbol="B",
            share_class_figi="FIGI00000001",
            last_price=Decimal(p),
        )
        for p in last_prices
    ]

    actual = merge(raw_equities)

    assert actual.last_price == Decimal("4.32")


def test_merge_last_price_all_identical_values() -> None:
    """
    ARRANGE: three duplicates, identical price values
    ACT:     merge
    ASSERT:  median returns that identical value (no float wobble)
    """
    raw_equities = [
        RawEquity(
            name="S",
            symbol="S",
            share_class_figi="FIGI00000001",
            last_price=Decimal("7.77"),
        ),
        RawEquity(
            name="S",
            symbol="S",
            share_class_figi="FIGI00000001",
            last_price=Decimal("7.77"),
        ),
        RawEquity(
            name="S",
            symbol="S",
            share_class_figi="FIGI00000001",
            last_price=Decimal("7.77"),
        ),
    ]

    actual = merge(raw_equities)

    assert actual.last_price == Decimal("7.77")


def test_merge_isin_majority_wins() -> None:
    """
    ARRANGE: three duplicates, ISIN appears twice vs once
    ACT:     merge
    ASSERT:  actual isin == majority value
    """
    raw_equities = [
        RawEquity(
            name="T",
            symbol="T",
            share_class_figi="FIGI00000001",
            isin="US0123456789",
        ),
        RawEquity(
            name="T",
            symbol="T",
            share_class_figi="FIGI00000001",
            isin="US0123456789",
        ),
        RawEquity(
            name="T",
            symbol="T",
            share_class_figi="FIGI00000001",
            isin="US9999999999",
        ),
    ]

    actual = merge(raw_equities)

    assert actual.isin == "US0123456789"


def test_merge_isin_tie_keeps_first_seen() -> None:
    """
    ARRANGE: two distinct ISINs, each once
    ACT:     merge
    ASSERT:  earliest ISIN kept
    """
    raw_equities = [
        RawEquity(
            name="T",
            symbol="T",
            share_class_figi="FIGI00000001",
            isin="US9999999999",
        ),
        RawEquity(
            name="T",
            symbol="T",
            share_class_figi="FIGI00000001",
            isin="US0000000000",
        ),
    ]

    actual = merge(raw_equities)

    assert actual.isin == "US9999999999"


def test_merge_isin_all_none_results_in_none() -> None:
    """
    ARRANGE: identifiers missing
    ACT:     merge
    ASSERT:  actual isin is None
    """
    raw_equities = [
        RawEquity(
            name="T",
            symbol="T",
            share_class_figi="FIGI00000001",
        ),
        RawEquity(
            name="T",
            symbol="T",
            share_class_figi="FIGI00000001",
        ),
    ]

    actual = merge(raw_equities)

    assert actual.isin is None


def test_merge_isin_case_insensitive_majority() -> None:
    """
    ARRANGE: same ISIN differing only by case vs one different ISIN
    ACT:     merge
    ASSERT:  validator upper-cases, so they collapse to majority of 2
    """
    raw_equities = [
        RawEquity(
            name="Y",
            symbol="Y",
            share_class_figi="FIGI00000001",
            isin="us1234567890",
        ),
        RawEquity(
            name="Y",
            symbol="Y",
            share_class_figi="FIGI00000001",
            isin="US1234567890",
        ),
        RawEquity(
            name="Y",
            symbol="Y",
            share_class_figi="FIGI00000001",
            isin="US0000000000",
        ),
    ]

    actual = merge(raw_equities)

    assert actual.isin == "US1234567890"


def test_merge_cusip_logic_mirrors_isin() -> None:
    """
    Simple mirror check for CUSIP
    """
    raw_equities = [
        RawEquity(
            name="X",
            symbol="X",
            share_class_figi="FIGI00000001",
            cusip="037833100",
        ),
        RawEquity(
            name="X",
            symbol="X",
            share_class_figi="FIGI00000001",
            cusip="594918104",
        ),
        RawEquity(
            name="X",
            symbol="X",
            share_class_figi="FIGI00000001",
            cusip="037833100",
        ),
    ]

    actual = merge(raw_equities)

    assert actual.cusip == "037833100"


def test_merge_mics_union_preserves_first_seen_order() -> None:
    """
    ARRANGE: overlap & duplicates across lists
    ACT:     merge
    ASSERT:  actual.mics == union in first-seen order
    """
    raw_equities = [
        RawEquity(
            name="M",
            symbol="M",
            share_class_figi="FIGI00000001",
            mics=["XNYS", "XNAS"],
        ),
        RawEquity(
            name="M",
            symbol="M",
            share_class_figi="FIGI00000001",
            mics=["XNAS", "XLON"],
        ),
        RawEquity(
            name="M",
            symbol="M",
            share_class_figi="FIGI00000001",
            mics=["XETR"],
        ),
    ]

    actual = merge(raw_equities)

    assert actual.mics == ["XNYS", "XNAS", "XLON", "XETR"]


def test_merge_mics_all_empty_results_in_none() -> None:
    """
    ARRANGE: no MIC information in any record
    ACT:     merge
    ASSERT:  actual.mics is None
    """
    raw_equities = [
        RawEquity(
            name="N",
            symbol="N",
            share_class_figi="FIGI00000001",
            mics=[],
        ),
        RawEquity(
            name="N",
            symbol="N",
            share_class_figi="FIGI00000001",
            mics=None,
        ),
    ]

    actual = merge(raw_equities)

    assert actual.mics is None


def test_merge_mics_with_blank_and_whitespace_entries() -> None:
    """
    ARRANGE: some MIC elements are '', '  ', None - should be ignored.
    ACT:     merge
    ASSERT:  actual list only contains real MICs, order preserved.
    """
    raw_equities = [
        RawEquity(
            name="Z",
            symbol="Z",
            share_class_figi="FIGI00000001",
            mics=["XNYS", "", "  "],
        ),
        RawEquity(
            name="Z",
            symbol="Z",
            share_class_figi="FIGI00000001",
            mics=[None, "XNYS", "XPAR"],
        ),
    ]

    actual = merge(raw_equities)

    assert actual.mics == ["XNYS", "XPAR"]


def test_merge_currency_majority_wins() -> None:
    """
    ARRANGE: three records, currency EUR appears twice vs GBP once
    ACT:     merge
    ASSERT:  merged currency == EUR
    """
    raw_equities = [
        RawEquity(
            name="C",
            symbol="C",
            share_class_figi="FIGI00000001",
            currency="EUR",
        ),
        RawEquity(
            name="C",
            symbol="C",
            share_class_figi="FIGI00000001",
            currency="eur",
        ),
        RawEquity(
            name="C",
            symbol="C",
            share_class_figi="FIGI00000001",
            currency="GBP",
        ),
    ]

    actual = merge(raw_equities)

    assert actual.currency == "EUR"


def test_merge_currency_tie_keeps_first_seen() -> None:
    """
    ARRANGE: EUR then USD (each once)
    ACT:     merge
    ASSERT:  EUR retained (earliest)
    """
    raw_equities = [
        RawEquity(
            name="D",
            symbol="D",
            share_class_figi="FIGI00000001",
            currency="EUR",
        ),
        RawEquity(
            name="D",
            symbol="D",
            share_class_figi="FIGI00000001",
            currency="USD",
        ),
    ]

    actual = merge(raw_equities)

    assert actual.currency == "EUR"


def test_merge_currency_all_none_results_none() -> None:
    """
    ARRANGE: every record missing currency
    ACT:     merge
    ASSERT:  actual currency is None
    """
    raw_equities = [
        RawEquity(
            name="E",
            symbol="E",
            share_class_figi="FIGI00000001",
            currency=None,
        ),
        RawEquity(
            name="E",
            symbol="E",
            share_class_figi="FIGI00000001",
            currency=None,
        ),
    ]

    actual = merge(raw_equities)

    assert actual.currency is None


def test_merge_mismatched_share_class_figi_raises_error() -> None:
    """
    ARRANGE: group with two different FIGIs
    ACT:     merge
    ASSERT:  ValueError is raised
    """
    raw_equities = [
        RawEquity(
            name="FIRST CORP",
            symbol="FST",
            share_class_figi="FIGI00000001",
        ),
        RawEquity(
            name="SECOND CORP",
            symbol="SND",
            share_class_figi="FIGI00000002",
        ),
    ]

    with pytest.raises(ValueError):
        merge(raw_equities)


def test_merge_name_best_cluster_appears_later() -> None:
    """
    ARRANGE: first name belongs to a 1-member cluster,
             a later 2-member cluster has higher weight.
    ACT:     merge
    ASSERT:  earliest spelling from majority cluster is chosen.
    """
    equities = [
        RawEquity(name="BAR CORP", symbol="X", share_class_figi="FIGI00000001"),
        RawEquity(name="FOO INC", symbol="X", share_class_figi="FIGI00000001"),
        RawEquity(name="FOO INC.", symbol="X", share_class_figi="FIGI00000001"),
    ]

    merged = merge(equities)

    assert merged.name == "FOO INC"


def test_merge_isin_majority_appears_later() -> None:
    """
    ARRANGE: first ISIN unique, majority value follows.
    ACT:     merge
    ASSERT:  majority ISIN wins even though it is not first.
    """
    equities = [
        RawEquity(
            name="Z",
            symbol="Z",
            share_class_figi="FIGI00000001",
            isin="US1234567890",
        ),
        RawEquity(
            name="Z",
            symbol="Z",
            share_class_figi="FIGI00000001",
            isin="US1234567890",
        ),
        RawEquity(
            name="Z",
            symbol="Z",
            share_class_figi="FIGI00000001",
            isin="US1234567890",
        ),
    ]

    merged = merge(equities)

    assert merged.isin == "US1234567890"


def test_merge_id_majority_appears_later() -> None:
    """
    ARRANGE: minority identifier comes first; majority identifier follows twice.
    ACT:     merge
    ASSERT:  majority identifier returned.
    """
    equities = [
        RawEquity(
            name="Z",
            symbol="Z",
            share_class_figi="FIGI00000001",
            isin="MIN111111111",
        ),
        RawEquity(
            name="Z",
            symbol="Z",
            share_class_figi="FIGI00000001",
            isin="MAJ222222222",
        ),
        RawEquity(
            name="Z",
            symbol="Z",
            share_class_figi="FIGI00000001",
            isin="MAJ222222222",
        ),
    ]

    merged = merge(equities)

    assert merged.isin == "MAJ222222222"


def test_merge_currency_majority_appears_later() -> None:
    """
    ARRANGE: EUR once, then USD twice.
    ACT:     merge
    ASSERT:  majority currency USD returned.
    """
    equities = [
        RawEquity(
            name="C",
            symbol="C",
            share_class_figi="FIGI00000001",
            currency="EUR",
        ),
        RawEquity(
            name="C",
            symbol="C",
            share_class_figi="FIGI00000001",
            currency="usd",
        ),
        RawEquity(
            name="C",
            symbol="C",
            share_class_figi="FIGI00000001",
            currency="USD",
        ),
    ]

    merged = merge(equities)

    assert merged.currency == "USD"
