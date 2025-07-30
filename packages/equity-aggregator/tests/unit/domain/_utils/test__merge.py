# pipeline/test__merge.py

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


def test_identifiers_accept_valid() -> None:
    """
    ARRANGE: identifiers as valid values
    ACT:     construct RawEquity
    ASSERT:  share_class_figi is set as expected
    """
    payload = {
        "name": "ACME CORP",
        "symbol": "ACME",
        "isin": "US0378331005",
        "cusip": "037833100",
        "cik": "0000320193",
        "share_class_figi": "BBG001S5N8V8",
    }

    actual = RawEquity(**payload)

    assert actual.share_class_figi == "BBG001S5N8V8"


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


def test_merge_cusip_logic() -> None:
    """
    ARRANGE: two distinct CUSIPs with one repeating
    ACT:     merge
    ASSERT:  majority CUSIP wins
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


def test_merge_cik_logic() -> None:
    """
    ARRANGE: two distinct CIKs with one repeating
    ACT:     merge
    ASSERT:  majority CIK wins
    """
    raw_equities = [
        RawEquity(
            name="X",
            symbol="X",
            share_class_figi="FIGI00000001",
            cik="0000320193",
        ),
        RawEquity(
            name="X",
            symbol="X",
            share_class_figi="FIGI00000001",
            cik="0000789019",
        ),
        RawEquity(
            name="X",
            symbol="X",
            share_class_figi="FIGI00000001",
            cik="0000320193",
        ),
    ]

    actual = merge(raw_equities)

    assert actual.cik == "0000320193"


def test_cik_invalid_pattern() -> None:
    """
    ARRANGE: CIK with non-digit characters
    ACT:     construct RawEquity
    ASSERT:  raises ValueError
    """
    payload = {
        "name": "ACME CORP",
        "symbol": "ACME",
        "cik": "000032019X",
    }

    with pytest.raises(ValueError):
        RawEquity(**payload)


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


def test_merge_fifty_two_week_min_even_number_median() -> None:
    """
    ARRANGE: two duplicates with 52-week lows 4 and 10
    ACT:     merge
    ASSERT:  median == 7
    """
    equities = [
        RawEquity(
            name="LOW",
            symbol="L",
            share_class_figi="FIGI00000001",
            fifty_two_week_min=Decimal("4"),
        ),
        RawEquity(
            name="LOW",
            symbol="L",
            share_class_figi="FIGI00000001",
            fifty_two_week_min=Decimal("10"),
        ),
    ]

    merged = merge(equities)

    assert merged.fifty_two_week_min == Decimal("7")


def test_merge_fifty_two_week_max_even_number_median() -> None:
    """
    ARRANGE: two duplicates with 52-week highs 14 and 26
    ACT:     merge
    ASSERT:  median == 20
    """
    equities = [
        RawEquity(
            name="HIGH",
            symbol="H",
            share_class_figi="FIGI00000001",
            fifty_two_week_max=Decimal("14"),
        ),
        RawEquity(
            name="HIGH",
            symbol="H",
            share_class_figi="FIGI00000001",
            fifty_two_week_max=Decimal("26"),
        ),
    ]

    merged = merge(equities)

    assert merged.fifty_two_week_max == Decimal("20")


def test_merge_dividend_yield_even_number_median() -> None:
    """
    ARRANGE: two duplicates with dividend yields 2 and 4
    ACT:     merge
    ASSERT:  median == 3
    """
    equities = [
        RawEquity(
            name="DIV",
            symbol="D",
            share_class_figi="FIGI00000001",
            dividend_yield=Decimal("2"),
        ),
        RawEquity(
            name="DIV",
            symbol="D",
            share_class_figi="FIGI00000001",
            dividend_yield=Decimal("4"),
        ),
    ]

    merged = merge(equities)

    assert merged.dividend_yield == Decimal("3")


def test_merge_market_volume_even_number_median() -> None:
    """
    ARRANGE: two duplicates with market volumes 1 000 and 3 000
    ACT:     merge
    ASSERT:  median == 2 000
    """
    equities = [
        RawEquity(
            name="VOL",
            symbol="V",
            share_class_figi="FIGI00000001",
            market_volume=Decimal("1000"),
        ),
        RawEquity(
            name="VOL",
            symbol="V",
            share_class_figi="FIGI00000001",
            market_volume=Decimal("3000"),
        ),
    ]

    merged = merge(equities)

    assert merged.market_volume == Decimal("2000")


def test_merge_insiders_even_number_median() -> None:
    """
    ARRANGE: two duplicates with held_insiders 10 and 20
    ACT:     merge
    ASSERT:  median == 15
    """
    equities = [
        RawEquity(
            name="I",
            symbol="I",
            share_class_figi="FIGI00000001",
            held_insiders=Decimal("10"),
        ),
        RawEquity(
            name="I",
            symbol="I",
            share_class_figi="FIGI00000001",
            held_insiders=Decimal("20"),
        ),
    ]

    merged = merge(equities)

    assert merged.held_insiders == Decimal("15")


def test_merge_institutions_even_number_median() -> None:
    """
    ARRANGE: two duplicates with held_institutions 5 and 15
    ACT:     merge
    ASSERT:  median == 10
    """
    equities = [
        RawEquity(
            name="INST",
            symbol="I",
            share_class_figi="FIGI00000001",
            held_institutions=Decimal("5"),
        ),
        RawEquity(
            name="INST",
            symbol="I",
            share_class_figi="FIGI00000001",
            held_institutions=Decimal("15"),
        ),
    ]

    merged = merge(equities)

    assert merged.held_institutions == Decimal("10")


def test_merge_short_interest_even_number_median() -> None:
    """
    ARRANGE: two duplicates with short_interest 12 and 18
    ACT:     merge
    ASSERT:  median == 15
    """
    equities = [
        RawEquity(
            name="SHRT",
            symbol="S",
            share_class_figi="FIGI00000001",
            short_interest=Decimal("12"),
        ),
        RawEquity(
            name="SHRT",
            symbol="S",
            share_class_figi="FIGI00000001",
            short_interest=Decimal("18"),
        ),
    ]

    merged = merge(equities)

    assert merged.short_interest == Decimal("15")


def test_merge_share_float_even_number_median() -> None:
    """
    ARRANGE: two duplicates with share_float 1 000 and 3 000
    ACT:     merge
    ASSERT:  median == 2 000
    """
    equities = [
        RawEquity(
            name="FLT",
            symbol="F",
            share_class_figi="FIGI00000001",
            share_float=Decimal("1000"),
        ),
        RawEquity(
            name="FLT",
            symbol="F",
            share_class_figi="FIGI00000001",
            share_float=Decimal("3000"),
        ),
    ]

    merged = merge(equities)

    assert merged.share_float == Decimal("2000")


def test_merge_shares_outstanding_even_number_median() -> None:
    """
    ARRANGE: two duplicates with shares_outstanding 100 and 300
    ACT:     merge
    ASSERT:  median == 200
    """
    equities = [
        RawEquity(
            name="OUT",
            symbol="O",
            share_class_figi="FIGI00000001",
            shares_outstanding=Decimal("100"),
        ),
        RawEquity(
            name="OUT",
            symbol="O",
            share_class_figi="FIGI00000001",
            shares_outstanding=Decimal("300"),
        ),
    ]

    merged = merge(equities)

    assert merged.shares_outstanding == Decimal("200")


def test_merge_revenue_per_share_even_number_median() -> None:
    """
    ARRANGE: two duplicates with revenue_per_share 4 and 6
    ACT:     merge
    ASSERT:  median == 5
    """
    equities = [
        RawEquity(
            name="RPS",
            symbol="R",
            share_class_figi="FIGI00000001",
            revenue_per_share=Decimal("4"),
        ),
        RawEquity(
            name="RPS",
            symbol="R",
            share_class_figi="FIGI00000001",
            revenue_per_share=Decimal("6"),
        ),
    ]

    merged = merge(equities)

    assert merged.revenue_per_share == Decimal("5")


def test_merge_profit_margin_even_number_median() -> None:
    """
    ARRANGE: two duplicates with profit_margin 8 and 12
    ACT:     merge
    ASSERT:  median == 10
    """
    equities = [
        RawEquity(
            name="PM",
            symbol="P",
            share_class_figi="FIGI00000001",
            profit_margin=Decimal("8"),
        ),
        RawEquity(
            name="PM",
            symbol="P",
            share_class_figi="FIGI00000001",
            profit_margin=Decimal("12"),
        ),
    ]

    merged = merge(equities)

    assert merged.profit_margin == Decimal("10")


def test_merge_gross_margin_even_number_median() -> None:
    """
    ARRANGE: two duplicates with gross_margin 30 and 50
    ACT:     merge
    ASSERT:  median == 40
    """
    equities = [
        RawEquity(
            name="GM",
            symbol="G",
            share_class_figi="FIGI00000001",
            gross_margin=Decimal("30"),
        ),
        RawEquity(
            name="GM",
            symbol="G",
            share_class_figi="FIGI00000001",
            gross_margin=Decimal("50"),
        ),
    ]

    merged = merge(equities)

    assert merged.gross_margin == Decimal("40")


def test_merge_operating_margin_even_number_median() -> None:
    """
    ARRANGE: two duplicates with operating_margin 15 and 25
    ACT:     merge
    ASSERT:  median == 20
    """
    equities = [
        RawEquity(
            name="OPM",
            symbol="O",
            share_class_figi="FIGI00000001",
            operating_margin=Decimal("15"),
        ),
        RawEquity(
            name="OPM",
            symbol="O",
            share_class_figi="FIGI00000001",
            operating_margin=Decimal("25"),
        ),
    ]

    merged = merge(equities)

    assert merged.operating_margin == Decimal("20")


def test_merge_free_cash_flow_even_number_median() -> None:
    """
    ARRANGE: two duplicates with free_cash_flow 1 and 3
    ACT:     merge
    ASSERT:  median == 2
    """
    equities = [
        RawEquity(
            name="FCF",
            symbol="F",
            share_class_figi="FIGI00000001",
            free_cash_flow=Decimal("1"),
        ),
        RawEquity(
            name="FCF",
            symbol="F",
            share_class_figi="FIGI00000001",
            free_cash_flow=Decimal("3"),
        ),
    ]

    merged = merge(equities)

    assert merged.free_cash_flow == Decimal("2")


def test_merge_operating_cash_flow_even_number_median() -> None:
    """
    ARRANGE: two duplicates with operating_cash_flow 2 and 6
    ACT:     merge
    ASSERT:  median == 4
    """
    equities = [
        RawEquity(
            name="OCF",
            symbol="O",
            share_class_figi="FIGI00000001",
            operating_cash_flow=Decimal("2"),
        ),
        RawEquity(
            name="OCF",
            symbol="O",
            share_class_figi="FIGI00000001",
            operating_cash_flow=Decimal("6"),
        ),
    ]

    merged = merge(equities)

    assert merged.operating_cash_flow == Decimal("4")


def test_merge_return_on_equity_even_number_median() -> None:
    """
    ARRANGE: two duplicates with return_on_equity 10 and 14
    ACT:     merge
    ASSERT:  median == 12
    """
    equities = [
        RawEquity(
            name="ROE",
            symbol="R",
            share_class_figi="FIGI00000001",
            return_on_equity=Decimal("10"),
        ),
        RawEquity(
            name="ROE",
            symbol="R",
            share_class_figi="FIGI00000001",
            return_on_equity=Decimal("14"),
        ),
    ]

    merged = merge(equities)

    assert merged.return_on_equity == Decimal("12")


def test_merge_return_on_assets_even_number_median() -> None:
    """
    ARRANGE: two duplicates with return_on_assets 6 and 10
    ACT:     merge
    ASSERT:  median == 8
    """
    equities = [
        RawEquity(
            name="ROA",
            symbol="R",
            share_class_figi="FIGI00000001",
            return_on_assets=Decimal("6"),
        ),
        RawEquity(
            name="ROA",
            symbol="R",
            share_class_figi="FIGI00000001",
            return_on_assets=Decimal("10"),
        ),
    ]

    merged = merge(equities)

    assert merged.return_on_assets == Decimal("8")


def test_merge_performance_1_year_even_number_median() -> None:
    """
    ARRANGE: two duplicates with performance_1_year -4 and 8
    ACT:     merge
    ASSERT:  median == 2
    """
    equities = [
        RawEquity(
            name="P1Y",
            symbol="P",
            share_class_figi="FIGI00000001",
            performance_1_year=Decimal("-4"),
        ),
        RawEquity(
            name="P1Y",
            symbol="P",
            share_class_figi="FIGI00000001",
            performance_1_year=Decimal("8"),
        ),
    ]

    merged = merge(equities)

    assert merged.performance_1_year == Decimal("2")


def test_merge_total_debt_even_number_median() -> None:
    """
    ARRANGE: two duplicates with total_debt 1 000 000 and 3 000 000
    ACT:     merge
    ASSERT:  median == 2 000 000
    """
    equities = [
        RawEquity(
            name="DEBT",
            symbol="D",
            share_class_figi="FIGI00000001",
            total_debt=Decimal("1000000"),
        ),
        RawEquity(
            name="DEBT",
            symbol="D",
            share_class_figi="FIGI00000001",
            total_debt=Decimal("3000000"),
        ),
    ]

    merged = merge(equities)

    assert merged.total_debt == Decimal("2000000")


def test_merge_revenue_even_number_median() -> None:
    """
    ARRANGE: two duplicates with revenue 20 and 60
    ACT:     merge
    ASSERT:  median == 40
    """
    equities = [
        RawEquity(
            name="REV",
            symbol="R",
            share_class_figi="FIGI00000001",
            revenue=Decimal("20"),
        ),
        RawEquity(
            name="REV",
            symbol="R",
            share_class_figi="FIGI00000001",
            revenue=Decimal("60"),
        ),
    ]

    merged = merge(equities)

    assert merged.revenue == Decimal("40")


def test_merge_ebitda_even_number_median() -> None:
    """
    ARRANGE: two duplicates with ebitda 9 and 15
    ACT:     merge
    ASSERT:  median == 12
    """
    equities = [
        RawEquity(
            name="EBIT",
            symbol="E",
            share_class_figi="FIGI00000001",
            ebitda=Decimal("9"),
        ),
        RawEquity(
            name="EBIT",
            symbol="E",
            share_class_figi="FIGI00000001",
            ebitda=Decimal("15"),
        ),
    ]

    merged = merge(equities)

    assert merged.ebitda == Decimal("12")


def test_merge_trailing_pe_even_number_median() -> None:
    """
    ARRANGE: two duplicates with trailing_pe 11 and 19
    ACT:     merge
    ASSERT:  median == 15
    """
    equities = [
        RawEquity(
            name="PE",
            symbol="P",
            share_class_figi="FIGI00000001",
            trailing_pe=Decimal("11"),
        ),
        RawEquity(
            name="PE",
            symbol="P",
            share_class_figi="FIGI00000001",
            trailing_pe=Decimal("19"),
        ),
    ]

    merged = merge(equities)

    assert merged.trailing_pe == Decimal("15")


def test_merge_price_to_book_even_number_median() -> None:
    """
    ARRANGE: two duplicates with price_to_book 1.1 and 3.3
    ACT:     merge
    ASSERT:  median == 2.2
    """
    equities = [
        RawEquity(
            name="P2B",
            symbol="P",
            share_class_figi="FIGI00000001",
            price_to_book=Decimal("1.1"),
        ),
        RawEquity(
            name="P2B",
            symbol="P",
            share_class_figi="FIGI00000001",
            price_to_book=Decimal("3.3"),
        ),
    ]

    merged = merge(equities)

    assert merged.price_to_book == Decimal("2.2")


def test_merge_trailing_eps_even_number_median() -> None:
    """
    ARRANGE: two duplicates with trailing_eps 0.50 and 1.50
    ACT:     merge
    ASSERT:  median == 1.00
    """
    equities = [
        RawEquity(
            name="EPS",
            symbol="E",
            share_class_figi="FIGI00000001",
            trailing_eps=Decimal("0.50"),
        ),
        RawEquity(
            name="EPS",
            symbol="E",
            share_class_figi="FIGI00000001",
            trailing_eps=Decimal("1.50"),
        ),
    ]

    merged = merge(equities)

    assert merged.trailing_eps == Decimal("1.00")


def test_merge_analyst_rating_majority_wins() -> None:
    """
    ARRANGE: ratings BUY, SELL, BUY
    ACT:     merge
    ASSERT:  majority BUY retained
    """
    equities = [
        RawEquity(
            name="R",
            symbol="R",
            share_class_figi="FIGI00000001",
            analyst_rating="BUY",
        ),
        RawEquity(
            name="R",
            symbol="R",
            share_class_figi="FIGI00000001",
            analyst_rating="SELL",
        ),
        RawEquity(
            name="R",
            symbol="R",
            share_class_figi="FIGI00000001",
            analyst_rating="BUY",
        ),
    ]

    merged = merge(equities)

    assert merged.analyst_rating == "BUY"


def test_merge_analyst_rating_majority_appears_later() -> None:
    """
    ARRANGE: first item has no rating, second is a minority rating,
             third and fourth are the majority rating.
    ACT:     merge
    ASSERT:  majority rating BUY is selected.
    """
    equities = [
        RawEquity(
            name="R",
            symbol="R",
            share_class_figi="FIGI00000001",
            analyst_rating=None,
        ),
        RawEquity(
            name="R",
            symbol="R",
            share_class_figi="FIGI00000001",
            analyst_rating="SELL",
        ),
        RawEquity(
            name="R",
            symbol="R",
            share_class_figi="FIGI00000001",
            analyst_rating="BUY",
        ),
        RawEquity(
            name="R",
            symbol="R",
            share_class_figi="FIGI00000001",
            analyst_rating="BUY",
        ),
    ]

    merged = merge(equities)

    assert merged.analyst_rating == "BUY"


def test_merge_industry_cluster_majority() -> None:
    """
    ARRANGE: three spellings, two reduce to the same normalised form.
    ACT:     merge
    ASSERT:  majority cluster 'CONSUMER ELECTRONICS' kept.
    """
    equities = [
        RawEquity(
            name="I",
            symbol="I",
            share_class_figi="FIGI00000001",
            industry="ELECTRONIC COMPUTERS",
        ),
        RawEquity(
            name="I",
            symbol="I",
            share_class_figi="FIGI00000001",
            industry="Consumer Electronics",
        ),
        RawEquity(
            name="I",
            symbol="I",
            share_class_figi="FIGI00000001",
            industry="consumer-electronics",
        ),
    ]

    merged = merge(equities)

    assert merged.industry == "CONSUMER ELECTRONICS"


def test_merge_industry_all_none_results_in_none() -> None:
    """
    ARRANGE: industry missing everywhere
    ACT:     merge
    ASSERT:  merged.industry is None
    """
    equities = [
        RawEquity(
            name="I",
            symbol="I",
            share_class_figi="FIGI00000001",
            industry=None,
        ),
        RawEquity(
            name="I",
            symbol="I",
            share_class_figi="FIGI00000001",
            industry=None,
        ),
    ]

    merged = merge(equities)

    assert merged.industry is None


def test_merge_industry_cluster_tie_keeps_earliest() -> None:
    """
    ARRANGE: Two distinct industry clusters, each with the same weight.
             The first cluster seen should win the tie.
    ACT:     merge
    ASSERT:  'ALPHA ENGINES' (earliest spelling from the first cluster) is kept.
    """
    equities = [
        RawEquity(
            name="I",
            symbol="I",
            share_class_figi="FIGI00000001",
            industry="ALPHA ENGINES",
        ),
        RawEquity(
            name="I",
            symbol="I",
            share_class_figi="FIGI00000001",
            industry="ALPHA-ENGINES",
        ),
        RawEquity(
            name="I",
            symbol="I",
            share_class_figi="FIGI00000001",
            industry="BETA ENERGY",
        ),
        RawEquity(
            name="I",
            symbol="I",
            share_class_figi="FIGI00000001",
            industry="BETA-ENERGY",
        ),
    ]

    merged = merge(equities)

    assert merged.industry == "ALPHA ENGINES"


def test_merge_sector_cluster_majority() -> None:
    """
    ARRANGE: three spellings, two collapse into the same normalised form.
    ACT:     merge
    ASSERT:  majority cluster 'CONSUMER DISCRETIONARY' kept.
    """
    equities = [
        RawEquity(
            name="S",
            symbol="S",
            share_class_figi="FIGI00000001",
            sector="Consumer Discretionary",
        ),
        RawEquity(
            name="S",
            symbol="S",
            share_class_figi="FIGI00000001",
            sector="consumer-discretionary",
        ),
        RawEquity(
            name="S",
            symbol="S",
            share_class_figi="FIGI00000001",
            sector="Information Technology",
        ),
    ]

    merged = merge(equities)

    assert merged.sector == "CONSUMER DISCRETIONARY"


def test_merge_sector_all_none_results_in_none() -> None:
    """
    ARRANGE: sector missing everywhere
    ACT:     merge
    ASSERT:  merged.sector is None
    """
    equities = [
        RawEquity(name="S", symbol="S", share_class_figi="FIGI00000001", sector=None),
        RawEquity(name="S", symbol="S", share_class_figi="FIGI00000001", sector=None),
    ]

    merged = merge(equities)

    assert merged.sector is None


def test_merge_sector_cluster_tie_keeps_earliest() -> None:
    """
    ARRANGE: two distinct sector clusters with equal weight.
             First cluster seen should win tie.
    ACT:     merge
    ASSERT:  'ENERGY' (earliest spelling from first cluster) kept.
    """
    equities = [
        RawEquity(
            name="S",
            symbol="S",
            share_class_figi="FIGI00000001",
            sector="Energy",
        ),
        RawEquity(
            name="S",
            symbol="S",
            share_class_figi="FIGI00000001",
            sector="energy",
        ),
        RawEquity(
            name="S",
            symbol="S",
            share_class_figi="FIGI00000001",
            sector="Health-Care",
        ),
        RawEquity(
            name="S",
            symbol="S",
            share_class_figi="FIGI00000001",
            sector="HEALTH CARE",
        ),
    ]

    merged = merge(equities)

    assert merged.sector == "ENERGY"


def test_merge_sector_best_cluster_appears_later() -> None:
    """
    ARRANGE: first record's sector is a 1-member cluster; a later 2-member
             cluster has greater weight.
    ACT:     merge
    ASSERT:  majority cluster 'TECHNOLOGY' wins even though it starts later.
    """
    equities = [
        RawEquity(
            name="S",
            symbol="S",
            share_class_figi="FIGI00000001",
            sector="ENERGY",
        ),
        RawEquity(
            name="S",
            symbol="S",
            share_class_figi="FIGI00000001",
            sector="Technology",
        ),
        RawEquity(
            name="S",
            symbol="S",
            share_class_figi="FIGI00000001",
            sector="TECHNOLOGY",
        ),
    ]

    merged = merge(equities)

    assert merged.sector == "TECHNOLOGY"
