# _test_utils/test_backoff.py

import random

import pytest

from equity_aggregator.adapters.data_sources.enrichment_feeds.yfinance.utils.backoff import (
    backoff_delays,
)

pytestmark = pytest.mark.unit


def test_backoff_delays_sequence_length_matches_attempts() -> None:
    """
    ARRANGE: attempts = 7
    ACT:     collect all delays
    ASSERT:  exactly 7 values are produced
    """

    expected_attempts = 7
    delays = list(backoff_delays(attempts=7))

    assert len(delays) == expected_attempts


def test_backoff_delays_first_value_within_jitter_range() -> None:
    """
    ARRANGE: base = 4, jitter = 0.20
    ACT:     get first delay
    ASSERT:  delay ∈ [base * (1 - jitter), base * (1 + jitter)]
    """

    random.seed(42)
    base, jitter = 4.0, 0.20
    first_delay = next(backoff_delays(base=base, jitter=jitter, attempts=1))

    assert base * (1 - jitter) <= first_delay <= base * (1 + jitter)


def test_backoff_delays_never_exceeds_cap() -> None:
    """
    ARRANGE: cap = 1.5 (deliberately low)
    ACT:     collect delays
    ASSERT:  max(delay) ≤ cap
    """

    cap = 1.5
    delays = list(backoff_delays(base=1.0, cap=cap, jitter=0.5, attempts=10))

    assert max(delays) <= cap


def test_backoff_delays_is_non_decreasing() -> None:
    """
    ARRANGE: default settings
    ACT:     collect delays
    ASSERT:  sequence is monotonically non-decreasing
    """

    random.seed(7)
    delays = list(backoff_delays(attempts=6))

    assert all(
        earlier <= later for earlier, later in zip(delays, delays[1:], strict=False)
    )
