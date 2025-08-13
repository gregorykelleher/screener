# utils/backoff.py


import random
from collections.abc import Iterator


def backoff_delays(
    *,
    base: float = 5.0,
    cap: float = 128.0,
    jitter: float = 0.10,
    attempts: int = 5,
) -> Iterator[float]:
    """
    Yield an exponential backoff sequence with bounded jitter for retry delays.

    Each delay is calculated as: delay * (1 ± jitter), doubling each time up to cap.

    Args:
        base (float): Initial delay in seconds.
        cap (float): Maximum delay in seconds.
        jitter (float): Fractional jitter (+/-) applied to each delay.
        attempts (int): Number of delay values to yield.

    Returns:
        Iterator[float]: Sequence of delay values in seconds.
    """
    delay: float = base
    for _ in range(attempts):
        delta: float = delay * jitter * (2 * random.random() - 1)
        yield max(0.0, min(delay + delta, cap))
        delay = min(delay * 2, cap)
