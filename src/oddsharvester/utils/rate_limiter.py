"""Rate limiting and request management utilities (synced from SofaScore backfill)."""

from __future__ import annotations

import random
from datetime import UTC, datetime


class RequestCounter:
    """
    Track daily requests and enforce limits.
    Prevents hitting rate limits by pausing when approaching daily quota.
    """

    def __init__(
        self,
        daily_limit: int = 5000,
        warn_at: float = 0.8,
    ):
        self.daily_limit = daily_limit
        self.warn_at = warn_at
        self._reset()

    def _reset(self):
        self.today = datetime.now(UTC).date()
        self.count = 0
        self.warnings_issued = 0

    def _check_date(self):
        """Reset counter if new day."""
        today = datetime.now(UTC).date()
        if today != self.today:
            self._reset()

    def increment(self) -> int:
        """Increment counter, return new count."""
        self._check_date()
        self.count += 1
        return self.count

    @property
    def remaining(self) -> int:
        """Number of requests remaining today."""
        self._check_date()
        return max(0, self.daily_limit - self.count)

    @property
    def usage_pct(self) -> float:
        """Usage percentage (0.0 to 1.0+)."""
        self._check_date()
        return self.count / self.daily_limit

    @property
    def is_exhausted(self) -> bool:
        """True if daily limit reached."""
        return self.remaining <= 0

    def should_warn(self) -> bool:
        """True if we should warn about approaching limit."""
        return self.usage_pct >= self.warn_at and self.warnings_issued == 0

    def acknowledge_warning(self):
        """Acknowledge warning so we don't repeat it."""
        self.warnings_issued += 1


def shuffle_targets(targets: list, conservatively: bool = True) -> list:
    """
    Shuffle targets to avoid sequential access patterns.

    Args:
        targets: List of match links, event IDs, or similar
        conservatively: If True, do a light shuffle; if False, full random

    Returns:
        Shuffled list
    """
    if not targets:
        return targets

    if conservatively:
        # Light shuffle: swap random pairs, don't fully randomize
        result = targets.copy()
        n_swaps = min(len(result) // 10, 50)
        for _ in range(n_swaps):
            i, j = random.sample(range(len(result)), 2)
            result[i], result[j] = result[j], result[i]
        return result
    else:
        # Full shuffle using Fisher-Yates
        result = targets.copy()
        random.shuffle(result)
        return result
