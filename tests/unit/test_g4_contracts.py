from __future__ import annotations

import pytest

from amazon_recommender.phases.g4 import EXPECTED_HARD_COUNTS, validate_hard_counts


@pytest.mark.unit
def test_exact_canonical_hard_counts_pass() -> None:
    validate_hard_counts(dict(EXPECTED_HARD_COUNTS))


@pytest.mark.unit
def test_any_canonical_hard_count_difference_stops_gate() -> None:
    actual = dict(EXPECTED_HARD_COUNTS)
    actual["physical_reviews"] -= 1
    with pytest.raises(RuntimeError, match="physical_reviews"):
        validate_hard_counts(actual)
