from __future__ import annotations

import pytest

from amazon_recommender.phases.g6 import validate_split_invariants


def _valid() -> dict[str, int]:
    return {
        "source_interactions": 10,
        "split_total": 10,
        "train_interactions": 6,
        "validation_interactions": 2,
        "test_interactions": 2,
        "eligible_users": 2,
        "split_pair_overlap": 0,
        "validation_cardinality_violations": 0,
        "test_cardinality_violations": 0,
        "temporal_position_violations": 0,
        "validation_target_seen_violations": 0,
        "test_target_seen_violations": 0,
        "test_seen_missing_validation": 0,
        "als_user_degree_violations": 0,
        "als_item_degree_violations": 0,
        "common_warm_universe_violations": 0,
        "stable_hash_violations": 0,
        "sample_limit_violations": 0,
        "kcore_converged": 1,
    }


@pytest.mark.unit
def test_g6_invariants_require_exact_reconciliation_and_no_leakage() -> None:
    validate_split_invariants(_valid())
    invalid = _valid()
    invalid["test_seen_missing_validation"] = 1
    with pytest.raises(RuntimeError, match="test_seen_missing_validation"):
        validate_split_invariants(invalid)


@pytest.mark.unit
def test_g6_invariants_reject_missing_held_out_user() -> None:
    invalid = _valid()
    invalid["validation_interactions"] = 1
    with pytest.raises(RuntimeError, match="one row per eligible user"):
        validate_split_invariants(invalid)
