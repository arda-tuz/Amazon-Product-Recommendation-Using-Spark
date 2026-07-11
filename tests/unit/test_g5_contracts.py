from __future__ import annotations

import pytest

from amazon_recommender.phases.g5 import EXPECTED_PROFILE_COUNTS, validate_profile_counts
from amazon_recommender.quality.profile import REQUIRED_EVENT_TYPES


@pytest.mark.unit
def test_g5_profile_contract_accepts_only_exact_canonical_counts() -> None:
    validate_profile_counts(EXPECTED_PROFILE_COUNTS)
    wrong = dict(EXPECTED_PROFILE_COUNTS)
    wrong["duplicate_review_extra"] -= 1
    with pytest.raises(RuntimeError, match="duplicate_review_extra"):
        validate_profile_counts(wrong)


@pytest.mark.unit
def test_quality_taxonomy_contains_every_binding_event_once() -> None:
    assert len(REQUIRED_EVENT_TYPES) == len(set(REQUIRED_EVENT_TYPES)) == 18
    assert {
        "PARSE_ERROR",
        "FIELD_ORDER_ERROR",
        "INVALID_DATE",
        "INVALID_RATING",
        "MISSING_REQUIRED_ID",
        "SIMILAR_COUNT_MISMATCH",
        "CATEGORY_COUNT_MISMATCH",
        "DOWNLOADED_ROW_COUNT_MISMATCH",
        "DECLARED_GT_DOWNLOADED",
        "DECLARED_LT_DOWNLOADED",
        "REVIEW_COVERAGE_ZERO_TOTAL",
        "AVG_RATING_MISMATCH",
        "INVALID_SALESRANK",
        "DUPLICATE_REVIEW_OCCURRENCE",
        "ORPHAN_GRAPH_TARGET",
        "DUPLICATE_GRAPH_EDGE",
        "CATEGORY_LABEL_VARIANT",
    }.issubset(REQUIRED_EVENT_TYPES)
