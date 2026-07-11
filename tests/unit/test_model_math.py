from __future__ import annotations

import math

import pytest

from amazon_recommender.models.math import (
    H_A_WEIGHTS,
    association_rule_statistics,
    bayesian_weighted_rating,
    category_depth_weight,
    category_idf,
    cosine_similarity,
    fp_minimum_count,
    fp_minimum_support,
    graph_position_decay,
    graph_seed_contribution,
    preference_weight,
    rule_strength,
    single_positive_metrics_at_10,
    weighted_rrf,
)

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    ("rating", "expected"),
    [(1.0, 0.0), (3.0, 0.0), (4.0, 0.5), (5.0, 1.0), (7.0, 1.0)],
)
def test_preference_weight_clips_binding_formula(rating: float, expected: float) -> None:
    assert preference_weight(rating) == expected


def test_preference_weight_rejects_non_finite_rating() -> None:
    with pytest.raises(ValueError, match="finite"):
        preference_weight(math.nan)


def test_bayesian_weighted_rating_matches_binding_m20_formula() -> None:
    # Section 10 fixes m=20.  Therefore (10*5 + 20*4) / 30 = 4.3333...;
    # the 4.1667 value repeated in section 19.3 is an arithmetic typo.
    assert bayesian_weighted_rating(
        global_mean=4,
        item_mean=5,
        unique_rater_count=10,
    ) == pytest.approx(4.333333333333333)


def test_bayesian_weighted_rating_with_no_raters_returns_global_mean() -> None:
    assert bayesian_weighted_rating(3.75, 5.0, 0) == 3.75


def test_bayesian_weighted_rating_rejects_invalid_contract_values() -> None:
    with pytest.raises(ValueError, match="item_mean"):
        bayesian_weighted_rating(4.0, 5.1, 10)
    with pytest.raises(ValueError, match="unique_rater_count"):
        bayesian_weighted_rating(4.0, 5.0, -1)


@pytest.mark.parametrize(
    ("basket_count", "expected_count", "expected_support"),
    [
        (100, 200, 2.0),
        (200_000, 200, 0.001),
        (200_001, 201, 201 / 200_001),
        (1_000_001, 1_001, 1_001 / 1_000_001),
    ],
)
def test_fp_thresholds_follow_exact_count_then_support_formula(
    basket_count: int,
    expected_count: int,
    expected_support: float,
) -> None:
    assert fp_minimum_count(basket_count) == expected_count
    assert fp_minimum_support(basket_count) == pytest.approx(expected_support)


def test_fp_thresholds_reject_empty_basket_universe() -> None:
    with pytest.raises(ValueError, match="basket_count"):
        fp_minimum_support(0)


def test_association_rule_statistics_match_hand_calculation() -> None:
    statistics = association_rule_statistics(
        pair_count=100,
        antecedent_count=400,
        consequent_count=250,
        basket_count=2_000,
    )

    assert statistics.confidence == 0.25
    assert statistics.lift == 2.0
    assert statistics.strength == 0.25


def test_rule_strength_rejects_non_positive_lift() -> None:
    with pytest.raises(ValueError, match="lift"):
        rule_strength(0.2, 0.0)


def test_graph_position_decay_matches_binding_examples() -> None:
    assert graph_position_decay(1) == 1.0
    assert graph_position_decay(3) == 0.5


def test_graph_seed_contribution_separates_direct_and_reciprocal_terms() -> None:
    direct = graph_seed_contribution(0.5, direct_position=1)
    reciprocal = graph_seed_contribution(0.5, direct_position=1, reciprocal=True)

    assert direct == 0.5
    assert reciprocal == 0.625


def test_graph_seed_contribution_sums_two_step_paths() -> None:
    contribution = graph_seed_contribution(
        1.0,
        two_step_positions=[(1, 3), (3, 3)],
    )

    assert contribution == 0.375


def test_graph_contribution_rejects_reciprocal_without_direct_edge() -> None:
    with pytest.raises(ValueError, match="direct edge"):
        graph_seed_contribution(1.0, reciprocal=True)


def test_category_idf_and_depth_weight_match_hand_calculation() -> None:
    assert category_idf(product_count=99, document_frequency=9) == pytest.approx(
        math.log(10) + 1
    )
    assert category_depth_weight(depth=2, path_length=4) == 0.5


def test_category_cosine_similarity_matches_sparse_hand_calculation() -> None:
    similarity = cosine_similarity({"a": 1.0, "b": 2.0}, {"a": 2.0, "b": 1.0})

    assert similarity == pytest.approx(0.8)


def test_category_cosine_similarity_marks_zero_norm_as_coverage_loss() -> None:
    assert cosine_similarity({"a": 0.0}, {"a": 1.0}) is None
    assert cosine_similarity({}, {"a": 1.0}) is None


def test_weighted_rrf_matches_section_15_5_fixed_scores() -> None:
    candidates = {
        "als": [10],
        "graph": [90, 91, 10],
        "category": [20],
        "fp": [92, 20],
        "popularity": [20],
    }
    bayesian_scores = {product_id: 4.0 for product_id in {10, 20, 90, 91, 92}}

    ranked = weighted_rrf(candidates, H_A_WEIGHTS, bayesian_scores)
    by_product = {item.product_id: item for item in ranked}

    assert by_product[10].score == pytest.approx(0.0089123, abs=5e-8)
    assert by_product[20].score == pytest.approx(0.0073374, abs=5e-8)
    assert by_product[10].score > by_product[20].score
    assert by_product[10].rank_from("als") == 1
    assert by_product[10].rank_from("category") is None


def test_weighted_rrf_normalizes_only_models_active_for_user() -> None:
    candidates = {"als": [1], "graph": [2], "category": [], "fp": [], "popularity": []}

    ranked = weighted_rrf(candidates, H_A_WEIGHTS, {1: 4.0, 2: 4.0})
    by_product = {item.product_id: item for item in ranked}

    assert by_product[1].score == pytest.approx((0.35 / 0.55) / 61)
    assert by_product[2].score == pytest.approx((0.20 / 0.55) / 61)


def test_weighted_rrf_applies_all_deterministic_tie_breaks() -> None:
    weights = {"a": 0.50, "b": 0.25, "c": 0.25}
    candidates = {"a": [20, 40, 30], "b": [10], "c": [10]}
    bayesian_scores = {10: 1.0, 20: 5.0, 30: 4.0, 40: 4.0}

    ranked = weighted_rrf(candidates, weights, bayesian_scores)

    # Products 10 and 20 have equal scores; model count outranks Bayesian score.
    assert [item.product_id for item in ranked[:2]] == [10, 20]
    # Products 40 and 30 differ by rank before the remaining tie breakers apply.
    assert [item.product_id for item in ranked[2:]] == [40, 30]


def test_weighted_rrf_uses_bayesian_then_product_id_for_equal_candidates() -> None:
    weights = {"a": 0.5, "b": 0.5}
    candidates = {"a": [2], "b": [1]}

    assert [item.product_id for item in weighted_rrf(candidates, weights, {1: 5.0, 2: 4.0})] == [
        1,
        2,
    ]
    assert [item.product_id for item in weighted_rrf(candidates, weights, {1: 4.0, 2: 4.0})] == [
        1,
        2,
    ]


def test_weighted_rrf_rejects_duplicate_or_unscored_candidates() -> None:
    with pytest.raises(ValueError, match="duplicate"):
        weighted_rrf({"als": [1, 1]}, {"als": 1.0}, {1: 4.0})
    with pytest.raises(ValueError, match="missing Bayesian"):
        weighted_rrf({"als": [1]}, {"als": 1.0}, {})


def test_single_positive_metrics_match_rank_four_example() -> None:
    metrics = single_positive_metrics_at_10(4)

    assert metrics.hit_rate_at_10 == 1.0
    assert metrics.mrr_at_10 == 0.25
    assert metrics.ndcg_at_10 == pytest.approx(1 / math.log2(5))


@pytest.mark.parametrize("rank", [None, 11, 100])
def test_single_positive_metrics_are_zero_when_target_is_missing_from_top_10(
    rank: int | None,
) -> None:
    metrics = single_positive_metrics_at_10(rank)

    assert metrics.hit_rate_at_10 == 0.0
    assert metrics.mrr_at_10 == 0.0
    assert metrics.ndcg_at_10 == 0.0


def test_single_positive_metrics_reject_invalid_rank() -> None:
    with pytest.raises(ValueError, match="rank"):
        single_positive_metrics_at_10(0)
