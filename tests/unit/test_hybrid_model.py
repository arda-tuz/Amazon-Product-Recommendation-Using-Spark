from __future__ import annotations

import pytest

from amazon_recommender.models.hybrid import (
    H_A_WEIGHTS,
    H_B_WEIGHTS,
    build_hybrid_frames,
    select_hybrid_configuration,
)


SCHEMA = "stage string, customer_id string, product_id int, rank int"


def _frames(spark, rows_by_model):
    return {
        model: spark.createDataFrame(rows_by_model.get(model, []), SCHEMA)
        for model in ("popularity", "als", "fp", "graph", "category")
    }


def _bayes(spark, product_ids, overrides=None):
    overrides = overrides or {}
    return spark.createDataFrame(
        [(product_id, float(overrides.get(product_id, 4.0))) for product_id in product_ids],
        "product_id int, global_bayesian_score double",
    )


@pytest.mark.unit
def test_h_a_matches_binding_hand_calculation(spark) -> None:
    rows = {
        "als": [("validation", "u", 10, 1)],
        "graph": [
            ("validation", "u", 90, 1),
            ("validation", "u", 91, 2),
            ("validation", "u", 10, 3),
        ],
        "category": [("validation", "u", 20, 1)],
        "fp": [("validation", "u", 92, 1), ("validation", "u", 20, 2)],
        "popularity": [("validation", "u", 20, 1)],
    }
    frames = build_hybrid_frames(_frames(spark, rows), _bayes(spark, [10, 20, 90, 91, 92]))
    by_product = {
        row.product_id: row for row in frames.h_a_recommendations.collect()
    }
    assert by_product[10].hybrid_score == pytest.approx(0.0089123, abs=5e-8)
    assert by_product[20].hybrid_score == pytest.approx(0.0073374, abs=5e-8)
    assert by_product[10].rank < by_product[20].rank
    assert by_product[10].als_rank == 1
    assert by_product[10].category_rank is None
    assert by_product[10].active_weight_sum == pytest.approx(1.0)


@pytest.mark.unit
def test_active_weights_are_renormalized_per_user_and_variant(spark) -> None:
    rows = {
        "als": [("validation", "u1", 1, 1)],
        "graph": [("validation", "u1", 2, 1)],
        "popularity": [("validation", "u2", 3, 1)],
    }
    result = build_hybrid_frames(_frames(spark, rows), _bayes(spark, [1, 2, 3]))
    a = {(row.customer_id, row.product_id): row for row in result.h_a_recommendations.collect()}
    b = {(row.customer_id, row.product_id): row for row in result.h_b_recommendations.collect()}

    assert a[("u1", 1)].hybrid_score == pytest.approx((0.35 / 0.55) / 61.0)
    assert a[("u1", 2)].hybrid_score == pytest.approx((0.20 / 0.55) / 61.0)
    assert b[("u1", 1)].hybrid_score == pytest.approx((0.50 / 0.70) / 61.0)
    assert b[("u1", 2)].hybrid_score == pytest.approx((0.20 / 0.70) / 61.0)
    assert a[("u2", 3)].hybrid_score == pytest.approx(1.0 / 61.0)
    assert b[("u2", 3)].hybrid_score == pytest.approx(1.0 / 61.0)
    assert a[("u1", 1)].active_models == ["als", "graph"]
    assert a[("u2", 3)].active_models == ["popularity"]


@pytest.mark.unit
def test_all_four_deterministic_ranking_keys_are_applied(spark) -> None:
    rows = {
        # Equal score: two contributing models outrank one.
        "als": [("validation", "models", 20, 1)],
        "graph": [
            ("validation", "models", 10, 1),
            ("validation", "bayes", 30, 1),
            ("validation", "id", 50, 1),
        ],
        "fp": [("validation", "models", 10, 1)],
        # Equal score/model count: Bayesian score, then product ID.
        "category": [
            ("validation", "bayes", 40, 1),
            ("validation", "id", 60, 1),
        ],
    }
    bayes = _bayes(
        spark,
        [10, 20, 30, 40, 50, 60],
        {10: 1.0, 20: 5.0, 30: 4.0, 40: 5.0, 50: 4.0, 60: 4.0},
    )
    result = build_hybrid_frames(_frames(spark, rows), bayes).h_a_recommendations
    ordered = {
        user: [row.product_id for row in result.filter(result.customer_id == user).orderBy("rank").collect()]
        for user in ("models", "bayes", "id")
    }
    assert ordered["models"] == [10, 20]
    assert ordered["bayes"] == [40, 30]
    assert ordered["id"] == [50, 60]


@pytest.mark.unit
def test_both_variants_share_candidate_evidence_and_exact_weights() -> None:
    assert H_A_WEIGHTS == {
        "als": 0.35,
        "graph": 0.20,
        "category": 0.20,
        "fp": 0.15,
        "popularity": 0.10,
    }
    assert H_B_WEIGHTS == {
        "als": 0.50,
        "graph": 0.20,
        "category": 0.10,
        "fp": 0.15,
        "popularity": 0.05,
    }


@pytest.mark.unit
def test_validation_selection_uses_strict_ndcg_threshold_then_coverage_then_h_a() -> None:
    assert select_hybrid_configuration(
        h_a_ndcg_at_10=0.31,
        h_a_user_coverage=0.60,
        h_b_ndcg_at_10=0.30,
        h_b_user_coverage=0.99,
    ).selected_variant == "h_a"
    assert select_hybrid_configuration(
        h_a_ndcg_at_10=0.3004,
        h_a_user_coverage=0.60,
        h_b_ndcg_at_10=0.30,
        h_b_user_coverage=0.70,
    ).selected_variant == "h_b"
    assert select_hybrid_configuration(
        h_a_ndcg_at_10=0.30,
        h_a_user_coverage=0.70,
        h_b_ndcg_at_10=0.30,
        h_b_user_coverage=0.70,
    ).selected_variant == "h_a"
    # The specification says "less than 0.001"; exactly 0.001 is not a tie.
    assert select_hybrid_configuration(
        h_a_ndcg_at_10=0.301,
        h_a_user_coverage=0.10,
        h_b_ndcg_at_10=0.300,
        h_b_user_coverage=0.90,
    ).selected_variant == "h_a"


@pytest.mark.unit
def test_only_the_five_models_and_valid_one_based_depths_are_accepted(spark) -> None:
    frames = _frames(spark, {"als": [("validation", "u", 1, 0)]})
    with pytest.raises(Exception, match="RRF rank for als"):
        build_hybrid_frames(frames, _bayes(spark, [1])).h_a_recommendations.collect()

    del frames["fp"]
    with pytest.raises(ValueError, match="exactly popularity"):
        build_hybrid_frames(frames, _bayes(spark, [1]))


@pytest.mark.unit
def test_duplicate_candidate_fails_but_missing_bayesian_tie_score_is_retained(spark) -> None:
    duplicate = _frames(
        spark,
        {
            "als": [
                ("validation", "u", 1, 1),
                ("validation", "u", 1, 1),
            ]
        },
    )
    with pytest.raises(Exception, match="duplicate model candidate"):
        build_hybrid_frames(duplicate, _bayes(spark, [1])).h_a_recommendations.collect()

    missing_score = _frames(spark, {"als": [("validation", "u", 1, 1)]})
    empty_bayes = spark.createDataFrame(
        [], "product_id int, global_bayesian_score double"
    )
    row = build_hybrid_frames(
        missing_score, empty_bayes
    ).h_a_recommendations.first()
    assert row.product_id == 1
    assert row.global_bayesian_score is None
    assert row.has_bayesian_score is False
