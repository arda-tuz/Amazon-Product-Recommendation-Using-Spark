from __future__ import annotations

import math

import pytest

from amazon_recommender.evaluation.metrics import (
    build_evaluation_population,
    evaluate_als_predictions,
    evaluate_ranking_recommendations,
)


pytestmark = pytest.mark.integration


def _catalog(spark):
    return spark.createDataFrame(
        [(product_id, "Book" if product_id in {10, 30} else "Music") for product_id in range(1, 101)],
        "product_id int, group string",
    )


def _evaluation_users(spark):
    return spark.createDataFrame(
        [
            ("validation", "operational", "u-hit", 10, 5.0),
            ("validation", "operational", "u-miss", 20, 4.0),
            ("validation", "operational", "u-empty", 30, 5.0),
            # Cohort membership is attached after recommendations exist.  The same
            # frozen u-hit list must therefore appear in both denominators.
            ("validation", "common_warm", "u-hit", 10, 5.0),
        ],
        "stage string, cohort string, customer_id string, "
        "target_product_id int, rating double",
    )


def _recommendations(spark):
    return spark.createDataFrame(
        [
            ("validation", "u-hit", 1, 1),
            ("validation", "u-hit", 2, 2),
            ("validation", "u-hit", 3, 3),
            ("validation", "u-hit", 10, 4),
            ("validation", "u-miss", 4, 1),
            ("validation", "u-miss", 5, 2),
        ],
        "stage string, customer_id string, product_id int, rank int",
    )


def test_ranking_metrics_keep_empty_users_and_match_rank_four_by_hand(spark) -> None:
    frames = evaluate_ranking_recommendations(
        _recommendations(spark),
        _evaluation_users(spark),
        _catalog(spark),
        _catalog(spark).select("product_id"),
        model="graph",
    )

    operational_overall = {
        row.customer_id: row
        for row in frames.per_user.filter(
            "cohort = 'operational' AND slice = 'overall'"
        ).collect()
    }
    hit = operational_overall["u-hit"]
    assert hit.target_rank == 4
    assert hit.hit_rate_at_10 == 1.0
    assert hit.mrr_at_10 == pytest.approx(0.25)
    assert hit.ndcg_at_10 == pytest.approx(1.0 / math.log2(5.0))

    miss = operational_overall["u-miss"]
    assert miss.target_rank is None
    assert miss.hit_rate_at_10 == 0.0
    assert miss.mrr_at_10 == 0.0
    assert miss.ndcg_at_10 == 0.0

    empty = operational_overall["u-empty"]
    assert empty.list_length == 0
    assert empty.top_k_list_length == 0
    assert not empty.has_output
    assert empty.hit_rate_at_10 == 0.0
    assert empty.mrr_at_10 == 0.0
    assert empty.ndcg_at_10 == 0.0

    summary = frames.summary.filter(
        "cohort = 'operational' AND slice = 'overall'"
    ).first()
    assert summary.evaluated_users == 3
    assert summary.users_with_output == 2
    assert summary.hit_rate_at_10 == pytest.approx(1.0 / 3.0)
    assert summary.mrr_at_10 == pytest.approx(0.25 / 3.0)
    assert summary.ndcg_at_10 == pytest.approx((1.0 / math.log2(5.0)) / 3.0)
    assert summary.user_coverage == pytest.approx(2.0 / 3.0)
    assert summary.fill_rate_at_10 == pytest.approx(6.0 / 30.0)
    assert summary.distinct_recommended_products_at_10 == 6
    assert summary.active_catalog_size == 100
    assert summary.catalog_coverage_at_10 == pytest.approx(6.0 / 100.0)


def test_population_and_summaries_use_fixed_book_slices_without_model_filtering(
    spark,
) -> None:
    population = build_evaluation_population(_evaluation_users(spark), _catalog(spark))
    assert population.count() == 8
    # Build the mapping explicitly to keep this check independent of ordering.
    slices = {}
    for row in population.collect():
        slices.setdefault((row.cohort, row.customer_id), set()).add(row.slice)
    assert slices[("operational", "u-hit")] == {"overall", "Book"}
    assert slices[("operational", "u-empty")] == {"overall", "Book"}
    assert slices[("operational", "u-miss")] == {"overall", "non-Book"}

    frames = evaluate_ranking_recommendations(
        _recommendations(spark),
        _evaluation_users(spark),
        _catalog(spark),
        _catalog(spark).select("product_id"),
        model="graph",
    )
    summaries = {
        row.slice: row
        for row in frames.summary.filter("cohort = 'operational'").collect()
    }
    assert summaries["Book"].evaluated_users == 2
    assert summaries["Book"].user_coverage == pytest.approx(0.5)
    assert summaries["Book"].fill_rate_at_10 == pytest.approx(4.0 / 20.0)
    assert summaries["Book"].catalog_coverage_at_10 == pytest.approx(4.0 / 100.0)
    assert summaries["non-Book"].evaluated_users == 1
    assert summaries["non-Book"].user_coverage == 1.0
    assert summaries["non-Book"].fill_rate_at_10 == pytest.approx(2.0 / 10.0)

    common = frames.summary.filter(
        "cohort = 'common_warm' AND slice = 'overall'"
    ).first()
    assert common.evaluated_users == 1
    assert common.ndcg_at_10 == pytest.approx(1.0 / math.log2(5.0))


def test_population_validates_only_target_groups_not_unrelated_discontinued_products(
    spark,
) -> None:
    catalog = _catalog(spark).unionByName(
        spark.createDataFrame([(999, None)], "product_id int, group string")
    )
    population = build_evaluation_population(_evaluation_users(spark), catalog)
    assert population.count() == 8

    invalid_target = spark.createDataFrame(
        [("test", "operational", "u", 999, 5.0)],
        "stage string, cohort string, customer_id string, "
        "target_product_id int, rating double",
    )
    with pytest.raises(Exception, match="evaluation target must have one catalog product group"):
        build_evaluation_population(invalid_target, catalog).collect()


def test_target_below_cutoff_is_a_zero_ranking_success_but_list_has_coverage(
    spark,
) -> None:
    users = spark.createDataFrame(
        [("test", "operational", "u", 11, 5.0)],
        "stage string, cohort string, customer_id string, "
        "target_product_id int, rating double",
    )
    recommendations = spark.createDataFrame(
        [("test", "u", product_id, product_id) for product_id in range(1, 12)],
        "stage string, customer_id string, product_id int, rank int",
    )
    row = evaluate_ranking_recommendations(
        recommendations,
        users,
        _catalog(spark),
        _catalog(spark).select("product_id"),
        model="als",
    ).per_user.filter("slice = 'overall'").first()

    assert row.target_rank == 11
    assert row.list_length == 11
    assert row.top_k_list_length == 10
    assert row.has_output
    assert row.hit_rate_at_10 == 0.0
    assert row.mrr_at_10 == 0.0
    assert row.ndcg_at_10 == 0.0
    assert row.fill_fraction_at_10 == 1.0


def test_ranking_evaluator_rejects_nonbinding_cutoff(spark) -> None:
    with pytest.raises(ValueError, match="must be 10"):
        evaluate_ranking_recommendations(
            _recommendations(spark),
            _evaluation_users(spark),
            _catalog(spark),
            _catalog(spark).select("product_id"),
            model="graph",
            k=5,
        )


def test_als_metrics_use_all_rows_and_raw_unclipped_predictions(spark) -> None:
    predictions = spark.createDataFrame(
        [
            ("validation", "u-1", 1, 5.0, 6.0, True, "PREDICTED"),
            ("validation", "u-2", 2, 1.0, 0.0, True, "PREDICTED"),
            ("validation", "u-3", 3, 4.0, None, False, "COLD_ITEM"),
            ("test", "u-4", 4, 5.0, None, False, "COLD_USER"),
        ],
        "stage string, customer_id string, product_id int, rating double, "
        "als_prediction double, is_predicted boolean, prediction_status string",
    )
    frames = evaluate_als_predictions(predictions)

    raw = frames.per_prediction.filter("customer_id = 'u-1'").first()
    assert raw.als_prediction == 6.0
    assert raw.absolute_error == 1.0
    assert raw.squared_error == 1.0

    validation = frames.summary.filter("stage = 'validation'").first()
    assert validation.prediction_scope == "all_heldout_ratings"
    assert validation.heldout_rows == 3
    assert validation.predicted_rows == 2
    assert validation.dropped_rows == 1
    assert validation.prediction_coverage == pytest.approx(2.0 / 3.0)
    assert validation.drop_rate == pytest.approx(1.0 / 3.0)
    # Clipping 6->5 or 0->1 would make these zero.  The exact value proves raw use.
    assert validation.rmse == pytest.approx(1.0)
    assert validation.mae == pytest.approx(1.0)

    test = frames.summary.filter("stage = 'test'").first()
    assert test.heldout_rows == 1
    assert test.predicted_rows == 0
    assert test.dropped_rows == 1
    assert test.prediction_coverage == 0.0
    assert test.drop_rate == 1.0
    assert test.rmse is None
    assert test.mae is None
