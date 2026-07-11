from __future__ import annotations

import pytest
from pyspark.ml.fpm import FPGrowthModel

from amazon_recommender.models.fp_growth import (
    build_singleton_rules,
    fit_fp_growth,
    score_fp_recommendations,
)


@pytest.mark.integration
def test_fit_fp_growth_uses_binding_parameters_and_scores_joined_rules(spark) -> None:
    baskets = spark.createDataFrame(
        [(f"a-{index}", [1, 2, 5]) for index in range(200)]
        + [(f"b-{index}", [3, 4]) for index in range(200)],
        "customer_id string, items array<long>",
    )
    preferences = spark.createDataFrame(
        [("user-1", 1, 1.0), ("user-1", 2, 0.5)],
        "customer_id string, product_id long, q_ui double",
    )
    # Duplicate cohort memberships must not duplicate model recommendations.
    users = spark.createDataFrame(
        [
            ("validation", "operational", "user-1"),
            ("validation", "common_warm", "user-1"),
            ("test", "operational", "user-1"),
        ],
        "stage string, cohort string, customer_id string",
    )
    active = spark.createDataFrame([(5,)], "product_id long")
    seen = spark.createDataFrame(
        [
            ("validation", "user-1", 1),
            ("validation", "user-1", 2),
            ("test", "user-1", 1),
            ("test", "user-1", 2),
            ("test", "user-1", 5),
        ],
        "stage string, customer_id string, product_id long",
    )
    bayesian = spark.createDataFrame([(5, 4.25)], "product_id long, bayesian_score double")

    artifacts = fit_fp_growth(baskets, preferences, users, active, seen, bayesian)

    assert isinstance(artifacts.model, FPGrowthModel)
    assert artifacts.parameters == {
        "basket_count": 400,
        "minimum_count": 200,
        "min_support": 0.5,
        "min_support_fraction": 0.001,
        "min_support_count_floor": 200,
        "min_confidence": 0.05,
        "min_lift": 1.10,
        "num_partitions": 64,
        "min_basket_size": 2,
        "max_basket_size": 50,
        "max_rules_per_antecedent": 20,
        "candidate_depth": 50,
        "fit_count": 1,
    }
    assert artifacts.rules.count() == 8
    row = artifacts.recommendations.collect()
    assert len(row) == 1
    assert row[0].stage == "validation"
    assert row[0].product_id == 5
    assert row[0].rank == 1
    assert row[0].fp_score == pytest.approx(1.5)
    assert row[0].aggregate_support_count == 400


@pytest.mark.integration
def test_singleton_rules_apply_lift_filter_exact_support_and_top_twenty(spark) -> None:
    association_rules = spark.createDataFrame(
        [([1], [product_id], 0.5, 2.0) for product_id in range(2, 27)]
        + [([1], [99], 0.9, 1.09), ([1, 2], [3], 0.9, 3.0)],
        "antecedent array<long>, consequent array<long>, confidence double, lift double",
    )
    itemsets = spark.createDataFrame(
        [([1, product_id], 100) for product_id in range(2, 27)]
        + [([1, 99], 900), ([1, 2, 3], 100)],
        "items array<long>, freq long",
    )

    rules = build_singleton_rules(
        association_rules, itemsets, basket_count=1_000
    ).orderBy("rule_rank")
    rows = rules.collect()

    assert [row.consequent_product_id for row in rows] == list(range(2, 22))
    assert [row.rule_rank for row in rows] == list(range(1, 21))
    assert all(row.support_count == 100 for row in rows)
    assert all(row.support == pytest.approx(0.1) for row in rows)
    assert all(row.rule_strength == pytest.approx(0.5) for row in rows)


@pytest.mark.integration
def test_fp_scoring_uses_support_bayes_product_ties_and_stage_seen_filter(spark) -> None:
    rules = spark.createDataFrame(
        [
            (1, 10, 100, 1.0),
            (2, 10, 200, 1.0),
            (3, 20, 300, 2.0),
            (4, 30, 300, 2.0),
            (4, 40, 999, 99.0),  # inactive and therefore ineligible
        ],
        "antecedent_product_id long, consequent_product_id long, "
        "support_count long, rule_strength double",
    )
    preferences = spark.createDataFrame(
        [("u", item, 1.0) for item in range(1, 5)],
        "customer_id string, product_id long, q_ui double",
    )
    users = spark.createDataFrame(
        [
            ("validation", "operational", "u"),
            ("validation", "common_warm", "u"),
            ("test", "operational", "u"),
        ],
        "stage string, cohort string, customer_id string",
    )
    active = spark.createDataFrame([(10,), (20,), (30,)], "product_id long")
    seen = spark.createDataFrame(
        [(stage, "u", item) for stage in ("validation", "test") for item in range(1, 5)]
        + [("test", "u", 20)],
        "stage string, customer_id string, product_id long",
    )
    bayesian = spark.createDataFrame(
        [(10, 3.0), (20, 4.0), (30, 4.0)],
        "product_id long, bayesian_score double",
    )

    rows = score_fp_recommendations(
        rules, preferences, users, active, seen, bayesian
    ).orderBy("stage", "rank").collect()

    validation = [row for row in rows if row.stage == "validation"]
    test = [row for row in rows if row.stage == "test"]
    assert [row.product_id for row in validation] == [20, 30, 10]
    assert [row.rank for row in validation] == [1, 2, 3]
    assert validation[2].fp_score == pytest.approx(2.0)
    assert validation[2].aggregate_support_count == 300
    assert validation[2].contributing_antecedent_count == 2
    assert [row.product_id for row in test] == [30, 10]
    assert len({(row.stage, row.customer_id, row.product_id) for row in rows}) == len(rows)


@pytest.mark.integration
def test_fit_fp_growth_rejects_non_unique_or_out_of_range_baskets(spark) -> None:
    invalid = spark.createDataFrame(
        [("u-1", [1, 1]), ("u-2", [2])],
        "customer_id string, items array<long>",
    )
    empty_preferences = spark.createDataFrame(
        [], "customer_id string, product_id long, q_ui double"
    )
    users = spark.createDataFrame([], "stage string, customer_id string")
    active = spark.createDataFrame([], "product_id long")
    seen = spark.createDataFrame(
        [], "stage string, customer_id string, product_id long"
    )
    bayesian = spark.createDataFrame([], "product_id long, bayesian_score double")

    with pytest.raises(ValueError, match="unique, non-null arrays of 2 to 50"):
        fit_fp_growth(
            invalid, empty_preferences, users, active, seen, bayesian
        )
