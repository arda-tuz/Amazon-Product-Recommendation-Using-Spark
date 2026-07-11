from __future__ import annotations

import math

import pytest

from amazon_recommender.models.popularity import (
    build_active_global_popularity_catalog,
    build_popularity_scores,
    generate_popularity_recommendations,
)


@pytest.mark.unit
def test_popularity_uses_train_only_bayesian_and_exact_group_fallback(spark) -> None:
    # Books has 101 interactions and therefore its own prior. Toys has one and
    # must use the global prior. Music makes the two priors materially different.
    rows = [(f"book-{index:03d}", 1, 5.0) for index in range(100)]
    rows += [("book-other", 2, 4.0)]
    rows += [(f"music-{index:03d}", 3, 1.0) for index in range(100)]
    rows += [("toy-one", 4, 5.0)]
    train = spark.createDataFrame(
        rows, "customer_id string, product_id int, rating double"
    )
    groups = spark.createDataFrame(
        [(1, "Book"), (2, "Book"), (3, "Music"), (4, "Toy")],
        "product_id int, group string",
    )

    by_product = {
        row.product_id: row
        for row in build_popularity_scores(train, groups).collect()
    }
    global_mean = (100 * 5.0 + 4.0 + 100 * 1.0 + 5.0) / 202.0
    assert math.isclose(by_product[1].global_mean_rating, global_mean)
    assert by_product[2].group_interaction_count == 101
    assert not by_product[2].group_uses_global_fallback
    assert math.isclose(by_product[2].group_prior_rating, 504.0 / 101.0)
    assert by_product[4].group_interaction_count == 1
    assert by_product[4].group_uses_global_fallback
    assert math.isclose(by_product[4].group_prior_rating, global_mean)
    assert math.isclose(
        by_product[4].global_bayesian_score,
        (1.0 * 5.0 + 20.0 * global_mean) / 21.0,
    )


@pytest.mark.unit
def test_active_catalog_and_stage_seen_filtering_are_deterministic(spark) -> None:
    scores = spark.createDataFrame(
        [
            (10, 4.5, 10),
            (20, 4.5, 10),
            (30, 4.0, 20),
            (40, 5.0, 100),
        ],
        "product_id int, global_bayesian_score double, unique_reviewers long",
    )
    active = spark.createDataFrame(
        [(10, True), (20, True), (30, True), (40, False)],
        "product_id int, is_active boolean",
    )
    catalog = build_active_global_popularity_catalog(scores, active)
    assert [row.product_id for row in catalog.orderBy("popularity_rank").collect()] == [
        10,
        20,
        30,
    ]

    requests = spark.createDataFrame(
        [
            ("validation", "u1", "operational"),
            ("validation", "u1", "common_warm"),
            ("test", "u1", "operational"),
        ],
        "stage string, customer_id string, cohort string",
    )
    seen = spark.createDataFrame(
        [
            ("validation", "u1", 10),
            ("test", "u1", 10),
            ("test", "u1", 20),
        ],
        "stage string, customer_id string, product_id int",
    )
    result = generate_popularity_recommendations(catalog, requests, seen)
    collected = {
        stage: [(row.product_id, row.recommendation_rank) for row in rows]
        for stage, rows in (
            (stage, result.filter(result.stage == stage).orderBy("recommendation_rank").collect())
            for stage in ("validation", "test")
        )
    }
    assert collected["validation"] == [(20, 1), (30, 2)]
    assert collected["test"] == [(30, 1)]


@pytest.mark.unit
def test_popularity_rejects_unreported_parameter_variants(spark) -> None:
    train = spark.createDataFrame(
        [("u", 1, 5.0)], "customer_id string, product_id int, rating double"
    )
    groups = spark.createDataFrame([(1, "Book")], "product_id int, group string")
    with pytest.raises(ValueError, match="must be 20"):
        build_popularity_scores(train, groups, m=19)
