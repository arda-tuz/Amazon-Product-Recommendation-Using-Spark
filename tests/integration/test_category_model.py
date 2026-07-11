from __future__ import annotations

from datetime import date

import pytest

from amazon_recommender.models.category_model import (
    build_category_candidate_pool,
    build_category_item_features,
    build_category_top_products,
    build_category_user_features,
    rank_category_recommendations,
    score_category_candidate_pool,
)


@pytest.mark.integration
def test_category_pipeline_scores_and_filters_seen_products(spark) -> None:
    item_features = spark.createDataFrame(
        [
            (1, 10, 1.0),
            (2, 10, 1.0),
            (2, 20, 0.5),
            (3, 20, 1.0),
        ],
        "product_id int, category_id int, normalized_depth_weight double",
    )
    active = spark.createDataFrame(
        [(1, "Book"), (2, "Book"), (3, "Book")],
        "product_id int, group string",
    )
    train = spark.createDataFrame(
        [
            ("u", 1, True, 1.0, date(2005, 1, 1)),
            ("u", 2, True, 0.5, date(2005, 1, 2)),
        ],
        "customer_id string, product_id int, is_positive boolean, q_ui double, interaction_date date",
    )
    requests = spark.createDataFrame([("validation", "u")], "stage string, customer_id string")
    popularity = spark.createDataFrame(
        [(1, 4.0, 0.5, 10), (2, 4.2, 0.7, 20), (3, 4.8, 1.0, 30)],
        "product_id int, bayesian_score double, popularity_percentile double, rater_count long",
    )
    seen = spark.createDataFrame(
        [("validation", "u", 1), ("validation", "u", 2)],
        "stage string, customer_id string, product_id int",
    )

    items = build_category_item_features(item_features, 3)
    users = build_category_user_features(
        train, requests, items.item_vectors, active
    )
    top = build_category_top_products(
        items.item_vectors,
        popularity,
        generic_category_ratio=1.0,
        products_per_category=200,
    )
    pool = build_category_candidate_pool(
        users.user_category_profiles,
        top,
        popularity,
        max_profile_categories=20,
        max_candidate_pool=5_000,
    )
    scored = score_category_candidate_pool(
        pool,
        users.user_category_profiles,
        users.user_norms,
        items.item_vectors,
        items.item_norms,
        users.user_group_affinity,
        active,
    )
    recommendations = rank_category_recommendations(
        scored, requests, seen, candidate_depth=50
    ).collect()

    assert [row.product_id for row in recommendations] == [3]
    assert recommendations[0].rank == 1
    assert recommendations[0].category_similarity > 0.0
