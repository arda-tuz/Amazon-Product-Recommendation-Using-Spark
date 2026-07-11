from __future__ import annotations

import pytest

from amazon_recommender.phases.g7 import validate_recommendation_table


pytestmark = pytest.mark.unit


def test_g7_common_recommendation_contract_accepts_dense_active_unseen_rows(spark) -> None:
    recommendations = spark.createDataFrame(
        [
            ("validation", "u", 2, 1, 4.5),
            ("validation", "u", 3, 2, 4.4),
        ],
        "stage string, customer_id string, product_id int, rank int, model_score double",
    )
    requests = spark.createDataFrame(
        [("validation", "u")], "stage string, customer_id string"
    )
    active = spark.createDataFrame([(1,), (2,), (3,)], "product_id int")
    seen = spark.createDataFrame(
        [("validation", "u", 1)],
        "stage string, customer_id string, product_id int",
    )

    evidence = validate_recommendation_table(
        recommendations,
        model="popularity",
        requests=requests,
        active_catalog=active,
        stage_seen_items=seen,
    )

    assert evidence["rows"] == 2
    assert evidence["users_with_output"] == 1
    assert evidence["min_candidates"] == evidence["max_candidates"] == 2
    assert evidence["dense_rank_violations"] == 0


def test_g7_common_recommendation_contract_rejects_seen_item(spark) -> None:
    recommendations = spark.createDataFrame(
        [("test", "u", 2, 1, 1.0)],
        "stage string, customer_id string, product_id int, rank int, graph_score double",
    )
    requests = spark.createDataFrame([("test", "u")], "stage string, customer_id string")
    active = spark.createDataFrame([(2,)], "product_id int")
    seen = spark.createDataFrame(
        [("test", "u", 2)], "stage string, customer_id string, product_id int"
    )

    with pytest.raises(RuntimeError, match="seen_violations"):
        validate_recommendation_table(
            recommendations,
            model="graph",
            requests=requests,
            active_catalog=active,
            stage_seen_items=seen,
        )
