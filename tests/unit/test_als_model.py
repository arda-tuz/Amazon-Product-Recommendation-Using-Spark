from __future__ import annotations

import math

import pytest
from pyspark.sql import functions as F

from amazon_recommender.models.als_model import (
    build_als_prediction_table,
    build_explicit_als_estimator,
    fit_explicit_als,
    generate_als_recommendations,
    prepare_als_training_data,
)


class _RecommendationModel:
    def __init__(self, spark) -> None:
        self.spark = spark
        self.requested_depth: int | None = None

    def getUserCol(self) -> str:
        return "customer_int_id"

    def getItemCol(self) -> str:
        return "product_id"

    def recommendForUserSubset(self, users, depth: int):
        self.requested_depth = depth
        requested = {row.customer_int_id for row in users.collect()}
        rows = []
        if 1 in requested:
            rows.append((1, [(2, 5.0), (1, 4.0), (3, 3.0), (4, 2.0)]))
        return self.spark.createDataFrame(
            rows,
            "customer_int_id int, recommendations array<struct<product_id:int,rating:float>>",
        )


class _PredictionModel:
    def __init__(self, spark) -> None:
        self.userFactors = spark.createDataFrame(
            [(1, [0.1])], "id int, features array<float>"
        )
        self.itemFactors = spark.createDataFrame(
            [(10, [0.2]), (11, [0.3])], "id int, features array<float>"
        )

    def getUserCol(self) -> str:
        return "customer_int_id"

    def getItemCol(self) -> str:
        return "product_id"

    def transform(self, frame):
        # Emulate coldStartStrategy=drop and prove that predictions are not clipped.
        return frame.filter(
            (F.col("customer_int_id") == 1) & (F.col("product_id") == 10)
        ).withColumn("prediction", F.lit(6.25).cast("float"))


@pytest.mark.unit
def test_explicit_als_estimator_has_the_only_binding_configuration(spark) -> None:
    estimator = build_explicit_als_estimator()
    assert estimator.getRank() == 20
    assert estimator.getRegParam() == pytest.approx(0.10)
    assert estimator.getMaxIter() == 10
    assert estimator.getImplicitPrefs() is False
    assert estimator.getNonnegative() is False
    assert estimator.getColdStartStrategy() == "drop"
    assert estimator.getSeed() == 42
    assert estimator.getUserCol() == "customer_int_id"
    assert estimator.getItemCol() == "product_id"
    assert estimator.getRatingCol() == "rating"


@pytest.mark.unit
def test_training_materialization_preserves_exact_rows_and_validates_types(spark) -> None:
    train = spark.createDataFrame(
        [(1, 10, 4.0), (1, 11, 5.0), (2, 10, 3.0)],
        "customer_int_id int, product_id int, rating double",
    )
    prepared = prepare_als_training_data(train)
    try:
        assert prepared.is_cached
        assert [tuple(row) for row in prepared.collect()] == [
            (1, 10, 4.0),
            (1, 11, 5.0),
            (2, 10, 3.0),
        ]
    finally:
        prepared.unpersist(blocking=True)

    wrong_ids = spark.createDataFrame(
        [(1, 10, 4.0)], "customer_int_id long, product_id int, rating double"
    )
    with pytest.raises(TypeError, match="must be Spark IntType"):
        prepare_als_training_data(wrong_ids)


@pytest.mark.integration
def test_explicit_als_fit_and_transform_smoke(spark) -> None:
    train = spark.createDataFrame(
        [
            (user_id, product_id, float(1 + ((user_id + product_id) % 5)))
            for user_id in range(6)
            for product_id in range(6)
        ],
        "customer_int_id int, product_id int, rating double",
    )
    model = fit_explicit_als(train)
    assert model.rank == 20
    assert model.userFactors.count() == 6
    assert model.itemFactors.count() == 6
    assert model.transform(train).count() == 36


@pytest.mark.unit
def test_als_recommendations_are_raw_200_then_active_unseen_without_fallback(spark) -> None:
    model = _RecommendationModel(spark)
    requests = spark.createDataFrame(
        [
            ("validation", "u1", 1, "operational"),
            ("validation", "u1", 1, "common_warm"),
            ("test", "u1", 1, "operational"),
            ("validation", "cold", 99, "operational"),
        ],
        "stage string, customer_id string, customer_int_id int, cohort string",
    )
    seen = spark.createDataFrame(
        [
            ("validation", "u1", 2),
            ("test", "u1", 2),
            ("test", "u1", 1),
        ],
        "stage string, customer_id string, product_id int",
    )
    active = spark.createDataFrame(
        [(1,), (2,), (3,)], "product_id int"
    )
    result = generate_als_recommendations(model, requests, seen, active)
    assert model.requested_depth == 200
    validation = result.filter("stage = 'validation'").orderBy(
        "recommendation_rank"
    ).collect()
    test = result.filter("stage = 'test'").orderBy("recommendation_rank").collect()
    assert [(row.product_id, row.recommendation_rank) for row in validation] == [
        (1, 1),
        (3, 2),
    ]
    assert [(row.product_id, row.recommendation_rank) for row in test] == [(3, 1)]
    assert result.filter("customer_id = 'cold'").count() == 0


@pytest.mark.unit
def test_prediction_table_retains_drops_and_raw_unclipped_prediction(spark) -> None:
    model = _PredictionModel(spark)
    held_out = spark.createDataFrame(
        [
            ("validation", "known", 10, 5.0),
            ("validation", "known", 99, 4.0),
            ("validation", "cold", 99, 3.0),
        ],
        "stage string, customer_id string, product_id int, rating double",
    )
    mapping = spark.createDataFrame(
        [("known", 1), ("cold", 2)],
        "customer_id string, customer_int_id int",
    )
    rows = {
        (row.customer_id, row.product_id): row
        for row in build_als_prediction_table(model, held_out, mapping).collect()
    }
    assert rows[("known", 10)].is_predicted
    assert rows[("known", 10)].prediction_status == "PREDICTED"
    assert math.isclose(rows[("known", 10)].als_prediction, 6.25)
    assert rows[("known", 99)].prediction_status == "COLD_ITEM"
    assert rows[("known", 99)].als_prediction is None
    assert rows[("cold", 99)].prediction_status == "COLD_USER_AND_ITEM"


@pytest.mark.unit
def test_als_rejects_candidate_depth_variants(spark) -> None:
    empty_users = spark.createDataFrame(
        [], "stage string, customer_id string, customer_int_id int"
    )
    seen = spark.createDataFrame([], "stage string, customer_id string, product_id int")
    active = spark.createDataFrame([], "product_id int")
    with pytest.raises(ValueError, match="must be 200"):
        generate_als_recommendations(
            _RecommendationModel(spark),
            empty_users,
            seen,
            active,
            raw_candidate_depth=199,
        )
