"""Binding explicit-feedback Spark ALS training and serving helpers."""

from __future__ import annotations

from pyspark import StorageLevel
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, NumericType


ALS_RANK = 20
ALS_REG_PARAM = 0.10
ALS_MAX_ITER = 10
ALS_SEED = 42
ALS_RAW_CANDIDATE_DEPTH = 200
ALS_CANDIDATE_DEPTH = 100

USER_COLUMN = "customer_int_id"
ITEM_COLUMN = "product_id"
RATING_COLUMN = "rating"
PREDICTION_COLUMN = "prediction"


def _require_columns(frame: DataFrame, required: set[str], *, name: str) -> None:
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"{name} is missing required columns: {missing}")


def _field(frame: DataFrame, name: str):
    return frame.schema[name].dataType


def validate_als_input_schema(frame: DataFrame) -> None:
    """Enforce the persistent IntType identifiers required by Spark ALS."""

    _require_columns(
        frame, {USER_COLUMN, ITEM_COLUMN, RATING_COLUMN}, name="ALS training input"
    )
    for column in (USER_COLUMN, ITEM_COLUMN):
        if not isinstance(_field(frame, column), IntegerType):
            raise TypeError(
                f"ALS {column} must be Spark IntType; observed "
                f"{_field(frame, column).simpleString()}"
            )
    if not isinstance(_field(frame, RATING_COLUMN), NumericType):
        raise TypeError(
            f"ALS rating must be numeric; observed "
            f"{_field(frame, RATING_COLUMN).simpleString()}"
        )


def build_explicit_als_estimator() -> ALS:
    """Construct the project's sole, non-tunable explicit ALS estimator."""

    return ALS(
        userCol=USER_COLUMN,
        itemCol=ITEM_COLUMN,
        ratingCol=RATING_COLUMN,
        rank=ALS_RANK,
        regParam=ALS_REG_PARAM,
        maxIter=ALS_MAX_ITER,
        implicitPrefs=False,
        nonnegative=False,
        coldStartStrategy="drop",
        seed=ALS_SEED,
    )


def prepare_als_training_data(train_interactions: DataFrame) -> DataFrame:
    """Cut lineage and cache one deterministic, validated ALS training universe.

    A durable Spark checkpoint is used when the application configured a checkpoint
    directory; local checkpointing is the safe local-mode fallback.  The eager
    checkpoint and subsequent cache ensure validation and fitting consume the same
    materialized rows.
    """

    validate_als_input_schema(train_interactions)
    ordered = train_interactions.select(
        F.col(USER_COLUMN),
        F.col(ITEM_COLUMN),
        F.col(RATING_COLUMN).cast("double").alias(RATING_COLUMN),
    ).orderBy(F.col(USER_COLUMN).asc(), F.col(ITEM_COLUMN).asc())
    if train_interactions.sparkSession.sparkContext.getCheckpointDir():
        materialized = ordered.checkpoint(eager=True)
    else:
        materialized = ordered.localCheckpoint(eager=True)
    materialized = materialized.persist(StorageLevel.MEMORY_AND_DISK)
    validation = materialized.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(
            F.when(
                F.col(USER_COLUMN).isNull()
                | F.col(ITEM_COLUMN).isNull()
                | F.col(RATING_COLUMN).isNull()
                | F.isnan(F.col(RATING_COLUMN))
                | ~F.col(RATING_COLUMN).between(1.0, 5.0),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("invalid_rows"),
    ).first()
    if int(validation["rows"]) == 0:
        materialized.unpersist(blocking=True)
        raise ValueError("ALS training input is empty")
    if int(validation["invalid_rows"] or 0):
        materialized.unpersist(blocking=True)
        raise ValueError(
            "ALS training input contains null, NaN, or out-of-range identifier/rating rows"
        )
    return materialized


def fit_explicit_als(train_interactions: DataFrame) -> ALSModel:
    """Fit exactly one ALS model on the materialized binding training universe."""

    prepared = prepare_als_training_data(train_interactions)
    try:
        return build_explicit_als_estimator().fit(prepared)
    finally:
        prepared.unpersist(blocking=True)


def generate_als_recommendations(
    model: ALSModel,
    evaluation_users: DataFrame,
    stage_seen_items: DataFrame,
    active_catalog: DataFrame,
    *,
    raw_candidate_depth: int = ALS_RAW_CANDIDATE_DEPTH,
    candidate_depth: int = ALS_CANDIDATE_DEPTH,
) -> DataFrame:
    """Generate raw-200 ALS candidates, then active/unseen top-100 per request.

    There is intentionally no popularity fill or any other fallback.  Users for
    whom MLlib cannot produce candidates simply have no rows, allowing coverage
    loss to remain observable during evaluation.
    """

    if raw_candidate_depth != ALS_RAW_CANDIDATE_DEPTH:
        raise ValueError("ALS raw candidate depth is binding and must be 200")
    if candidate_depth != ALS_CANDIDATE_DEPTH:
        raise ValueError("ALS candidate depth is binding and must be 100")
    _require_columns(
        evaluation_users,
        {"stage", "customer_id", USER_COLUMN},
        name="evaluation_users",
    )
    _require_columns(
        stage_seen_items,
        {"stage", "customer_id", ITEM_COLUMN},
        name="stage_seen_items",
    )
    _require_columns(active_catalog, {ITEM_COLUMN}, name="active_catalog")

    model_user_col = model.getUserCol()
    model_item_col = model.getItemCol()
    if model_user_col != USER_COLUMN or model_item_col != ITEM_COLUMN:
        raise ValueError(
            "ALS model columns do not match the persistent project identifier contract"
        )

    requests = evaluation_users.select(
        "stage", "customer_id", USER_COLUMN
    ).dropDuplicates()
    model_users = requests.select(USER_COLUMN).dropDuplicates()
    raw = model.recommendForUserSubset(model_users, ALS_RAW_CANDIDATE_DEPTH)
    recommendation_type = raw.schema["recommendations"].dataType.elementType
    score_field = "rating" if "rating" in recommendation_type.names else PREDICTION_COLUMN
    exploded = (
        raw.select(
            USER_COLUMN,
            F.posexplode("recommendations").alias("_raw_position", "_recommendation"),
        )
        .select(
            USER_COLUMN,
            F.col(f"_recommendation.{model_item_col}").cast("int").alias(ITEM_COLUMN),
            F.col(f"_recommendation.{score_field}").cast("double").alias(
                "als_prediction"
            ),
            (F.col("_raw_position") + F.lit(1)).cast("int").alias(
                "raw_candidate_rank"
            ),
        )
        .join(requests, USER_COLUMN, "inner")
    )

    catalog = active_catalog
    if "is_active" in catalog.columns:
        catalog = catalog.filter(F.col("is_active"))
    active_ids = catalog.select(ITEM_COLUMN).dropDuplicates([ITEM_COLUMN])
    eligible = exploded.join(active_ids, ITEM_COLUMN, "inner")
    seen = stage_seen_items.select(
        "stage", "customer_id", ITEM_COLUMN
    ).dropDuplicates()
    unseen = eligible.join(
        seen, ["stage", "customer_id", ITEM_COLUMN], "left_anti"
    )
    ordering = Window.partitionBy("stage", "customer_id").orderBy(
        F.col("als_prediction").desc(), F.col(ITEM_COLUMN).asc()
    )
    return (
        unseen.withColumn("recommendation_rank", F.row_number().over(ordering))
        .filter(F.col("recommendation_rank") <= F.lit(ALS_CANDIDATE_DEPTH))
        .select(
            "stage",
            "customer_id",
            USER_COLUMN,
            ITEM_COLUMN,
            "als_prediction",
            "raw_candidate_rank",
            "recommendation_rank",
        )
    )


def build_als_prediction_table(
    model: ALSModel,
    held_out_interactions: DataFrame,
    customer_mapping: DataFrame | None = None,
) -> DataFrame:
    """Return one auditable row per held-out rating, including cold-start drops.

    MLlib's binding ``coldStartStrategy='drop'`` removes unscorable rows from
    ``transform``.  This helper left-joins those predictions back to the held-out
    universe and classifies the missing rows with the learned factor universes.
    Predictions remain raw and are never clipped to the 1--5 rating range.
    """

    _require_columns(
        held_out_interactions,
        {ITEM_COLUMN, RATING_COLUMN},
        name="held_out_interactions",
    )
    base = held_out_interactions
    if USER_COLUMN not in base.columns:
        if customer_mapping is None:
            raise ValueError(
                "held_out_interactions needs customer_int_id or a customer_mapping"
            )
        _require_columns(
            base, {"customer_id"}, name="held_out_interactions"
        )
        _require_columns(
            customer_mapping,
            {"customer_id", USER_COLUMN},
            name="customer_mapping",
        )
        base = base.join(
            customer_mapping.select("customer_id", USER_COLUMN).dropDuplicates(
                ["customer_id"]
            ),
            "customer_id",
            "left",
        )
    validate_als_input_schema(base)

    model_user_col = model.getUserCol()
    model_item_col = model.getItemCol()
    if model_user_col != USER_COLUMN or model_item_col != ITEM_COLUMN:
        raise ValueError(
            "ALS model columns do not match the persistent project identifier contract"
        )
    join_keys = [
        column
        for column in ("stage", "customer_id", USER_COLUMN, ITEM_COLUMN)
        if column in base.columns
    ]
    # customer_int_id and product_id are always present, so the join key cannot be
    # empty.  ``stage`` prevents a repeated user-item target across stages from
    # being conflated.
    scored = model.transform(base).select(
        *join_keys, F.col(PREDICTION_COLUMN).cast("double").alias("als_prediction")
    )
    scored = scored.dropDuplicates(join_keys)
    result = base.join(scored, join_keys, "left")

    known_users = model.userFactors.select(
        F.col("id").cast("int").alias(USER_COLUMN)
    ).withColumn("_known_als_user", F.lit(True))
    known_items = model.itemFactors.select(
        F.col("id").cast("int").alias(ITEM_COLUMN)
    ).withColumn("_known_als_item", F.lit(True))
    result = result.join(known_users, USER_COLUMN, "left").join(
        known_items, ITEM_COLUMN, "left"
    )
    known_user = F.coalesce(F.col("_known_als_user"), F.lit(False))
    known_item = F.coalesce(F.col("_known_als_item"), F.lit(False))
    return (
        result.withColumn("is_predicted", F.col("als_prediction").isNotNull())
        .withColumn(
            "prediction_status",
            F.when(F.col("als_prediction").isNotNull(), F.lit("PREDICTED"))
            .when(~known_user & ~known_item, F.lit("COLD_USER_AND_ITEM"))
            .when(~known_user, F.lit("COLD_USER"))
            .when(~known_item, F.lit("COLD_ITEM"))
            .otherwise(F.lit("UNPREDICTABLE")),
        )
        .drop("_known_als_user", "_known_als_item")
    )
