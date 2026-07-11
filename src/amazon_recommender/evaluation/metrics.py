"""Leakage-safe Spark evaluation for the binding offline protocol.

Ranking populations originate exclusively from G6 ``evaluation_users``.  Model
recommendations are attached with left joins, so a model that produces no list for a
sampled user contributes zero ranking success and zero coverage instead of disappearing
from the denominator.  The same frozen recommendation rows are expanded into the
``overall``, ``Book``, and ``non-Book`` target slices; no model is rerun for a slice.

ALS rating prediction metrics intentionally use every held-out row supplied by G7, not
only positive ranking targets.  RMSE and MAE are calculated on cold-start-predictable
rows with raw, unclipped predictions, while prediction coverage and drop rate retain the
complete held-out denominator.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegralType, NumericType


EVALUATION_K: Final[int] = 10
BOOK_GROUP: Final[str] = "Book"
OVERALL_SLICE: Final[str] = "overall"
NON_BOOK_SLICE: Final[str] = "non-Book"

_POPULATION_KEYS: Final[tuple[str, str, str]] = (
    "stage",
    "cohort",
    "customer_id",
)
_REQUEST_KEYS: Final[tuple[str, str]] = ("stage", "customer_id")


@dataclass(frozen=True)
class RankingEvaluationFrames:
    """Per-user evidence and aggregate ranking/coverage metrics."""

    per_user: DataFrame
    summary: DataFrame

    def as_dict(self) -> dict[str, DataFrame]:
        return {
            "evaluation_per_user": self.per_user,
            "evaluation_summary": self.summary,
        }


@dataclass(frozen=True)
class ALSPredictionEvaluationFrames:
    """Auditable held-out ALS errors and their stage-level summary."""

    per_prediction: DataFrame
    summary: DataFrame

    def as_dict(self) -> dict[str, DataFrame]:
        return {
            "als_prediction_per_row": self.per_prediction,
            "als_prediction_summary": self.summary,
        }


def _require_columns(frame: DataFrame, required: set[str], *, name: str) -> None:
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"{name} is missing required columns: {missing}")


def _require_numeric(frame: DataFrame, column: str, *, name: str) -> None:
    if not isinstance(frame.schema[column].dataType, NumericType):
        observed = frame.schema[column].dataType.simpleString()
        raise TypeError(f"{name}.{column} must be numeric; observed {observed}")


def _require_binding_k(k: int) -> None:
    if k != EVALUATION_K:
        raise ValueError("evaluation cutoff is binding and must be 10")


def build_evaluation_population(
    evaluation_users: DataFrame, product_catalog: DataFrame
) -> DataFrame:
    """Return the authoritative G6 population expanded to the three fixed slices.

    Each G6 row is emitted once for ``overall`` and once for its target-product group
    slice.  Exact duplicate input rows are collapsed, but conflicting targets or
    ratings for one ``(stage, cohort, customer_id)`` fail lazily when the result is
    materialized.  Cohorts are never inferred from model output.
    """

    _require_columns(
        evaluation_users,
        {
            "stage",
            "cohort",
            "customer_id",
            "target_product_id",
            "rating",
        },
        name="evaluation_users",
    )
    _require_columns(
        product_catalog, {"product_id", "group"}, name="product_catalog"
    )
    _require_numeric(evaluation_users, "rating", name="evaluation_users")

    targets = evaluation_users.groupBy(*_POPULATION_KEYS).agg(
        F.min("target_product_id").alias("target_product_id"),
        F.min(F.col("rating").cast("double")).alias("target_rating"),
        F.countDistinct(
            F.struct("target_product_id", F.col("rating").cast("double"))
        ).alias("_target_value_count"),
    )
    targets = targets.withColumn(
        "target_product_id",
        F.when(
            (F.col("_target_value_count") != F.lit(1))
            | F.col("target_product_id").isNull()
            | F.col("target_rating").isNull()
            | F.isnan("target_rating")
            | ~F.col("target_rating").between(4.0, 5.0),
            F.raise_error(
                F.lit(
                    "each ranking population key must have one positive held-out target"
                )
            ),
        ).otherwise(F.col("target_product_id")),
    ).drop("_target_value_count")

    groups = product_catalog.groupBy("product_id").agg(
        F.min("group").alias("target_group"),
        F.countDistinct("group").alias("_group_value_count"),
    )
    # Validate only products that are actual held-out targets.  Raising on the
    # complete catalog before this join incorrectly rejects the 5,868 legitimate
    # discontinued records whose source blocks have no group metadata, even though
    # none can enter the active evaluation target universe.
    classified = (
        targets.join(
            groups,
            targets.target_product_id == groups.product_id,
            "left",
        )
        .drop("product_id")
        .withColumn(
            "target_group",
            F.when(
                (F.col("_group_value_count") != F.lit(1))
                | F.col("target_group").isNull(),
                F.raise_error(
                    F.lit("every evaluation target must have one catalog product group")
                ),
            ).otherwise(F.col("target_group")),
        )
        .drop("_group_value_count")
    )
    overall = classified.withColumn("slice", F.lit(OVERALL_SLICE))
    by_group = classified.withColumn(
        "slice",
        F.when(F.col("target_group") == F.lit(BOOK_GROUP), F.lit(BOOK_GROUP)).otherwise(
            F.lit(NON_BOOK_SLICE)
        ),
    )
    return overall.unionByName(by_group).select(
        "stage",
        "cohort",
        "slice",
        "customer_id",
        "target_product_id",
        "target_rating",
        "target_group",
    )


def _canonical_recommendations(recommendations: DataFrame) -> DataFrame:
    _require_columns(
        recommendations,
        {"stage", "customer_id", "product_id", "rank"},
        name="recommendations",
    )
    if not isinstance(recommendations.schema["rank"].dataType, IntegralType):
        observed = recommendations.schema["rank"].dataType.simpleString()
        raise TypeError(f"recommendations.rank must be integral; observed {observed}")

    canonical = recommendations.groupBy(
        "stage", "customer_id", "product_id"
    ).agg(
        F.min(F.col("rank").cast("int")).alias("rank"),
        F.count(F.lit(1)).alias("_source_occurrences"),
    )
    return canonical.withColumn(
        "rank",
        F.when(
            (F.col("_source_occurrences") != F.lit(1))
            | F.col("stage").isNull()
            | F.col("customer_id").isNull()
            | F.col("product_id").isNull()
            | F.col("rank").isNull()
            | (F.col("rank") < F.lit(1)),
            F.raise_error(
                F.lit("recommendations must have unique non-null keys and positive ranks")
            ),
        ).otherwise(F.col("rank")),
    ).drop("_source_occurrences")


def evaluate_ranking_recommendations(
    recommendations: DataFrame,
    evaluation_users: DataFrame,
    product_catalog: DataFrame,
    active_catalog: DataFrame,
    *,
    model: str,
    k: int = EVALUATION_K,
) -> RankingEvaluationFrames:
    """Evaluate one frozen model list on G6 cohorts without survivor bias.

    ``UserCoverage`` and ``FillRate@10`` retain every sampled user.  Catalog
    coverage uses the exact active recommendable catalog as its denominator for all
    three target slices, following the global denominator in section 16.2.
    """

    _require_binding_k(k)
    if not isinstance(model, str) or not model.strip():
        raise ValueError("model must be a non-empty string")
    _require_columns(active_catalog, {"product_id"}, name="active_catalog")

    population = build_evaluation_population(evaluation_users, product_catalog)
    recs = _canonical_recommendations(recommendations)

    list_statistics = recs.groupBy(*_REQUEST_KEYS).agg(
        F.count(F.lit(1)).cast("long").alias("list_length"),
        F.sum(F.when(F.col("rank") <= F.lit(k), F.lit(1)).otherwise(F.lit(0)))
        .cast("long")
        .alias("top_k_list_length"),
    )
    target_ranks = (
        population.select(
            *_POPULATION_KEYS, "target_product_id"
        )
        .dropDuplicates()
        .alias("population")
        .join(
            recs.alias("recommendation"),
            (F.col("population.stage") == F.col("recommendation.stage"))
            & (
                F.col("population.customer_id")
                == F.col("recommendation.customer_id")
            )
            & (
                F.col("population.target_product_id")
                == F.col("recommendation.product_id")
            ),
            "left",
        )
        .groupBy(
            *[F.col(f"population.{key}") for key in _POPULATION_KEYS],
            F.col("population.target_product_id"),
        )
        .agg(F.min(F.col("recommendation.rank")).cast("int").alias("target_rank"))
    )

    per_user = (
        population.join(list_statistics, list(_REQUEST_KEYS), "left")
        .join(
            target_ranks,
            [*_POPULATION_KEYS, "target_product_id"],
            "left",
        )
        .fillna({"list_length": 0, "top_k_list_length": 0})
        .withColumn("model", F.lit(model.strip()))
        .withColumn("has_output", F.col("list_length") > F.lit(0))
        .withColumn(
            "hit_rate_at_10",
            F.when(F.col("target_rank").between(1, k), F.lit(1.0)).otherwise(
                F.lit(0.0)
            ),
        )
        .withColumn(
            "mrr_at_10",
            F.when(
                F.col("target_rank").between(1, k),
                F.lit(1.0) / F.col("target_rank").cast("double"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "ndcg_at_10",
            F.when(
                F.col("target_rank").between(1, k),
                F.lit(1.0)
                / F.log2(F.col("target_rank").cast("double") + F.lit(1.0)),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "fill_fraction_at_10",
            F.least(F.col("top_k_list_length"), F.lit(k)).cast("double")
            / F.lit(float(k)),
        )
        .select(
            "model",
            "stage",
            "cohort",
            "slice",
            "customer_id",
            "target_product_id",
            "target_rating",
            "target_group",
            "target_rank",
            "list_length",
            "top_k_list_length",
            "has_output",
            "ndcg_at_10",
            "hit_rate_at_10",
            "mrr_at_10",
            "fill_fraction_at_10",
        )
    )

    summary_base = per_user.groupBy("model", "stage", "cohort", "slice").agg(
        F.count(F.lit(1)).cast("long").alias("evaluated_users"),
        F.sum(F.col("has_output").cast("long")).alias("users_with_output"),
        F.avg("ndcg_at_10").alias("ndcg_at_10"),
        F.avg("hit_rate_at_10").alias("hit_rate_at_10"),
        F.avg("mrr_at_10").alias("mrr_at_10"),
        F.avg(F.col("has_output").cast("double")).alias("user_coverage"),
        F.avg("fill_fraction_at_10").alias("fill_rate_at_10"),
    )

    top_k_catalog_counts = (
        population.select("stage", "cohort", "slice", "customer_id")
        .join(recs.filter(F.col("rank") <= F.lit(k)), list(_REQUEST_KEYS), "inner")
        .groupBy("stage", "cohort", "slice")
        .agg(
            F.countDistinct("product_id")
            .cast("long")
            .alias("distinct_recommended_products_at_10")
        )
    )
    active_catalog_count = active_catalog.select("product_id").dropDuplicates().agg(
        F.count(F.lit(1)).cast("long").alias("active_catalog_size")
    )
    summary = (
        summary_base.join(
            top_k_catalog_counts, ["stage", "cohort", "slice"], "left"
        )
        .fillna({"distinct_recommended_products_at_10": 0})
        .crossJoin(active_catalog_count)
        .withColumn(
            "catalog_coverage_at_10",
            F.when(
                F.col("active_catalog_size") <= F.lit(0),
                F.raise_error(F.lit("active recommendable catalog must not be empty")),
            ).otherwise(
                F.col("distinct_recommended_products_at_10").cast("double")
                / F.col("active_catalog_size").cast("double")
            ),
        )
        .select(
            "model",
            "stage",
            "cohort",
            "slice",
            "evaluated_users",
            "users_with_output",
            "ndcg_at_10",
            "hit_rate_at_10",
            "mrr_at_10",
            "user_coverage",
            "fill_rate_at_10",
            "catalog_coverage_at_10",
            "distinct_recommended_products_at_10",
            "active_catalog_size",
        )
    )
    return RankingEvaluationFrames(per_user=per_user, summary=summary)


def evaluate_als_predictions(
    als_predictions: DataFrame,
) -> ALSPredictionEvaluationFrames:
    """Evaluate raw ALS predictions over every supplied held-out rating.

    RMSE/MAE denominators contain only rows with a finite prediction, matching
    ``coldStartStrategy='drop'``.  Prediction coverage and drop rate retain all
    held-out rows.  Predictions outside 1--5 are valid and are deliberately not
    clipped.  This official rating-prediction scope is separate from the sampled,
    positive-target ranking cohorts.
    """

    _require_columns(
        als_predictions,
        {
            "stage",
            "customer_id",
            "product_id",
            "rating",
            "als_prediction",
        },
        name="als_predictions",
    )
    _require_numeric(als_predictions, "rating", name="als_predictions")
    _require_numeric(
        als_predictions, "als_prediction", name="als_predictions"
    )

    base = als_predictions.select(
        "stage",
        "customer_id",
        "product_id",
        F.col("rating").cast("double").alias("rating"),
        F.col("als_prediction").cast("double").alias("als_prediction"),
        *(
            ["prediction_status"]
            if "prediction_status" in als_predictions.columns
            else []
        ),
        *(["is_predicted"] if "is_predicted" in als_predictions.columns else []),
    )
    has_prediction = F.col("als_prediction").isNotNull()
    invalid_rating = (
        F.col("rating").isNull()
        | F.isnan("rating")
        | ~F.col("rating").between(1.0, 5.0)
    )
    invalid_prediction = F.isnan("als_prediction")
    if "is_predicted" in base.columns:
        invalid_prediction = invalid_prediction | (
            F.col("is_predicted").isNull()
            | (F.col("is_predicted") != has_prediction)
        )

    per_prediction = (
        base.withColumn(
            "rating",
            F.when(
                F.col("stage").isNull()
                | F.col("customer_id").isNull()
                | F.col("product_id").isNull()
                | invalid_rating,
                F.raise_error(
                    F.lit("ALS held-out rows require valid keys and ratings in [1,5]")
                ),
            ).otherwise(F.col("rating")),
        )
        .withColumn(
            "als_prediction",
            F.when(
                invalid_prediction,
                F.raise_error(
                    F.lit("ALS prediction flags must match a finite raw prediction")
                ),
            ).otherwise(F.col("als_prediction")),
        )
        .withColumn("is_predicted", has_prediction)
        .withColumn(
            "absolute_error",
            F.when(
                has_prediction, F.abs(F.col("als_prediction") - F.col("rating"))
            ),
        )
        .withColumn(
            "squared_error",
            F.when(has_prediction, F.pow(F.col("als_prediction") - F.col("rating"), 2.0)),
        )
        .withColumn("model", F.lit("als"))
        .withColumn("prediction_scope", F.lit("all_heldout_ratings"))
    )

    summary = (
        per_prediction.groupBy("model", "stage", "prediction_scope")
        .agg(
            F.count(F.lit(1)).cast("long").alias("heldout_rows"),
            F.sum(F.col("is_predicted").cast("long")).alias("predicted_rows"),
            F.sqrt(F.avg("squared_error")).alias("rmse"),
            F.avg("absolute_error").alias("mae"),
        )
        .withColumn("dropped_rows", F.col("heldout_rows") - F.col("predicted_rows"))
        .withColumn(
            "prediction_coverage",
            F.col("predicted_rows").cast("double")
            / F.col("heldout_rows").cast("double"),
        )
        .withColumn(
            "drop_rate",
            F.col("dropped_rows").cast("double")
            / F.col("heldout_rows").cast("double"),
        )
        .select(
            "model",
            "stage",
            "prediction_scope",
            "heldout_rows",
            "predicted_rows",
            "dropped_rows",
            "prediction_coverage",
            "drop_rate",
            "rmse",
            "mae",
        )
    )
    return ALSPredictionEvaluationFrames(
        per_prediction=per_prediction,
        summary=summary,
    )
