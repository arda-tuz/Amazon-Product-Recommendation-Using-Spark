"""Binding FP-Growth association recommender.

The baskets consumed here are *positive review histories*, not purchase baskets.  The
implementation deliberately fits Spark MLlib once and scores users by joining the
bounded singleton-rule table to their positive training preferences.  In particular,
``FPGrowthModel.transform`` is not applied to the complete customer population.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Final, Mapping

from pyspark.ml.fpm import FPGrowth, FPGrowthModel
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType

from amazon_recommender.models.math import fp_minimum_count


MIN_SUPPORT_FRACTION: Final[float] = 0.001
MIN_SUPPORT_COUNT: Final[int] = 200
MIN_CONFIDENCE: Final[float] = 0.05
MIN_LIFT: Final[float] = 1.10
NUM_PARTITIONS: Final[int] = 64
MIN_BASKET_SIZE: Final[int] = 2
MAX_BASKET_SIZE: Final[int] = 50
MAX_RULES_PER_ANTECEDENT: Final[int] = 20
CANDIDATE_DEPTH: Final[int] = 50


@dataclass(frozen=True, slots=True)
class FPGrowthArtifacts:
    """The fitted model and all reproducibility-critical FP outputs."""

    model: FPGrowthModel
    rules: DataFrame
    recommendations: DataFrame
    parameters: Mapping[str, Any]


def _require_columns(frame: DataFrame, frame_name: str, *columns: str) -> None:
    missing = sorted(set(columns).difference(frame.columns))
    if missing:
        raise ValueError(f"{frame_name} is missing required column(s): {missing}")


def _validated_basket_count(baskets: DataFrame) -> int:
    """Return exact ``B`` after enforcing the upstream 2--50 basket contract."""

    _require_columns(baskets, "positive_user_baskets", "customer_id", "items")
    items_field = baskets.schema["items"]
    if not isinstance(items_field.dataType, ArrayType):
        raise TypeError("positive_user_baskets.items must be an array")

    invalid_items = (
        F.col("items").isNull()
        | F.exists("items", lambda item: item.isNull())
        | (F.size("items") < F.lit(MIN_BASKET_SIZE))
        | (F.size("items") > F.lit(MAX_BASKET_SIZE))
        | (F.size(F.array_distinct("items")) != F.size("items"))
    )
    summary = baskets.agg(
        F.count(F.lit(1)).cast("long").alias("basket_count"),
        F.countDistinct("customer_id").cast("long").alias("customer_count"),
        F.sum(
            F.when(F.col("customer_id").isNull() | invalid_items, F.lit(1)).otherwise(
                F.lit(0)
            )
        )
        .cast("long")
        .alias("invalid_baskets"),
    ).first()
    basket_count = int(summary["basket_count"])
    if basket_count == 0:
        raise ValueError("positive_user_baskets must contain at least one basket")
    if int(summary["customer_count"]) != basket_count:
        raise ValueError("positive_user_baskets must contain exactly one basket per customer")
    if int(summary["invalid_baskets"] or 0):
        raise ValueError(
            "positive_user_baskets must contain unique, non-null arrays of 2 to 50 items"
        )
    return basket_count


def build_singleton_rules(
    association_rules: DataFrame,
    frequent_itemsets: DataFrame,
    *,
    basket_count: int,
) -> DataFrame:
    """Filter, score, and deterministically cap singleton association rules.

    Exact pair support comes from ``freqItemsets.freq`` rather than recovering a
    count from a floating-point support fraction.
    """

    _require_columns(
        association_rules,
        "association_rules",
        "antecedent",
        "consequent",
        "confidence",
        "lift",
    )
    _require_columns(frequent_itemsets, "frequent_itemsets", "items", "freq")
    if basket_count < 1:
        raise ValueError("basket_count must be >= 1")

    pair_counts = (
        frequent_itemsets.filter(F.size("items") == F.lit(2))
        .select(
            F.sort_array("items").alias("_pair"),
            F.col("freq").cast("long").alias("support_count"),
        )
        .dropDuplicates(["_pair"])
    )
    singleton = (
        association_rules.filter(
            (F.size("antecedent") == F.lit(1))
            & (F.size("consequent") == F.lit(1))
            & (F.col("confidence") >= F.lit(MIN_CONFIDENCE))
            & (F.col("lift") >= F.lit(MIN_LIFT))
        )
        .select(
            F.element_at("antecedent", 1).cast("long").alias(
                "antecedent_product_id"
            ),
            F.element_at("consequent", 1).cast("long").alias(
                "consequent_product_id"
            ),
            F.col("confidence").cast("double").alias("confidence"),
            F.col("lift").cast("double").alias("lift"),
        )
        .withColumn(
            "_pair",
            F.sort_array(
                F.array("antecedent_product_id", "consequent_product_id")
            ),
        )
        .join(pair_counts, "_pair", "inner")
        .drop("_pair")
        .withColumn(
            "support", F.col("support_count").cast("double") / F.lit(basket_count)
        )
        .withColumn(
            "rule_strength", F.col("confidence") * F.log2(F.col("lift"))
        )
    )
    rule_order = Window.partitionBy("antecedent_product_id").orderBy(
        F.col("rule_strength").desc(),
        F.col("support_count").desc(),
        F.col("consequent_product_id").asc(),
    )
    return (
        singleton.withColumn("rule_rank", F.row_number().over(rule_order))
        .filter(F.col("rule_rank") <= F.lit(MAX_RULES_PER_ANTECEDENT))
        .select(
            "antecedent_product_id",
            "consequent_product_id",
            "confidence",
            "lift",
            "support_count",
            "support",
            "rule_strength",
            "rule_rank",
        )
    )


def score_fp_recommendations(
    rules: DataFrame,
    positive_preferences: DataFrame,
    recommendation_users: DataFrame,
    active_catalog: DataFrame,
    stage_seen_items: DataFrame,
    item_bayesian_scores: DataFrame,
) -> DataFrame:
    """Score and rank active unseen candidates for requested stage/user pairs."""

    _require_columns(
        rules,
        "rules",
        "antecedent_product_id",
        "consequent_product_id",
        "support_count",
        "rule_strength",
    )
    _require_columns(
        positive_preferences,
        "positive_preferences",
        "customer_id",
        "product_id",
        "q_ui",
    )
    _require_columns(
        recommendation_users, "recommendation_users", "stage", "customer_id"
    )
    _require_columns(active_catalog, "active_catalog", "product_id")
    _require_columns(
        stage_seen_items, "stage_seen_items", "stage", "customer_id", "product_id"
    )
    _require_columns(
        item_bayesian_scores,
        "item_bayesian_scores",
        "product_id",
        "bayesian_score",
    )

    # Cohort membership is intentionally not part of the recommendation key.  G9 can
    # attach one or more cohorts without causing duplicate model fitting or scoring.
    users = recommendation_users.select("stage", "customer_id").dropDuplicates()
    preferences = (
        positive_preferences.select(
            "customer_id",
            F.col("product_id").cast("long").alias("liked_product_id"),
            F.col("q_ui").cast("double").alias("q_ui"),
        )
        # q_ui >= 0.5 is exactly rating >= 4 under q=clip((rating-3)/2,0,1).
        # Keeping this guard here prevents a caller from accidentally admitting a
        # fractional, non-positive averaged rating into the liked-item join.
        .filter(F.col("q_ui") >= F.lit(0.5))
        .groupBy("customer_id", "liked_product_id")
        .agg(F.max("q_ui").alias("q_ui"))
    )
    matched = (
        users.join(preferences, "customer_id", "inner")
        .join(
            rules,
            F.col("liked_product_id") == F.col("antecedent_product_id"),
            "inner",
        )
        .select(
            "stage",
            "customer_id",
            F.col("consequent_product_id").alias("product_id"),
            "antecedent_product_id",
            "q_ui",
            "rule_strength",
            "support_count",
        )
    )
    candidates = matched.groupBy("stage", "customer_id", "product_id").agg(
        F.sum(F.col("q_ui") * F.col("rule_strength")).alias("fp_score"),
        F.sum("support_count").cast("long").alias("aggregate_support_count"),
        F.countDistinct("antecedent_product_id")
        .cast("int")
        .alias("contributing_antecedent_count"),
    )

    active = active_catalog.select(F.col("product_id").cast("long")).dropDuplicates()
    scores = (
        item_bayesian_scores.select(
            F.col("product_id").cast("long"),
            F.col("bayesian_score").cast("double"),
        )
        .groupBy("product_id")
        .agg(F.max("bayesian_score").alias("bayesian_score"))
    )
    seen = stage_seen_items.select(
        "stage", "customer_id", F.col("product_id").cast("long").alias("product_id")
    ).dropDuplicates()
    eligible = (
        candidates.join(active, "product_id", "inner")
        .join(seen, ["stage", "customer_id", "product_id"], "left_anti")
        .join(scores, "product_id", "inner")
    )
    candidate_order = Window.partitionBy("stage", "customer_id").orderBy(
        F.col("fp_score").desc(),
        F.col("aggregate_support_count").desc(),
        F.col("bayesian_score").desc(),
        F.col("product_id").asc(),
    )
    return (
        eligible.withColumn("rank", F.row_number().over(candidate_order))
        .filter(F.col("rank") <= F.lit(CANDIDATE_DEPTH))
        .select(
            "stage",
            "customer_id",
            "product_id",
            "rank",
            "fp_score",
            "aggregate_support_count",
            "contributing_antecedent_count",
            "bayesian_score",
        )
    )


def fit_fp_growth(
    positive_user_baskets: DataFrame,
    positive_preferences: DataFrame,
    recommendation_users: DataFrame,
    active_catalog: DataFrame,
    stage_seen_items: DataFrame,
    item_bayesian_scores: DataFrame,
) -> FPGrowthArtifacts:
    """Fit exactly one binding FP-Growth model and produce top-50 candidates."""

    basket_count = _validated_basket_count(positive_user_baskets)
    minimum_count = fp_minimum_count(basket_count)
    if minimum_count > basket_count:
        raise ValueError(
            "the binding minimum support count (200) exceeds the basket universe"
        )
    min_support = minimum_count / basket_count

    estimator = FPGrowth(
        itemsCol="items",
        minSupport=min_support,
        minConfidence=MIN_CONFIDENCE,
        numPartitions=NUM_PARTITIONS,
    )
    # This is the sole fit call.  Downstream scoring uses the extracted rules via join.
    model = estimator.fit(positive_user_baskets.select("items"))
    rules = build_singleton_rules(
        model.associationRules,
        model.freqItemsets,
        basket_count=basket_count,
    )
    recommendations = score_fp_recommendations(
        rules,
        positive_preferences,
        recommendation_users,
        active_catalog,
        stage_seen_items,
        item_bayesian_scores,
    )
    parameters: dict[str, Any] = {
        "basket_count": basket_count,
        "minimum_count": minimum_count,
        "min_support": min_support,
        "min_support_fraction": MIN_SUPPORT_FRACTION,
        "min_support_count_floor": MIN_SUPPORT_COUNT,
        "min_confidence": MIN_CONFIDENCE,
        "min_lift": MIN_LIFT,
        "num_partitions": NUM_PARTITIONS,
        "min_basket_size": MIN_BASKET_SIZE,
        "max_basket_size": MAX_BASKET_SIZE,
        "max_rules_per_antecedent": MAX_RULES_PER_ANTECEDENT,
        "candidate_depth": CANDIDATE_DEPTH,
        "fit_count": 1,
    }
    return FPGrowthArtifacts(model, rules, recommendations, parameters)
