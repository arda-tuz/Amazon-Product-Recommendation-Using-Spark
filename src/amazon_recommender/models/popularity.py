"""Train-only Bayesian popularity model and deterministic recommendations.

The implementation deliberately accepts only the constants fixed by the binding
project specification.  In particular, callers cannot turn the production path
into an unreported hyperparameter experiment by changing ``m`` or list depths.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


BAYESIAN_M = 20
GROUP_FALLBACK_MIN_INTERACTIONS = 100
GLOBAL_CATALOG_DEPTH = 1_000
POPULARITY_CANDIDATE_DEPTH = 100


def _require_columns(frame: DataFrame, required: set[str], *, name: str) -> None:
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"{name} is missing required columns: {missing}")


def _require_binding_value(name: str, actual: int, expected: int) -> None:
    if actual != expected:
        raise ValueError(
            f"{name} is binding and must be {expected}; received {actual}"
        )


def build_popularity_scores(
    train_interactions: DataFrame,
    product_groups: DataFrame,
    *,
    m: int = BAYESIAN_M,
    group_min_train_interactions: int = GROUP_FALLBACK_MIN_INTERACTIONS,
) -> DataFrame:
    """Compute the three specified popularity views from training rows only.

    ``train_interactions`` must contain the already deduplicated and aggregated
    customer-product interactions.  ``product_groups`` supplies the static catalog
    group for each product.  The global prior ``C`` is calculated over *all* input
    training interactions; it is not recomputed after the metadata join.

    The returned frame has one row per rated product with reviewer-count, global
    Bayesian and group-Bayesian ranks.  A group with fewer than 100 training
    interactions (including a missing group) uses the global prior exactly.
    """

    _require_binding_value("Bayesian m", m, BAYESIAN_M)
    _require_binding_value(
        "group fallback threshold",
        group_min_train_interactions,
        GROUP_FALLBACK_MIN_INTERACTIONS,
    )
    _require_columns(
        train_interactions,
        {"customer_id", "product_id", "rating"},
        name="train_interactions",
    )
    _require_columns(product_groups, {"product_id", "group"}, name="product_groups")

    # Preserve the catalog's one-product/one-group contract even if a caller passes
    # a wider table.  A conflicting duplicate group remains visible as two rows and
    # should be rejected by the gate's data-contract validation, not selected here.
    groups = product_groups.select("product_id", "group").dropDuplicates()
    global_prior = train_interactions.agg(
        F.avg(F.col("rating").cast("double")).alias("global_mean_rating"),
        F.count(F.lit(1)).cast("long").alias("global_interaction_count"),
    )
    item_statistics = train_interactions.groupBy("product_id").agg(
        F.avg(F.col("rating").cast("double")).alias("item_mean_rating"),
        F.countDistinct("customer_id").cast("long").alias("unique_reviewers"),
        F.count(F.lit(1)).cast("long").alias("item_interaction_count"),
    )

    interactions_with_group = train_interactions.join(groups, "product_id", "left")
    group_statistics = interactions_with_group.groupBy("group").agg(
        F.avg(F.col("rating").cast("double")).alias("group_mean_rating"),
        F.count(F.lit(1)).cast("long").alias("group_interaction_count"),
    )

    scored = (
        item_statistics.join(groups, "product_id", "left")
        .join(group_statistics, "group", "left")
        .crossJoin(global_prior)
        .withColumn(
            "group_uses_global_fallback",
            F.col("group_interaction_count").isNull()
            | (
                F.col("group_interaction_count")
                < F.lit(GROUP_FALLBACK_MIN_INTERACTIONS)
            ),
        )
        .withColumn(
            "group_prior_rating",
            F.when(
                F.col("group_uses_global_fallback"), F.col("global_mean_rating")
            ).otherwise(F.col("group_mean_rating")),
        )
        .withColumn(
            "global_bayesian_score",
            (
                F.col("unique_reviewers") * F.col("item_mean_rating")
                + F.lit(float(BAYESIAN_M)) * F.col("global_mean_rating")
            )
            / (F.col("unique_reviewers") + F.lit(float(BAYESIAN_M))),
        )
        .withColumn(
            "group_bayesian_score",
            (
                F.col("unique_reviewers") * F.col("item_mean_rating")
                + F.lit(float(BAYESIAN_M)) * F.col("group_prior_rating")
            )
            / (F.col("unique_reviewers") + F.lit(float(BAYESIAN_M))),
        )
    )

    reviewer_order = Window.orderBy(
        F.col("unique_reviewers").desc(),
        F.col("global_bayesian_score").desc(),
        F.col("product_id").asc(),
    )
    global_order = Window.orderBy(
        F.col("global_bayesian_score").desc(),
        F.col("unique_reviewers").desc(),
        F.col("product_id").asc(),
    )
    group_order = Window.partitionBy("group").orderBy(
        F.col("group_bayesian_score").desc(),
        F.col("unique_reviewers").desc(),
        F.col("product_id").asc(),
    )
    return (
        scored.withColumn("reviewer_count_rank", F.row_number().over(reviewer_order))
        .withColumn("global_bayesian_rank", F.row_number().over(global_order))
        .withColumn("group_bayesian_rank", F.row_number().over(group_order))
    )


def build_active_global_popularity_catalog(
    popularity_scores: DataFrame,
    active_catalog: DataFrame,
    *,
    catalog_depth: int = GLOBAL_CATALOG_DEPTH,
) -> DataFrame:
    """Materialize the deterministic top-1,000 active global Bayesian products."""

    _require_binding_value("global popularity catalog depth", catalog_depth, GLOBAL_CATALOG_DEPTH)
    _require_columns(
        popularity_scores,
        {"product_id", "global_bayesian_score", "unique_reviewers"},
        name="popularity_scores",
    )
    _require_columns(active_catalog, {"product_id"}, name="active_catalog")

    catalog = active_catalog
    if "is_active" in catalog.columns:
        catalog = catalog.filter(F.col("is_active"))
    active_ids = catalog.select("product_id").dropDuplicates(["product_id"])
    active_scores = popularity_scores.join(active_ids, "product_id", "inner")
    ordering = Window.orderBy(
        F.col("global_bayesian_score").desc(),
        F.col("unique_reviewers").desc(),
        F.col("product_id").asc(),
    )
    return (
        active_scores.withColumn("popularity_rank", F.row_number().over(ordering))
        .filter(F.col("popularity_rank") <= F.lit(GLOBAL_CATALOG_DEPTH))
    )


def generate_popularity_recommendations(
    global_popularity_catalog: DataFrame,
    evaluation_users: DataFrame,
    stage_seen_items: DataFrame,
    *,
    candidate_depth: int = POPULARITY_CANDIDATE_DEPTH,
) -> DataFrame:
    """Return stage-aware unseen top-100 recommendations for each request.

    A request is the unique ``(stage, customer_id)`` pair.  Cohort duplicates in
    ``evaluation_users`` intentionally share the same persisted recommendation
    list.  Seen products are removed *before* the result is re-ranked, so ranks are
    dense from one through at most 100 for every request.
    """

    _require_binding_value(
        "popularity recommendation depth", candidate_depth, POPULARITY_CANDIDATE_DEPTH
    )
    _require_columns(
        global_popularity_catalog,
        {
            "product_id",
            "global_bayesian_score",
            "unique_reviewers",
            "popularity_rank",
        },
        name="global_popularity_catalog",
    )
    _require_columns(
        evaluation_users, {"stage", "customer_id"}, name="evaluation_users"
    )
    _require_columns(
        stage_seen_items,
        {"stage", "customer_id", "product_id"},
        name="stage_seen_items",
    )

    requests = evaluation_users.select("stage", "customer_id").dropDuplicates()
    candidates = requests.crossJoin(global_popularity_catalog)
    seen = stage_seen_items.select(
        "stage", "customer_id", "product_id"
    ).dropDuplicates()
    unseen = candidates.join(
        seen, ["stage", "customer_id", "product_id"], "left_anti"
    )
    order = Window.partitionBy("stage", "customer_id").orderBy(
        F.col("global_bayesian_score").desc(),
        F.col("unique_reviewers").desc(),
        F.col("product_id").asc(),
    )
    return (
        unseen.withColumn("recommendation_rank", F.row_number().over(order))
        .filter(F.col("recommendation_rank") <= F.lit(POPULARITY_CANDIDATE_DEPTH))
        .select(
            "stage",
            "customer_id",
            "product_id",
            F.col("global_bayesian_score").alias("model_score"),
            "unique_reviewers",
            "popularity_rank",
            "recommendation_rank",
        )
    )
