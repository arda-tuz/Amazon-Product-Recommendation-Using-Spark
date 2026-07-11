"""Train-only sparse category recommender with the binding TF-IDF-like weights."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


@dataclass(frozen=True)
class CategoryFeatureFrames:
    item_vectors: DataFrame
    category_statistics: DataFrame
    item_norms: DataFrame


@dataclass(frozen=True)
class CategoryUserFrames:
    user_category_profiles: DataFrame
    user_norms: DataFrame
    user_group_affinity: DataFrame


def build_category_item_features(
    item_features: DataFrame, active_catalog_size: int
) -> CategoryFeatureFrames:
    if active_catalog_size <= 0:
        raise ValueError("active_catalog_size must be positive")
    unique = item_features.groupBy("product_id", "category_id").agg(
        F.max("normalized_depth_weight").alias("depth_weight")
    )
    stats = unique.groupBy("category_id").agg(
        F.countDistinct("product_id").cast("long").alias("document_frequency")
    ).withColumn(
        "idf",
        F.log(
            (F.lit(float(active_catalog_size)) + F.lit(1.0))
            / (F.col("document_frequency").cast("double") + F.lit(1.0))
        )
        + F.lit(1.0),
    ).withColumn(
        "document_ratio",
        F.col("document_frequency").cast("double")
        / F.lit(float(active_catalog_size)),
    )
    vectors = unique.join(stats, "category_id", "inner").withColumn(
        "item_category_weight", F.col("idf") * F.col("depth_weight")
    )
    norms = vectors.groupBy("product_id").agg(
        F.sqrt(F.sum(F.pow("item_category_weight", 2.0))).alias("item_norm")
    )
    return CategoryFeatureFrames(vectors, stats, norms)


def build_category_user_features(
    train: DataFrame,
    request_users: DataFrame,
    item_vectors: DataFrame,
    active_catalog: DataFrame,
) -> CategoryUserFrames:
    liked = (
        train.filter(F.col("is_positive"))
        .join(request_users.select("customer_id").distinct(), "customer_id", "inner")
        .select("customer_id", "product_id", "q_ui")
    )
    profiles = (
        liked.join(
            item_vectors.select(
                "product_id", "category_id", "item_category_weight"
            ),
            "product_id",
            "inner",
        )
        .groupBy("customer_id", "category_id")
        .agg(
            F.sum(F.col("q_ui") * F.col("item_category_weight")).alias(
                "profile_weight"
            )
        )
    )
    norms = profiles.groupBy("customer_id").agg(
        F.sqrt(F.sum(F.pow("profile_weight", 2.0))).alias("user_norm")
    )
    group_weights = (
        liked.join(active_catalog.select("product_id", "group"), "product_id", "inner")
        .groupBy("customer_id", "group")
        .agg(F.sum("q_ui").alias("group_preference_weight"))
    )
    totals = group_weights.groupBy("customer_id").agg(
        F.sum("group_preference_weight").alias("total_preference_weight")
    )
    affinity = group_weights.join(totals, "customer_id", "inner").withColumn(
        "group_affinity",
        F.col("group_preference_weight") / F.col("total_preference_weight"),
    )
    return CategoryUserFrames(profiles, norms, affinity)


def build_category_top_products(
    item_vectors: DataFrame,
    popularity_scores: DataFrame,
    *,
    generic_category_ratio: float = 0.10,
    products_per_category: int = 200,
) -> DataFrame:
    candidates = item_vectors.filter(
        F.col("document_ratio") <= F.lit(generic_category_ratio)
    ).join(
        popularity_scores.select(
            "product_id", "bayesian_score", "popularity_percentile", "rater_count"
        ),
        "product_id",
        "inner",
    )
    ranking = Window.partitionBy("category_id").orderBy(
        F.col("bayesian_score").desc(),
        F.col("rater_count").desc(),
        F.col("product_id").asc(),
    )
    return (
        candidates.withColumn("category_product_rank", F.row_number().over(ranking))
        .filter(F.col("category_product_rank") <= F.lit(products_per_category))
        .select(
            "category_id",
            "product_id",
            "bayesian_score",
            "popularity_percentile",
            "rater_count",
            "category_product_rank",
        )
    )


def build_category_candidate_pool(
    user_profiles: DataFrame,
    category_top_products: DataFrame,
    popularity_scores: DataFrame,
    *,
    max_profile_categories: int = 20,
    max_candidate_pool: int = 5_000,
) -> DataFrame:
    strongest = Window.partitionBy("customer_id").orderBy(
        F.col("profile_weight").desc(), F.col("category_id").asc()
    )
    top_categories = (
        user_profiles.withColumn("profile_category_rank", F.row_number().over(strongest))
        .filter(F.col("profile_category_rank") <= F.lit(max_profile_categories))
    )
    pool = (
        top_categories.join(category_top_products, "category_id", "inner")
        .groupBy("customer_id", "product_id")
        .agg(
            F.max("bayesian_score").alias("bayesian_score"),
            F.max("popularity_percentile").alias("popularity_percentile"),
            F.max("rater_count").alias("rater_count"),
            F.countDistinct("category_id").cast("long").alias(
                "matched_seed_categories"
            ),
        )
    )
    ranking = Window.partitionBy("customer_id").orderBy(
        F.col("bayesian_score").desc(),
        F.col("rater_count").desc(),
        F.col("product_id").asc(),
    )
    return (
        pool.withColumn("candidate_pool_rank", F.row_number().over(ranking))
        .filter(F.col("candidate_pool_rank") <= F.lit(max_candidate_pool))
    )


def score_category_candidate_pool(
    candidate_pool: DataFrame,
    user_profiles: DataFrame,
    user_norms: DataFrame,
    item_vectors: DataFrame,
    item_norms: DataFrame,
    user_group_affinity: DataFrame,
    active_catalog: DataFrame,
    *,
    similarity_weight: float = 0.80,
    group_affinity_weight: float = 0.10,
    popularity_percentile_weight: float = 0.10,
) -> DataFrame:
    candidate_components = candidate_pool.select(
        "customer_id", "product_id"
    ).join(
        item_vectors.select(
            "product_id", "category_id", "item_category_weight"
        ),
        "product_id",
        "inner",
    )
    dot_products = (
        candidate_components.join(
            user_profiles,
            ["customer_id", "category_id"],
            "inner",
        )
        .groupBy("customer_id", "product_id")
        .agg(
            F.sum(
                F.col("profile_weight") * F.col("item_category_weight")
            ).alias("dot_product")
        )
    )
    scored = (
        candidate_pool.join(dot_products, ["customer_id", "product_id"], "left")
        .join(user_norms, "customer_id", "inner")
        .join(item_norms, "product_id", "inner")
        .join(active_catalog.select("product_id", "group"), "product_id", "inner")
        .join(
            user_group_affinity.select(
                "customer_id", "group", "group_affinity"
            ),
            ["customer_id", "group"],
            "left",
        )
        .fillna({"dot_product": 0.0, "group_affinity": 0.0})
        .filter((F.col("user_norm") > 0.0) & (F.col("item_norm") > 0.0))
        .withColumn(
            "category_similarity",
            F.col("dot_product") / (F.col("user_norm") * F.col("item_norm")),
        )
        .withColumn(
            "category_score",
            F.lit(similarity_weight) * F.col("category_similarity")
            + F.lit(group_affinity_weight) * F.col("group_affinity")
            + F.lit(popularity_percentile_weight)
            * F.col("popularity_percentile"),
        )
    )
    return scored


def rank_category_recommendations(
    scored_candidates: DataFrame,
    evaluation_requests: DataFrame,
    stage_seen_items: DataFrame,
    *,
    candidate_depth: int = 50,
) -> DataFrame:
    stage_candidates = evaluation_requests.select(
        "stage", "customer_id"
    ).distinct().join(scored_candidates, "customer_id", "inner")
    unseen = stage_candidates.join(
        stage_seen_items.select("stage", "customer_id", "product_id"),
        ["stage", "customer_id", "product_id"],
        "left_anti",
    )
    ranking = Window.partitionBy("stage", "customer_id").orderBy(
        F.col("category_score").desc(),
        F.col("bayesian_score").desc(),
        F.col("rater_count").desc(),
        F.col("product_id").asc(),
    )
    return (
        unseen.withColumn("rank", F.row_number().over(ranking))
        .filter(F.col("rank") <= F.lit(candidate_depth))
        .select(
            "stage",
            "customer_id",
            "product_id",
            "rank",
            "category_score",
            "category_similarity",
            "group_affinity",
            "popularity_percentile",
            "bayesian_score",
            "rater_count",
            "matched_seed_categories",
        )
    )
