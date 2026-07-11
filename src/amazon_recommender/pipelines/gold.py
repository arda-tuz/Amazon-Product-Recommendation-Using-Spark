"""Leakage-safe temporal split and base Gold feature contracts."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from amazon_recommender.pipelines.bronze import BronzeFrames
from amazon_recommender.pipelines.silver import SilverFrames


@dataclass(frozen=True)
class GoldFrames:
    train_interactions: DataFrame
    validation_interactions: DataFrame
    test_interactions: DataFrame
    active_catalog: DataFrame
    positive_user_baskets: DataFrame
    user_profiles: DataFrame
    item_features: DataFrame
    stage_seen_items: DataFrame
    cohort_candidates: DataFrame
    smoke_selection: DataFrame

    def as_dict(self) -> dict[str, DataFrame]:
        return {name: getattr(self, name) for name in self.__dataclass_fields__}


def temporal_split(
    interactions: DataFrame, *, evaluation_min_distinct_items: int = 5
) -> tuple[DataFrame, DataFrame, DataFrame]:
    valid = interactions.filter(F.col("interaction_date").isNotNull())
    eligible = (
        valid.groupBy("customer_id")
        .agg(F.countDistinct("product_id").alias("_dated_items"))
        .filter(F.col("_dated_items") >= F.lit(evaluation_min_distinct_items))
        .select("customer_id")
        .withColumn("_eligible", F.lit(True))
    )
    order = Window.partitionBy("customer_id").orderBy(
        F.col("interaction_date").asc(), F.col("product_id").asc()
    )
    population = Window.partitionBy("customer_id")
    marked = (
        interactions.join(eligible, "customer_id", "left")
        .withColumn("_position", F.row_number().over(order))
        .withColumn("_count", F.count(F.lit(1)).over(population))
    )
    helper_columns = ("_eligible", "_position", "_count")
    validation = marked.filter(
        F.coalesce(F.col("_eligible"), F.lit(False))
        & F.col("interaction_date").isNotNull()
        & (F.col("_position") == F.col("_count") - F.lit(1))
    ).drop(*helper_columns)
    test = marked.filter(
        F.coalesce(F.col("_eligible"), F.lit(False))
        & F.col("interaction_date").isNotNull()
        & (F.col("_position") == F.col("_count"))
    ).drop(*helper_columns)
    train = marked.filter(
        (~F.coalesce(F.col("_eligible"), F.lit(False)))
        | F.col("interaction_date").isNull()
        | (F.col("_position") <= F.col("_count") - F.lit(2))
    ).drop(*helper_columns)
    return train, validation, test


def build_gold(
    bronze: BronzeFrames,
    silver: SilverFrames,
    *,
    evaluation_min_distinct_items: int = 5,
) -> GoldFrames:
    train, validation, test = temporal_split(
        silver.user_item_interactions,
        evaluation_min_distinct_items=evaluation_min_distinct_items,
    )
    active_catalog = silver.products.filter(F.col("is_active")).select(
        "product_id",
        "asin",
        "title",
        "group",
        "salesrank_clean",
        "avg_rating_raw",
        "reviews_downloaded",
    )
    positive_train = train.filter(F.col("is_positive"))
    basket_order = Window.partitionBy("customer_id").orderBy(
        F.col("interaction_date").desc_nulls_last(), F.col("product_id").asc()
    )
    positive_ranked = positive_train.withColumn("_recent_rank", F.row_number().over(basket_order))
    positive_user_baskets = (
        positive_ranked.filter(F.col("_recent_rank") <= 50)
        .groupBy("customer_id")
        .agg(
            F.sort_array(F.collect_set("product_id")).alias("items"),
            F.countDistinct("product_id").alias("basket_size"),
        )
        .filter(F.col("basket_size") >= 2)
    )
    user_profiles = positive_train.groupBy("customer_id").agg(
        F.collect_list(
            F.struct("product_id", "q_ui", "interaction_date", "rating")
        ).alias("positive_history"),
        F.countDistinct("product_id").alias("positive_item_count"),
        F.max("interaction_date").alias("profile_as_of"),
    )
    item_features = silver.product_category_nodes.join(
        active_catalog.select("product_id"), "product_id", "inner"
    )

    train_seen = train.select("customer_id", "product_id").withColumn(
        "stage", F.lit("validation")
    )
    test_seen = train.select("customer_id", "product_id").unionByName(
        validation.select("customer_id", "product_id")
    ).withColumn("stage", F.lit("test"))
    stage_seen_items = train_seen.unionByName(test_seen).dropDuplicates(
        ["stage", "customer_id", "product_id"]
    )

    target_columns = ["customer_id", "product_id", "rating", "is_positive"]
    validation_targets = validation.select(*target_columns).withColumn(
        "stage", F.lit("validation")
    )
    test_targets = test.select(*target_columns).withColumn("stage", F.lit("test"))
    targets = validation_targets.unionByName(test_targets)
    operational = (
        targets.filter(F.col("is_positive"))
        .join(
            active_catalog.select(F.col("product_id").alias("active_product_id")),
            F.col("product_id") == F.col("active_product_id"),
            "inner",
        )
        .drop("active_product_id")
        .withColumn("cohort", F.lit("operational"))
    )
    positive_users = positive_train.select("customer_id").distinct()
    common_warm = operational.join(positive_users, "customer_id", "inner").withColumn(
        "cohort", F.lit("common_warm")
    )
    cohort_candidates = operational.unionByName(common_warm).select(
        "stage", "cohort", "customer_id", F.col("product_id").alias("target_product_id"), "rating"
    )

    return GoldFrames(
        train,
        validation,
        test,
        active_catalog,
        positive_user_baskets,
        user_profiles,
        item_features,
        stage_seen_items,
        cohort_candidates,
        bronze.smoke_selection,
    )
