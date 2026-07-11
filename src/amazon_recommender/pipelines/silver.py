"""Lossless normalization and binding cleaning order for Silver tables."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from amazon_recommender.pipelines.bronze import BronzeFrames


@dataclass(frozen=True)
class SilverFrames:
    products: DataFrame
    reviews_raw: DataFrame
    reviews_deduplicated: DataFrame
    user_item_interactions: DataFrame
    customers: DataFrame
    similar_edges: DataFrame
    category_paths: DataFrame
    product_category_nodes: DataFrame
    category_nodes: DataFrame
    category_edges: DataFrame
    data_quality_events: DataFrame

    def as_dict(self) -> dict[str, DataFrame]:
        return {name: getattr(self, name) for name in self.__dataclass_fields__}


def build_category_edges(category_paths: DataFrame) -> DataFrame:
    """Count adjacent taxonomy segments without a memory-heavy self join."""

    adjacent = (
        category_paths.filter(F.size("segments") >= F.lit(2))
        .select(
            "segments",
            F.explode(
                F.sequence(F.lit(1), F.size("segments") - F.lit(1))
            ).alias("segment_index"),
        )
        .select(
            F.element_at("segments", F.col("segment_index"))[
                "category_id"
            ].alias("parent_category_id"),
            F.element_at("segments", F.col("segment_index") + F.lit(1))[
                "category_id"
            ].alias("child_category_id"),
        )
    )
    return adjacent.groupBy("parent_category_id", "child_category_id").agg(
        F.count(F.lit(1)).cast("long").alias("path_occurrences")
    )


def build_silver(bronze: BronzeFrames) -> SilverFrames:
    products = bronze.products.select(
        "product_id",
        "asin",
        "title",
        F.col("product_group").alias("group"),
        "salesrank_raw",
        F.when(F.col("salesrank_raw") > 0, F.col("salesrank_raw")).alias(
            "salesrank_clean"
        ),
        "status",
        F.col("status").eqNullSafe("active").alias("is_active"),
        "reviews_total",
        "reviews_downloaded",
        "avg_rating_raw",
        "similar_declared",
        "categories_declared",
        "source_offset",
        "source_block_sha256",
    )

    reviews_raw = (
        bronze.products.select(
            "product_id", "asin", "source_offset", F.explode("reviews").alias("review")
        )
        .select(
            "product_id",
            "asin",
            "source_offset",
            F.col("review.review_ordinal").alias("review_ordinal"),
            F.col("review.review_date_raw").alias("review_date_raw"),
            F.col("review.review_date").alias("review_date"),
            F.col("review.customer_id").alias("customer_id"),
            F.col("review.rating").alias("rating"),
            F.col("review.votes").alias("votes"),
            F.col("review.helpful").alias("helpful"),
            F.col("review.content_hash").alias("content_hash"),
            F.col("review.quality_codes").alias("quality_codes"),
        )
    )

    exact_key = [
        "asin",
        "customer_id",
        "review_date_raw",
        "rating",
        "votes",
        "helpful",
    ]
    survivor_window = Window.partitionBy(*exact_key).orderBy(
        F.col("source_offset").asc(), F.col("review_ordinal").asc()
    )
    reviews_deduplicated = (
        reviews_raw.withColumn("_survivor_rank", F.row_number().over(survivor_window))
        .filter(F.col("_survivor_rank") == 1)
        .drop("_survivor_rank")
    )

    valid_model_reviews = reviews_deduplicated.filter(
        F.col("review_date").isNotNull() & F.col("rating").between(1, 5)
    )
    user_item_interactions = (
        valid_model_reviews.groupBy("customer_id", "product_id", "asin")
        .agg(
            F.avg(F.col("rating").cast("double")).alias("rating"),
            F.min("review_date").alias("first_review_date"),
            F.max("review_date").alias("last_review_date"),
            F.count(F.lit(1)).cast("long").alias("review_count"),
            F.min("source_offset").alias("first_source_offset"),
        )
        .withColumn("interaction_date", F.col("last_review_date"))
        .withColumn("is_positive", F.col("rating") >= F.lit(4.0))
        .withColumn(
            "q_ui",
            F.greatest(
                F.lit(0.0),
                F.least(F.lit(1.0), (F.col("rating") - F.lit(3.0)) / F.lit(2.0)),
            ),
        )
    )

    customer_order = Window.orderBy(F.col("customer_id").asc())
    customers = (
        user_item_interactions.groupBy("customer_id")
        .agg(
            F.countDistinct("product_id").cast("long").alias("distinct_items"),
            F.count(F.lit(1)).cast("long").alias("interactions"),
            F.min("first_review_date").alias("first_interaction_date"),
            F.max("interaction_date").alias("last_interaction_date"),
            F.sum(F.col("is_positive").cast("long")).alias("positive_items"),
        )
        .withColumn(
            "customer_int_id", (F.dense_rank().over(customer_order) - F.lit(1)).cast("int")
        )
    )

    catalog_lookup = products.select(
        F.col("asin").alias("target_asin"), F.col("product_id").alias("target_product_id")
    )
    similar_edges = (
        bronze.products.select(
            F.col("product_id").alias("source_product_id"),
            F.col("asin").alias("source_asin"),
            "source_offset",
            F.explode("similars").alias("similar"),
        )
        .select(
            "source_product_id",
            "source_asin",
            "source_offset",
            F.col("similar.target_asin").alias("target_asin"),
            F.col("similar.position").alias("similar_position"),
        )
        .join(catalog_lookup, "target_asin", "left")
        .withColumn("is_internal", F.col("target_product_id").isNotNull())
    )

    category_paths = (
        bronze.products.select(
            "product_id", "asin", "source_offset", F.explode("category_paths").alias("path")
        )
        .select(
            "product_id",
            "asin",
            "source_offset",
            F.col("path.path_ordinal").alias("path_ordinal"),
            F.col("path.raw_path").alias("raw_path"),
            F.col("path.segments").alias("segments"),
            F.size("path.segments").alias("path_length"),
        )
    )

    category_occurrences = category_paths.select(
        "product_id",
        "asin",
        "path_ordinal",
        "path_length",
        F.explode("segments").alias("segment"),
    ).select(
        "product_id",
        "asin",
        "path_ordinal",
        "path_length",
        F.col("segment.depth").alias("depth"),
        F.col("segment.label").alias("category_label"),
        F.col("segment.category_id").alias("category_id"),
    )
    product_category_nodes = (
        category_occurrences.withColumn(
            "normalized_depth_weight",
            F.col("depth").cast("double") / F.col("path_length").cast("double"),
        )
        .groupBy("product_id", "asin", "category_id")
        .agg(
            F.min("category_label").alias("category_label"),
            F.max("normalized_depth_weight").alias("normalized_depth_weight"),
            F.max("depth").alias("max_depth"),
        )
    )
    category_nodes = category_occurrences.groupBy("category_id").agg(
        F.min("category_label").alias("category_label"),
        F.countDistinct("category_label").alias("label_variants"),
        F.countDistinct("product_id").alias("product_count"),
    )
    category_edges = build_category_edges(category_paths)

    product_quality = bronze.products.select(
        "product_id",
        "asin",
        "source_offset",
        F.explode("quality_codes").alias("event_code"),
    ).withColumn("event_scope", F.lit("product"))
    review_quality = reviews_raw.select(
        "product_id",
        "asin",
        "source_offset",
        F.explode("quality_codes").alias("event_code"),
    ).withColumn("event_scope", F.lit("review"))
    data_quality_events = product_quality.unionByName(review_quality).select(
        "event_scope", "event_code", "product_id", "asin", "source_offset"
    )

    return SilverFrames(
        products,
        reviews_raw,
        reviews_deduplicated,
        user_item_interactions,
        customers,
        similar_edges,
        category_paths,
        product_category_nodes,
        category_nodes,
        category_edges,
        data_quality_events,
    )
