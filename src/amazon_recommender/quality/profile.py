"""Canonical G5 quality views derived from immutable Bronze/Silver tables."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


REQUIRED_EVENT_TYPES = (
    "PARSE_ERROR",
    "FIELD_ORDER_ERROR",
    "INVALID_DATE",
    "INVALID_RATING",
    "MISSING_REQUIRED_ID",
    "SIMILAR_COUNT_MISMATCH",
    "CATEGORY_COUNT_MISMATCH",
    "DOWNLOADED_ROW_COUNT_MISMATCH",
    "DECLARED_GT_DOWNLOADED",
    "DECLARED_LT_DOWNLOADED",
    "REVIEW_COVERAGE_ZERO_TOTAL",
    "AVG_RATING_MISMATCH",
    "INVALID_SALESRANK",
    "DUPLICATE_REVIEW_OCCURRENCE",
    "ORPHAN_GRAPH_TARGET",
    "DUPLICATE_GRAPH_EDGE",
    "CATEGORY_LABEL_VARIANT",
    "MULTILINE_TITLE",
)

QUALITY_EVENT_SCHEMA = StructType(
    [
        StructField("event_type", StringType(), False),
        StructField("event_scope", StringType(), False),
        StructField("entity_id", StringType(), False),
        StructField("product_id", IntegerType(), True),
        StructField("asin", StringType(), True),
        StructField("source_offset", LongType(), True),
        StructField("event_ordinal", LongType(), True),
        StructField("detail_json", StringType(), True),
    ]
)


@dataclass(frozen=True)
class QualityFrames:
    product_quality_profile: DataFrame
    graph_edges_deduplicated: DataFrame
    orphan_graph_targets: DataFrame
    review_duplicate_groups: DataFrame
    data_quality_events: DataFrame
    data_quality_summary: DataFrame
    data_quality_samples: DataFrame


def build_product_quality_profile(
    products: DataFrame, reviews_raw: DataFrame
) -> DataFrame:
    review_stats = reviews_raw.groupBy("product_id").agg(
        F.count(F.lit(1)).cast("long").alias("physical_review_count"),
        F.avg(F.col("rating").cast("double")).alias("avg_rating_computed"),
    )
    result = (
        products.join(review_stats, "product_id", "left")
        .fillna({"physical_review_count": 0})
        .withColumn(
            "review_coverage",
            F.when(
                F.col("reviews_total") > F.lit(0),
                F.col("reviews_downloaded").cast("double")
                / F.col("reviews_total").cast("double"),
            ),
        )
        .withColumn(
            "avg_rating_rounded_half_up",
            F.when(
                F.col("physical_review_count") > F.lit(0),
                F.round(F.col("avg_rating_computed") * F.lit(2.0), 0) / F.lit(2.0),
            ),
        )
        .withColumn(
            "is_complete_download",
            F.col("reviews_total").isNotNull()
            & (F.col("reviews_total") == F.col("reviews_downloaded")),
        )
        .withColumn(
            "avg_rating_mismatch",
            F.col("is_complete_download")
            & (
                F.abs(
                    F.coalesce(F.col("avg_rating_rounded_half_up"), F.lit(0.0))
                    - F.col("avg_rating_raw").cast("double")
                )
                > F.lit(1e-12)
            ),
        )
    )
    return result


def build_graph_edges_deduplicated(similar_edges: DataFrame) -> DataFrame:
    pair_window = Window.partitionBy("source_product_id", "target_asin").orderBy(
        F.col("similar_position").asc(), F.col("source_offset").asc()
    )
    return (
        similar_edges.filter(
            F.col("is_internal")
            & (F.col("source_product_id") != F.col("target_product_id"))
        )
        .withColumn("_pair_rank", F.row_number().over(pair_window))
        .filter(F.col("_pair_rank") == F.lit(1))
        .drop("_pair_rank")
    )


def build_orphan_graph_targets(similar_edges: DataFrame) -> DataFrame:
    return (
        similar_edges.filter(~F.col("is_internal"))
        .groupBy("target_asin")
        .agg(
            F.count(F.lit(1)).cast("long").alias("occurrence_count"),
            F.countDistinct("source_product_id").cast("long").alias(
                "distinct_source_products"
            ),
            F.min("source_offset").alias("first_source_offset"),
        )
    )


def build_review_duplicate_groups(reviews_raw: DataFrame) -> DataFrame:
    key = ["asin", "customer_id", "review_date_raw", "rating", "votes", "helpful"]
    return (
        reviews_raw.groupBy(*key)
        .agg(
            F.count(F.lit(1)).cast("long").alias("occurrence_count"),
            F.min(F.struct("source_offset", "review_ordinal")).alias("survivor"),
            F.min("product_id").alias("product_id"),
        )
        .filter(F.col("occurrence_count") > F.lit(1))
        .select(
            *key,
            "product_id",
            "occurrence_count",
            (F.col("occurrence_count") - F.lit(1)).cast("long").alias(
                "duplicate_extra_count"
            ),
            F.col("survivor.source_offset").alias("survivor_source_offset"),
            F.col("survivor.review_ordinal").alias("survivor_review_ordinal"),
        )
    )


def _null(data_type: str) -> Column:
    return F.lit(None).cast(data_type)


def _event_rows(
    frame: DataFrame,
    event_type: str,
    event_scope: str,
    *,
    entity_id: Column,
    product_id: Column | None = None,
    asin: Column | None = None,
    source_offset: Column | None = None,
    event_ordinal: Column | None = None,
    detail_json: Column | None = None,
) -> DataFrame:
    return frame.select(
        F.lit(event_type).alias("event_type"),
        F.lit(event_scope).alias("event_scope"),
        entity_id.cast("string").alias("entity_id"),
        (product_id if product_id is not None else _null("int"))
        .cast("int")
        .alias("product_id"),
        (asin if asin is not None else _null("string"))
        .cast("string")
        .alias("asin"),
        (source_offset if source_offset is not None else _null("long"))
        .cast("long")
        .alias("source_offset"),
        (event_ordinal if event_ordinal is not None else _null("long"))
        .cast("long")
        .alias("event_ordinal"),
        (detail_json if detail_json is not None else _null("string"))
        .cast("string")
        .alias("detail_json"),
    )


def build_quality_events(
    bronze_products: DataFrame,
    quarantine: DataFrame,
    product_profile: DataFrame,
    reviews_raw: DataFrame,
    similar_edges: DataFrame,
    category_nodes: DataFrame,
) -> DataFrame:
    product_detail = F.to_json(
        F.struct("reviews_total", "reviews_downloaded", "physical_review_count")
    )
    events = [
        _event_rows(
            quarantine,
            "PARSE_ERROR",
            "record",
            entity_id=F.col("raw_block_sha256"),
            source_offset=F.col("source_offset"),
            detail_json=F.to_json(F.struct("error_code", "error_detail")),
        ),
        _event_rows(
            quarantine.filter(F.lower("error_code").contains("order")),
            "FIELD_ORDER_ERROR",
            "record",
            entity_id=F.col("raw_block_sha256"),
            source_offset=F.col("source_offset"),
            detail_json=F.to_json(F.struct("error_code", "error_detail")),
        ),
        _event_rows(
            quarantine.filter(F.lower("error_code").contains("required_id")),
            "MISSING_REQUIRED_ID",
            "record",
            entity_id=F.col("raw_block_sha256"),
            source_offset=F.col("source_offset"),
            detail_json=F.to_json(F.struct("error_code", "error_detail")),
        ),
        _event_rows(
            bronze_products.filter(F.size("similars") != F.col("similar_declared")),
            "SIMILAR_COUNT_MISMATCH",
            "product",
            entity_id=F.col("asin"),
            product_id=F.col("product_id"),
            asin=F.col("asin"),
            source_offset=F.col("source_offset"),
            detail_json=F.to_json(
                F.struct("similar_declared", F.size("similars").alias("observed"))
            ),
        ),
        _event_rows(
            bronze_products.filter(
                F.size("category_paths") != F.col("categories_declared")
            ),
            "CATEGORY_COUNT_MISMATCH",
            "product",
            entity_id=F.col("asin"),
            product_id=F.col("product_id"),
            asin=F.col("asin"),
            source_offset=F.col("source_offset"),
            detail_json=F.to_json(
                F.struct(
                    "categories_declared",
                    F.size("category_paths").alias("observed"),
                )
            ),
        ),
        _event_rows(
            bronze_products.filter(F.size("reviews") != F.col("reviews_downloaded")),
            "DOWNLOADED_ROW_COUNT_MISMATCH",
            "product",
            entity_id=F.col("asin"),
            product_id=F.col("product_id"),
            asin=F.col("asin"),
            source_offset=F.col("source_offset"),
            detail_json=F.to_json(
                F.struct(
                    "reviews_downloaded", F.size("reviews").alias("observed")
                )
            ),
        ),
        _event_rows(
            product_profile.filter(F.col("reviews_total") > F.col("reviews_downloaded")),
            "DECLARED_GT_DOWNLOADED",
            "product",
            entity_id=F.col("asin"),
            product_id=F.col("product_id"),
            asin=F.col("asin"),
            source_offset=F.col("source_offset"),
            detail_json=product_detail,
        ),
        _event_rows(
            product_profile.filter(F.col("reviews_total") < F.col("reviews_downloaded")),
            "DECLARED_LT_DOWNLOADED",
            "product",
            entity_id=F.col("asin"),
            product_id=F.col("product_id"),
            asin=F.col("asin"),
            source_offset=F.col("source_offset"),
            detail_json=product_detail,
        ),
        _event_rows(
            product_profile.filter(F.col("reviews_total") == F.lit(0)),
            "REVIEW_COVERAGE_ZERO_TOTAL",
            "product",
            entity_id=F.col("asin"),
            product_id=F.col("product_id"),
            asin=F.col("asin"),
            source_offset=F.col("source_offset"),
            detail_json=product_detail,
        ),
        _event_rows(
            product_profile.filter(F.col("avg_rating_mismatch")),
            "AVG_RATING_MISMATCH",
            "product",
            entity_id=F.col("asin"),
            product_id=F.col("product_id"),
            asin=F.col("asin"),
            source_offset=F.col("source_offset"),
            detail_json=F.to_json(
                F.struct(
                    "avg_rating_raw",
                    "avg_rating_computed",
                    "avg_rating_rounded_half_up",
                )
            ),
        ),
        _event_rows(
            product_profile.filter(F.col("salesrank_raw") <= F.lit(0)),
            "INVALID_SALESRANK",
            "product",
            entity_id=F.col("asin"),
            product_id=F.col("product_id"),
            asin=F.col("asin"),
            source_offset=F.col("source_offset"),
            detail_json=F.to_json(F.struct("salesrank_raw")),
        ),
        _event_rows(
            bronze_products.filter(F.array_contains("quality_codes", "multiline_title")),
            "MULTILINE_TITLE",
            "product",
            entity_id=F.col("asin"),
            product_id=F.col("product_id"),
            asin=F.col("asin"),
            source_offset=F.col("source_offset"),
        ),
    ]

    invalid_date = reviews_raw.filter(F.col("review_date").isNull())
    invalid_rating = reviews_raw.filter(~F.col("rating").between(1, 5))
    events.extend(
        [
            _event_rows(
                invalid_date,
                "INVALID_DATE",
                "review",
                entity_id=F.col("content_hash"),
                product_id=F.col("product_id"),
                asin=F.col("asin"),
                source_offset=F.col("source_offset"),
                event_ordinal=F.col("review_ordinal"),
                detail_json=F.to_json(F.struct("review_date_raw")),
            ),
            _event_rows(
                invalid_rating,
                "INVALID_RATING",
                "review",
                entity_id=F.col("content_hash"),
                product_id=F.col("product_id"),
                asin=F.col("asin"),
                source_offset=F.col("source_offset"),
                event_ordinal=F.col("review_ordinal"),
                detail_json=F.to_json(F.struct("rating")),
            ),
        ]
    )

    review_key = [
        "asin",
        "customer_id",
        "review_date_raw",
        "rating",
        "votes",
        "helpful",
    ]
    review_window = Window.partitionBy(*review_key).orderBy(
        F.col("source_offset").asc(), F.col("review_ordinal").asc()
    )
    duplicate_reviews = reviews_raw.withColumn(
        "_duplicate_rank", F.row_number().over(review_window)
    ).filter(F.col("_duplicate_rank") > F.lit(1))
    events.append(
        _event_rows(
            duplicate_reviews,
            "DUPLICATE_REVIEW_OCCURRENCE",
            "review",
            entity_id=F.col("content_hash"),
            product_id=F.col("product_id"),
            asin=F.col("asin"),
            source_offset=F.col("source_offset"),
            event_ordinal=F.col("review_ordinal"),
            detail_json=F.to_json(F.struct("_duplicate_rank")),
        )
    )

    orphan_edges = similar_edges.filter(~F.col("is_internal"))
    events.append(
        _event_rows(
            orphan_edges,
            "ORPHAN_GRAPH_TARGET",
            "similar_edge",
            entity_id=F.col("target_asin"),
            product_id=F.col("source_product_id"),
            asin=F.col("source_asin"),
            source_offset=F.col("source_offset"),
            event_ordinal=F.col("similar_position"),
            detail_json=F.to_json(F.struct("target_asin")),
        )
    )
    edge_window = Window.partitionBy("source_product_id", "target_asin").orderBy(
        F.col("similar_position").asc(), F.col("source_offset").asc()
    )
    duplicate_edges = similar_edges.withColumn(
        "_edge_rank", F.row_number().over(edge_window)
    ).filter(F.col("_edge_rank") > F.lit(1))
    events.append(
        _event_rows(
            duplicate_edges,
            "DUPLICATE_GRAPH_EDGE",
            "similar_edge",
            entity_id=F.col("target_asin"),
            product_id=F.col("source_product_id"),
            asin=F.col("source_asin"),
            source_offset=F.col("source_offset"),
            event_ordinal=F.col("similar_position"),
            detail_json=F.to_json(F.struct("_edge_rank")),
        )
    )
    events.append(
        _event_rows(
            category_nodes.filter(F.col("label_variants") > F.lit(1)),
            "CATEGORY_LABEL_VARIANT",
            "category",
            entity_id=F.col("category_id"),
            detail_json=F.to_json(F.struct("category_label", "label_variants")),
        )
    )

    result = events[0]
    for event_frame in events[1:]:
        result = result.unionByName(event_frame)
    return result


def build_quality_summary(events: DataFrame) -> DataFrame:
    spark = events.sparkSession
    catalog = spark.createDataFrame(
        [(event_type,) for event_type in REQUIRED_EVENT_TYPES],
        StructType([StructField("event_type", StringType(), False)]),
    )
    counts = events.groupBy("event_type").agg(
        F.count(F.lit(1)).cast("long").alias("event_count"),
        F.countDistinct("entity_id").cast("long").alias("distinct_entities"),
    )
    return (
        catalog.join(counts, "event_type", "left")
        .fillna({"event_count": 0, "distinct_entities": 0})
        .orderBy("event_type")
    )


def build_quality_samples(events: DataFrame, limit_per_type: int = 3) -> DataFrame:
    sample_window = Window.partitionBy("event_type").orderBy(
        F.col("source_offset").asc_nulls_last(),
        F.col("entity_id").asc(),
        F.col("product_id").asc_nulls_last(),
        F.col("event_ordinal").asc_nulls_last(),
    )
    return (
        events.withColumn("sample_rank", F.row_number().over(sample_window))
        .filter(F.col("sample_rank") <= F.lit(limit_per_type))
        .orderBy("event_type", "sample_rank")
    )
