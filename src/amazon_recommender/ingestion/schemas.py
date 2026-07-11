"""Explicit Spark schemas for the Bronze ingestion contract."""

from pyspark.sql.types import (
    ArrayType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


CATEGORY_SEGMENT_SCHEMA = StructType(
    [
        StructField("depth", IntegerType(), False),
        StructField("label", StringType(), False),
        StructField("category_id", IntegerType(), False),
    ]
)

CATEGORY_PATH_SCHEMA = StructType(
    [
        StructField("path_ordinal", IntegerType(), False),
        StructField("raw_path", StringType(), False),
        StructField("segments", ArrayType(CATEGORY_SEGMENT_SCHEMA, False), False),
    ]
)

SIMILAR_SCHEMA = StructType(
    [
        StructField("target_asin", StringType(), False),
        StructField("position", IntegerType(), False),
    ]
)

REVIEW_SCHEMA = StructType(
    [
        StructField("review_ordinal", IntegerType(), False),
        StructField("review_date_raw", StringType(), False),
        StructField("review_date", DateType(), True),
        StructField("customer_id", StringType(), False),
        StructField("rating", IntegerType(), False),
        StructField("votes", IntegerType(), False),
        StructField("helpful", IntegerType(), False),
        StructField("content_hash", StringType(), False),
        StructField("quality_codes", ArrayType(StringType(), False), False),
    ]
)

BRONZE_PRODUCT_SCHEMA = StructType(
    [
        StructField("source_path", StringType(), False),
        StructField("source_offset", LongType(), False),
        StructField("record_ordinal", LongType(), True),
        StructField("source_block_sha256", StringType(), False),
        StructField("product_id", IntegerType(), False),
        StructField("asin", StringType(), False),
        StructField("title", StringType(), True),
        StructField("product_group", StringType(), True),
        StructField("salesrank_raw", LongType(), True),
        StructField("status", StringType(), False),
        StructField("similar_declared", IntegerType(), True),
        StructField("similars", ArrayType(SIMILAR_SCHEMA, False), False),
        StructField("categories_declared", IntegerType(), True),
        StructField("category_paths", ArrayType(CATEGORY_PATH_SCHEMA, False), False),
        StructField("reviews_total", LongType(), True),
        StructField("reviews_downloaded", LongType(), True),
        StructField("avg_rating_raw", DecimalType(3, 1), True),
        StructField("reviews", ArrayType(REVIEW_SCHEMA, False), False),
        StructField("quality_codes", ArrayType(StringType(), False), False),
    ]
)

HEADER_SCHEMA = StructType(
    [
        StructField("source_path", StringType(), False),
        StructField("source_offset", LongType(), False),
        StructField("description", StringType(), False),
        StructField("declared_items", LongType(), False),
        StructField("source_block_sha256", StringType(), False),
    ]
)

QUARANTINE_SCHEMA = StructType(
    [
        StructField("source_path", StringType(), False),
        StructField("source_offset", LongType(), False),
        StructField("record_ordinal", LongType(), True),
        StructField("raw_block", StringType(), False),
        StructField("raw_block_sha256", StringType(), False),
        StructField("error_code", StringType(), False),
        StructField("error_detail", StringType(), False),
    ]
)

INGESTION_ENVELOPE_SCHEMA = StructType(
    [
        StructField("kind", StringType(), False),
        StructField("product", BRONZE_PRODUCT_SCHEMA, True),
        StructField("quarantine", QUARANTINE_SCHEMA, True),
        StructField("header", HEADER_SCHEMA, True),
    ]
)
