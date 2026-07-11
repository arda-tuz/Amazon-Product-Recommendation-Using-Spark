from __future__ import annotations

import pytest

from amazon_recommender.performance.workload import execute_fixed_workload


@pytest.mark.integration
def test_fixed_performance_workload_reads_joins_aggregates_writes_and_counts(
    spark, tmp_path
) -> None:
    reviews_path = tmp_path / "reviews_deduplicated"
    products_path = tmp_path / "products"
    output_path = tmp_path / "output"
    reviews = spark.createDataFrame(
        [
            (1, "2000-01-01", "u1", 5),
            (1, "2000-06-01", "u2", 3),
            (2, "2001-01-01", "u1", 4),
            (99, "2002-01-01", "orphan", 1),
        ],
        "product_id int, review_date string, customer_id string, rating int",
    ).selectExpr(
        "product_id", "cast(review_date as date) review_date", "customer_id", "rating"
    )
    products = spark.createDataFrame(
        [(1, "Book"), (2, "Music")], "product_id int, group string"
    )
    reviews.write.parquet(str(reviews_path))
    products.write.parquet(str(products_path))

    old_shuffle = spark.conf.get("spark.sql.shuffle.partitions")
    old_aqe = spark.conf.get("spark.sql.adaptive.enabled")
    spark.conf.set("spark.sql.shuffle.partitions", "64")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    try:
        measurement = execute_fixed_workload(
            spark, reviews_path, products_path, output_path
        )
    finally:
        spark.conf.set("spark.sql.shuffle.partitions", old_shuffle)
        spark.conf.set("spark.sql.adaptive.enabled", old_aqe)

    rows = {
        (row.review_year, row.product_group): row
        for row in spark.read.parquet(str(output_path)).collect()
    }
    assert measurement.output_rows == 2
    assert rows[(2000, "Book")].review_count == 2
    assert rows[(2000, "Book")].distinct_customer_count == 2
    assert rows[(2000, "Book")].average_rating == pytest.approx(4.0)
    assert rows[(2001, "Music")].review_count == 1
    assert measurement.cache_enabled is False
    assert measurement.plan.adaptive_plan_present
    assert measurement.plan.exchange_node_count >= 1
    assert measurement.partitions.reviews_parquet_files >= 1
    assert measurement.partitions.products_parquet_files >= 1
    assert measurement.partitions.output_parquet_files >= 1
    assert measurement.partitions.output_parquet_bytes > 0
