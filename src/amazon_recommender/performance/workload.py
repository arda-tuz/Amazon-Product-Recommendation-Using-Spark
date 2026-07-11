"""The single immutable Spark SQL workload used by the G11 experiment."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
from pathlib import Path
import re
import time
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


SHUFFLE_PARTITIONS = 64


@dataclass(frozen=True)
class PlanEvidence:
    formatted_plan: str
    executed_plan: str
    formatted_plan_sha256: str
    executed_plan_sha256: str
    exchange_node_count: int
    exchange_node_lines: tuple[str, ...]
    adaptive_plan_present: bool


@dataclass(frozen=True)
class PartitionEvidence:
    reviews_input_partitions: int
    products_input_partitions: int
    aggregate_output_partitions: int
    reviews_parquet_files: int
    products_parquet_files: int
    output_parquet_files: int
    output_parquet_bytes: int


@dataclass(frozen=True)
class WorkloadMeasurement:
    wall_seconds: float
    output_rows: int
    output_schema_json: str
    output_schema_sha256: str
    plan: PlanEvidence
    partitions: PartitionEvidence
    cache_enabled: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _require_columns(frame: DataFrame, required: set[str], name: str) -> None:
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"{name} is missing required columns: {missing}")


def fixed_aggregation(reviews: DataFrame, products: DataFrame) -> DataFrame:
    """Build the exact §17 aggregation without materializing extra actions.

    The Silver product table calls the source's product-group field ``group``.
    It is explicitly renamed to ``product_group`` in the performance output so
    the measured schema states the specification's semantics unambiguously.
    """

    _require_columns(
        reviews,
        {"product_id", "review_date", "customer_id", "rating"},
        "reviews_deduplicated",
    )
    _require_columns(products, {"product_id", "group"}, "products")
    review_projection = reviews.select(
        "product_id", "review_date", "customer_id", "rating"
    )
    product_projection = products.select(
        "product_id", F.col("group").alias("product_group")
    )
    return (
        review_projection.join(product_projection, "product_id", "inner")
        .groupBy(
            F.year("review_date").cast("int").alias("review_year"),
            "product_group",
        )
        .agg(
            F.count(F.lit(1)).cast("long").alias("review_count"),
            F.countDistinct("customer_id").cast("long").alias(
                "distinct_customer_count"
            ),
            F.avg(F.col("rating").cast("double")).alias("average_rating"),
        )
    )


def load_fixed_workload(
    spark: SparkSession,
    reviews_deduplicated_path: Path,
    products_path: Path,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Scan the two immutable Silver Parquet inputs and construct the workload."""

    reviews = spark.read.parquet(str(reviews_deduplicated_path))
    products = spark.read.parquet(str(products_path))
    return reviews, products, fixed_aggregation(reviews, products)


def _formatted_plan(frame: DataFrame) -> str:
    jvm = frame.sparkSession.sparkContext._jvm
    return str(
        jvm.PythonSQLUtils.explainString(frame._jdf.queryExecution(), "formatted")
    )


def _executed_plan(frame: DataFrame) -> str:
    return str(frame._jdf.queryExecution().executedPlan().toString())


def _plan_evidence(formatted: str, executed: str) -> PlanEvidence:
    exchange_lines = tuple(
        line.strip()
        for line in formatted.splitlines()
        if re.search(r"\b(?:Broadcast)?Exchange\b", line)
    )
    return PlanEvidence(
        formatted_plan=formatted,
        executed_plan=executed,
        formatted_plan_sha256=hashlib.sha256(formatted.encode("utf-8")).hexdigest(),
        executed_plan_sha256=hashlib.sha256(executed.encode("utf-8")).hexdigest(),
        exchange_node_count=len(exchange_lines),
        exchange_node_lines=exchange_lines,
        adaptive_plan_present=(
            "AdaptiveSparkPlan" in formatted or "AdaptiveSparkPlan" in executed
        ),
    )


def _parquet_stats(path: Path) -> tuple[int, int]:
    files = [
        item
        for item in path.rglob("*.parquet")
        if item.is_file() and not item.name.startswith(".")
    ]
    return len(files), sum(item.stat().st_size for item in files)


def _persistent_rdd_count(spark: SparkSession) -> int:
    return int(spark.sparkContext._jsc.getPersistentRDDs().size())


def execute_fixed_workload(
    spark: SparkSession,
    reviews_deduplicated_path: Path,
    products_path: Path,
    output_path: Path,
) -> WorkloadMeasurement:
    """Execute, write and count the fixed workload once with cache disabled.

    Spark startup is intentionally outside this timer.  Parquet scans, join,
    aggregation, temporary Parquet write, and the required output row count are
    inside it.  The caller must provide a fresh output path for every trial.
    """

    if output_path.exists():
        raise FileExistsError(f"performance output already exists: {output_path}")
    if spark.conf.get("spark.sql.shuffle.partitions") != str(SHUFFLE_PARTITIONS):
        raise RuntimeError("performance workload requires spark.sql.shuffle.partitions=64")
    if spark.conf.get("spark.sql.adaptive.enabled").lower() != "true":
        raise RuntimeError("performance workload requires AQE enabled")
    spark.catalog.clearCache()
    if _persistent_rdd_count(spark) != 0:
        raise RuntimeError("performance trial must begin without persisted RDDs")

    reviews, products, aggregate = load_fixed_workload(
        spark, reviews_deduplicated_path, products_path
    )
    formatted_before_execution = _formatted_plan(aggregate)
    reviews_files, _ = _parquet_stats(reviews_deduplicated_path)
    products_files, _ = _parquet_stats(products_path)
    # File-scan partition inspection is metadata-only. Do not ask for the
    # aggregate RDD here: with AQE, converting that plan to an RDD can execute
    # query stages before the wall-clock timer starts.
    reviews_input_partitions = reviews.rdd.getNumPartitions()
    products_input_partitions = products.rdd.getNumPartitions()

    started = time.perf_counter_ns()
    (
        aggregate.write.mode("error")
        .option("compression", "snappy")
        .parquet(str(output_path))
    )
    materialized = spark.read.parquet(str(output_path))
    output_rows = materialized.count()
    wall_seconds = (time.perf_counter_ns() - started) / 1_000_000_000.0

    executed_after_action = _executed_plan(aggregate)
    aggregate_output_partitions = materialized.rdd.getNumPartitions()
    output_files, output_bytes = _parquet_stats(output_path)
    schema_json = materialized.schema.json()
    if _persistent_rdd_count(spark) != 0:
        raise RuntimeError("fixed performance workload must not persist Spark RDDs")
    return WorkloadMeasurement(
        wall_seconds=wall_seconds,
        output_rows=int(output_rows),
        output_schema_json=schema_json,
        output_schema_sha256=hashlib.sha256(schema_json.encode("utf-8")).hexdigest(),
        plan=_plan_evidence(formatted_before_execution, executed_after_action),
        partitions=PartitionEvidence(
            reviews_input_partitions=int(reviews_input_partitions),
            products_input_partitions=int(products_input_partitions),
            aggregate_output_partitions=int(aggregate_output_partitions),
            reviews_parquet_files=reviews_files,
            products_parquet_files=products_files,
            output_parquet_files=output_files,
            output_parquet_bytes=output_bytes,
        ),
        cache_enabled=False,
    )
