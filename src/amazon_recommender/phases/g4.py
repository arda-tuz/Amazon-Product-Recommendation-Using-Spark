"""G4 full distributed ETL and canonical hard-count acceptance."""

from __future__ import annotations

import hashlib
import os
import shutil
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Mapping

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from amazon_recommender.gate_handlers import register
from amazon_recommender.ingestion.delimiter import detect_delimiter
from amazon_recommender.pipelines.bronze import (
    BronzeFrames,
    SELECTION_SCHEMA,
    read_bronze_envelope,
    write_bronze_envelope,
)
from amazon_recommender.pipelines.silver import build_category_edges, build_silver
from amazon_recommender.pipelines.storage import (
    cleanup_incomplete_publications,
    publish_or_reuse_sized_parquet,
)


EXPECTED_HARD_COUNTS = {
    "products": 548_552,
    "distinct_product_ids": 548_552,
    "distinct_asins": 548_552,
    "min_product_id": 0,
    "max_product_id": 548_551,
    "active_products": 542_684,
    "discontinued_products": 5_868,
    "reviews_total_sum": 7_781_990,
    "reviews_downloaded_sum": 7_593_244,
    "physical_reviews": 7_593_244,
    "similar_occurrences": 1_788_725,
    "category_path_occurrences": 2_509_699,
    "distinct_customers": 1_555_170,
}

FACT_TABLES = {
    "product_records",
    "reviews_raw",
    "reviews_deduplicated",
    "user_item_interactions",
    "similar_edges",
    "category_paths",
    "product_category_nodes",
    "data_quality_events",
}


def validate_hard_counts(actual: Mapping[str, int]) -> None:
    differences = {
        key: {"expected": expected, "actual": actual.get(key)}
        for key, expected in EXPECTED_HARD_COUNTS.items()
        if actual.get(key) != expected
    }
    if differences:
        raise RuntimeError(f"Canonical hard-count mismatch: {differences}")


def scan_source_identity(path: Path) -> dict[str, Any]:
    digest = hashlib.sha256()
    size = 0
    line_count = 0
    crlf_count = 0
    previous = b""
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(4 * 1024 * 1024), b""):
            digest.update(chunk)
            size += len(chunk)
            joined = previous + chunk
            line_count += chunk.count(b"\n")
            crlf_count += joined.count(b"\r\n")
            previous = chunk[-1:]
    return {
        "path": str(path.resolve()),
        "size_bytes": size,
        "line_count": line_count,
        "sha256": digest.hexdigest(),
        "crlf_lines": crlf_count,
        "lf_only_lines": line_count - crlf_count,
        "mtime_ns": path.stat().st_mtime_ns,
    }


def _junit(path: Path) -> dict[str, Any]:
    root = ET.parse(path).getroot()
    suites = [root] if root.tag == "testsuite" else list(root.findall("testsuite"))
    summary = {
        key: sum(int(suite.attrib.get(key, "0")) for suite in suites)
        for key in ("tests", "failures", "errors", "skipped")
    }
    if not summary["tests"] or summary["failures"] or summary["errors"]:
        raise RuntimeError(f"G4 JUnit evidence is not passing: {summary}")
    summary["path"] = str(path.resolve())
    return summary


@register("G4")
def run_g4(config: Any, paths: Any, evidence_file: Path | None) -> dict[str, Any]:
    if evidence_file is None:
        raise RuntimeError("G4 requires passing JUnit XML evidence")
    source = config.resolve("source", "path")
    identity = scan_source_identity(source)
    for key in ("size_bytes", "line_count", "sha256"):
        expected = config.get("source", key)
        if identity[key] != expected:
            raise RuntimeError(
                f"Source identity mismatch for {key}: {identity[key]!r} != {expected!r}"
            )
    if identity["lf_only_lines"]:
        raise RuntimeError("Canonical source contains LF-only lines")
    delimiter = detect_delimiter(source)
    spark = SparkSession.builder.appName("amazon-recommender-g4").getOrCreate()
    working = paths.temporary / "G4-publish"
    final = paths.data / "full"
    if final.exists():
        raise FileExistsError(f"Full ETL output exists without reusable G4: {final}")
    final.parent.mkdir(parents=True, exist_ok=True)
    envelope_path = working / "_ingestion_envelope"
    resumed_envelope = (envelope_path / "_SUCCESS").is_file()
    if resumed_envelope:
        cleanup_incomplete_publications(working)
    else:
        shutil.rmtree(working, ignore_errors=True)
        working.mkdir(parents=True)
        write_bronze_envelope(
            spark,
            source,
            delimiter.delimiter,
            envelope_path,
            split_max_bytes=config.get("spark", "max_partition_bytes"),
        )
    bronze = read_bronze_envelope(spark, envelope_path)
    summary = bronze.products.agg(
        F.count(F.lit(1)).alias("products"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.countDistinct("asin").alias("distinct_asins"),
        F.min("product_id").alias("min_product_id"),
        F.max("product_id").alias("max_product_id"),
        F.sum((F.col("status") == "active").cast("long")).alias("active_products"),
        F.sum((F.col("status") == "discontinued").cast("long")).alias(
            "discontinued_products"
        ),
        F.sum(F.coalesce(F.col("reviews_total"), F.lit(0))).alias("reviews_total_sum"),
        F.sum(F.coalesce(F.col("reviews_downloaded"), F.lit(0))).alias(
            "reviews_downloaded_sum"
        ),
        F.sum(F.size("reviews")).alias("physical_reviews"),
        F.sum(F.size("similars")).alias("similar_occurrences"),
        F.sum(F.size("category_paths")).alias("category_path_occurrences"),
    ).first().asDict()
    hard_counts = {key: int(value) for key, value in summary.items()}
    header_count = bronze.header.count()
    quarantine_count = bronze.quarantine.count()
    if header_count != 1 or quarantine_count:
        raise RuntimeError(
            f"Full Bronze boundary failure: header={header_count}, quarantine={quarantine_count}"
        )

    scale = EXPECTED_HARD_COUNTS["products"] / 2_710
    tables: dict[str, Any] = {}
    reused_tables: list[str] = []

    try:
        for name, frame in (
            (
                "product_records",
                bronze.products.withColumn("parse_status", F.lit("PARSED")),
            ),
            (
                "quarantine_records",
                bronze.quarantine.withColumn(
                    "observed_at",
                    F.to_timestamp(F.lit("2026-07-11T03:05:00Z")),
                ),
            ),
            ("header", bronze.header),
        ):
            table_key = f"bronze.{name}"
            tables[table_key], reused = publish_or_reuse_sized_parquet(
                frame,
                working / "bronze" / name,
                kind="fact" if name in FACT_TABLES else "dimension",
                sort_columns=tuple(
                    column for column in ("product_id", "source_offset") if column in frame.columns
                ),
            )
            if reused:
                reused_tables.append(table_key)

        persisted_bronze = BronzeFrames(
            products=spark.read.parquet(str(working / "bronze/product_records")),
            quarantine=spark.read.parquet(str(working / "bronze/quarantine_records")),
            header=spark.read.parquet(str(working / "bronze/header")),
            smoke_selection=spark.createDataFrame([], SELECTION_SCHEMA),
        )
        silver = build_silver(persisted_bronze)

        def publish_silver(name: str, frame: Any) -> None:
            table_key = f"silver.{name}"
            tables[table_key], reused = publish_or_reuse_sized_parquet(
                frame,
                working / "silver" / name,
                kind="fact" if name in FACT_TABLES else "dimension",
                sort_columns=tuple(
                    column
                    for column in ("product_id", "customer_id", "source_offset")
                    if column in frame.columns
                ),
            )
            if reused:
                reused_tables.append(table_key)

        # Materialize dependent review stages in order and release each cache
        # as soon as every consumer has been published.  This bounds local-mode
        # memory/disk pressure on the 7.6M-review canonical input.
        publish_silver("products", silver.products)
        silver.reviews_raw.persist(StorageLevel.DISK_ONLY)
        publish_silver("reviews_raw", silver.reviews_raw)
        publish_silver("data_quality_events", silver.data_quality_events)
        silver.reviews_deduplicated.persist(StorageLevel.DISK_ONLY)
        publish_silver("reviews_deduplicated", silver.reviews_deduplicated)
        silver.reviews_raw.unpersist()
        silver.user_item_interactions.persist(StorageLevel.DISK_ONLY)
        publish_silver("user_item_interactions", silver.user_item_interactions)
        silver.reviews_deduplicated.unpersist()
        publish_silver("customers", silver.customers)
        silver.user_item_interactions.unpersist()
        publish_silver("similar_edges", silver.similar_edges)
        silver.category_paths.persist(StorageLevel.DISK_ONLY)
        publish_silver("category_paths", silver.category_paths)
        publish_silver("product_category_nodes", silver.product_category_nodes)
        publish_silver("category_nodes", silver.category_nodes)
        publish_silver(
            "category_edges",
            build_category_edges(
                spark.read.parquet(str(working / "silver/category_paths"))
            ),
        )
        silver.category_paths.unpersist()

        customer_map = spark.read.parquet(str(working / "silver/customers"))
        customer_map_summary = customer_map.agg(
            F.count(F.lit(1)).alias("rows"),
            F.countDistinct("customer_id").alias("distinct_customer_ids"),
            F.countDistinct("customer_int_id").alias("distinct_customer_int_ids"),
            F.min("customer_int_id").alias("min_customer_int_id"),
            F.max("customer_int_id").alias("max_customer_int_id"),
        ).first().asDict()
        hard_counts["distinct_customers"] = int(
            customer_map_summary["distinct_customer_ids"]
        )
        expected_customer_mapping = {
            "rows": hard_counts["distinct_customers"],
            "distinct_customer_ids": hard_counts["distinct_customers"],
            "distinct_customer_int_ids": hard_counts["distinct_customers"],
            "min_customer_int_id": 0,
            "max_customer_int_id": hard_counts["distinct_customers"] - 1,
        }
        actual_customer_mapping = {
            key: int(value) for key, value in customer_map_summary.items()
        }
        if actual_customer_mapping != expected_customer_mapping:
            raise RuntimeError(
                "Customer IntType mapping invariant failed: "
                f"{actual_customer_mapping} != {expected_customer_mapping}"
            )
        if dict(customer_map.dtypes)["customer_int_id"] != "int":
            raise RuntimeError("customer_int_id must be persistent Spark IntType")
        validate_hard_counts(hard_counts)
        # The envelope is intentionally retained until all downstream tables
        # and canonical counts pass, making a killed terminal safely resumable.
        shutil.rmtree(envelope_path)
        os.replace(working, final)
    except Exception:
        # Completed atomic publications and the ingestion envelope are valid
        # restart checkpoints.  Only Spark's hidden scratch directories are
        # unsafe after a process failure.
        cleanup_incomplete_publications(working)
        raise
    for table in tables.values():
        table["path"] = table["path"].replace(str(working), str(final), 1)
    return {
        "junit": _junit(evidence_file),
        "source": identity,
        "delimiter": {
            "style": delimiter.style,
            "hex": delimiter.delimiter.hex(),
            "sample_occurrences": delimiter.delimiter_occurrences,
        },
        "header_records": header_count,
        "quarantine_records": quarantine_count,
        "resume": {
            "ingestion_envelope_reused": resumed_envelope,
            "tables_reused": sorted(reused_tables),
        },
        "hard_counts": hard_counts,
        "customer_mapping": actual_customer_mapping,
        "tables": tables,
        "capacity_projection": {
            "g3_sample_products": 2_710,
            "linear_scale": scale,
            "disk_free_before_bytes": shutil.disk_usage(paths.project_root).free,
            "full_etl_cache_storage_level": "DISK_ONLY",
            "spark_task_cpus": spark.sparkContext.getConf().get("spark.task.cpus"),
            "spark_memory_fraction": spark.sparkContext.getConf().get(
                "spark.memory.fraction"
            ),
            "spark_storage_fraction": spark.sparkContext.getConf().get(
                "spark.memory.storageFraction"
            ),
            "cache_reason": (
                "Host swap was saturated by unrelated desktop workloads; disk-only "
                "intermediate caching prevents JVM OOM without changing semantics."
            ),
        },
    }
