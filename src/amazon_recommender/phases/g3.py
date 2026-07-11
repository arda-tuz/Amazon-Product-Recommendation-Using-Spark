"""G3 deterministic smoke pipeline from Bronze through base Gold."""

from __future__ import annotations

import hashlib
import os
import shutil
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from amazon_recommender.gate_handlers import register
from amazon_recommender.ingestion.delimiter import detect_delimiter
from amazon_recommender.pipelines.bronze import load_bronze
from amazon_recommender.pipelines.gold import build_gold
from amazon_recommender.pipelines.silver import build_silver
from amazon_recommender.pipelines.storage import publish_parquet


FACT_TABLES = {
    "reviews_raw",
    "reviews_deduplicated",
    "user_item_interactions",
    "similar_edges",
    "category_paths",
    "data_quality_events",
    "train_interactions",
    "validation_interactions",
    "test_interactions",
    "stage_seen_items",
    "cohort_candidates",
}


def _junit(path: Path) -> dict[str, Any]:
    root = ET.parse(path).getroot()
    suites = [root] if root.tag == "testsuite" else list(root.findall("testsuite"))
    summary = {
        key: sum(int(suite.attrib.get(key, "0")) for suite in suites)
        for key in ("tests", "failures", "errors", "skipped")
    }
    if not summary["tests"] or summary["failures"] or summary["errors"]:
        raise RuntimeError(f"G3 tests are not passing: {summary}")
    summary["path"] = str(path.resolve())
    return summary


def _logical_hash(frame: DataFrame) -> str:
    columns = sorted(frame.columns)
    row_hashes = frame.select(
        F.sha2(F.to_json(F.struct(*[F.col(name) for name in columns])), 256).alias(
            "row_hash"
        )
    ).orderBy("row_hash")
    digest = hashlib.sha256()
    for row in row_hashes.toLocalIterator():
        digest.update(row["row_hash"].encode("ascii"))
    return digest.hexdigest()


@register("G3")
def run_g3(config: Any, paths: Any, evidence_file: Path | None) -> dict[str, Any]:
    if evidence_file is None:
        raise RuntimeError("G3 requires passing JUnit XML evidence")
    spark = SparkSession.builder.appName("amazon-recommender-g3").getOrCreate()
    source = config.resolve("source", "path")
    delimiter = detect_delimiter(source)
    bronze = load_bronze(
        spark,
        source,
        delimiter.delimiter,
        split_max_bytes=config.get("spark", "max_partition_bytes"),
        sample=True,
        seed=config.get("project", "seed"),
        threshold_exclusive=config.get("sampling", "smoke_threshold_exclusive"),
    )
    bronze.products.persist(StorageLevel.MEMORY_AND_DISK)
    product_count = bronze.products.count()
    header_count = bronze.header.count()
    quarantine_count = bronze.quarantine.count()
    if header_count != 1 or quarantine_count:
        raise RuntimeError(
            f"Smoke Bronze invalid: header={header_count}, quarantine={quarantine_count}"
        )
    strata = bronze.products.agg(
        F.sum((F.col("status") == "active").cast("long")).alias("active"),
        F.sum((F.col("status") == "discontinued").cast("long")).alias("discontinued"),
        F.sum((F.col("reviews_downloaded") > 0).cast("long")).alias("reviewed"),
        F.sum((F.coalesce(F.col("reviews_downloaded"), F.lit(0)) == 0).cast("long")).alias(
            "not_reviewed"
        ),
        F.sum((F.col("categories_declared") > 0).cast("long")).alias("categorized"),
        F.sum((F.col("similar_declared") > 0).cast("long")).alias("graph_linked"),
    ).first().asDict()
    if any(not value for value in strata.values()):
        raise RuntimeError(f"Smoke sample misses required strata: {strata}")

    silver = build_silver(bronze)
    silver.reviews_deduplicated.persist(StorageLevel.MEMORY_AND_DISK)
    silver.user_item_interactions.persist(StorageLevel.MEMORY_AND_DISK)
    gold = build_gold(
        bronze,
        silver,
        evaluation_min_distinct_items=config.get(
            "split", "evaluation_min_distinct_items"
        ),
    )

    split_counts = {
        "train": gold.train_interactions.count(),
        "validation": gold.validation_interactions.count(),
        "test": gold.test_interactions.count(),
    }
    overlap = (
        gold.train_interactions.select("customer_id", "product_id")
        .intersect(gold.validation_interactions.select("customer_id", "product_id"))
        .count()
        + gold.train_interactions.select("customer_id", "product_id")
        .intersect(gold.test_interactions.select("customer_id", "product_id"))
        .count()
        + gold.validation_interactions.select("customer_id", "product_id")
        .intersect(gold.test_interactions.select("customer_id", "product_id"))
        .count()
    )
    if overlap:
        raise RuntimeError(f"Smoke split leakage detected: {overlap}")

    working = paths.temporary / "G3-publish"
    final = paths.data / "g3"
    if final.exists():
        raise FileExistsError(f"G3 output already exists without reusable manifest: {final}")
    final.parent.mkdir(parents=True, exist_ok=True)
    shutil.rmtree(working, ignore_errors=True)
    working.mkdir(parents=True)
    tables: dict[str, Any] = {}
    logical_hashes: dict[str, str] = {}
    frames: list[tuple[str, str, DataFrame]] = [
        ("bronze", "product_records", bronze.products),
        ("bronze", "quarantine_records", bronze.quarantine),
        ("bronze", "header", bronze.header),
    ]
    frames.extend(("silver", name, frame) for name, frame in silver.as_dict().items())
    frames.extend(("gold", name, frame) for name, frame in gold.as_dict().items())
    try:
        for layer, name, frame in frames:
            key = f"{layer}.{name}"
            logical_hashes[key] = _logical_hash(frame)
            tables[key] = publish_parquet(
                frame,
                working / layer / name,
                partitions=8 if name in FACT_TABLES else 1,
                sort_columns=tuple(
                    column
                    for column in ("product_id", "customer_id", "source_offset")
                    if column in frame.columns
                ),
            )
        os.replace(working, final)
    except Exception:
        shutil.rmtree(working, ignore_errors=True)
        raise
    for table in tables.values():
        table["path"] = table["path"].replace(str(working), str(final), 1)

    bronze.products.unpersist()
    bronze.release()
    silver.reviews_deduplicated.unpersist()
    silver.user_item_interactions.unpersist()
    return {
        "junit": _junit(evidence_file),
        "sample": {
            "algorithm": config.get("sampling", "algorithm"),
            "threshold_exclusive": config.get(
                "sampling", "smoke_threshold_exclusive"
            ),
            "products": product_count,
            "strata": strata,
        },
        "bronze": {
            "header": header_count,
            "quarantine": quarantine_count,
        },
        "split": {**split_counts, "pair_overlap": overlap},
        "tables": tables,
        "logical_hashes": logical_hashes,
        "model_metrics_produced": False,
    }
