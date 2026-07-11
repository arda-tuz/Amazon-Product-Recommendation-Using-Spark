"""G5 canonical cleaning reconciliation and full-data quality profile."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Mapping

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from amazon_recommender.core.manifest import atomic_write_json
from amazon_recommender.gate_handlers import register
from amazon_recommender.pipelines.storage import (
    cleanup_incomplete_publications,
    publish_or_reuse_sized_parquet,
)
from amazon_recommender.quality.profile import (
    REQUIRED_EVENT_TYPES,
    build_graph_edges_deduplicated,
    build_orphan_graph_targets,
    build_product_quality_profile,
    build_quality_events,
    build_quality_samples,
    build_quality_summary,
    build_review_duplicate_groups,
)


EXPECTED_PROFILE_COUNTS = {
    "products": 548_552,
    "reviews_raw": 7_593_244,
    "reviews_deduplicated": 7_446_499,
    "duplicate_review_extra": 146_745,
    "duplicate_review_affected_products": 25_262,
    "distinct_customers": 1_555_170,
    "category_nodes": 49_732,
    "internal_graph_edges": 1_231_439,
    "orphan_graph_target_occurrences": 557_286,
    "orphan_graph_targets": 172_790,
    "declared_gt_downloaded": 8_615,
    "declared_lt_downloaded": 131,
    "complete_download_products": 533_938,
    "review_coverage_zero_total": 139_949,
    "avg_rating_mismatches": 487,
    "salesrank_minus_one": 459,
    "salesrank_zero": 41,
    "invalid_salesrank": 500,
    "invalid_dates": 0,
    "invalid_ratings": 0,
    "similar_count_mismatches": 0,
    "category_count_mismatches": 0,
    "downloaded_row_count_mismatches": 0,
    "duplicate_graph_edge_extra": 0,
    "category_label_variants": 0,
    "multiline_titles": 10,
    "deduplicated_key_violations": 0,
    "survivor_rule_violations": 0,
    "interaction_key_violations": 0,
    "total_quality_events": 853_723,
}


def validate_profile_counts(actual: Mapping[str, int]) -> None:
    differences = {
        key: {"expected": expected, "actual": actual.get(key)}
        for key, expected in EXPECTED_PROFILE_COUNTS.items()
        if actual.get(key) != expected
    }
    if differences:
        raise RuntimeError(f"G5 canonical profile mismatch: {differences}")


def _junit(path: Path) -> dict[str, Any]:
    root = ET.parse(path).getroot()
    suites = [root] if root.tag == "testsuite" else list(root.findall("testsuite"))
    summary = {
        key: sum(int(suite.attrib.get(key, "0")) for suite in suites)
        for key in ("tests", "failures", "errors", "skipped")
    }
    if not summary["tests"] or summary["failures"] or summary["errors"]:
        raise RuntimeError(f"G5 JUnit evidence is not passing: {summary}")
    summary["path"] = str(path.resolve())
    return summary


def _implementation_signature() -> str:
    digest = hashlib.sha256()
    files = [Path(__file__), Path(__file__).parents[1] / "quality" / "profile.py"]
    for path in files:
        digest.update(path.name.encode("utf-8"))
        digest.update(path.read_bytes())
    return digest.hexdigest()


def _prepare_workspace(working: Path, signature: str) -> list[str]:
    marker = working / "_checkpoint_contract.json"
    if working.exists():
        try:
            existing = json.loads(marker.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError):
            existing = {}
        if existing.get("implementation_sha256") != signature:
            shutil.rmtree(working)
    working.mkdir(parents=True, exist_ok=True)
    atomic_write_json(marker, {"implementation_sha256": signature, "gate": "G5"})
    return cleanup_incomplete_publications(working)


def _integer(value: Any) -> int:
    return 0 if value is None else int(value)


@register("G5")
def run_g5(config: Any, paths: Any, evidence_file: Path | None) -> dict[str, Any]:
    if evidence_file is None:
        raise RuntimeError("G5 requires passing JUnit XML evidence")
    full = paths.data / "full"
    if not full.exists():
        raise FileNotFoundError(f"G4 full data is missing: {full}")
    final = paths.data / "g5"
    if final.exists():
        raise FileExistsError(f"G5 output exists without reusable manifest: {final}")
    working = paths.temporary / "G5-publish"
    signature = _implementation_signature()
    cleaned_scratch = _prepare_workspace(working, signature)
    final.parent.mkdir(parents=True, exist_ok=True)

    spark = SparkSession.builder.appName("amazon-recommender-g5").getOrCreate()
    bronze_products = spark.read.parquet(str(full / "bronze/product_records"))
    quarantine = spark.read.parquet(str(full / "bronze/quarantine_records"))
    products = spark.read.parquet(str(full / "silver/products"))
    reviews_raw = spark.read.parquet(str(full / "silver/reviews_raw"))
    reviews_deduplicated = spark.read.parquet(
        str(full / "silver/reviews_deduplicated")
    )
    interactions = spark.read.parquet(str(full / "silver/user_item_interactions"))
    similar_edges = spark.read.parquet(str(full / "silver/similar_edges"))
    category_nodes = spark.read.parquet(str(full / "silver/category_nodes"))

    tables: dict[str, dict[str, Any]] = {}
    reused_tables: list[str] = []

    def publish(name: str, frame: Any, kind: str) -> None:
        table_path = working / name
        evidence, reused = publish_or_reuse_sized_parquet(
            frame,
            table_path,
            kind=kind,
            sort_columns=tuple(
                column
                for column in (
                    "event_type",
                    "product_id",
                    "customer_id",
                    "source_product_id",
                    "target_asin",
                    "source_offset",
                )
                if column in frame.columns
            ),
        )
        tables[name] = evidence
        if reused:
            reused_tables.append(name)

    try:
        product_profile = build_product_quality_profile(products, reviews_raw)
        publish("product_quality_profile", product_profile, "dimension")
        materialized_profile = spark.read.parquet(
            str(working / "product_quality_profile")
        )

        graph_edges = build_graph_edges_deduplicated(similar_edges)
        publish("graph_edges_deduplicated", graph_edges, "fact")
        orphan_targets = build_orphan_graph_targets(similar_edges)
        publish("orphan_graph_targets", orphan_targets, "dimension")
        duplicate_groups = build_review_duplicate_groups(reviews_raw)
        publish("review_duplicate_groups", duplicate_groups, "dimension")

        events = build_quality_events(
            bronze_products,
            quarantine,
            materialized_profile,
            reviews_raw,
            similar_edges,
            category_nodes,
        )
        publish("data_quality_events", events, "fact")
        materialized_events = spark.read.parquet(str(working / "data_quality_events"))
        summary = build_quality_summary(materialized_events)
        publish("data_quality_summary", summary, "dimension")
        samples = build_quality_samples(materialized_events)
        publish("data_quality_samples", samples, "dimension")

        materialized_groups = spark.read.parquet(
            str(working / "review_duplicate_groups")
        )
        duplicate_summary = materialized_groups.agg(
            F.count(F.lit(1)).alias("duplicate_groups"),
            F.sum("duplicate_extra_count").alias("duplicate_review_extra"),
            F.countDistinct("product_id").alias(
                "duplicate_review_affected_products"
            ),
        ).first().asDict()
        product_summary = materialized_profile.agg(
            F.count(F.lit(1)).alias("products"),
            F.sum(F.col("physical_review_count")).alias("profile_physical_reviews"),
            F.sum(F.col("is_complete_download").cast("long")).alias(
                "complete_download_products"
            ),
            F.sum((F.col("reviews_total") > F.col("reviews_downloaded")).cast("long")).alias(
                "declared_gt_downloaded"
            ),
            F.sum((F.col("reviews_total") < F.col("reviews_downloaded")).cast("long")).alias(
                "declared_lt_downloaded"
            ),
            F.sum((F.col("reviews_total") == F.lit(0)).cast("long")).alias(
                "review_coverage_zero_total"
            ),
            F.sum(F.col("avg_rating_mismatch").cast("long")).alias(
                "avg_rating_mismatches"
            ),
            F.sum((F.col("salesrank_raw") == F.lit(-1)).cast("long")).alias(
                "salesrank_minus_one"
            ),
            F.sum((F.col("salesrank_raw") == F.lit(0)).cast("long")).alias(
                "salesrank_zero"
            ),
            F.sum((F.col("salesrank_raw") <= F.lit(0)).cast("long")).alias(
                "invalid_salesrank"
            ),
            F.sum(
                (
                    F.col("physical_review_count")
                    != F.coalesce(F.col("reviews_downloaded"), F.lit(0))
                ).cast("long")
            ).alias("downloaded_row_count_mismatches"),
        ).first().asDict()
        raw_summary = reviews_raw.agg(
            F.count(F.lit(1)).alias("reviews_raw"),
            F.countDistinct("customer_id").alias("distinct_customers"),
            F.sum(F.col("review_date").isNull().cast("long")).alias(
                "invalid_dates"
            ),
            F.sum((~F.col("rating").between(1, 5)).cast("long")).alias(
                "invalid_ratings"
            ),
        ).first().asDict()
        bronze_summary = bronze_products.agg(
            F.sum((F.size("similars") != F.col("similar_declared")).cast("long")).alias(
                "similar_count_mismatches"
            ),
            F.sum(
                (F.size("category_paths") != F.col("categories_declared")).cast(
                    "long"
                )
            ).alias("category_count_mismatches"),
        ).first().asDict()

        review_key = [
            "asin",
            "customer_id",
            "review_date_raw",
            "rating",
            "votes",
            "helpful",
        ]
        deduplicated_key_violations = (
            reviews_deduplicated.groupBy(*review_key)
            .count()
            .filter(F.col("count") > F.lit(1))
            .limit(1)
            .count()
        )
        interaction_key_violations = (
            interactions.groupBy("customer_id", "product_id")
            .count()
            .filter(F.col("count") > F.lit(1))
            .limit(1)
            .count()
        )
        survivor_check = (
            F.broadcast(materialized_groups.alias("groups"))
            .join(reviews_deduplicated.alias("dedup"), review_key, "left")
            .filter(
                F.col("dedup.source_offset").isNull()
                | (
                    F.col("dedup.source_offset")
                    != F.col("groups.survivor_source_offset")
                )
                | (
                    F.col("dedup.review_ordinal")
                    != F.col("groups.survivor_review_ordinal")
                )
            )
            .count()
        )

        event_counts = {
            row.event_type: int(row.event_count)
            for row in spark.read.parquet(str(working / "data_quality_summary")).collect()
        }
        metrics = {
            **{key: _integer(value) for key, value in raw_summary.items()},
            **{key: _integer(value) for key, value in product_summary.items()},
            **{key: _integer(value) for key, value in bronze_summary.items()},
            **{key: _integer(value) for key, value in duplicate_summary.items()},
            "reviews_deduplicated": reviews_deduplicated.count(),
            "duplicate_review_extra": int(
                duplicate_summary["duplicate_review_extra"]
            ),
            "user_item_interactions": interactions.count(),
            "category_nodes": category_nodes.count(),
            "category_label_variants": category_nodes.filter(
                F.col("label_variants") > F.lit(1)
            ).count(),
            "internal_graph_edges": int(tables["graph_edges_deduplicated"]["rows"]),
            "orphan_graph_targets": int(tables["orphan_graph_targets"]["rows"]),
            "orphan_graph_target_occurrences": int(
                spark.read.parquet(str(working / "orphan_graph_targets"))
                .agg(F.sum("occurrence_count"))
                .first()[0]
            ),
            "deduplicated_key_violations": deduplicated_key_violations,
            "survivor_rule_violations": survivor_check,
            "interaction_key_violations": interaction_key_violations,
            "duplicate_graph_edge_extra": event_counts["DUPLICATE_GRAPH_EDGE"],
            "multiline_titles": event_counts["MULTILINE_TITLE"],
            "total_quality_events": int(sum(event_counts.values())),
        }
        # Parser mismatch counts are represented by the canonical event table;
        # this also asserts every required zero-valued event remains visible.
        if set(event_counts) != set(REQUIRED_EVENT_TYPES):
            raise RuntimeError(
                "G5 quality summary taxonomy mismatch: "
                f"{sorted(event_counts)} != {sorted(REQUIRED_EVENT_TYPES)}"
            )
        expected_event_counts = {
            "PARSE_ERROR": 0,
            "FIELD_ORDER_ERROR": 0,
            "INVALID_DATE": metrics["invalid_dates"],
            "INVALID_RATING": metrics["invalid_ratings"],
            "MISSING_REQUIRED_ID": 0,
            "SIMILAR_COUNT_MISMATCH": metrics["similar_count_mismatches"],
            "CATEGORY_COUNT_MISMATCH": metrics["category_count_mismatches"],
            "DOWNLOADED_ROW_COUNT_MISMATCH": metrics[
                "downloaded_row_count_mismatches"
            ],
            "DECLARED_GT_DOWNLOADED": metrics["declared_gt_downloaded"],
            "DECLARED_LT_DOWNLOADED": metrics["declared_lt_downloaded"],
            "REVIEW_COVERAGE_ZERO_TOTAL": metrics["review_coverage_zero_total"],
            "AVG_RATING_MISMATCH": metrics["avg_rating_mismatches"],
            "INVALID_SALESRANK": metrics["invalid_salesrank"],
            "DUPLICATE_REVIEW_OCCURRENCE": metrics["duplicate_review_extra"],
            "ORPHAN_GRAPH_TARGET": metrics["orphan_graph_target_occurrences"],
            "DUPLICATE_GRAPH_EDGE": metrics["duplicate_graph_edge_extra"],
            "CATEGORY_LABEL_VARIANT": metrics["category_label_variants"],
            "MULTILINE_TITLE": metrics["multiline_titles"],
        }
        if event_counts != expected_event_counts:
            raise RuntimeError(
                f"G5 event reconciliation failed: {event_counts} != "
                f"{expected_event_counts}"
            )
        validate_profile_counts(metrics)

        metrics_frame = spark.createDataFrame(
            sorted((key, int(value)) for key, value in metrics.items()),
            "metric string, value long",
        )
        publish("profile_metrics", metrics_frame, "dimension")
        os.replace(working, final)
    except Exception:
        cleanup_incomplete_publications(working)
        raise

    for table in tables.values():
        table["path"] = table["path"].replace(str(working), str(final), 1)
    return {
        "junit": _junit(evidence_file),
        "implementation_sha256": signature,
        "scratch_directories_removed": cleaned_scratch,
        "tables_reused": sorted(reused_tables),
        "profile_counts": metrics,
        "quality_event_counts": event_counts,
        "rounding_contract": "nearest-0.5 Spark round/HALF_UP",
        "deduplication_key": [
            "asin",
            "customer_id",
            "review_date_raw",
            "rating",
            "votes",
            "helpful",
        ],
        "survivor_order": ["source_offset ASC", "review_ordinal ASC"],
        "tables": tables,
    }
