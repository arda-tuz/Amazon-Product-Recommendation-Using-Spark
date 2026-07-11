"""G6 leakage-safe split, ALS k-core, cohorts, and train-only Gold features."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from amazon_recommender.core.manifest import atomic_write_json
from amazon_recommender.features.split import (
    HASH_SEPARATOR,
    assign_temporal_split,
    build_active_catalog,
    build_cohorts,
    build_positive_baskets,
    build_stage_seen_items,
    build_user_profiles,
    iteration_frame,
    iterative_als_k_core,
    stable_evaluation_users,
)
from amazon_recommender.gate_handlers import register
from amazon_recommender.pipelines.storage import (
    cleanup_incomplete_publications,
    publish_or_reuse_sized_parquet,
)


FACT_TABLES = {
    "split_assignments",
    "train_interactions",
    "stage_seen_items",
    "user_profiles",
    "item_features",
    "als_train_interactions",
}


def validate_split_invariants(invariants: dict[str, int]) -> None:
    required_zero = (
        "split_pair_overlap",
        "validation_cardinality_violations",
        "test_cardinality_violations",
        "temporal_position_violations",
        "validation_target_seen_violations",
        "test_target_seen_violations",
        "test_seen_missing_validation",
        "als_user_degree_violations",
        "als_item_degree_violations",
        "common_warm_universe_violations",
        "stable_hash_violations",
        "sample_limit_violations",
    )
    failures = {key: invariants.get(key) for key in required_zero if invariants.get(key) != 0}
    if failures:
        raise RuntimeError(f"G6 leakage/split invariant failure: {failures}")
    if invariants["split_total"] != invariants["source_interactions"]:
        raise RuntimeError("G6 split rows do not reconcile to source interactions")
    if invariants["validation_interactions"] != invariants["eligible_users"]:
        raise RuntimeError("G6 validation must contain one row per eligible user")
    if invariants["test_interactions"] != invariants["eligible_users"]:
        raise RuntimeError("G6 test must contain one row per eligible user")
    if invariants["kcore_converged"] != 1:
        raise RuntimeError("G6 ALS k-core did not reach a fixed point")


def _junit(path: Path) -> dict[str, Any]:
    root = ET.parse(path).getroot()
    suites = [root] if root.tag == "testsuite" else list(root.findall("testsuite"))
    summary = {
        key: sum(int(suite.attrib.get(key, "0")) for suite in suites)
        for key in ("tests", "failures", "errors", "skipped")
    }
    if not summary["tests"] or summary["failures"] or summary["errors"]:
        raise RuntimeError(f"G6 JUnit evidence is not passing: {summary}")
    summary["path"] = str(path.resolve())
    return summary


def _signature() -> str:
    digest = hashlib.sha256()
    for path in (Path(__file__), Path(__file__).parents[1] / "features" / "split.py"):
        digest.update(path.name.encode("utf-8"))
        digest.update(path.read_bytes())
    return digest.hexdigest()


def _prepare(working: Path, signature: str) -> list[str]:
    marker = working / "_checkpoint_contract.json"
    if working.exists():
        try:
            existing = json.loads(marker.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError):
            existing = {}
        if existing.get("implementation_sha256") != signature:
            # The first full attempt completed the immutable split/catalog
            # tables before only the k-core execution strategy changed.  Keep
            # those atomically published contracts and discard every dependent
            # or incomplete artifact.
            compatible = {
                "split_assignments",
                "train_interactions",
                "validation_interactions",
                "test_interactions",
                "active_catalog",
            }
            for child in working.iterdir():
                if child.name in compatible and (child / "_SUCCESS").is_file():
                    continue
                if child.is_dir():
                    shutil.rmtree(child)
                else:
                    child.unlink()
    working.mkdir(parents=True, exist_ok=True)
    atomic_write_json(marker, {"gate": "G6", "implementation_sha256": signature})
    return cleanup_incomplete_publications(working)


@register("G6")
def run_g6(config: Any, paths: Any, evidence_file: Path | None) -> dict[str, Any]:
    if evidence_file is None:
        raise RuntimeError("G6 requires passing JUnit XML evidence")
    full = paths.data / "full" / "silver"
    g5 = paths.data / "g5"
    if not full.exists() or not g5.exists():
        raise FileNotFoundError("G6 requires materialized G4 and G5 tables")
    final = paths.data / "g6"
    if final.exists():
        raise FileExistsError(f"G6 output exists without reusable manifest: {final}")
    working = paths.temporary / "G6-publish"
    signature = _signature()
    cleaned_scratch = _prepare(working, signature)
    final.parent.mkdir(parents=True, exist_ok=True)
    spark = SparkSession.builder.appName("amazon-recommender-g6").getOrCreate()

    interactions = spark.read.parquet(str(full / "user_item_interactions"))
    products = spark.read.parquet(str(full / "products"))
    customers = spark.read.parquet(str(full / "customers"))
    product_category_nodes = spark.read.parquet(str(full / "product_category_nodes"))
    tables: dict[str, dict[str, Any]] = {}
    reused_tables: list[str] = []

    def publish(name: str, frame: Any) -> None:
        evidence, reused = publish_or_reuse_sized_parquet(
            frame,
            working / name,
            kind="fact" if name in FACT_TABLES else "dimension",
            sort_columns=tuple(
                column
                for column in (
                    "stage",
                    "cohort",
                    "customer_id",
                    "product_id",
                    "sample_rank",
                )
                if column in frame.columns
            ),
        )
        tables[name] = evidence
        if reused:
            reused_tables.append(name)

    kcore_result = None
    try:
        assigned = assign_temporal_split(
            interactions,
            evaluation_min_distinct_items=config.get(
                "split", "evaluation_min_distinct_items"
            ),
        )
        publish("split_assignments", assigned)
        materialized = spark.read.parquet(str(working / "split_assignments"))
        helper_columns = [
            "split",
            "evaluation_eligible",
            "dated_distinct_items",
            "temporal_position",
            "customer_interaction_count",
        ]
        model_columns = [column for column in materialized.columns if column not in helper_columns]
        train = materialized.filter("split = 'train'").select(*model_columns)
        validation = materialized.filter("split = 'validation'").select(*model_columns)
        test = materialized.filter("split = 'test'").select(*model_columns)
        publish("train_interactions", train)
        publish("validation_interactions", validation)
        publish("test_interactions", test)
        train = spark.read.parquet(str(working / "train_interactions"))
        validation = spark.read.parquet(str(working / "validation_interactions"))
        test = spark.read.parquet(str(working / "test_interactions"))

        active_catalog = build_active_catalog(products)
        publish("active_catalog", active_catalog)
        active_catalog = spark.read.parquet(str(working / "active_catalog"))

        als_train_path = working / "als_train_interactions"
        iteration_path = working / "als_kcore_iterations"
        if (als_train_path / "_SUCCESS").is_file() and (
            iteration_path / "_SUCCESS"
        ).is_file():
            als_train = spark.read.parquet(str(als_train_path))
            iterations = [row.asDict() for row in spark.read.parquet(str(iteration_path)).collect()]
            publish("als_train_interactions", als_train)
            publish("als_kcore_iterations", spark.read.parquet(str(iteration_path)))
        else:
            train_with_ids = train.join(
                customers.select("customer_id", "customer_int_id"),
                "customer_id",
                "inner",
            )
            kcore_result = iterative_als_k_core(
                train_with_ids,
                min_user_items=config.get("als_k_core", "min_user_items"),
                min_item_users=config.get("als_k_core", "min_item_users"),
                checkpoint_root=working / "_kcore_work",
            )
            als_train = kcore_result.interactions
            iterations = kcore_result.iterations
            publish("als_train_interactions", als_train)
            publish("als_kcore_iterations", iteration_frame(spark, iterations))
            kcore_result.interactions.unpersist()
            shutil.rmtree(working / "_kcore_work", ignore_errors=True)
            als_train = spark.read.parquet(str(als_train_path))
        als_users = als_train.select("customer_id", "customer_int_id").distinct()
        als_items = als_train.select("product_id").distinct()
        publish("als_users", als_users)
        publish("als_items", als_items)
        als_users = spark.read.parquet(str(working / "als_users"))
        als_items = spark.read.parquet(str(working / "als_items"))

        stage_seen = build_stage_seen_items(train, validation)
        publish("stage_seen_items", stage_seen)
        stage_seen = spark.read.parquet(str(working / "stage_seen_items"))
        cohorts, cohort_flags = build_cohorts(
            train,
            validation,
            test,
            active_catalog,
            stage_seen,
            als_users,
            als_items,
        )
        publish("cohort_candidates", cohorts)
        publish("cohort_target_flags", cohort_flags)
        cohorts = spark.read.parquet(str(working / "cohort_candidates"))
        evaluation_users = stable_evaluation_users(
            cohorts,
            seed=config.get("project", "seed"),
            limit=config.get("sampling", "cohort_limit"),
        )
        publish("evaluation_users", evaluation_users)

        positive_baskets = build_positive_baskets(
            train,
            max_basket_size=config.get("models", "fp_growth", "max_basket_size"),
        )
        publish("positive_user_baskets", positive_baskets)
        user_profiles = build_user_profiles(train)
        publish("user_profiles", user_profiles)
        item_features = product_category_nodes.join(
            active_catalog.select("product_id"), "product_id", "inner"
        )
        publish("item_features", item_features)

        split_counts = {
            row.split: int(row["count"])
            for row in materialized.groupBy("split").count().collect()
        }
        eligible_users = materialized.filter("evaluation_eligible").select(
            "customer_id"
        ).distinct().count()
        split_pair_overlap = (
            materialized.groupBy("customer_id", "product_id")
            .agg(F.countDistinct("split").alias("split_count"))
            .filter(F.col("split_count") > F.lit(1))
            .limit(1)
            .count()
        )
        validation_cardinality_violations = (
            validation.groupBy("customer_id")
            .count()
            .filter(F.col("count") != F.lit(1))
            .limit(1)
            .count()
        )
        test_cardinality_violations = (
            test.groupBy("customer_id")
            .count()
            .filter(F.col("count") != F.lit(1))
            .limit(1)
            .count()
        )
        temporal_position_violations = materialized.filter(
            ((F.col("split") == "test") & (F.col("temporal_position") != F.col("customer_interaction_count")))
            | (
                (F.col("split") == "validation")
                & (
                    F.col("temporal_position")
                    != F.col("customer_interaction_count") - F.lit(1)
                )
            )
            | (
                (~F.col("evaluation_eligible"))
                & (F.col("split") != F.lit("train"))
            )
        ).limit(1).count()

        validation_target_seen = (
            validation.select("customer_id", "product_id")
            .join(
                stage_seen.filter("stage = 'validation'").select(
                    "customer_id", "product_id"
                ),
                ["customer_id", "product_id"],
                "inner",
            )
            .count()
        )
        test_target_seen = (
            test.select("customer_id", "product_id")
            .join(
                stage_seen.filter("stage = 'test'").select(
                    "customer_id", "product_id"
                ),
                ["customer_id", "product_id"],
                "inner",
            )
            .count()
        )
        missing_validation_in_test_seen = (
            validation.select("customer_id", "product_id")
            .join(
                stage_seen.filter("stage = 'test'").select(
                    "customer_id", "product_id"
                ),
                ["customer_id", "product_id"],
                "left_anti",
            )
            .count()
        )
        als_user_degree_violations = (
            als_train.groupBy("customer_id")
            .agg(F.countDistinct("product_id").alias("degree"))
            .filter(F.col("degree") < F.lit(config.get("als_k_core", "min_user_items")))
            .limit(1)
            .count()
        )
        als_item_degree_violations = (
            als_train.groupBy("product_id")
            .agg(F.countDistinct("customer_id").alias("degree"))
            .filter(F.col("degree") < F.lit(config.get("als_k_core", "min_item_users")))
            .limit(1)
            .count()
        )
        common = cohorts.filter("cohort = 'common_warm'")
        common_warm_universe_violations = (
            common.join(als_users.select("customer_id"), "customer_id", "left_anti").count()
            + common.join(
                als_items.select(F.col("product_id").alias("target_product_id")),
                "target_product_id",
                "left_anti",
            ).count()
        )
        materialized_evaluation = spark.read.parquet(str(working / "evaluation_users"))
        stable_hash_violations = materialized_evaluation.filter(
            F.col("stable_hash")
            != F.sha2(
                F.concat(
                    F.col("customer_id"),
                    F.lit(HASH_SEPARATOR),
                    F.lit(str(config.get("project", "seed"))),
                ),
                256,
            )
        ).count()
        sample_limit_violations = (
            materialized_evaluation.groupBy("stage", "cohort")
            .count()
            .filter(F.col("count") > F.lit(config.get("sampling", "cohort_limit")))
            .count()
        )
        kcore_rows = spark.read.parquet(str(working / "als_kcore_iterations")).orderBy(
            "iteration"
        ).collect()
        last_iteration = kcore_rows[-1].asDict()
        invariants = {
            "source_interactions": interactions.count(),
            "split_total": sum(split_counts.values()),
            "train_interactions": split_counts.get("train", 0),
            "validation_interactions": split_counts.get("validation", 0),
            "test_interactions": split_counts.get("test", 0),
            "eligible_users": eligible_users,
            "split_pair_overlap": split_pair_overlap,
            "validation_cardinality_violations": validation_cardinality_violations,
            "test_cardinality_violations": test_cardinality_violations,
            "temporal_position_violations": temporal_position_violations,
            "validation_target_seen_violations": validation_target_seen,
            "test_target_seen_violations": test_target_seen,
            "test_seen_missing_validation": missing_validation_in_test_seen,
            "als_user_degree_violations": als_user_degree_violations,
            "als_item_degree_violations": als_item_degree_violations,
            "common_warm_universe_violations": common_warm_universe_violations,
            "stable_hash_violations": stable_hash_violations,
            "sample_limit_violations": sample_limit_violations,
            "kcore_converged": int(bool(last_iteration["converged"])),
        }
        validate_split_invariants(invariants)
        split_summary = spark.createDataFrame(
            sorted((key, int(value)) for key, value in invariants.items()),
            "metric string, value long",
        )
        publish("split_validation_summary", split_summary)
        cohort_counts = cohorts.groupBy("stage", "cohort").count()
        publish("cohort_counts", cohort_counts)
        os.replace(working, final)
    except Exception:
        if kcore_result is not None:
            kcore_result.interactions.unpersist()
        cleanup_incomplete_publications(working)
        raise

    for table in tables.values():
        table["path"] = table["path"].replace(str(working), str(final), 1)
    return {
        "junit": _junit(evidence_file),
        "implementation_sha256": signature,
        "scratch_directories_removed": cleaned_scratch,
        "tables_reused": sorted(reused_tables),
        "split_order": ["interaction_date ASC", "product_id ASC"],
        "single_fit_contract": True,
        "validation_seen": "train only",
        "test_seen": "train plus validation target",
        "stable_hash": "SHA256(customer_id + U+001F + '42')",
        "sample_limit_per_stage_cohort": config.get("sampling", "cohort_limit"),
        "invariants": invariants,
        "kcore_iterations": [row.asDict() for row in kcore_rows],
        "cohort_counts": {
            f"{row.stage}.{row.cohort}": int(row["count"])
            for row in spark.read.parquet(str(final / "cohort_counts")).collect()
        },
        "tables": tables,
    }
