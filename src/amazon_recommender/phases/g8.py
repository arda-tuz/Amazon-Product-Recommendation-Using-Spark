"""G8 rank-only fusion of the five frozen G7 recommendation lists.

This gate materializes one shared candidate-evidence table and exactly two weighted
RRF variants (H-A and H-B).  It never imports or invokes a model estimator: every
input is an already-published G7 rank.  Publications use the same resume-aware,
measure-then-size Parquet protocol as the earlier durable gates.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import shutil
import time
import xml.etree.ElementTree as ET
from functools import reduce
from pathlib import Path
from typing import Any, Mapping

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType

from amazon_recommender.core.manifest import atomic_write_json
from amazon_recommender.gate_handlers import register
from amazon_recommender.models.hybrid import (
    H_A_WEIGHTS,
    H_B_WEIGHTS,
    HYBRID_STORED_DEPTH,
    MODEL_CANDIDATE_DEPTHS,
    MODEL_NAMES,
    RRF_C,
    build_hybrid_frames,
    score_materialized_hybrid_candidates,
)
from amazon_recommender.pipelines.storage import (
    cleanup_incomplete_publications,
    publish_or_reuse_sized_parquet,
)


G8_CONTRACT_VERSION = 2
HYBRID_VARIANTS: Mapping[str, Mapping[str, float]] = {
    "h_a": H_A_WEIGHTS,
    "h_b": H_B_WEIGHTS,
}
OUTPUT_TABLES = (
    "hybrid_candidates",
    "hybrid_a_recommendations",
    "hybrid_b_recommendations",
    "hybrid_experiment_budget",
    "hybrid_runtime_summary",
    "hybrid_contract_summary",
)
FACT_TABLES = {
    "hybrid_candidates",
    "hybrid_a_recommendations",
    "hybrid_b_recommendations",
}


def _junit(path: Path) -> dict[str, Any]:
    root = ET.parse(path).getroot()
    suites = [root] if root.tag == "testsuite" else list(root.findall("testsuite"))
    summary = {
        key: sum(int(suite.attrib.get(key, "0")) for suite in suites)
        for key in ("tests", "failures", "errors", "skipped")
    }
    if not summary["tests"] or summary["failures"] or summary["errors"]:
        raise RuntimeError(f"G8 JUnit evidence is not passing: {summary}")
    summary["path"] = str(path.resolve())
    return summary


def _implementation_signature(config_sha256: str) -> str:
    digest = hashlib.sha256()
    digest.update(f"g8-contract-{G8_CONTRACT_VERSION}".encode("ascii"))
    digest.update(config_sha256.encode("ascii"))
    for path in (Path(__file__), Path(__file__).parents[1] / "models" / "hybrid.py"):
        digest.update(path.name.encode("utf-8"))
        digest.update(path.read_bytes())
    return digest.hexdigest()


def _prepare_workspace(working: Path, signature: str) -> list[str]:
    """Keep complete same-contract tables and discard incompatible/partial state."""

    marker = working / "_checkpoint_contract.json"
    if working.exists():
        try:
            existing = json.loads(marker.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError):
            existing = {}
        if existing.get("implementation_sha256") != signature:
            shutil.rmtree(working)
    working.mkdir(parents=True, exist_ok=True)
    atomic_write_json(
        marker,
        {
            "gate": "G8",
            "contract_version": G8_CONTRACT_VERSION,
            "implementation_sha256": signature,
        },
    )
    return cleanup_incomplete_publications(working)


def _require_columns(frame: DataFrame, required: set[str], name: str) -> None:
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise RuntimeError(f"{name} is missing required columns: {missing}")


def _raise_for_violations(context: str, violations: Mapping[str, int]) -> None:
    failures = {name: int(value) for name, value in violations.items() if value}
    if failures:
        raise RuntimeError(f"{context} invariant failure: {failures}")


def _literal_map(values: Mapping[str, float | int]) -> F.Column:
    entries: list[F.Column] = []
    for key, value in values.items():
        entries.extend((F.lit(key), F.lit(value)))
    return F.create_map(*entries)


def _canonical_source_candidates(
    candidate_frames: Mapping[str, DataFrame],
) -> DataFrame:
    actual = set(candidate_frames)
    expected = set(MODEL_NAMES)
    if actual != expected:
        raise RuntimeError(
            "G8 requires exactly the five frozen G7 candidate tables; "
            f"missing={sorted(expected - actual)}, extra={sorted(actual - expected)}"
        )
    frames: list[DataFrame] = []
    for model in MODEL_NAMES:
        frame = candidate_frames[model]
        _require_columns(
            frame,
            {"stage", "customer_id", "product_id", "rank"},
            f"{model}_recommendations",
        )
        frames.append(
            frame.select(
                "stage",
                "customer_id",
                "product_id",
                F.lit(model).alias("model"),
                F.col("rank").cast("int").alias("rank"),
            )
        )
    return reduce(DataFrame.unionByName, frames)


def validate_bayesian_score_evidence(bayesian_scores: DataFrame) -> dict[str, int]:
    """Require one finite G7 tie-break score row per represented product."""

    _require_columns(
        bayesian_scores,
        {"product_id", "global_bayesian_score"},
        "G7 popularity_scores",
    )
    if not isinstance(
        bayesian_scores.schema["global_bayesian_score"].dataType, NumericType
    ):
        raise RuntimeError("G7 popularity_scores.global_bayesian_score must be numeric")
    row = bayesian_scores.agg(
        F.count(F.lit(1)).cast("long").alias("rows"),
        F.sum(
            F.when(
                F.col("product_id").isNull()
                | F.col("global_bayesian_score").isNull(),
                1,
            ).otherwise(0)
        ).cast("long").alias("null_violations"),
        F.sum(
            F.when(
                F.isnan("global_bayesian_score")
                | (
                    F.abs(F.col("global_bayesian_score").cast("double"))
                    > F.lit(1.7976931348623157e308)
                ),
                1,
            ).otherwise(0)
        ).cast("long").alias("nonfinite_violations"),
    ).first().asDict()
    duplicate_products = (
        bayesian_scores.groupBy("product_id")
        .count()
        .filter(F.col("count") != F.lit(1))
        .count()
    )
    violations = {
        "null_violations": int(row["null_violations"] or 0),
        "nonfinite_violations": int(row["nonfinite_violations"] or 0),
        "duplicate_product_violations": duplicate_products,
    }
    if not int(row["rows"]):
        raise RuntimeError("G7 popularity_scores contains no tie-break evidence")
    _raise_for_violations("G8 Bayesian tie evidence", violations)
    return {"rows": int(row["rows"]), **violations}


def validate_hybrid_candidate_evidence(
    candidates: DataFrame,
    candidate_frames: Mapping[str, DataFrame],
    evaluation_users: DataFrame,
) -> dict[str, Any]:
    """Prove that the shared evidence is exactly the five frozen G7 rank lists."""

    required = {
        "stage",
        "customer_id",
        "product_id",
        "contributing_model_count",
        "global_bayesian_score",
        "has_bayesian_score",
        "model_ranks",
    }
    required.update(f"{model}_rank" for model in MODEL_NAMES)
    _require_columns(candidates, required, "hybrid_candidates")
    _require_columns(
        evaluation_users,
        {"stage", "customer_id"},
        "G6 evaluation_users",
    )
    source = _canonical_source_candidates(candidate_frames)
    depth_map = _literal_map(MODEL_CANDIDATE_DEPTHS)
    request_keys = evaluation_users.select("stage", "customer_id").dropDuplicates()

    source_row = source.agg(
        F.count(F.lit(1)).cast("long").alias("source_occurrences"),
        F.sum(
            F.when(
                F.col("stage").isNull()
                | F.col("customer_id").isNull()
                | F.col("product_id").isNull(),
                1,
            ).otherwise(0)
        ).cast("long").alias("source_null_key_violations"),
        F.sum(
            F.when(
                F.col("rank").isNull()
                | (F.col("rank") < F.lit(1))
                | (F.col("rank") > F.element_at(depth_map, F.col("model"))),
                1,
            ).otherwise(0)
        ).cast("long").alias("source_depth_violations"),
    ).first().asDict()
    source_duplicates = (
        source.groupBy("stage", "customer_id", "product_id", "model")
        .count()
        .filter(F.col("count") != F.lit(1))
        .count()
    )
    source_request_universe_violations = (
        source.select("stage", "customer_id")
        .dropDuplicates()
        .join(request_keys, ["stage", "customer_id"], "left_anti")
        .count()
    )

    evidence_long = candidates.select(
        "stage",
        "customer_id",
        "product_id",
        F.explode("model_ranks").alias("model", "rank"),
    ).select(
        "stage",
        "customer_id",
        "product_id",
        "model",
        F.col("rank").cast("int").alias("rank"),
    )
    source_minus_evidence = source.exceptAll(evidence_long).count()
    evidence_minus_source = evidence_long.exceptAll(source).count()

    candidate_row = candidates.agg(
        F.count(F.lit(1)).cast("long").alias("rows"),
        F.countDistinct(F.struct("stage", "customer_id")).cast("long").alias(
            "requests_with_candidates"
        ),
        F.countDistinct("product_id").cast("long").alias("catalog_products"),
        F.sum("contributing_model_count").cast("long").alias(
            "evidence_occurrences"
        ),
        F.sum(
            F.when(
                F.col("stage").isNull()
                | F.col("customer_id").isNull()
                | F.col("product_id").isNull(),
                1,
            ).otherwise(0)
        ).cast("long").alias("null_key_violations"),
        F.sum(
            F.when(
                F.col("contributing_model_count").isNull()
                | F.col("model_ranks").isNull()
                | F.col("has_bayesian_score").isNull(),
                1,
            ).otherwise(0)
        ).cast("long").alias("structural_null_violations"),
        F.sum(
            F.when(
                F.col("global_bayesian_score").isNotNull()
                & (
                    F.isnan("global_bayesian_score")
                    | (
                        F.abs(F.col("global_bayesian_score"))
                        > F.lit(1.7976931348623157e308)
                    )
                ),
                1,
            ).otherwise(0)
        ).cast("long").alias("bayesian_nonfinite_violations"),
        F.sum(
            F.when(
                (F.col("contributing_model_count") < F.lit(1))
                | (F.col("contributing_model_count") > F.lit(len(MODEL_NAMES)))
                | (
                    F.col("contributing_model_count")
                    != F.size(F.col("model_ranks"))
                ),
                1,
            ).otherwise(0)
        ).cast("long").alias("contributor_count_violations"),
        F.sum(
            F.when(
                F.col("has_bayesian_score")
                != F.col("global_bayesian_score").isNotNull(),
                1,
            ).otherwise(0)
        ).cast("long").alias("bayesian_presence_violations"),
    ).first().asDict()
    duplicate_keys = (
        candidates.groupBy("stage", "customer_id", "product_id")
        .count()
        .filter(F.col("count") != F.lit(1))
        .count()
    )
    evidence_depth_violations = evidence_long.filter(
        ~F.col("model").isin(*MODEL_NAMES)
        | F.col("rank").isNull()
        | (F.col("rank") < F.lit(1))
        | (F.col("rank") > F.element_at(depth_map, F.col("model")))
    ).count()

    violations = {
        "source_null_key_violations": source_row["source_null_key_violations"],
        "source_depth_violations": source_row["source_depth_violations"],
        "source_duplicate_model_candidates": source_duplicates,
        "source_request_universe_violations": source_request_universe_violations,
        "source_minus_evidence": source_minus_evidence,
        "evidence_minus_source": evidence_minus_source,
        "null_key_violations": candidate_row["null_key_violations"],
        "structural_null_violations": candidate_row[
            "structural_null_violations"
        ],
        "bayesian_nonfinite_violations": candidate_row[
            "bayesian_nonfinite_violations"
        ],
        "duplicate_key_violations": duplicate_keys,
        "contributor_count_violations": candidate_row[
            "contributor_count_violations"
        ],
        "bayesian_presence_violations": candidate_row[
            "bayesian_presence_violations"
        ],
        "evidence_depth_violations": evidence_depth_violations,
        "occurrence_reconciliation": abs(
            int(source_row["source_occurrences"])
            - int(candidate_row["evidence_occurrences"] or 0)
        ),
    }
    if not int(candidate_row["rows"]):
        raise RuntimeError("hybrid_candidates contains no rows")
    _raise_for_violations("G8 shared candidate evidence", violations)
    by_model = {
        row.model: int(row["count"])
        for row in source.groupBy("model").count().collect()
    }
    return {
        "rows": int(candidate_row["rows"]),
        "source_occurrences": int(source_row["source_occurrences"]),
        "requests_with_candidates": int(candidate_row["requests_with_candidates"]),
        "catalog_products": int(candidate_row["catalog_products"]),
        "source_rows_by_model": by_model,
        "authoritative_request_count": request_keys.count(),
        "candidate_depths": dict(MODEL_CANDIDATE_DEPTHS),
        **{name: int(value) for name, value in violations.items()},
    }


def validate_hybrid_recommendations(
    frame: DataFrame,
    candidates: DataFrame,
    *,
    variant: str,
    weights: Mapping[str, float],
) -> dict[str, Any]:
    """Prove one materialized variant's exact RRF, depth, and tie contracts."""

    if variant not in HYBRID_VARIANTS or set(weights) != set(MODEL_NAMES):
        raise RuntimeError(f"unsupported G8 hybrid variant: {variant}")
    required = {
        "stage",
        "customer_id",
        "product_id",
        "rank",
        "hybrid_variant",
        "hybrid_score",
        "contributing_model_count",
        "global_bayesian_score",
        "has_bayesian_score",
        "active_model_count",
        "active_models",
        "active_weight_sum",
        "model_ranks",
        "model_contributions",
    }
    _require_columns(frame, required, f"{variant}_recommendations")

    weight_map = _literal_map(weights)
    candidate_models = (
        candidates.select(
            "stage",
            "customer_id",
            F.explode(F.map_keys("model_ranks")).alias("model"),
        )
        .dropDuplicates()
        .withColumn("_candidate_base_weight", F.element_at(weight_map, F.col("model")))
    )
    candidate_active = (
        candidate_models.groupBy("stage", "customer_id")
        .agg(
            F.count(F.lit(1)).cast("int").alias("_expected_active_model_count"),
            F.sort_array(F.collect_set("model")).alias("_expected_active_models"),
        )
        .withColumn(
            "_expected_active_weight_sum",
            F.aggregate(
                F.col("_expected_active_models"),
                F.lit(0.0),
                lambda total, model: total + F.element_at(weight_map, model),
            ),
        )
    )
    candidate_expected_score = F.aggregate(
        F.map_entries(F.col("model_ranks")),
        F.lit(0.0),
        lambda total, entry: total
        + (
            F.element_at(weight_map, entry["key"])
            / F.col("_expected_active_weight_sum")
        )
        / (F.lit(float(RRF_C)) + entry["value"].cast("double")),
    )
    candidate_order = Window.partitionBy("stage", "customer_id").orderBy(
        F.col("_candidate_expected_score").desc(),
        F.col("contributing_model_count").desc(),
        F.col("global_bayesian_score").desc_nulls_last(),
        F.col("product_id").asc(),
    )
    expected_top_k = (
        candidates.join(candidate_active, ["stage", "customer_id"], "inner")
        .withColumn("_candidate_expected_score", candidate_expected_score)
        .withColumn(
            "_candidate_expected_contributions",
            F.transform_values(
                F.col("model_ranks"),
                lambda model, rank: (
                    F.element_at(weight_map, model)
                    / F.col("_expected_active_weight_sum")
                )
                / (F.lit(float(RRF_C)) + rank.cast("double")),
            ),
        )
        .withColumn("_candidate_expected_rank", F.row_number().over(candidate_order))
        .filter(F.col("_candidate_expected_rank") <= F.lit(HYBRID_STORED_DEPTH))
        .persist(StorageLevel.DISK_ONLY)
    )
    expected_active_weight = F.aggregate(
        F.col("active_models"),
        F.lit(0.0),
        lambda total, model: total + F.element_at(weight_map, model),
    )
    expected_score = F.aggregate(
        F.map_entries(F.col("model_ranks")),
        F.lit(0.0),
        lambda total, entry: total
        + (
            F.element_at(weight_map, entry["key"]) / expected_active_weight
        )
        / (F.lit(float(RRF_C)) + entry["value"].cast("double")),
    )
    contribution_sum = F.aggregate(
        F.map_values(F.col("model_contributions")),
        F.lit(0.0),
        lambda total, contribution: total + contribution,
    )
    expected_order = Window.partitionBy("stage", "customer_id").orderBy(
        F.col("hybrid_score").desc(),
        F.col("contributing_model_count").desc(),
        F.col("global_bayesian_score").desc_nulls_last(),
        F.col("product_id").asc(),
    )
    checked = (
        frame.withColumn("_expected_active_weight", expected_active_weight)
        .withColumn("_expected_score", expected_score)
        .withColumn("_contribution_sum", contribution_sum)
        .withColumn("_expected_rank", F.row_number().over(expected_order))
    )
    row = checked.agg(
        F.count(F.lit(1)).cast("long").alias("rows"),
        F.countDistinct(F.struct("stage", "customer_id")).cast("long").alias(
            "requests_with_output"
        ),
        F.countDistinct("product_id").cast("long").alias("catalog_products"),
        F.sum(
            F.when(
                F.col("stage").isNull()
                | F.col("customer_id").isNull()
                | F.col("product_id").isNull(),
                1,
            ).otherwise(0)
        ).cast("long").alias("null_key_violations"),
        F.sum(
            F.when(F.col("hybrid_variant") != F.lit(variant), 1).otherwise(0)
        ).cast("long").alias("variant_label_violations"),
        F.sum(
            F.when(
                F.col("rank").isNull()
                | (F.col("rank") < F.lit(1))
                | (F.col("rank") > F.lit(HYBRID_STORED_DEPTH)),
                1,
            ).otherwise(0)
        ).cast("long").alias("depth_violations"),
        F.sum(
            F.when(F.col("rank") != F.col("_expected_rank"), 1).otherwise(0)
        ).cast("long").alias("deterministic_order_violations"),
        F.sum(
            F.when(
                F.col("hybrid_score").isNull()
                | F.isnan("hybrid_score")
                | (F.col("hybrid_score") <= F.lit(0.0))
                | (F.abs(F.col("hybrid_score")) > F.lit(1.7976931348623157e308)),
                1,
            ).otherwise(0)
        ).cast("long").alias("finite_score_violations"),
        F.sum(
            F.when(
                F.col("active_model_count") != F.size(F.col("active_models")), 1
            ).otherwise(0)
        ).cast("long").alias("active_model_count_violations"),
        F.sum(
            F.when(
                (F.col("active_model_count") < F.lit(1))
                | (F.col("active_model_count") > F.lit(len(MODEL_NAMES)))
                | F.col("_expected_active_weight").isNull()
                | (
                    F.abs(
                        F.col("active_weight_sum")
                        - F.col("_expected_active_weight")
                    )
                    > F.lit(1e-12)
                ),
                1,
            ).otherwise(0)
        ).cast("long").alias("active_weight_violations"),
        F.sum(
            F.when(
                F.col("_expected_score").isNull()
                | (
                    F.abs(F.col("hybrid_score") - F.col("_expected_score"))
                    > F.lit(1e-12)
                ),
                1,
            ).otherwise(0)
        ).cast("long").alias("rrf_score_violations"),
        F.sum(
            F.when(
                F.col("_contribution_sum").isNull()
                | (
                    F.abs(F.col("hybrid_score") - F.col("_contribution_sum"))
                    > F.lit(1e-12)
                ),
                1,
            ).otherwise(0)
        ).cast("long").alias("contribution_sum_violations"),
        F.sum(
            F.when(
                (F.col("contributing_model_count") != F.size("model_ranks"))
                | (
                    F.col("contributing_model_count")
                    != F.size("model_contributions")
                ),
                1,
            ).otherwise(0)
        ).cast("long").alias("contribution_count_violations"),
    ).first().asDict()
    duplicate_keys = (
        frame.groupBy("stage", "customer_id", "product_id")
        .count()
        .filter(F.col("count") != F.lit(1))
        .count()
    )

    expected_rank_rows = expected_top_k.select(
        "stage",
        "customer_id",
        "product_id",
        F.col("_candidate_expected_rank").cast("int").alias("rank"),
    )
    output_rank_rows = frame.select(
        "stage", "customer_id", "product_id", F.col("rank").cast("int")
    )
    expected_top_k_minus_output = expected_rank_rows.exceptAll(output_rank_rows).count()
    output_minus_expected_top_k = output_rank_rows.exceptAll(expected_rank_rows).count()
    candidate_derived_contract_violations = (
        frame.join(
            expected_top_k.select(
                "stage",
                "customer_id",
                "product_id",
                "_candidate_expected_score",
                "_candidate_expected_rank",
                "_expected_active_weight_sum",
                "_expected_active_model_count",
                "_expected_active_models",
                F.col("contributing_model_count").alias(
                    "_expected_contributing_model_count"
                ),
                F.col("global_bayesian_score").alias(
                    "_expected_global_bayesian_score"
                ),
                F.col("has_bayesian_score").alias("_expected_has_bayesian_score"),
            ),
            ["stage", "customer_id", "product_id"],
            "left",
        )
        .filter(
            F.col("_candidate_expected_rank").isNull()
            | F.col("active_weight_sum").isNull()
            | F.col("active_model_count").isNull()
            | F.col("active_models").isNull()
            | F.col("contributing_model_count").isNull()
            | F.col("has_bayesian_score").isNull()
            | F.col("model_ranks").isNull()
            | F.col("model_contributions").isNull()
            | (F.col("rank") != F.col("_candidate_expected_rank"))
            | (
                F.abs(F.col("hybrid_score") - F.col("_candidate_expected_score"))
                > F.lit(1e-12)
            )
            | (
                F.abs(
                    F.col("active_weight_sum")
                    - F.col("_expected_active_weight_sum")
                )
                > F.lit(1e-12)
            )
            | (F.col("active_model_count") != F.col("_expected_active_model_count"))
            | (F.col("active_models") != F.col("_expected_active_models"))
            | (
                F.col("contributing_model_count")
                != F.col("_expected_contributing_model_count")
            )
            | ~F.col("global_bayesian_score").eqNullSafe(
                F.col("_expected_global_bayesian_score")
            )
            | (
                F.col("has_bayesian_score")
                != F.col("_expected_has_bayesian_score")
            )
        )
        .count()
    )
    expected_model_ranks_long = expected_top_k.select(
        "stage",
        "customer_id",
        "product_id",
        F.explode("model_ranks").alias("model", "model_rank"),
    )
    output_model_ranks_long = frame.select(
        "stage",
        "customer_id",
        "product_id",
        F.explode("model_ranks").alias("model", "model_rank"),
    )
    candidate_model_rank_violations = (
        expected_model_ranks_long.exceptAll(output_model_ranks_long).count()
        + output_model_ranks_long.exceptAll(expected_model_ranks_long).count()
    )
    expected_contributions_long = expected_top_k.select(
        "stage",
        "customer_id",
        "product_id",
        F.explode("_candidate_expected_contributions").alias(
            "model", "_expected_contribution"
        ),
    )
    output_contributions_long = frame.select(
        "stage",
        "customer_id",
        "product_id",
        F.explode("model_contributions").alias("model", "_output_contribution"),
    )
    candidate_contribution_violations = (
        expected_contributions_long.join(
            output_contributions_long,
            ["stage", "customer_id", "product_id", "model"],
            "full",
        )
        .filter(
            F.col("_expected_contribution").isNull()
            | F.col("_output_contribution").isNull()
            | (
                F.abs(
                    F.col("_expected_contribution")
                    - F.col("_output_contribution")
                )
                > F.lit(1e-12)
            )
        )
        .count()
    )

    evidence_keys = candidates.select(
        "stage", "customer_id", "product_id", F.lit(True).alias("_candidate_exists")
    )
    candidate_membership_violations = (
        frame.select("stage", "customer_id", "product_id")
        .join(evidence_keys, ["stage", "customer_id", "product_id"], "left")
        .filter(F.col("_candidate_exists").isNull())
        .count()
    )
    candidate_per_request = candidates.groupBy("stage", "customer_id").agg(
        F.count(F.lit(1)).cast("long").alias("candidate_count")
    )
    output_per_request = frame.groupBy("stage", "customer_id").agg(
        F.count(F.lit(1)).cast("long").alias("output_count"),
        F.countDistinct("rank").cast("long").alias("distinct_ranks"),
        F.min("rank").cast("int").alias("min_rank"),
        F.max("rank").cast("int").alias("max_rank"),
        F.countDistinct(F.to_json("active_models")).cast("long").alias(
            "active_model_set_count"
        ),
        F.countDistinct("active_weight_sum").cast("long").alias(
            "active_weight_sum_count"
        ),
    )
    per_request = candidate_per_request.join(
        output_per_request, ["stage", "customer_id"], "full"
    )
    request_contract_violations = per_request.filter(
        F.col("candidate_count").isNull()
        | F.col("output_count").isNull()
        | (
            F.col("output_count")
            != F.least(F.col("candidate_count"), F.lit(HYBRID_STORED_DEPTH))
        )
        | (F.col("min_rank") != F.lit(1))
        | (F.col("max_rank") != F.col("output_count"))
        | (F.col("distinct_ranks") != F.col("output_count"))
        | (F.col("active_model_set_count") != F.lit(1))
        | (F.col("active_weight_sum_count") != F.lit(1))
    ).count()

    violations = {
        "null_key_violations": row["null_key_violations"],
        "duplicate_key_violations": duplicate_keys,
        "variant_label_violations": row["variant_label_violations"],
        "depth_violations": row["depth_violations"],
        "deterministic_order_violations": row[
            "deterministic_order_violations"
        ],
        "finite_score_violations": row["finite_score_violations"],
        "active_model_count_violations": row[
            "active_model_count_violations"
        ],
        "active_weight_violations": row["active_weight_violations"],
        "rrf_score_violations": row["rrf_score_violations"],
        "contribution_sum_violations": row["contribution_sum_violations"],
        "contribution_count_violations": row[
            "contribution_count_violations"
        ],
        "candidate_membership_violations": candidate_membership_violations,
        "request_contract_violations": request_contract_violations,
        "expected_top_k_minus_output": expected_top_k_minus_output,
        "output_minus_expected_top_k": output_minus_expected_top_k,
        "candidate_derived_contract_violations": candidate_derived_contract_violations,
        "candidate_model_rank_violations": candidate_model_rank_violations,
        "candidate_contribution_violations": candidate_contribution_violations,
    }
    if not int(row["rows"]):
        expected_top_k.unpersist(blocking=True)
        raise RuntimeError(f"{variant} recommendations contains no rows")
    try:
        _raise_for_violations(f"G8 {variant}", violations)
    except Exception:
        expected_top_k.unpersist(blocking=True)
        raise
    distribution = output_per_request.agg(
        F.min("output_count").cast("long").alias("min_candidates"),
        F.avg("output_count").alias("avg_candidates"),
        F.max("output_count").cast("long").alias("max_candidates"),
    ).first().asDict()
    result = {
        "variant": variant,
        "rows": int(row["rows"]),
        "requests_with_output": int(row["requests_with_output"]),
        "catalog_products": int(row["catalog_products"]),
        "min_candidates": int(distribution["min_candidates"]),
        "avg_candidates": float(distribution["avg_candidates"]),
        "max_candidates": int(distribution["max_candidates"]),
        "rrf_c": RRF_C,
        "stored_depth": HYBRID_STORED_DEPTH,
        "weights": dict(weights),
        **{name: int(value) for name, value in violations.items()},
    }
    expected_top_k.unpersist(blocking=True)
    return result


def validate_experiment_budget(
    g7_budget: DataFrame, hybrid_budget: DataFrame
) -> dict[str, Any]:
    """Prove five prior single fits and exactly two zero-fit rank fusions."""

    _require_columns(
        g7_budget,
        {"model", "fit_count", "candidate_depth", "training_contract"},
        "G7 experiment_budget_summary",
    )
    _require_columns(
        hybrid_budget,
        {
            "variant",
            "model_fit_count",
            "independent_model_count",
            "rrf_c",
            "stored_depth",
            "selection_status",
            "candidate_source",
            "weights_json",
        },
        "G8 hybrid_experiment_budget",
    )
    g7_rows = {row.model: row.asDict() for row in g7_budget.collect()}
    if set(g7_rows) != set(MODEL_NAMES) or len(g7_rows) != len(MODEL_NAMES):
        raise RuntimeError("G8 requires exactly five G7 independent-model budget rows")
    for model, expected_depth in MODEL_CANDIDATE_DEPTHS.items():
        row = g7_rows[model]
        if (
            int(row["fit_count"]) != 1
            or int(row["candidate_depth"]) != expected_depth
            or row["training_contract"] != "train_only_single_fit"
        ):
            raise RuntimeError(f"G7 frozen-model budget mismatch for {model}: {row}")

    hybrid_rows = {row.variant: row.asDict() for row in hybrid_budget.collect()}
    if set(hybrid_rows) != set(HYBRID_VARIANTS) or len(hybrid_rows) != 2:
        raise RuntimeError("G8 experiment budget permits exactly h_a and h_b")
    for variant, expected_weights in HYBRID_VARIANTS.items():
        row = hybrid_rows[variant]
        try:
            weights = json.loads(row["weights_json"])
        except (TypeError, json.JSONDecodeError) as error:
            raise RuntimeError(f"invalid weights_json for {variant}") from error
        if (
            int(row["model_fit_count"]) != 0
            or int(row["independent_model_count"]) != len(MODEL_NAMES)
            or int(row["rrf_c"]) != RRF_C
            or int(row["stored_depth"]) != HYBRID_STORED_DEPTH
            or row["selection_status"] != "pending_validation_g9"
            or row["candidate_source"] != "g7_frozen_rank_only"
            or weights != dict(expected_weights)
        ):
            raise RuntimeError(f"G8 hybrid budget mismatch for {variant}: {row}")
    return {
        "g7_independent_model_count": len(g7_rows),
        "g7_total_fit_count": sum(int(row["fit_count"]) for row in g7_rows.values()),
        "hybrid_variant_count": len(hybrid_rows),
        "g8_model_refit_count": sum(
            int(row["model_fit_count"]) for row in hybrid_rows.values()
        ),
        "variants": sorted(hybrid_rows),
        "selection_status": "pending_validation_g9",
    }


def _validate_binding_config(config: Any) -> None:
    actual = {
        "rrf_c": config.get("hybrid", "rrf_c"),
        "stored_depth": config.get("hybrid", "stored_depth"),
        "h_a": dict(config.get("hybrid", "h_a")),
        "h_b": dict(config.get("hybrid", "h_b")),
    }
    expected = {
        "rrf_c": RRF_C,
        "stored_depth": HYBRID_STORED_DEPTH,
        "h_a": dict(H_A_WEIGHTS),
        "h_b": dict(H_B_WEIGHTS),
    }
    if actual != expected:
        raise RuntimeError(f"G8 binding hybrid configuration mismatch: {actual}")


@register("G8")
def run_g8(config: Any, paths: Any, evidence_file: Path | None) -> dict[str, Any]:
    """Materialize and prove the only two allowed rank-fusion variants."""

    if evidence_file is None:
        raise RuntimeError("G8 requires passing JUnit XML evidence")
    junit = _junit(evidence_file)
    _validate_binding_config(config)

    g7 = paths.data / "g7"
    g6 = paths.data / "g6"
    required_inputs = [
        g7 / f"{model}_recommendations" for model in MODEL_NAMES
    ] + [
        g7 / "popularity_scores",
        g7 / "experiment_budget_summary",
        g6 / "evaluation_users",
    ]
    missing_inputs = [
        str(path) for path in required_inputs if not (path / "_SUCCESS").is_file()
    ]
    if missing_inputs:
        raise FileNotFoundError(f"G8 prerequisite G7 tables are missing: {missing_inputs}")
    final = paths.data / "g8"
    if final.exists():
        raise FileExistsError(f"G8 output exists without reusable manifest: {final}")

    signature = _implementation_signature(config.sha256)
    working = paths.temporary / "G8-publish"
    cleaned_scratch = _prepare_workspace(working, signature)
    final.parent.mkdir(parents=True, exist_ok=True)
    spark = SparkSession.builder.appName("amazon-recommender-g8").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    candidate_frames = {
        model: spark.read.parquet(str(g7 / f"{model}_recommendations"))
        for model in MODEL_NAMES
    }
    bayesian_scores = spark.read.parquet(str(g7 / "popularity_scores"))
    evaluation_users = spark.read.parquet(str(g6 / "evaluation_users"))
    tables: dict[str, dict[str, Any]] = {}
    reused_tables: list[str] = []

    def publish(name: str, frame: DataFrame) -> bool:
        evidence, reused = publish_or_reuse_sized_parquet(
            frame,
            working / name,
            kind="fact" if name in FACT_TABLES else "dimension",
            sort_columns=tuple(
                column
                for column in (
                    "stage",
                    "customer_id",
                    "rank",
                    "product_id",
                    "variant",
                )
                if column in frame.columns
            ),
        )
        tables[name] = evidence
        if reused:
            reused_tables.append(name)
        return reused

    def publish_timed(name: str, frame: DataFrame) -> float:
        """Publish once and durably retain its actual wall-clock duration."""

        marker = working / "_runtime" / f"{name}.json"
        try:
            recorded = json.loads(marker.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError):
            recorded = {}
        valid_marker = (
            recorded.get("implementation_sha256") == signature
            and isinstance(recorded.get("duration_seconds"), (int, float))
            and math.isfinite(float(recorded["duration_seconds"]))
            and float(recorded["duration_seconds"]) >= 0.0
        )
        if (working / name / "_SUCCESS").is_file() and not valid_marker:
            # A crash between the table rename and timing-marker write cannot be
            # assigned an invented duration. Recompute this run-scoped table once.
            shutil.rmtree(working / name)
        started = time.perf_counter()
        reused = publish(name, frame)
        if reused:
            return float(recorded["duration_seconds"])
        duration = time.perf_counter() - started
        atomic_write_json(
            marker,
            {
                "gate": "G8",
                "table": name,
                "implementation_sha256": signature,
                "duration_seconds": duration,
                "measurement": "wall_clock_materialized_parquet_publication",
            },
        )
        return duration

    try:
        bayesian_validation = validate_bayesian_score_evidence(bayesian_scores)
        hybrid = build_hybrid_frames(candidate_frames, bayesian_scores)
        shared_candidate_seconds = publish_timed(
            "hybrid_candidates", hybrid.candidates
        )
        materialized_candidates = spark.read.parquet(
            str(working / "hybrid_candidates")
        )
        h_a_candidate_seconds = publish_timed(
            "hybrid_a_recommendations",
            score_materialized_hybrid_candidates(
                materialized_candidates,
                variant="h_a",
                weights=H_A_WEIGHTS,
            ),
        )
        h_b_candidate_seconds = publish_timed(
            "hybrid_b_recommendations",
            score_materialized_hybrid_candidates(
                materialized_candidates,
                variant="h_b",
                weights=H_B_WEIGHTS,
            ),
        )

        budget_rows = [
            (
                variant,
                0,
                len(MODEL_NAMES),
                RRF_C,
                HYBRID_STORED_DEPTH,
                "pending_validation_g9",
                "g7_frozen_rank_only",
                json.dumps(dict(weights), sort_keys=True, separators=(",", ":")),
            )
            for variant, weights in HYBRID_VARIANTS.items()
        ]
        budget = spark.createDataFrame(
            budget_rows,
            "variant string, model_fit_count int, independent_model_count int, "
            "rrf_c int, stored_depth int, selection_status string, "
            "candidate_source string, weights_json string",
        )
        publish("hybrid_experiment_budget", budget)
        runtime_summary = spark.createDataFrame(
            [
                (
                    "h_a",
                    0.0,
                    h_a_candidate_seconds,
                    0,
                    shared_candidate_seconds,
                    "measured_in_g8",
                    "measured",
                ),
                (
                    "h_b",
                    0.0,
                    h_b_candidate_seconds,
                    0,
                    shared_candidate_seconds,
                    "measured_in_g8",
                    "measured",
                ),
            ],
            "model string, training_seconds double, candidate_generation_seconds double, "
            "fit_count int, shared_candidate_generation_seconds double, "
            "runtime_source string, candidate_runtime_status string",
        )
        publish("hybrid_runtime_summary", runtime_summary)

        candidate_validation = validate_hybrid_candidate_evidence(
            materialized_candidates, candidate_frames, evaluation_users
        )
        variant_validations = {
            "h_a": validate_hybrid_recommendations(
                spark.read.parquet(str(working / "hybrid_a_recommendations")),
                materialized_candidates,
                variant="h_a",
                weights=H_A_WEIGHTS,
            ),
            "h_b": validate_hybrid_recommendations(
                spark.read.parquet(str(working / "hybrid_b_recommendations")),
                materialized_candidates,
                variant="h_b",
                weights=H_B_WEIGHTS,
            ),
        }
        budget_validation = validate_experiment_budget(
            spark.read.parquet(str(g7 / "experiment_budget_summary")),
            spark.read.parquet(str(working / "hybrid_experiment_budget")),
        )

        summary = spark.createDataFrame(
            [
                (
                    "shared_candidates",
                    int(candidate_validation["rows"]),
                    int(candidate_validation["requests_with_candidates"]),
                    None,
                    None,
                    None,
                    0,
                ),
                *[
                    (
                        variant,
                        int(validation["rows"]),
                        int(validation["requests_with_output"]),
                        int(validation["min_candidates"]),
                        float(validation["avg_candidates"]),
                        int(validation["max_candidates"]),
                        0,
                    )
                    for variant, validation in variant_validations.items()
                ],
            ],
            "output string, rows long, requests long, min_candidates int, "
            "avg_candidates double, max_candidates int, invariant_violations long",
        )
        publish("hybrid_contract_summary", summary)

        if set(tables) != set(OUTPUT_TABLES):
            raise RuntimeError(
                f"G8 output budget mismatch: {sorted(tables)} != {sorted(OUTPUT_TABLES)}"
            )
        os.replace(working, final)
    except Exception:
        cleanup_incomplete_publications(working)
        raise

    def final_path(value: Any) -> Any:
        if isinstance(value, str):
            return value.replace(str(working), str(final), 1)
        if isinstance(value, dict):
            return {key: final_path(item) for key, item in value.items()}
        if isinstance(value, list):
            return [final_path(item) for item in value]
        return value

    tables = final_path(tables)
    return {
        "junit": junit,
        "implementation_sha256": signature,
        "scratch_directories_removed": cleaned_scratch,
        "tables_reused": sorted(set(reused_tables)),
        "fusion_method": "weighted_reciprocal_rank_fusion",
        "rrf_c": RRF_C,
        "stored_depth": HYBRID_STORED_DEPTH,
        "hybrid_variants": ["h_a", "h_b"],
        "shared_candidate_table": tables["hybrid_candidates"],
        "candidate_validation": candidate_validation,
        "bayesian_score_validation": bayesian_validation,
        "variant_validations": variant_validations,
        "experiment_budget": budget_validation,
        "independent_models_refit": 0,
        "hybrid_models_fit": 0,
        "runtime": {
            "shared_candidate_generation_seconds": shared_candidate_seconds,
            "h_a_candidate_generation_seconds": h_a_candidate_seconds,
            "h_b_candidate_generation_seconds": h_b_candidate_seconds,
            "measurement": "wall_clock_materialized_parquet_publication",
        },
        "selection_deferred_to_g9_validation": True,
        "tables": tables,
    }
