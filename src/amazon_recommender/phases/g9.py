"""G9 leakage-safe evaluation and validation-only hybrid selection.

The phase deliberately has two durable halves.  First, all five independent models
and both approved hybrid variants are evaluated on validation.  The H-A/H-B choice is
then written to an atomic one-row table and a companion freeze marker while no test
output exists.  Only after that marker is durable are test metrics constructed, and
the losing hybrid is never evaluated on test.

All ranking denominators originate in G6 ``evaluation_users``.  Consequently users
with an empty model list remain in ``evaluation_per_user`` with zero ranking success
and zero coverage.  RMSE/MAE come only from G7's raw, unclipped ALS held-out
predictions and remain null for every other recommendation model.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import shutil
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from functools import reduce
from pathlib import Path
from typing import Any, Mapping

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from amazon_recommender.core.manifest import atomic_write_json
from amazon_recommender.evaluation.metrics import (
    EVALUATION_K,
    evaluate_als_predictions,
    evaluate_ranking_recommendations,
)
from amazon_recommender.gate_handlers import register
from amazon_recommender.models.hybrid import (
    H_A_WEIGHTS,
    H_B_WEIGHTS,
    HYBRID_STORED_DEPTH,
    MODEL_CANDIDATE_DEPTHS,
    MODEL_NAMES,
    NDCG_TIE_THRESHOLD,
    RRF_C,
    select_hybrid_configuration,
)
from amazon_recommender.pipelines.storage import (
    cleanup_incomplete_publications,
    publish_or_reuse_sized_parquet,
)


G9_CONTRACT_VERSION = 2
INDEPENDENT_MODELS = tuple(MODEL_NAMES)
HYBRID_MODELS = ("h_a", "h_b")
HYBRID_WEIGHTS: Mapping[str, Mapping[str, float]] = {
    "h_a": H_A_WEIGHTS,
    "h_b": H_B_WEIGHTS,
}
VALIDATION_MODELS = (*INDEPENDENT_MODELS, *HYBRID_MODELS)
COHORTS = ("common_warm", "operational")
SLICES = ("overall", "Book", "non-Book")
SELECTION_STAGE = "validation"
SELECTION_COHORT = "common_warm"
SELECTION_SLICE = "overall"

FACT_TABLES = {
    "validation_evaluation_per_user",
    "test_evaluation_per_user",
    "evaluation_per_user",
    "als_prediction_per_row",
}
OUTPUT_TABLES = (
    "validation_evaluation_per_user",
    "validation_evaluation_summary",
    "selected_hybrid",
    "validation_hybrid_comparison",
    "test_evaluation_per_user",
    "test_evaluation_summary",
    "als_prediction_per_row",
    "als_prediction_summary",
    "model_runtime_summary",
    "experiment_budget",
    "evaluation_per_user",
    "evaluation_summary",
    "coverage_summary",
    "official_test_comparison",
    "evaluation_contract_summary",
)
TEST_OUTPUT_TABLES = {
    "test_evaluation_per_user",
    "test_evaluation_summary",
    "als_prediction_per_row",
    "als_prediction_summary",
    "evaluation_per_user",
    "evaluation_summary",
    "coverage_summary",
    "official_test_comparison",
    "evaluation_contract_summary",
}


def _junit(path: Path) -> dict[str, Any]:
    root = ET.parse(path).getroot()
    suites = [root] if root.tag == "testsuite" else list(root.findall("testsuite"))
    summary = {
        key: sum(int(suite.attrib.get(key, "0")) for suite in suites)
        for key in ("tests", "failures", "errors", "skipped")
    }
    if not summary["tests"] or summary["failures"] or summary["errors"]:
        raise RuntimeError(f"G9 JUnit evidence is not passing: {summary}")
    summary["path"] = str(path.resolve())
    return summary


def _implementation_signature(config_sha256: str) -> str:
    digest = hashlib.sha256()
    digest.update(f"g9-contract-{G9_CONTRACT_VERSION}".encode("ascii"))
    digest.update(config_sha256.encode("ascii"))
    root = Path(__file__).parents[1]
    for relative in (
        "evaluation/metrics.py",
        "models/hybrid.py",
        "phases/g8.py",
        "phases/g9.py",
    ):
        path = root / relative
        digest.update(relative.encode("utf-8"))
        digest.update(path.read_bytes())
    return digest.hexdigest()


def _prepare_workspace(working: Path, signature: str) -> list[str]:
    """Retain complete same-contract tables and remove partial publications."""

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
            "gate": "G9",
            "contract_version": G9_CONTRACT_VERSION,
            "implementation_sha256": signature,
        },
    )
    return cleanup_incomplete_publications(working)


def _require_columns(frame: DataFrame, required: set[str], *, name: str) -> None:
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise RuntimeError(f"{name} is missing required columns: {missing}")


def _union(frames: list[DataFrame]) -> DataFrame:
    if not frames:
        raise ValueError("at least one DataFrame is required")
    return reduce(DataFrame.unionByName, frames)


def evaluate_model_set(
    recommendation_frames: Mapping[str, DataFrame],
    evaluation_users: DataFrame,
    product_catalog: DataFrame,
    active_catalog: DataFrame,
    *,
    stage: str,
) -> tuple[DataFrame, DataFrame]:
    """Evaluate exactly the supplied frozen lists for one stage.

    Filtering the authoritative population before each call prevents rows from the
    other stage from entering any denominator.  The left-join behavior itself lives
    in :func:`evaluate_ranking_recommendations` and preserves empty-list users.
    """

    if stage not in {"validation", "test"}:
        raise ValueError(f"unsupported evaluation stage: {stage}")
    if not recommendation_frames:
        raise ValueError("recommendation_frames must not be empty")
    population = evaluation_users.filter(F.col("stage") == F.lit(stage))
    per_user: list[DataFrame] = []
    summaries: list[DataFrame] = []
    for model, recommendations in recommendation_frames.items():
        frames = evaluate_ranking_recommendations(
            recommendations.filter(F.col("stage") == F.lit(stage)),
            population,
            product_catalog,
            active_catalog,
            model=model,
            k=EVALUATION_K,
        )
        per_user.append(frames.per_user)
        summaries.append(frames.summary)
    return _union(per_user), _union(summaries)


def select_validation_hybrid(validation_summary: DataFrame) -> dict[str, Any]:
    """Select H-A/H-B using only validation/common_warm/overall metrics."""

    _require_columns(
        validation_summary,
        {
            "model",
            "stage",
            "cohort",
            "slice",
            "evaluated_users",
            "ndcg_at_10",
            "user_coverage",
        },
        name="validation_summary",
    )
    rows = (
        validation_summary.filter(
            (F.col("stage") == F.lit(SELECTION_STAGE))
            & (F.col("cohort") == F.lit(SELECTION_COHORT))
            & (F.col("slice") == F.lit(SELECTION_SLICE))
            & F.col("model").isin(*HYBRID_MODELS)
        )
        .select("model", "evaluated_users", "ndcg_at_10", "user_coverage")
        .collect()
    )
    by_model = {row.model: row.asDict() for row in rows}
    if set(by_model) != set(HYBRID_MODELS) or len(rows) != 2:
        raise RuntimeError(
            "hybrid selection requires exactly one h_a and one h_b "
            "validation/common_warm/overall row"
        )
    evaluated_counts = {int(row["evaluated_users"]) for row in by_model.values()}
    if len(evaluated_counts) != 1 or next(iter(evaluated_counts)) <= 0:
        raise RuntimeError("hybrid selection denominators must be equal and non-zero")

    selection = select_hybrid_configuration(
        h_a_ndcg_at_10=float(by_model["h_a"]["ndcg_at_10"]),
        h_a_user_coverage=float(by_model["h_a"]["user_coverage"]),
        h_b_ndcg_at_10=float(by_model["h_b"]["ndcg_at_10"]),
        h_b_user_coverage=float(by_model["h_b"]["user_coverage"]),
    )
    return {
        "selected_model": selection.selected_variant,
        "selected_weights_json": json.dumps(
            dict(HYBRID_WEIGHTS[selection.selected_variant]),
            sort_keys=True,
            separators=(",", ":"),
        ),
        "rrf_c": RRF_C,
        "stored_depth": HYBRID_STORED_DEPTH,
        "selection_reason": selection.reason,
        "selection_stage": SELECTION_STAGE,
        "selection_cohort": SELECTION_COHORT,
        "selection_slice": SELECTION_SLICE,
        "evaluated_users": next(iter(evaluated_counts)),
        "h_a_ndcg_at_10": float(by_model["h_a"]["ndcg_at_10"]),
        "h_a_user_coverage": float(by_model["h_a"]["user_coverage"]),
        "h_b_ndcg_at_10": float(by_model["h_b"]["ndcg_at_10"]),
        "h_b_user_coverage": float(by_model["h_b"]["user_coverage"]),
        "ndcg_difference_h_a_minus_h_b": float(selection.ndcg_difference),
        "coverage_difference_h_a_minus_h_b": float(selection.coverage_difference),
        "ndcg_tie_threshold": NDCG_TIE_THRESHOLD,
        "test_metrics_used": False,
        "selection_status": "frozen_before_test_evaluation",
    }


_SELECTION_SCHEMA = StructType(
    [
        StructField("selected_model", StringType(), False),
        StructField("selected_weights_json", StringType(), False),
        StructField("rrf_c", LongType(), False),
        StructField("stored_depth", LongType(), False),
        StructField("selection_reason", StringType(), False),
        StructField("selection_stage", StringType(), False),
        StructField("selection_cohort", StringType(), False),
        StructField("selection_slice", StringType(), False),
        StructField("evaluated_users", LongType(), False),
        StructField("h_a_ndcg_at_10", DoubleType(), False),
        StructField("h_a_user_coverage", DoubleType(), False),
        StructField("h_b_ndcg_at_10", DoubleType(), False),
        StructField("h_b_user_coverage", DoubleType(), False),
        StructField("ndcg_difference_h_a_minus_h_b", DoubleType(), False),
        StructField("coverage_difference_h_a_minus_h_b", DoubleType(), False),
        StructField("ndcg_tie_threshold", DoubleType(), False),
        StructField("test_metrics_used", BooleanType(), False),
        StructField("selection_status", StringType(), False),
        StructField("frozen_at_utc", StringType(), False),
    ]
)


def selection_frame(
    spark: SparkSession, selection: Mapping[str, Any], *, frozen_at_utc: str
) -> DataFrame:
    """Build the durable, explicitly test-blind one-row selection table."""

    row = dict(selection)
    row["test_metrics_used"] = bool(row["test_metrics_used"])
    row["frozen_at_utc"] = frozen_at_utc
    return spark.createDataFrame([row], schema=_SELECTION_SCHEMA)


def build_model_runtime_summary(
    g7_runtime: DataFrame,
    g8_budget: DataFrame,
    g8_runtime: DataFrame,
) -> DataFrame:
    """Return the five G7 and two measured G8 runtime rows."""

    _require_columns(
        g7_runtime,
        {
            "model",
            "training_seconds",
            "candidate_generation_seconds",
            "fit_count",
            "parameters_json",
        },
        name="G7 model_runtime_summary",
    )
    _require_columns(
        g8_budget,
        {"variant", "model_fit_count", "weights_json"},
        name="G8 hybrid_experiment_budget",
    )
    _require_columns(
        g8_runtime,
        {
            "model",
            "training_seconds",
            "candidate_generation_seconds",
            "fit_count",
            "shared_candidate_generation_seconds",
            "runtime_source",
            "candidate_runtime_status",
        },
        name="G8 hybrid_runtime_summary",
    )
    g7_rows = {row.model: row.asDict() for row in g7_runtime.collect()}
    if set(g7_rows) != set(INDEPENDENT_MODELS) or len(g7_rows) != 5:
        raise RuntimeError("G7 model_runtime_summary must contain exactly five models")
    for model, row in g7_rows.items():
        values = (row["training_seconds"], row["candidate_generation_seconds"])
        if (
            int(row["fit_count"]) != 1
            or row["parameters_json"] is None
            or any(
                value is None
                or not math.isfinite(float(value))
                or float(value) < 0.0
                for value in values
            )
        ):
            raise RuntimeError(f"invalid measured G7 runtime for {model}: {row}")
    g8_rows = {row.model: row.asDict() for row in g8_runtime.collect()}
    if set(g8_rows) != set(HYBRID_MODELS) or len(g8_rows) != 2:
        raise RuntimeError("G8 hybrid_runtime_summary must contain exactly h_a and h_b")
    shared_durations: set[float] = set()
    for model, row in g8_rows.items():
        candidate_seconds = row["candidate_generation_seconds"]
        shared_seconds = row["shared_candidate_generation_seconds"]
        training_seconds = row["training_seconds"]
        if (
            int(row["fit_count"]) != 0
            or training_seconds is None
            or not math.isfinite(float(training_seconds))
            or float(training_seconds) != 0.0
            or candidate_seconds is None
            or not math.isfinite(float(candidate_seconds))
            or float(candidate_seconds) < 0.0
            or shared_seconds is None
            or not math.isfinite(float(shared_seconds))
            or float(shared_seconds) < 0.0
            or row["runtime_source"] != "measured_in_g8"
            or row["candidate_runtime_status"] != "measured"
        ):
            raise RuntimeError(f"invalid measured G8 runtime for {model}: {row}")
        shared_durations.add(float(shared_seconds))
    if len(shared_durations) != 1:
        raise RuntimeError("G8 variants must reference one shared candidate duration")

    independent = g7_runtime.select(
        "model",
        F.col("training_seconds").cast("double"),
        F.col("candidate_generation_seconds").cast("double"),
        F.col("fit_count").cast("int"),
        "parameters_json",
        F.lit("measured_in_g7").alias("runtime_source"),
        F.lit("measured").alias("candidate_runtime_status"),
        F.lit(None).cast("double").alias("shared_candidate_generation_seconds"),
    )
    hybrid = g8_runtime.join(
        g8_budget.select(
            F.col("variant").alias("model"),
            F.col("weights_json").alias("parameters_json"),
        ),
        "model",
        "inner",
    ).select(
        "model",
        F.col("training_seconds").cast("double"),
        F.col("candidate_generation_seconds").cast("double"),
        F.col("fit_count").cast("int"),
        "parameters_json",
        "runtime_source",
        "candidate_runtime_status",
        F.col("shared_candidate_generation_seconds").cast("double"),
    )
    return independent.unionByName(hybrid)


def validate_upstream_experiment_budget(
    g7_budget: DataFrame, g8_budget: DataFrame
) -> dict[str, Any]:
    """Prove the frozen five-model and two-variant budget without phase imports."""

    _require_columns(
        g7_budget,
        {"model", "fit_count", "candidate_depth", "training_contract"},
        name="G7 experiment_budget_summary",
    )
    _require_columns(
        g8_budget,
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
        name="G8 hybrid_experiment_budget",
    )
    g7_rows = {row.model: row.asDict() for row in g7_budget.collect()}
    if set(g7_rows) != set(INDEPENDENT_MODELS) or len(g7_rows) != 5:
        raise RuntimeError("G9 requires exactly five G7 independent-model budget rows")
    for model, expected_depth in MODEL_CANDIDATE_DEPTHS.items():
        row = g7_rows[model]
        if (
            int(row["fit_count"]) != 1
            or int(row["candidate_depth"]) != expected_depth
            or row["training_contract"] != "train_only_single_fit"
        ):
            raise RuntimeError(f"G7 frozen-model budget mismatch for {model}: {row}")

    g8_rows = {row.variant: row.asDict() for row in g8_budget.collect()}
    if set(g8_rows) != set(HYBRID_MODELS) or len(g8_rows) != 2:
        raise RuntimeError("G9 experiment budget permits exactly h_a and h_b")
    for variant, expected_weights in HYBRID_WEIGHTS.items():
        row = g8_rows[variant]
        try:
            observed_weights = json.loads(row["weights_json"])
        except (TypeError, json.JSONDecodeError) as error:
            raise RuntimeError(f"invalid weights_json for {variant}") from error
        if (
            int(row["model_fit_count"]) != 0
            or int(row["independent_model_count"]) != 5
            or int(row["rrf_c"]) != RRF_C
            or int(row["stored_depth"]) != HYBRID_STORED_DEPTH
            or row["selection_status"] != "pending_validation_g9"
            or row["candidate_source"] != "g7_frozen_rank_only"
            or observed_weights != dict(expected_weights)
        ):
            raise RuntimeError(f"G8 hybrid budget mismatch for {variant}: {row}")
    return {
        "g7_independent_model_count": len(g7_rows),
        "g7_total_fit_count": sum(int(row["fit_count"]) for row in g7_rows.values()),
        "hybrid_variant_count": len(g8_rows),
        "g8_model_refit_count": sum(
            int(row["model_fit_count"]) for row in g8_rows.values()
        ),
        "variants": sorted(g8_rows),
        "selection_status": "pending_validation_g9",
    }


def build_experiment_budget(
    spark: SparkSession,
    g7_budget: DataFrame,
    g8_budget: DataFrame,
    *,
    selected_model: str,
) -> DataFrame:
    """Materialize the exact seven-row official experiment matrix."""

    validate_upstream_experiment_budget(g7_budget, g8_budget)
    if selected_model not in HYBRID_MODELS:
        raise ValueError(f"selected_model must be h_a or h_b: {selected_model}")
    g7_rows = {row.model: row.asDict() for row in g7_budget.collect()}
    g8_rows = {row.variant: row.asDict() for row in g8_budget.collect()}
    experiment_ids = {
        "popularity": "S1",
        "als": "S2",
        "fp": "S3",
        "graph": "S4",
        "category": "S5",
        "h_a": "H-A",
        "h_b": "H-B",
    }
    rows: list[tuple[Any, ...]] = []
    for model in INDEPENDENT_MODELS:
        source = g7_rows[model]
        rows.append(
            (
                experiment_ids[model],
                model,
                "independent",
                int(source["fit_count"]),
                int(source["candidate_depth"]),
                "evaluated",
                "evaluated_official",
                "not_applicable",
                source["training_contract"],
                None,
            )
        )
    for model in HYBRID_MODELS:
        source = g8_rows[model]
        selected = model == selected_model
        rows.append(
            (
                experiment_ids[model],
                model,
                "hybrid_rank_fusion",
                int(source["model_fit_count"]),
                int(source["stored_depth"]),
                "evaluated",
                (
                    "evaluated_official_selected_winner"
                    if selected
                    else "not_evaluated_validation_loser"
                ),
                "selected" if selected else "not_selected",
                source["candidate_source"],
                source["weights_json"],
            )
        )
    return spark.createDataFrame(
        rows,
        "experiment_id string, model string, model_family string, fit_count int, "
        "candidate_depth int, validation_status string, test_status string, "
        "selection_status string, training_contract string, weights_json string",
    )


def attach_runtime_and_als_metrics(
    ranking_summary: DataFrame,
    runtime_summary: DataFrame,
    als_prediction_summary: DataFrame,
) -> DataFrame:
    """Build comparison rows; prediction errors can populate ALS only."""

    runtime = runtime_summary.select(
        "model",
        "training_seconds",
        "candidate_generation_seconds",
        "fit_count",
        "parameters_json",
        "runtime_source",
        "candidate_runtime_status",
    )
    als = als_prediction_summary.select(
        "model",
        "stage",
        "prediction_scope",
        "heldout_rows",
        "predicted_rows",
        "dropped_rows",
        "prediction_coverage",
        "drop_rate",
        "rmse",
        "mae",
    )
    return (
        ranking_summary.join(runtime, "model", "left")
        .join(als, ["model", "stage"], "left")
        .select(
            "model",
            "stage",
            "cohort",
            "slice",
            "evaluated_users",
            "users_with_output",
            "ndcg_at_10",
            "hit_rate_at_10",
            "mrr_at_10",
            "user_coverage",
            "fill_rate_at_10",
            "catalog_coverage_at_10",
            "distinct_recommended_products_at_10",
            "active_catalog_size",
            "training_seconds",
            "candidate_generation_seconds",
            "fit_count",
            "parameters_json",
            "runtime_source",
            "candidate_runtime_status",
            "prediction_scope",
            "heldout_rows",
            "predicted_rows",
            "dropped_rows",
            "prediction_coverage",
            "drop_rate",
            "rmse",
            "mae",
        )
    )


def validate_evaluation_contract(
    evaluation_per_user: DataFrame,
    evaluation_summary: DataFrame,
    selection_table: DataFrame,
    runtime_summary: DataFrame,
    experiment_budget: DataFrame,
    evaluation_users: DataFrame,
) -> dict[str, Any]:
    """Prove populations, official model budget, metric bounds, and test blindness."""

    selection_rows = selection_table.collect()
    if len(selection_rows) != 1:
        raise RuntimeError("selected_hybrid must contain exactly one row")
    selected = selection_rows[0].selected_model
    if selected not in HYBRID_MODELS or selection_rows[0].test_metrics_used:
        raise RuntimeError("selected_hybrid is not a frozen validation-only decision")
    expected_weights = json.dumps(
        dict(HYBRID_WEIGHTS[selected]), sort_keys=True, separators=(",", ":")
    )
    if (
        selection_rows[0].selected_weights_json != expected_weights
        or int(selection_rows[0].rrf_c) != RRF_C
        or int(selection_rows[0].stored_depth) != HYBRID_STORED_DEPTH
    ):
        raise RuntimeError("selected_hybrid does not preserve the binding RRF configuration")

    expected_test_models = {*INDEPENDENT_MODELS, selected}
    observed_by_stage = {
        stage: {row.model for row in rows}
        for stage, rows in (
            (
                stage,
                evaluation_summary.filter(F.col("stage") == F.lit(stage))
                .select("model")
                .distinct()
                .collect(),
            )
            for stage in ("validation", "test")
        )
    }
    if observed_by_stage["validation"] != set(VALIDATION_MODELS):
        raise RuntimeError(
            f"validation must contain exactly seven models: {observed_by_stage['validation']}"
        )
    if observed_by_stage["test"] != expected_test_models:
        raise RuntimeError(
            "test must contain five independent models and only the selected hybrid: "
            f"{observed_by_stage['test']}"
        )

    duplicate_summary_keys = (
        evaluation_summary.groupBy("model", "stage", "cohort", "slice")
        .count()
        .filter(F.col("count") != F.lit(1))
        .count()
    )
    invalid_dimension_labels = evaluation_summary.filter(
        F.col("stage").isNull()
        | ~F.col("stage").isin("validation", "test")
        | F.col("cohort").isNull()
        | ~F.col("cohort").isin(*COHORTS)
        | F.col("slice").isNull()
        | ~F.col("slice").isin(*SLICES)
    ).count()
    expected_summary_rows = len(VALIDATION_MODELS) * len(COHORTS) * len(SLICES)
    expected_summary_rows += len(expected_test_models) * len(COHORTS) * len(SLICES)
    actual_summary_rows = evaluation_summary.count()
    if actual_summary_rows != expected_summary_rows or duplicate_summary_keys:
        raise RuntimeError(
            "evaluation_summary stage/model/cohort/slice matrix is incomplete or duplicated: "
            f"rows={actual_summary_rows}, expected={expected_summary_rows}, "
            f"duplicates={duplicate_summary_keys}"
        )

    metric_columns = (
        "ndcg_at_10",
        "hit_rate_at_10",
        "mrr_at_10",
        "user_coverage",
        "fill_rate_at_10",
        "catalog_coverage_at_10",
    )
    invalid_metric_condition = F.lit(False)
    for column in metric_columns:
        invalid_metric_condition = invalid_metric_condition | (
            F.col(column).isNull()
            | F.isnan(column)
            | ~F.col(column).between(0.0, 1.0)
        )
    invalid_metrics = evaluation_summary.filter(invalid_metric_condition).count()
    count_violations = evaluation_summary.filter(
        (F.col("evaluated_users") <= F.lit(0))
        | (F.col("users_with_output") < F.lit(0))
        | (F.col("users_with_output") > F.col("evaluated_users"))
        | (F.col("active_catalog_size") <= F.lit(0))
    ).count()

    non_als_prediction_metric_violations = evaluation_summary.filter(
        (F.col("model") != F.lit("als"))
        & (
            F.col("prediction_scope").isNotNull()
            | F.col("heldout_rows").isNotNull()
            | F.col("predicted_rows").isNotNull()
            | F.col("dropped_rows").isNotNull()
            | F.col("rmse").isNotNull()
            | F.col("mae").isNotNull()
            | F.col("prediction_coverage").isNotNull()
            | F.col("drop_rate").isNotNull()
        )
    ).count()
    als_scope_violations = evaluation_summary.filter(
        (F.col("model") == F.lit("als"))
        & (
            (F.col("prediction_scope") != F.lit("all_heldout_ratings"))
            | (F.col("heldout_rows") <= F.lit(0))
            | (F.col("predicted_rows") < F.lit(0))
            | (F.col("dropped_rows") < F.lit(0))
            | (
                F.col("heldout_rows")
                != F.col("predicted_rows") + F.col("dropped_rows")
            )
            | F.col("prediction_coverage").isNull()
            | ~F.col("prediction_coverage").between(0.0, 1.0)
            | F.col("drop_rate").isNull()
            | ~F.col("drop_rate").between(0.0, 1.0)
            | (
                F.abs(
                    F.col("prediction_coverage") + F.col("drop_rate") - F.lit(1.0)
                )
                > F.lit(1e-12)
            )
            | (
                (F.col("predicted_rows") > F.lit(0))
                & (
                    F.col("rmse").isNull()
                    | F.isnan("rmse")
                    | (F.col("rmse") < F.lit(0.0))
                    | (F.col("rmse") > F.lit(1.7976931348623157e308))
                    | F.col("mae").isNull()
                    | F.isnan("mae")
                    | (F.col("mae") < F.lit(0.0))
                    | (F.col("mae") > F.lit(1.7976931348623157e308))
                )
            )
            | (
                (F.col("predicted_rows") == F.lit(0))
                & (F.col("rmse").isNotNull() | F.col("mae").isNotNull())
            )
        )
    ).count()

    # Every authoritative sampled user appears twice (overall + its Book/non-Book
    # slice) for every official model, whether or not that model emitted candidates.
    official_model_stages = evaluation_summary.select("model", "stage").distinct()
    expected_population = (
        evaluation_users.groupBy("stage", "cohort")
        .count()
        .withColumn("expected_per_model", F.col("count") * F.lit(2))
        .select("stage", "cohort", "expected_per_model")
        .join(official_model_stages, "stage", "inner")
        .select("model", "stage", "cohort", "expected_per_model")
    )
    per_user_counts = evaluation_per_user.groupBy("model", "stage", "cohort").count()
    population_count_violations = (
        per_user_counts.join(
            expected_population,
            ["model", "stage", "cohort"],
            "full",
        )
        .filter(
            F.col("count").isNull()
            | F.col("expected_per_model").isNull()
            | (F.col("count") != F.col("expected_per_model"))
        )
        .count()
    )
    duplicate_per_user_keys = (
        evaluation_per_user.groupBy(
            "model", "stage", "cohort", "slice", "customer_id"
        )
        .count()
        .filter(F.col("count") != F.lit(1))
        .count()
    )
    per_user_slice_violations = evaluation_per_user.filter(
        F.col("stage").isNull()
        | ~F.col("stage").isin("validation", "test")
        | F.col("cohort").isNull()
        | ~F.col("cohort").isin(*COHORTS)
        | F.col("slice").isNull()
        | ~F.col("slice").isin(*SLICES)
        | F.col("target_group").isNull()
        | (
            (F.col("slice") != F.lit("overall"))
            & (
                (
                    (F.col("target_group") == F.lit("Book"))
                    & (F.col("slice") != F.lit("Book"))
                )
                | (
                    (F.col("target_group") != F.lit("Book"))
                    & (F.col("slice") != F.lit("non-Book"))
                )
            )
        )
    ).count()
    per_user_shape_violations = (
        evaluation_per_user.groupBy(
            "model", "stage", "cohort", "customer_id"
        )
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.countDistinct("slice").alias("distinct_slices"),
            F.sum((F.col("slice") == F.lit("overall")).cast("int")).alias(
                "overall_rows"
            ),
            F.countDistinct(
                F.struct(
                    "target_product_id", "target_rating", "target_group"
                )
            ).alias("target_value_count"),
            F.countDistinct(
                F.struct(
                    "target_rank",
                    "list_length",
                    "top_k_list_length",
                    "has_output",
                    "ndcg_at_10",
                    "hit_rate_at_10",
                    "mrr_at_10",
                    "fill_fraction_at_10",
                )
            ).alias("list_evidence_count"),
        )
        .filter(
            (F.col("rows") != F.lit(2))
            | (F.col("distinct_slices") != F.lit(2))
            | (F.col("overall_rows") != F.lit(1))
            | (F.col("target_value_count") != F.lit(1))
            | (F.col("list_evidence_count") != F.lit(1))
        )
        .count()
    )
    recomputed_summary = evaluation_per_user.groupBy(
        "model", "stage", "cohort", "slice"
    ).agg(
        F.count(F.lit(1)).cast("long").alias("_evaluated_users"),
        F.sum(F.col("has_output").cast("long")).alias("_users_with_output"),
        F.avg("ndcg_at_10").alias("_ndcg_at_10"),
        F.avg("hit_rate_at_10").alias("_hit_rate_at_10"),
        F.avg("mrr_at_10").alias("_mrr_at_10"),
        F.avg(F.col("has_output").cast("double")).alias("_user_coverage"),
        F.avg("fill_fraction_at_10").alias("_fill_rate_at_10"),
    )
    summary_metric_reconciliation_violations = (
        evaluation_summary.join(
            recomputed_summary,
            ["model", "stage", "cohort", "slice"],
            "full",
        )
        .filter(
            F.col("evaluated_users").isNull()
            | F.col("_evaluated_users").isNull()
            | (F.col("evaluated_users") != F.col("_evaluated_users"))
            | (F.col("users_with_output") != F.col("_users_with_output"))
            | (F.abs(F.col("ndcg_at_10") - F.col("_ndcg_at_10")) > F.lit(1e-12))
            | (
                F.abs(F.col("hit_rate_at_10") - F.col("_hit_rate_at_10"))
                > F.lit(1e-12)
            )
            | (F.abs(F.col("mrr_at_10") - F.col("_mrr_at_10")) > F.lit(1e-12))
            | (
                F.abs(F.col("user_coverage") - F.col("_user_coverage"))
                > F.lit(1e-12)
            )
            | (
                F.abs(F.col("fill_rate_at_10") - F.col("_fill_rate_at_10"))
                > F.lit(1e-12)
            )
        )
        .count()
    )
    empty_user_metric_violations = evaluation_per_user.filter(
        (~F.col("has_output"))
        & (
            (F.col("list_length") != F.lit(0))
            | (F.col("top_k_list_length") != F.lit(0))
            | (F.col("ndcg_at_10") != F.lit(0.0))
            | (F.col("hit_rate_at_10") != F.lit(0.0))
            | (F.col("mrr_at_10") != F.lit(0.0))
        )
    ).count()

    runtime_rows = {row.model: row.asDict() for row in runtime_summary.collect()}
    if set(runtime_rows) != set(VALIDATION_MODELS) or len(runtime_rows) != 7:
        raise RuntimeError("model_runtime_summary must contain exactly seven model rows")
    runtime_violations = 0
    for model, row in runtime_rows.items():
        if model in INDEPENDENT_MODELS:
            values = (row["training_seconds"], row["candidate_generation_seconds"])
            if (
                int(row["fit_count"]) != 1
                or any(value is None or not math.isfinite(value) or value < 0.0 for value in values)
                or row["candidate_runtime_status"] != "measured"
                or row["runtime_source"] != "measured_in_g7"
                or row["parameters_json"] is None
            ):
                runtime_violations += 1
        elif (
            int(row["fit_count"]) != 0
            or float(row["training_seconds"]) != 0.0
            or row["candidate_generation_seconds"] is None
            or not math.isfinite(float(row["candidate_generation_seconds"]))
            or float(row["candidate_generation_seconds"]) < 0.0
            or row["shared_candidate_generation_seconds"] is None
            or not math.isfinite(
                float(row["shared_candidate_generation_seconds"])
            )
            or float(row["shared_candidate_generation_seconds"]) < 0.0
            or row["candidate_runtime_status"] != "measured"
            or row["runtime_source"] != "measured_in_g8"
            or row["parameters_json"] is None
        ):
            runtime_violations += 1

    budget_rows = {row.model: row.asDict() for row in experiment_budget.collect()}
    budget_violations = 0
    if set(budget_rows) != set(VALIDATION_MODELS) or len(budget_rows) != 7:
        budget_violations += 1
    else:
        official_hybrid_tests = [
            model
            for model in HYBRID_MODELS
            if budget_rows[model]["test_status"]
            == "evaluated_official_selected_winner"
        ]
        if official_hybrid_tests != [selected]:
            budget_violations += 1
        if any(int(budget_rows[model]["fit_count"]) != 1 for model in INDEPENDENT_MODELS):
            budget_violations += 1
        if any(int(budget_rows[model]["fit_count"]) != 0 for model in HYBRID_MODELS):
            budget_violations += 1

    violations = {
        "duplicate_summary_keys": duplicate_summary_keys,
        "invalid_dimension_labels": invalid_dimension_labels,
        "invalid_metrics": invalid_metrics,
        "count_violations": count_violations,
        "non_als_prediction_metric_violations": non_als_prediction_metric_violations,
        "als_scope_violations": als_scope_violations,
        "population_count_violations": population_count_violations,
        "duplicate_per_user_keys": duplicate_per_user_keys,
        "per_user_slice_violations": per_user_slice_violations,
        "per_user_shape_violations": per_user_shape_violations,
        "summary_metric_reconciliation_violations": (
            summary_metric_reconciliation_violations
        ),
        "empty_user_metric_violations": empty_user_metric_violations,
        "runtime_violations": runtime_violations,
        "budget_violations": budget_violations,
    }
    failures = {key: int(value) for key, value in violations.items() if value}
    if failures:
        raise RuntimeError(f"G9 evaluation invariant failure: {failures}")
    return {
        "selected_hybrid": selected,
        "validation_models": sorted(VALIDATION_MODELS),
        "test_models": sorted(expected_test_models),
        "validation_model_count": 7,
        "test_model_count": 6,
        "summary_rows": actual_summary_rows,
        "per_user_rows": evaluation_per_user.count(),
        "experiment_budget_rows": len(budget_rows),
        "empty_users_preserved": True,
        "selection_inputs": [SELECTION_STAGE, SELECTION_COHORT, SELECTION_SLICE],
        "selection_test_blind": True,
        **{key: int(value) for key, value in violations.items()},
    }


def _validate_binding_config(config: Any) -> None:
    actual = {
        "selection_cohort": config.get("hybrid", "selection_cohort"),
        "ndcg_tie_threshold": config.get("hybrid", "ndcg_tie_threshold"),
        "final_tie_break": config.get("hybrid", "final_tie_break"),
        "evaluation_k": config.get("evaluation", "k"),
        "metrics": list(config.get("evaluation", "metrics")),
        "coverage": list(config.get("evaluation", "coverage")),
        "slices": list(config.get("evaluation", "slices")),
    }
    expected = {
        "selection_cohort": SELECTION_COHORT,
        "ndcg_tie_threshold": NDCG_TIE_THRESHOLD,
        "final_tie_break": "h_a",
        "evaluation_k": EVALUATION_K,
        "metrics": ["ndcg", "hit_rate", "mrr"],
        "coverage": ["user_coverage", "fill_rate", "catalog_coverage"],
        "slices": list(SLICES),
    }
    if actual != expected:
        raise RuntimeError(f"G9 binding evaluation configuration mismatch: {actual}")


@register("G9")
def run_g9(config: Any, paths: Any, evidence_file: Path | None) -> dict[str, Any]:
    """Evaluate, freeze the validation winner, then evaluate official test rows."""

    if evidence_file is None:
        raise RuntimeError("G9 requires passing JUnit XML evidence")
    junit = _junit(evidence_file)
    _validate_binding_config(config)

    g6 = paths.data / "g6"
    g7 = paths.data / "g7"
    g8 = paths.data / "g8"
    full = paths.data / "full" / "silver"
    required_inputs = [
        g6 / "evaluation_users",
        g6 / "active_catalog",
        full / "products",
        g7 / "als_predictions",
        g7 / "model_runtime_summary",
        g7 / "experiment_budget_summary",
        g8 / "hybrid_a_recommendations",
        g8 / "hybrid_b_recommendations",
        g8 / "hybrid_experiment_budget",
        g8 / "hybrid_runtime_summary",
        *[g7 / f"{model}_recommendations" for model in INDEPENDENT_MODELS],
    ]
    missing = [str(path) for path in required_inputs if not (path / "_SUCCESS").is_file()]
    if missing:
        raise FileNotFoundError(f"G9 prerequisite tables are missing/incomplete: {missing}")
    final = paths.data / "g9"
    if final.exists():
        raise FileExistsError(f"G9 output exists without reusable manifest: {final}")

    signature = _implementation_signature(config.sha256)
    working = paths.temporary / "G9-publish"
    cleaned_scratch = _prepare_workspace(working, signature)
    final.parent.mkdir(parents=True, exist_ok=True)
    spark = SparkSession.builder.appName("amazon-recommender-g9").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    evaluation_users = spark.read.parquet(str(g6 / "evaluation_users"))
    active_catalog = spark.read.parquet(str(g6 / "active_catalog"))
    product_catalog = spark.read.parquet(str(full / "products")).select(
        "product_id", "group"
    )
    independent_recommendations = {
        model: spark.read.parquet(str(g7 / f"{model}_recommendations"))
        for model in INDEPENDENT_MODELS
    }
    hybrid_recommendations = {
        "h_a": spark.read.parquet(str(g8 / "hybrid_a_recommendations")),
        "h_b": spark.read.parquet(str(g8 / "hybrid_b_recommendations")),
    }
    g7_budget = spark.read.parquet(str(g7 / "experiment_budget_summary"))
    g8_budget = spark.read.parquet(str(g8 / "hybrid_experiment_budget"))
    tables: dict[str, dict[str, Any]] = {}
    reused_tables: list[str] = []

    def publish(name: str, frame: DataFrame) -> None:
        evidence, reused = publish_or_reuse_sized_parquet(
            frame,
            working / name,
            kind="fact" if name in FACT_TABLES else "dimension",
            sort_columns=tuple(
                column
                for column in (
                    "stage",
                    "model",
                    "cohort",
                    "slice",
                    "customer_id",
                    "rank",
                    "product_id",
                    "experiment_id",
                )
                if column in frame.columns
            ),
        )
        tables[name] = evidence
        if reused:
            reused_tables.append(name)

    try:
        # Refuse an altered model/variant budget before any evaluation decision.
        upstream_budget_evidence = validate_upstream_experiment_budget(
            g7_budget, g8_budget
        )

        # Validation is the only information available to the selector.
        validation_per_user, validation_summary = evaluate_model_set(
            {**independent_recommendations, **hybrid_recommendations},
            evaluation_users,
            product_catalog,
            active_catalog,
            stage="validation",
        )
        publish("validation_evaluation_per_user", validation_per_user)
        publish("validation_evaluation_summary", validation_summary)
        materialized_validation = spark.read.parquet(
            str(working / "validation_evaluation_summary")
        )
        expected_selection = select_validation_hybrid(materialized_validation)

        selection_path = working / "selected_hybrid"
        freeze_marker = working / "_selection_frozen_before_test.json"
        if (selection_path / "_SUCCESS").is_file():
            existing = spark.read.parquet(str(selection_path)).collect()
            if len(existing) != 1:
                raise RuntimeError("reused selected_hybrid must contain one row")
            existing_payload = existing[0].asDict()
            for key, value in expected_selection.items():
                observed = existing_payload[key]
                if isinstance(value, float):
                    if not math.isclose(float(observed), value, rel_tol=0.0, abs_tol=1e-15):
                        raise RuntimeError(f"frozen hybrid selection drift for {key}")
                elif observed != value:
                    raise RuntimeError(f"frozen hybrid selection drift for {key}")
            if not freeze_marker.is_file():
                present = sorted(
                    name for name in TEST_OUTPUT_TABLES if (working / name).exists()
                )
                if present:
                    raise RuntimeError(
                        "cannot prove selection preceded existing test outputs: "
                        f"{present}"
                    )
                atomic_write_json(
                    freeze_marker,
                    {
                        "gate": "G9",
                        "implementation_sha256": signature,
                        "selected_model": expected_selection["selected_model"],
                        "test_outputs_present_at_freeze": [],
                        "frozen_at_utc": existing_payload["frozen_at_utc"],
                    },
                )
            publish("selected_hybrid", spark.read.parquet(str(selection_path)))
        else:
            present = sorted(
                name for name in TEST_OUTPUT_TABLES if (working / name).exists()
            )
            if present:
                raise RuntimeError(
                    "test outputs exist before hybrid selection freeze: " f"{present}"
                )
            frozen_at = datetime.now(UTC).isoformat()
            publish(
                "selected_hybrid",
                selection_frame(spark, expected_selection, frozen_at_utc=frozen_at),
            )
            atomic_write_json(
                freeze_marker,
                {
                    "gate": "G9",
                    "implementation_sha256": signature,
                    "selected_model": expected_selection["selected_model"],
                    "test_outputs_present_at_freeze": [],
                    "frozen_at_utc": frozen_at,
                },
            )
        frozen_marker_payload = json.loads(freeze_marker.read_text(encoding="utf-8"))
        if (
            frozen_marker_payload.get("implementation_sha256") != signature
            or frozen_marker_payload.get("test_outputs_present_at_freeze") != []
        ):
            raise RuntimeError("hybrid freeze marker is incompatible or not test-blind")
        selected_model = expected_selection["selected_model"]

        validation_hybrid_comparison = (
            materialized_validation.filter(
                (F.col("stage") == F.lit(SELECTION_STAGE))
                & (F.col("cohort") == F.lit(SELECTION_COHORT))
                & (F.col("slice") == F.lit(SELECTION_SLICE))
                & F.col("model").isin(*HYBRID_MODELS)
            )
            .withColumn("selected", F.col("model") == F.lit(selected_model))
            .withColumn("selection_reason", F.lit(expected_selection["selection_reason"]))
            .withColumn("selection_status", F.lit("frozen_before_test_evaluation"))
        )
        publish("validation_hybrid_comparison", validation_hybrid_comparison)

        # Freeze the official seven experiment rows before test evaluation too.
        budget = build_experiment_budget(
            spark, g7_budget, g8_budget, selected_model=selected_model
        )
        publish("experiment_budget", budget)
        runtime = build_model_runtime_summary(
            spark.read.parquet(str(g7 / "model_runtime_summary")),
            g8_budget,
            spark.read.parquet(str(g8 / "hybrid_runtime_summary")),
        )
        publish("model_runtime_summary", runtime)

        # Test transformations begin only after selected_hybrid + freeze marker exist.
        test_recommendations = {
            **independent_recommendations,
            selected_model: hybrid_recommendations[selected_model],
        }
        test_per_user, test_summary = evaluate_model_set(
            test_recommendations,
            evaluation_users,
            product_catalog,
            active_catalog,
            stage="test",
        )
        publish("test_evaluation_per_user", test_per_user)
        publish("test_evaluation_summary", test_summary)

        als_frames = evaluate_als_predictions(
            spark.read.parquet(str(g7 / "als_predictions"))
        )
        publish("als_prediction_per_row", als_frames.per_prediction)
        publish("als_prediction_summary", als_frames.summary)

        canonical_per_user = spark.read.parquet(
            str(working / "validation_evaluation_per_user")
        ).unionByName(spark.read.parquet(str(working / "test_evaluation_per_user")))
        publish("evaluation_per_user", canonical_per_user)

        ranking_summary = spark.read.parquet(
            str(working / "validation_evaluation_summary")
        ).unionByName(spark.read.parquet(str(working / "test_evaluation_summary")))
        canonical_summary = attach_runtime_and_als_metrics(
            ranking_summary,
            spark.read.parquet(str(working / "model_runtime_summary")),
            spark.read.parquet(str(working / "als_prediction_summary")),
        ).withColumns(
            {
                "selected_hybrid_model": F.lit(selected_model),
                "is_selected_hybrid": F.col("model") == F.lit(selected_model),
                "official_result": F.lit(True),
            }
        )
        publish("evaluation_summary", canonical_summary)
        materialized_summary = spark.read.parquet(str(working / "evaluation_summary"))
        coverage = materialized_summary.select(
            "model",
            "stage",
            "cohort",
            "slice",
            "evaluated_users",
            "users_with_output",
            "user_coverage",
            "fill_rate_at_10",
            "catalog_coverage_at_10",
            "distinct_recommended_products_at_10",
            "active_catalog_size",
        )
        publish("coverage_summary", coverage)
        official_test = materialized_summary.filter(F.col("stage") == F.lit("test"))
        publish("official_test_comparison", official_test)

        contract = validate_evaluation_contract(
            spark.read.parquet(str(working / "evaluation_per_user")),
            materialized_summary,
            spark.read.parquet(str(working / "selected_hybrid")),
            spark.read.parquet(str(working / "model_runtime_summary")),
            spark.read.parquet(str(working / "experiment_budget")),
            evaluation_users,
        )
        contract_summary = spark.createDataFrame(
            [
                ("selected_hybrid", contract["selected_hybrid"], None),
                ("validation_model_count", None, contract["validation_model_count"]),
                ("test_model_count", None, contract["test_model_count"]),
                ("summary_rows", None, contract["summary_rows"]),
                ("per_user_rows", None, contract["per_user_rows"]),
                ("experiment_budget_rows", None, contract["experiment_budget_rows"]),
                ("invariant_violations", None, 0),
            ],
            "metric string, string_value string, long_value long",
        )
        publish("evaluation_contract_summary", contract_summary)

        if set(tables) != set(OUTPUT_TABLES):
            raise RuntimeError(
                f"G9 output contract mismatch: {sorted(tables)} != {sorted(OUTPUT_TABLES)}"
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
    frozen_marker_payload = final_path(frozen_marker_payload)
    return {
        "junit": junit,
        "implementation_sha256": signature,
        "scratch_directories_removed": cleaned_scratch,
        "tables_reused": sorted(set(reused_tables)),
        "single_fit_evaluation": True,
        "selection": expected_selection,
        "selection_freeze_evidence": frozen_marker_payload,
        "selection_test_blind": True,
        "selection_rule": (
            "validation common_warm overall NDCG@10; abs diff < 0.001 -> "
            "user coverage -> H-A"
        ),
        "official_validation_models": list(VALIDATION_MODELS),
        "official_test_models": [*INDEPENDENT_MODELS, selected_model],
        "experiment_budget_rows": 7,
        "upstream_experiment_budget": upstream_budget_evidence,
        "hybrid_candidate_runtime": "measured_in_g8",
        "invariants": contract,
        "tables": tables,
    }
