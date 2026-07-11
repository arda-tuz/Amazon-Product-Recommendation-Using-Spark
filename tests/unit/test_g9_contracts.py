from __future__ import annotations

import json
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from amazon_recommender.gate_handlers import HANDLERS
from amazon_recommender.models.hybrid import MODEL_CANDIDATE_DEPTHS
from amazon_recommender.phases.g9 import (
    HYBRID_MODELS,
    INDEPENDENT_MODELS,
    OUTPUT_TABLES,
    VALIDATION_MODELS,
    _prepare_workspace,
    attach_runtime_and_als_metrics,
    build_experiment_budget,
    build_model_runtime_summary,
    select_validation_hybrid,
    selection_frame,
    validate_evaluation_contract,
)


pytestmark = pytest.mark.unit


def test_g8_and_g9_handlers_are_registered_without_an_import_cycle() -> None:
    assert HANDLERS["G8"].__name__ == "run_g8"
    assert HANDLERS["G9"].__name__ == "run_g9"


def _selection_summary(spark, *, a_ndcg, a_coverage, b_ndcg, b_coverage):
    return spark.createDataFrame(
        [
            ("h_a", "validation", "common_warm", "overall", 20, a_ndcg, a_coverage),
            ("h_b", "validation", "common_warm", "overall", 20, b_ndcg, b_coverage),
        ],
        "model string, stage string, cohort string, slice string, "
        "evaluated_users long, ndcg_at_10 double, user_coverage double",
    )


def _g7_budget(spark):
    return spark.createDataFrame(
        [
            (model, 1, MODEL_CANDIDATE_DEPTHS[model], "train_only_single_fit")
            for model in INDEPENDENT_MODELS
        ],
        "model string, fit_count int, candidate_depth int, training_contract string",
    )


def _g8_budget(spark, *, include_third=False):
    weights = {
        "h_a": {"als": 0.35, "graph": 0.20, "category": 0.20, "fp": 0.15, "popularity": 0.10},
        "h_b": {"als": 0.50, "graph": 0.20, "category": 0.10, "fp": 0.15, "popularity": 0.05},
    }
    if include_third:
        weights["h_c"] = weights["h_a"]
    return spark.createDataFrame(
        [
            (
                variant,
                0,
                5,
                60,
                100,
                "pending_validation_g9",
                "g7_frozen_rank_only",
                json.dumps(value, sort_keys=True, separators=(",", ":")),
            )
            for variant, value in weights.items()
        ],
        "variant string, model_fit_count int, independent_model_count int, "
        "rrf_c int, stored_depth int, selection_status string, "
        "candidate_source string, weights_json string",
    )


def _g7_runtime(spark):
    return spark.createDataFrame(
        [
            (model, float(index + 1), float(index + 2), 1, "{}")
            for index, model in enumerate(INDEPENDENT_MODELS)
        ],
        "model string, training_seconds double, candidate_generation_seconds double, "
        "fit_count int, parameters_json string",
    )


def _g8_runtime(spark):
    return spark.createDataFrame(
        [
            (model, 0.0, 3.0 + index, 0, 2.5, "measured_in_g8", "measured")
            for index, model in enumerate(HYBRID_MODELS)
        ],
        "model string, training_seconds double, candidate_generation_seconds double, "
        "fit_count int, shared_candidate_generation_seconds double, "
        "runtime_source string, candidate_runtime_status string",
    )


def test_g9_selection_is_validation_common_warm_overall_only_and_uses_strict_tie_rule(
    spark,
) -> None:
    validation = _selection_summary(
        spark,
        a_ndcg=0.3004,
        a_coverage=0.60,
        b_ndcg=0.3000,
        b_coverage=0.70,
    )
    # Deliberately inverted test scores prove that the selector's input signature
    # cannot leak test outcomes into the choice.
    misleading_test = _selection_summary(
        spark,
        a_ndcg=0.99,
        a_coverage=0.99,
        b_ndcg=0.01,
        b_coverage=0.01,
    ).withColumn("stage", F.lit("test"))
    coverage_winner = select_validation_hybrid(
        validation.unionByName(misleading_test)
    )
    assert coverage_winner["selected_model"] == "h_b"
    assert coverage_winner["selection_reason"] == "ndcg_tie_higher_user_coverage"
    assert coverage_winner["test_metrics_used"] is False
    assert coverage_winner["selection_cohort"] == "common_warm"
    assert coverage_winner["selection_slice"] == "overall"

    exact_threshold = select_validation_hybrid(
        _selection_summary(
            spark,
            a_ndcg=0.301,
            a_coverage=0.10,
            b_ndcg=0.300,
            b_coverage=0.90,
        )
    )
    assert exact_threshold["selected_model"] == "h_a"
    assert exact_threshold["selection_reason"] == "higher_validation_ndcg_at_10"

    complete_tie = select_validation_hybrid(
        _selection_summary(
            spark,
            a_ndcg=0.3,
            a_coverage=0.7,
            b_ndcg=0.3,
            b_coverage=0.7,
        )
    )
    assert complete_tie["selected_model"] == "h_a"
    assert complete_tie["selection_reason"] == "ndcg_and_coverage_tie_default_h_a"


def test_g9_selection_rejects_unequal_denominators(spark) -> None:
    invalid = _selection_summary(
        spark, a_ndcg=0.3, a_coverage=0.7, b_ndcg=0.3, b_coverage=0.7
    ).withColumn(
        "evaluated_users",
        F.when(F.col("model") == "h_b", 19).otherwise(20),
    )
    with pytest.raises(RuntimeError, match="denominators must be equal"):
        select_validation_hybrid(invalid)


def test_g9_budget_and_runtime_are_exactly_seven_and_do_not_invent_hybrid_time(
    spark,
) -> None:
    g7_budget = _g7_budget(spark)
    g8_budget = _g8_budget(spark)
    budget = build_experiment_budget(
        spark, g7_budget, g8_budget, selected_model="h_b"
    )
    rows = {row.model: row for row in budget.collect()}
    assert set(rows) == set(VALIDATION_MODELS)
    assert budget.count() == 7
    assert rows["h_b"].test_status == "evaluated_official_selected_winner"
    assert rows["h_a"].test_status == "not_evaluated_validation_loser"
    assert sum(row.fit_count for row in rows.values()) == 5

    runtime = build_model_runtime_summary(
        _g7_runtime(spark), g8_budget, _g8_runtime(spark)
    )
    runtime_rows = {row.model: row for row in runtime.collect()}
    assert set(runtime_rows) == set(VALIDATION_MODELS)
    for model in HYBRID_MODELS:
        assert runtime_rows[model].fit_count == 0
        assert runtime_rows[model].training_seconds == 0.0
        assert runtime_rows[model].candidate_generation_seconds >= 3.0
        assert runtime_rows[model].candidate_runtime_status == "measured"
        assert runtime_rows[model].runtime_source == "measured_in_g8"

    with pytest.raises(RuntimeError, match="exactly h_a and h_b"):
        build_experiment_budget(
            spark,
            g7_budget,
            _g8_budget(spark, include_third=True),
            selected_model="h_a",
        )


def _ranking_summary(spark, selected="h_b"):
    rows = []
    for stage, models in (
        ("validation", VALIDATION_MODELS),
        ("test", (*INDEPENDENT_MODELS, selected)),
    ):
        for model in models:
            for cohort in ("common_warm", "operational"):
                for slice_name, users in (("overall", 2), ("Book", 1), ("non-Book", 1)):
                    users_with_output = 1 if slice_name in {"overall", "Book"} else 0
                    success = users_with_output / users
                    fill_rate = 0.1 * users_with_output / users
                    rows.append(
                        (
                            model,
                            stage,
                            cohort,
                            slice_name,
                            users,
                            users_with_output,
                            success,
                            success,
                            success,
                            success,
                            fill_rate,
                            0.01,
                            3,
                            100,
                        )
                    )
    return spark.createDataFrame(
        rows,
        "model string, stage string, cohort string, slice string, "
        "evaluated_users long, users_with_output long, ndcg_at_10 double, "
        "hit_rate_at_10 double, mrr_at_10 double, user_coverage double, "
        "fill_rate_at_10 double, catalog_coverage_at_10 double, "
        "distinct_recommended_products_at_10 long, active_catalog_size long",
    )


def _als_summary(spark):
    return spark.createDataFrame(
        [
            ("als", stage, "all_heldout_ratings", 10, 8, 2, 0.8, 0.2, 1.1, 0.9)
            for stage in ("validation", "test")
        ],
        "model string, stage string, prediction_scope string, heldout_rows long, "
        "predicted_rows long, dropped_rows long, prediction_coverage double, "
        "drop_rate double, rmse double, mae double",
    )


def _population(spark):
    rows = []
    for stage in ("validation", "test"):
        for cohort in ("common_warm", "operational"):
            rows.extend(
                [
                    (stage, cohort, f"{stage}-{cohort}-book", 1, 5.0),
                    (stage, cohort, f"{stage}-{cohort}-other", 2, 4.0),
                ]
            )
    return spark.createDataFrame(
        rows,
        "stage string, cohort string, customer_id string, "
        "target_product_id int, rating double",
    )


def _per_user(spark, selected="h_b"):
    rows = []
    for stage, models in (
        ("validation", VALIDATION_MODELS),
        ("test", (*INDEPENDENT_MODELS, selected)),
    ):
        for model in models:
            for cohort in ("common_warm", "operational"):
                for suffix, target, target_group, group_slice in (
                    ("book", 1, "Book", "Book"),
                    ("other", 2, "Music", "non-Book"),
                ):
                    customer = f"{stage}-{cohort}-{suffix}"
                    has_output = suffix == "book"
                    for slice_name in ("overall", group_slice):
                        rows.append(
                            (
                                model,
                                stage,
                                cohort,
                                slice_name,
                                customer,
                                target,
                                5.0,
                                target_group,
                                1 if has_output else None,
                                1 if has_output else 0,
                                1 if has_output else 0,
                                has_output,
                                1.0 if has_output else 0.0,
                                1.0 if has_output else 0.0,
                                1.0 if has_output else 0.0,
                                0.1 if has_output else 0.0,
                            )
                        )
    return spark.createDataFrame(
        rows,
        "model string, stage string, cohort string, slice string, customer_id string, "
        "target_product_id int, target_rating double, target_group string, "
        "target_rank int, list_length long, top_k_list_length long, has_output boolean, "
        "ndcg_at_10 double, hit_rate_at_10 double, mrr_at_10 double, "
        "fill_fraction_at_10 double",
    )


def test_g9_contract_keeps_empty_users_and_limits_test_to_winner(spark) -> None:
    selection = select_validation_hybrid(
        _selection_summary(
            spark,
            a_ndcg=0.3000,
            a_coverage=0.6,
            b_ndcg=0.3020,
            b_coverage=0.5,
        )
    )
    selected_table = selection_frame(
        spark, selection, frozen_at_utc="2026-07-11T00:00:00+00:00"
    )
    runtime = build_model_runtime_summary(
        _g7_runtime(spark), _g8_budget(spark), _g8_runtime(spark)
    )
    budget = build_experiment_budget(
        spark, _g7_budget(spark), _g8_budget(spark), selected_model="h_b"
    )
    summary = attach_runtime_and_als_metrics(
        _ranking_summary(spark), runtime, _als_summary(spark)
    )
    evidence = validate_evaluation_contract(
        _per_user(spark),
        summary,
        selected_table,
        runtime,
        budget,
        _population(spark),
    )

    assert evidence["validation_model_count"] == 7
    assert evidence["test_model_count"] == 6
    assert evidence["experiment_budget_rows"] == 7
    assert evidence["empty_user_metric_violations"] == 0
    assert "h_a" not in evidence["test_models"]
    assert evidence["selection_test_blind"] is True
    assert summary.filter("model != 'als' AND rmse IS NOT NULL").count() == 0


def test_g9_resume_workspace_and_canonical_dashboard_outputs(tmp_path: Path) -> None:
    working = tmp_path / "G9-publish"
    _prepare_workspace(working, "signature-a")
    complete = working / "selected_hybrid"
    complete.mkdir()
    (complete / "_SUCCESS").write_bytes(b"")
    partial = working / ".official_test_comparison.deadbeef.tmp"
    partial.mkdir()

    removed = _prepare_workspace(working, "signature-a")
    assert (complete / "_SUCCESS").is_file()
    assert not partial.exists()
    assert removed == [str(partial)]

    _prepare_workspace(working, "signature-b")
    assert not complete.exists()
    assert {
        "evaluation_summary",
        "evaluation_per_user",
        "als_prediction_summary",
        "selected_hybrid",
        "validation_hybrid_comparison",
        "official_test_comparison",
    }.issubset(OUTPUT_TABLES)
