from __future__ import annotations

import copy
import csv
import json
import math
import shutil
from pathlib import Path

import pytest
import pyarrow as pa
import pyarrow.parquet as pq

from amazon_recommender.core.config import load_config
from amazon_recommender.core.manifest import (
    atomic_write_json,
    build_manifest,
    content_sha256,
)
from amazon_recommender.core.paths import RunPaths
from amazon_recommender.performance.experiment import (
    SparkEventMetrics,
    TrialResult,
    summarize_trials,
    trial_schedule,
)
from amazon_recommender.performance.workload import (
    PartitionEvidence,
    PlanEvidence,
    WorkloadMeasurement,
)
from amazon_recommender.phases import g9, g12
from amazon_recommender.pipelines.storage import table_fingerprint


pytestmark = pytest.mark.unit
PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _passing_junit(path: Path, tests: int = 5) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f'<testsuite tests="{tests}" failures="0" errors="0" skipped="0" />',
        encoding="utf-8",
    )
    return path


def _manifest_chain(
    root: Path,
    *,
    run_id: str = "fixture-run",
    config_sha256: str = "a" * 64,
    source_sha256: str = "b" * 64,
) -> tuple[dict[str, dict], dict[str, str]]:
    root.mkdir(parents=True, exist_ok=True)
    g0 = {"schema_version": 1, "gate": "G0", "status": "passed"}
    atomic_write_json(root / "G0.json", g0)
    manifests = {"G0": g0}
    digests = {"G0": content_sha256(g0)}
    for index in range(1, 12):
        gate = f"G{index}"
        manifest = build_manifest(
            gate=gate,
            run_id=run_id,
            status="passed",
            config_sha256=config_sha256,
            source_sha256=source_sha256,
            previous_evidence={f"G{i}": digests[f"G{i}"] for i in range(index)},
            evidence={"fixture": index},
        )
        atomic_write_json(root / f"{gate}.json", manifest)
        manifests[gate] = manifest
        digests[gate] = manifest["evidence_sha256"]
    return manifests, digests


def _producer_g11_summary() -> dict:
    trials = []
    timings = (4.0, 3.0, 2.0, 1.0)
    for trial_number, spec in enumerate(trial_schedule(8), start=1):
        workload = WorkloadMeasurement(
            wall_seconds=timings[spec.ordinal],
            output_rows=10,
            output_schema_json="schema",
            output_schema_sha256="e" * 64,
            plan=PlanEvidence(
                "formatted",
                "executed",
                "a" * 64,
                "b" * 64,
                2,
                ("Exchange",),
                True,
            ),
            partitions=PartitionEvidence(2, 1, 1, 2, 1, 1, 100),
            cache_enabled=False,
        )
        events = SparkEventMetrics(
            event_files=(f"events-{trial_number}",),
            event_log_sha256=f"{trial_number:064x}",
            applications_started=1,
            applications_ended=1,
            stages_completed=1,
            task_attempts=1,
            failed_task_attempts=0,
            executor_run_time_ms=1,
            executor_cpu_time_ns=1,
            executor_deserialize_time_ms=1,
            jvm_gc_time_ms=0,
            input_bytes_read=1,
            output_bytes_written=1,
            shuffle_read_bytes=1,
            shuffle_write_bytes=1,
            shuffle_fetch_wait_time_ms=0,
            memory_bytes_spilled=0,
            disk_bytes_spilled=0,
            sql_executions_started=1,
            sql_executions_ended=1,
        )
        trials.append(
            TrialResult(
                spec,
                workload,
                events,
                {
                    "spark.master": spec.condition.master,
                    "spark.task.cpus": "1",
                    "spark.sql.shuffle.partitions": "64",
                    "spark.sql.adaptive.enabled": "true",
                    "spark.eventLog.enabled": "true",
                    "spark.eventLog.compress": "false",
                },
                f"application-{trial_number}",
            )
        )
    return summarize_trials(trials, logical_cores=8)


def _valid_contract_manifests() -> dict[str, dict]:
    manifests = {gate: {"evidence": {}} for gate in g12.PRIOR_GATES}
    manifests["G0"] = {
        "java_home": "/usr/lib/jvm/java-21-openjdk-amd64",
        "python": {"version": "3.13.1"},
        "packages": {
            "graphframes-py": "0.12.1",
            "pyarrow": "25.0.0",
            "duckdb": "1.5.4",
            "streamlit": "1.59.1",
        },
        "hardware": {
            "logical_cores": 8,
            "memory_total_bytes": 16 * 1024**3,
        },
        "runtime": {
            "java_version": "21.0.11",
            "spark_version": "4.0.0",
            "scala_version": "2.13.16",
            "spark_conf": {"spark.driver.memory": "8g"},
            "tests": {
                "parquet": {"status": "passed"},
                "pagerank": {"status": "passed"},
                "wcc": {"status": "passed"},
                "checkpoint": {"status": "passed"},
            },
        },
    }
    manifests["G2"]["evidence"] = {
        "delimiter": {"hex": "0d0a0d0a"},
        "contracts": {"source_offsets": "LongWritable uncompressed offsets"},
        "hadoop_sample": {"offsets": [0, 80, 133]},
    }
    manifests["G4"]["evidence"] = {
        "hard_counts": dict(g12.EXPECTED_HARD_COUNTS),
        "header_records": 1,
        "quarantine_records": 0,
        "delimiter": {"hex": "0d0a0d0a"},
    }
    manifests["G5"]["evidence"] = {
        "profile_counts": {
            "reviews_raw": 7_593_244,
            "reviews_deduplicated": 7_446_499,
            "user_item_interactions": 6_359_182,
            "duplicate_review_extra": 146_745,
            "category_nodes": 49_732,
            "internal_graph_edges": 1_231_439,
            "orphan_graph_targets": 172_790,
            "invalid_dates": 0,
            "invalid_ratings": 0,
            "downloaded_row_count_mismatches": 0,
            "category_count_mismatches": 0,
            "similar_count_mismatches": 0,
            "deduplicated_key_violations": 0,
            "interaction_key_violations": 0,
        }
    }
    manifests["G6"]["evidence"] = {
        "split_order": ["interaction_date ASC", "product_id ASC"],
        "validation_seen": "train only",
        "test_seen": "train plus validation target",
        "stable_hash": "SHA256(customer_id + U+001F + '42')",
        "invariants": {
            "source_interactions": 6_359_182,
            "train_interactions": 5_849_830,
            "validation_interactions": 254_676,
            "test_interactions": 254_676,
            "split_total": 6_359_182,
            "split_pair_overlap": 0,
            "test_seen_missing_validation": 0,
            "temporal_position_violations": 0,
            "validation_target_seen_violations": 0,
            "test_target_seen_violations": 0,
            "kcore_converged": 1,
        },
    }
    manifests["G7"]["evidence"] = {
        "independent_model_count": 5,
        "independent_models": list(g12.INDEPENDENT_MODELS),
        "hybrid_models_trained": 0,
        "single_fit_contract": True,
        "train_only_feature_lineage": True,
        "stage_details": {
            model: {
                "fit_count": 1,
                "parameters": dict(g12.G7_PARAMETER_SUBSETS[model]),
            }
            for model in g12.INDEPENDENT_MODELS
        },
    }
    manifests["G8"]["evidence"] = {
        "hybrid_variants": ["h_a", "h_b"],
        "independent_models_refit": 0,
        "hybrid_models_fit": 0,
        "selection_deferred_to_g9_validation": True,
        "experiment_budget": {
            "g7_independent_model_count": 5,
            "g7_total_fit_count": 5,
            "hybrid_variant_count": 2,
            "g8_model_refit_count": 0,
            "variants": ["h_a", "h_b"],
        },
        "variant_validations": {
            variant: {
                "weights": dict(weights),
                "rrf_c": 60,
                "stored_depth": 100,
            }
            for variant, weights in g12.HYBRID_WEIGHTS.items()
        },
    }
    selected = "h_a"
    manifests["G9"]["evidence"] = {
        "selection": {
            "selected_model": selected,
            "test_metrics_used": False,
            "selection_status": "frozen_before_test_evaluation",
            "selection_stage": "validation",
            "selection_cohort": "common_warm",
            "selection_slice": "overall",
            "selected_weights_json": json.dumps(g12.HYBRID_WEIGHTS[selected]),
            "rrf_c": 60,
            "stored_depth": 100,
            "ndcg_tie_threshold": 0.001,
        },
        "selection_freeze_evidence": {
            "selected_model": selected,
            "test_outputs_present_at_freeze": [],
        },
        "selection_test_blind": True,
        "experiment_budget_rows": 7,
        "official_validation_models": [*g12.INDEPENDENT_MODELS, *g12.HYBRID_MODELS],
        "official_test_models": [*g12.INDEPENDENT_MODELS, selected],
        "invariants": {
            "selection_test_blind": True,
            "validation_model_count": 7,
            "test_model_count": 6,
            "experiment_budget_rows": 7,
        },
    }
    manifests["G10"]["evidence"] = {
        "spark_free_static_audit": {
            "status": "passed",
            "page_count": 4,
            "spark_session_construction": False,
            "streamlit_server_started": False,
        },
        "four_page_app_test": {
            "status": "passed",
            "files_executed": 4,
            "pages_executed": 4,
            "server_started": False,
            "browser_started": False,
            "new_spark_or_java_children": [],
        },
        "gold_source_contract": {
            "status": "passed",
            "silver_runtime_sources": [],
            "resolver_priority_violations": {},
            "all_tables_have_success_marker": True,
        },
        "duckdb_read_only_probe": {
            "status": "passed",
            "write_statement_rejected": True,
            "spark_imported_by_app": False,
        },
        "dashboard_exports": {"compact_aggregate_tables": 6},
        "servable_customer_contract": {
            "rows": 80_000,
            "online_arbitrary_user_promise": False,
        },
        "demo_user_contract": {"rows": 20},
        "streamlit_server_started": False,
        "browser_started": False,
        "spark_session_started": False,
        "tables": {
            name: {}
            for name in (
                "dashboard_overview",
                "dashboard_quality_summary",
                "dashboard_group_distribution",
                "dashboard_rating_distribution",
                "dashboard_review_year_distribution",
                "dashboard_activity_quantiles",
                "product_search_index",
                "category_search_index",
                "servable_customers",
                "demo_users",
                "dashboard_contract",
            )
        },
    }
    manifests["G11"]["evidence"] = {
        "condition_count": 2,
        "trial_count": 8,
        "warmups_per_condition": 1,
        "measured_runs_per_condition": 3,
        "cache_enabled": False,
        "shuffle_partitions": 64,
        "aqe_enabled": True,
        "summary": _producer_g11_summary(),
    }
    return manifests


def _official_rows(selected: str = "h_a") -> tuple[list[str], list[dict]]:
    rows = []
    models = [*g12.INDEPENDENT_MODELS, selected]
    for model in models:
        for cohort in g12.COHORTS:
            for slice_name in g12.SLICES:
                rows.append(
                    {
                        "model": model,
                        "stage": "test",
                        "cohort": cohort,
                        "slice": slice_name,
                        "evaluated_users": 100,
                        "users_with_output": 80,
                        "ndcg_at_10": 0.25,
                        "hit_rate_at_10": 0.3,
                        "mrr_at_10": 0.2,
                        "user_coverage": 0.8,
                        "fill_rate_at_10": 0.7,
                        "catalog_coverage_at_10": 0.1,
                        "selected_hybrid_model": selected,
                        "is_selected_hybrid": model == selected,
                        "official_result": True,
                        "rmse": 1.1 if model == "als" else None,
                        "mae": 0.9 if model == "als" else None,
                    }
                )
    return list(rows[0]), rows


def _write_parquet_fixture(path: Path, rows: list[dict]) -> None:
    path.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path / "part-00000.parquet")
    (path / "_SUCCESS").write_bytes(b"")


def test_manifest_chain_recomputes_every_digest_and_rejects_tampering(
    tmp_path: Path,
) -> None:
    manifests, digests = _manifest_chain(tmp_path / "manifests")
    observed, observed_digests = g12.validate_manifest_chain(
        tmp_path / "manifests",
        run_id="fixture-run",
        config_sha256="a" * 64,
        source_sha256="b" * 64,
    )
    assert observed.keys() == manifests.keys()
    assert observed_digests == digests

    path = tmp_path / "manifests" / "G6.json"
    tampered = json.loads(path.read_text())
    tampered["evidence"]["fixture"] = 999
    atomic_write_json(path, tampered)
    with pytest.raises(RuntimeError, match="G6 manifest digest mismatch"):
        g12.validate_manifest_chain(
            tmp_path / "manifests",
            run_id="fixture-run",
            config_sha256="a" * 64,
            source_sha256="b" * 64,
        )


def test_final_contract_accepts_exact_budget_and_rejects_test_informed_selection() -> (
    None
):
    manifests = _valid_contract_manifests()
    evidence = g12.validate_gate_contracts(manifests)
    assert evidence["selected_hybrid"] == "h_a"
    assert evidence["performance_trials"] == 8
    assert evidence["dashboard_spark_free"] is True

    tampered = copy.deepcopy(manifests)
    tampered["G9"]["evidence"]["selection"]["test_metrics_used"] = True
    with pytest.raises(RuntimeError, match="used test metrics"):
        g12.validate_gate_contracts(tampered)


def test_final_contract_rejects_missing_performance_trial() -> None:
    manifests = _valid_contract_manifests()
    manifests["G11"]["evidence"]["trial_count"] = 7
    with pytest.raises(RuntimeError, match="eight trials"):
        g12.validate_gate_contracts(manifests)


def test_final_contract_recomputes_g11_timings_from_raw_trials() -> None:
    manifests = _valid_contract_manifests()
    trial = manifests["G11"]["evidence"]["summary"]["conditions"]["single_core"][
        "trials"
    ][1]
    trial["workload"]["wall_seconds"] = 99.0
    with pytest.raises(RuntimeError, match="raw trials"):
        g12.validate_gate_contracts(manifests)


def test_g9_selection_is_recomputed_from_frozen_parquet_artifacts(
    tmp_path: Path,
) -> None:
    g9_root = tmp_path / "data" / "g9"
    selection = {
        "selected_model": "h_a",
        "selected_weights_json": json.dumps(
            g12.HYBRID_WEIGHTS["h_a"], sort_keys=True, separators=(",", ":")
        ),
        "rrf_c": 60,
        "stored_depth": 100,
        "selection_reason": "higher_validation_ndcg_at_10",
        "selection_stage": "validation",
        "selection_cohort": "common_warm",
        "selection_slice": "overall",
        "evaluated_users": 100,
        "h_a_ndcg_at_10": 0.20,
        "h_a_user_coverage": 0.80,
        "h_b_ndcg_at_10": 0.19,
        "h_b_user_coverage": 0.82,
        "ndcg_difference_h_a_minus_h_b": 0.01,
        "coverage_difference_h_a_minus_h_b": -0.02,
        "ndcg_tie_threshold": 0.001,
        "test_metrics_used": False,
        "selection_status": "frozen_before_test_evaluation",
        "frozen_at_utc": "2026-07-11T00:00:00Z",
    }
    assert set(selection) == {field.name for field in g9._SELECTION_SCHEMA}
    signature = "f" * 64
    marker = g9_root / "_selection_frozen_before_test.json"
    atomic_write_json(
        marker,
        {
            "gate": "G9",
            "implementation_sha256": signature,
            "selected_model": "h_a",
            "test_outputs_present_at_freeze": [],
            "frozen_at_utc": selection["frozen_at_utc"],
        },
    )
    _write_parquet_fixture(g9_root / "selected_hybrid", [selection])
    validation_rows = [
        {
            "model": model,
            "stage": "validation",
            "cohort": "common_warm",
            "slice": "overall",
            "evaluated_users": 100,
            "ndcg_at_10": selection[f"{model}_ndcg_at_10"],
            "user_coverage": selection[f"{model}_user_coverage"],
            "selected": model == "h_a",
            "selection_reason": selection["selection_reason"],
            "selection_status": selection["selection_status"],
        }
        for model in g12.HYBRID_MODELS
    ]
    validation_rows.append(
        {
            **validation_rows[0],
            "stage": "validation",
            "cohort": "operational",
            "selected": False,
        }
    )
    _write_parquet_fixture(g9_root / "validation_hybrid_comparison", validation_rows)
    _write_parquet_fixture(
        g9_root / "experiment_budget",
        [
            {
                "model": model,
                "fit_count": 1 if model in g12.INDEPENDENT_MODELS else 0,
                "test_status": (
                    "evaluated_official"
                    if model in g12.INDEPENDENT_MODELS
                    else (
                        "evaluated_official_selected_winner"
                        if model == "h_a"
                        else "not_evaluated_validation_loser"
                    )
                ),
            }
            for model in (*g12.INDEPENDENT_MODELS, *g12.HYBRID_MODELS)
        ],
    )
    for name in (
        "test_evaluation_per_user",
        "test_evaluation_summary",
        "official_test_comparison",
    ):
        path = g9_root / name
        path.mkdir(parents=True)
        (path / "_SUCCESS").write_bytes(b"")

    manifest = {
        "evidence": {
            "implementation_sha256": signature,
            "selection": dict(selection),
            "selection_freeze_evidence": {
                "gate": "G9",
                "implementation_sha256": signature,
                "selected_model": "h_a",
                "test_outputs_present_at_freeze": [],
                "frozen_at_utc": selection["frozen_at_utc"],
            },
        }
    }
    result = g12.verify_g9_selection_artifacts(tmp_path, manifest)
    assert result["selected_hybrid"] == "h_a"
    assert result["freeze_precedes_test_markers"] is True


def test_official_comparison_requires_exact_six_model_test_matrix_and_finite_metrics() -> (
    None
):
    columns, rows = _official_rows()
    result = g12.validate_official_comparison(columns, rows, selected_hybrid="h_a")
    assert result["rows"] == 36
    assert result["test_only"] is True

    nonfinite = copy.deepcopy(rows)
    nonfinite[0]["ndcg_at_10"] = math.nan
    with pytest.raises(RuntimeError, match="non-finite"):
        g12.validate_official_comparison(columns, nonfinite, selected_hybrid="h_a")

    extra_hybrid = copy.deepcopy(rows)
    extra_hybrid[0]["model"] = "h_b"
    with pytest.raises(RuntimeError, match="unofficial test model"):
        g12.validate_official_comparison(columns, extra_hybrid, selected_hybrid="h_a")


def test_artifact_fingerprint_rejects_changed_bytes(tmp_path: Path) -> None:
    artifact = tmp_path / "table"
    artifact.mkdir()
    (artifact / "_SUCCESS").write_bytes(b"")
    (artifact / "part-00000.parquet").write_bytes(b"verified")
    digest = table_fingerprint(artifact)
    result = g12._verify_directory_artifact(
        artifact,
        expected_sha256=digest,
        artifact_type="fixture",
        logical_name="fixture",
    )
    assert result["sha256"] == digest

    (artifact / "part-00000.parquet").write_bytes(b"tampered")
    with pytest.raises(RuntimeError, match="fingerprint mismatch"):
        g12._verify_directory_artifact(
            artifact,
            expected_sha256=digest,
            artifact_type="fixture",
            logical_name="fixture",
        )


def test_delivery_payload_fingerprint_preserves_path_content_boundaries(
    tmp_path: Path,
) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir()
    second.mkdir()
    (first / "README.md").write_text("#content", encoding="utf-8")
    (second / "README.md#").write_text("content", encoding="utf-8")

    # A naive path+content stream is identical for these two inventories.
    assert b"README.md" + b"#content" == b"README.md#" + b"content"
    assert g12._delivery_payload_fingerprint(
        first
    ) != g12._delivery_payload_fingerprint(second)


def test_readme_contract_passes_project_guide_and_rejects_placeholder(
    tmp_path: Path,
) -> None:
    evidence = g12.validate_readme(PROJECT_ROOT / "README.md")
    assert evidence["placeholder_matches"] == 0
    bad = tmp_path / "README.md"
    text = (PROJECT_ROOT / "README.md").read_text()
    bad.write_text(text + "\nTODO\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="placeholder"):
        g12.validate_readme(bad)


def _handler_context(tmp_path: Path):
    shutil.copy2(PROJECT_ROOT / "README.md", tmp_path / "README.md")
    config = load_config(
        PROJECT_ROOT / "configs" / "project.yaml", project_root=tmp_path
    )
    paths = RunPaths.create(tmp_path, tmp_path / "artifacts", "fixture-run")
    paths.ensure_control_dirs()
    junit = _passing_junit(tmp_path / "g12-junit.xml")
    return config, paths, junit


def test_g12_handler_does_not_publish_when_real_results_are_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config, paths, junit = _handler_context(tmp_path)

    def missing(*args, **kwargs):
        raise RuntimeError("G11 result is missing")

    monkeypatch.setattr(g12, "collect_verified_inputs", missing)
    with pytest.raises(RuntimeError, match="G11 result is missing"):
        g12.run_g12(config, paths, junit)
    assert not (paths.run / "delivery").exists()


def test_g12_handler_atomically_publishes_only_verified_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config, paths, junit = _handler_context(tmp_path)
    manifests, digests = _manifest_chain(
        paths.manifests,
        run_id=paths.run_id,
        config_sha256=config.sha256,
        source_sha256=config.get("source", "sha256"),
    )
    columns, rows = _official_rows()
    manifests["G9"]["evidence"] = {
        "tables": {"official_test_comparison": {"sha256": "c" * 64}},
        "selection": {
            "selected_model": "h_a",
            "selection_status": "frozen_before_test_evaluation",
            "h_a_ndcg_at_10": 0.2,
            "h_b_ndcg_at_10": 0.19,
            "h_a_user_coverage": 0.8,
            "h_b_user_coverage": 0.79,
        },
    }
    manifests["G11"]["evidence"] = _valid_contract_manifests()["G11"]["evidence"]
    manifests["G11"]["evidence"]["summary"]["local_parallel_speedup"] = 1.5
    verified = g12.VerifiedDeliveryInputs(
        manifests=manifests,
        manifest_digests=digests,
        source_identity={"sha256": config.get("source", "sha256"), "size_bytes": 1},
        contracts={"selected_hybrid": "h_a", "official_comparison": {"rows": 36}},
        test_summary={
            "all_passed": True,
            "gate_count": 13,
            "tests": 5,
            "failures": 0,
            "errors": 0,
            "skipped": 0,
            "per_gate": {"G12": {"tests": 5, "failures": 0, "errors": 0, "skipped": 0}},
        },
        test_files={"G12": str(junit)},
        artifact_inventory=[{"logical_name": "fixture", "sha256": "d" * 64}],
        comparison_columns=columns,
        comparison_rows=rows,
    )
    monkeypatch.setattr(g12, "collect_verified_inputs", lambda *args: verified)

    evidence = g12.run_g12(config, paths, junit)

    final = paths.run / "delivery"
    assert (final / "_PENDING_G12_MANIFEST.json").is_file()
    assert not (final / "_SUCCESS.json").exists()
    pending_acceptance = json.loads((final / "acceptance-report.json").read_text())
    assert pending_acceptance["passed"] is False
    assert (
        pending_acceptance["status"]
        == "acceptance_checks_passed_publication_requires_success_marker"
    )

    manifest = build_manifest(
        gate="G12",
        run_id=paths.run_id,
        status="passed",
        config_sha256=config.sha256,
        source_sha256=config.get("source", "sha256"),
        previous_evidence=digests,
        evidence=evidence,
    )
    atomic_write_json(paths.manifests / "G12.json", manifest)

    result_path = final / "final-results.md"
    original_result = result_path.read_bytes()
    result_path.write_bytes(original_result + b"tampered")
    with pytest.raises(RuntimeError, match="payload fingerprint mismatch"):
        g12.finalize_g12_delivery(paths, paths.manifests / "G12.json")
    assert not (final / "_SUCCESS.json").exists()
    result_path.write_bytes(original_result)

    acceptance_path = final / "acceptance-report.json"
    original_acceptance = acceptance_path.read_bytes()
    acceptance_path.write_bytes(original_acceptance + b"tampered")
    with pytest.raises(RuntimeError, match="acceptance report changed"):
        g12.finalize_g12_delivery(paths, paths.manifests / "G12.json")
    acceptance_path.write_bytes(original_acceptance)

    original_atomic_write = g12.atomic_write_json
    original_copy_file_atomic = g12._copy_file_atomic

    def crash_after_manifest_temp_copy(source: Path, destination: Path) -> None:
        temporary = destination.with_name(f".{destination.name}.tmp")
        temporary.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, temporary)
        raise OSError("simulated manifest replace crash")

    monkeypatch.setattr(g12, "_copy_file_atomic", crash_after_manifest_temp_copy)
    with pytest.raises(OSError, match="simulated manifest replace crash"):
        g12.finalize_g12_delivery(paths, paths.manifests / "G12.json")
    manifest_temp = final / "manifests" / ".G12.json.tmp"
    assert manifest_temp.is_file()
    assert not (final / "_SUCCESS.json").exists()
    monkeypatch.setattr(g12, "_copy_file_atomic", original_copy_file_atomic)

    def crash_during_index_commit(path: Path, payload: dict) -> None:
        if path.name == "manifest-index.json":
            raise OSError("simulated index commit crash")
        original_atomic_write(path, payload)

    monkeypatch.setattr(g12, "atomic_write_json", crash_during_index_commit)
    with pytest.raises(OSError, match="simulated index commit crash"):
        g12.finalize_g12_delivery(paths, paths.manifests / "G12.json")
    assert not manifest_temp.exists()
    assert (final / "_PENDING_G12_MANIFEST.json").is_file()
    assert not (final / "_SUCCESS.json").exists()

    def crash_before_success(path: Path, payload: dict) -> None:
        if path.name == "_SUCCESS.json":
            raise OSError("simulated final commit crash")
        original_atomic_write(path, payload)

    monkeypatch.setattr(g12, "atomic_write_json", crash_before_success)
    with pytest.raises(OSError, match="simulated final commit crash"):
        g12.finalize_g12_delivery(paths, paths.manifests / "G12.json")
    assert not (final / "_SUCCESS.json").exists()
    assert not (final / "_PENDING_G12_MANIFEST.json").exists()
    assert (final / "manifests" / "G12.json").is_file()

    monkeypatch.setattr(g12, "atomic_write_json", original_atomic_write)
    success = g12.finalize_g12_delivery(paths, paths.manifests / "G12.json")

    assert success["manifest_count"] == 13
    assert (final / "_SUCCESS.json").is_file()
    assert (final / "manifests" / "G12.json").is_file()
    assert not (final / "_PENDING_G12_MANIFEST.json").exists()
    assert g12.finalize_g12_delivery(paths, paths.manifests / "G12.json") == success
    original_atomic_write(
        final / "_PENDING_G12_MANIFEST.json", {"legacy_dual_marker": True}
    )
    assert g12.finalize_g12_delivery(paths, paths.manifests / "G12.json") == success
    assert not (final / "_PENDING_G12_MANIFEST.json").exists()
    assert evidence["official_comparison_rows"] == 36
    assert evidence["placeholder_results_published"] is False
    assert evidence["delivery"]["status"] == "pending_manifest_finalization"
    assert not (paths.temporary / "G12-publish").exists()
    with (final / "official-test-comparison.csv").open(newline="") as handle:
        published_rows = list(csv.DictReader(handle))
    assert len(published_rows) == 36
    result_text = (final / "final-results.md").read_text()
    assert "0.25" in result_text
    assert g12.PLACEHOLDER_PATTERN.search(result_text) is None
    acceptance = json.loads((final / "acceptance-report.json").read_text())
    assert acceptance["passed"] is False
    assert acceptance["acceptance_checks_passed"] is True
    assert acceptance["publication_authority"] == "_SUCCESS.json"
    assert acceptance["prior_manifest_chain_verified"] is True
    assert acceptance["unexecuted_or_placeholder_results_published"] is False

    result_path.write_bytes(original_result + b"post-success-tamper")
    with pytest.raises(RuntimeError, match="payload fingerprint mismatch"):
        g12.finalize_g12_delivery(paths, paths.manifests / "G12.json")
