from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from amazon_recommender.core.config import load_config
from amazon_recommender.core.paths import RunPaths
from amazon_recommender.performance.experiment import (
    SparkEventMetrics,
    TrialResult,
    TrialSpec,
    parse_spark_event_metrics,
)
from amazon_recommender.performance.workload import (
    PartitionEvidence,
    PlanEvidence,
    WorkloadMeasurement,
)
from amazon_recommender.phases import g11


pytestmark = pytest.mark.unit
PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _passing_junit(path: Path) -> Path:
    path.write_text(
        '<testsuite tests="3" failures="0" errors="0" skipped="0" />',
        encoding="utf-8",
    )
    return path


def _context(tmp_path: Path):
    config = load_config(
        PROJECT_ROOT / "configs" / "project.yaml", project_root=tmp_path
    )
    paths = RunPaths.create(tmp_path, tmp_path / "artifacts", "fixture-run")
    paths.ensure_control_dirs()
    for name in g11.FULL_SILVER_INPUTS:
        table = paths.data / "full" / "silver" / name
        table.mkdir(parents=True)
        (table / "_SUCCESS").write_bytes(b"")
    return config, paths


def _event_log(path: Path) -> SparkEventMetrics:
    path.mkdir(parents=True)
    events = [
        {"Event": "SparkListenerApplicationStart"},
        {"Event": "SparkListenerStageCompleted"},
        {
            "Event": "SparkListenerTaskEnd",
            "Task End Reason": {"Reason": "Success"},
            "Task Metrics": {
                "Executor Run Time": 100,
                "Executor CPU Time": 90_000_000,
                "Input Metrics": {"Bytes Read": 1000},
                "Output Metrics": {"Bytes Written": 200},
                "Shuffle Read Metrics": {
                    "Remote Bytes Read": 10,
                    "Local Bytes Read": 20,
                },
                "Shuffle Write Metrics": {"Shuffle Bytes Written": 30},
            },
        },
        {"Event": "SparkListenerApplicationEnd"},
    ]
    (path / "events_1").write_text(
        "".join(json.dumps(event) + "\n" for event in events), encoding="utf-8"
    )
    return parse_spark_event_metrics(path)


def _fake_trial(spec: TrialSpec, trial_root: Path) -> TrialResult:
    trial_root.mkdir(parents=True, exist_ok=False)
    events = _event_log(trial_root / "spark-events")
    formatted = "AdaptiveSparkPlan\n+- Exchange hashpartitioning"
    executed = "AdaptiveSparkPlan isFinalPlan=true\n+- Exchange hashpartitioning"
    seconds = (
        90.0
        if spec.is_warmup
        else float(5 - spec.ordinal)
        * (2.0 if spec.condition.name == "single_core" else 1.0)
    )
    return TrialResult(
        spec=spec,
        workload=WorkloadMeasurement(
            wall_seconds=seconds,
            output_rows=11,
            output_schema_json='{"fixture":true}',
            output_schema_sha256=hashlib.sha256(b'{"fixture":true}').hexdigest(),
            plan=PlanEvidence(
                formatted_plan=formatted,
                executed_plan=executed,
                formatted_plan_sha256=hashlib.sha256(formatted.encode()).hexdigest(),
                executed_plan_sha256=hashlib.sha256(executed.encode()).hexdigest(),
                exchange_node_count=1,
                exchange_node_lines=("+- Exchange hashpartitioning",),
                adaptive_plan_present=True,
            ),
            partitions=PartitionEvidence(8, 2, 1, 8, 2, 1, 128),
            cache_enabled=False,
        ),
        events=events,
        spark_conf={
            "spark.master": spec.condition.master,
            "spark.task.cpus": "1",
            "spark.sql.shuffle.partitions": "64",
            "spark.sql.adaptive.enabled": "true",
            "spark.eventLog.enabled": "true",
            "spark.eventLog.compress": "false",
        },
        application_id=f"fixture-{g11._trial_name(spec)}",
    )


def test_g11_handler_atomically_publishes_exact_eight_trial_contract(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config, paths = _context(tmp_path)
    junit = _passing_junit(tmp_path / "junit.xml")
    calls: list[str] = []

    def fake_run(spec, *, reviews_deduplicated_path, products_path, trial_root):
        assert reviews_deduplicated_path.name == "reviews_deduplicated"
        assert products_path.name == "products"
        calls.append(g11._trial_name(spec))
        return _fake_trial(spec, trial_root)

    monkeypatch.setattr(g11, "logical_core_count", lambda: 8)
    monkeypatch.setattr(g11, "run_performance_trial", fake_run)

    evidence = g11.run_g11(config, paths, junit)

    assert len(calls) == g11.EXPECTED_TRIAL_COUNT == 8
    assert evidence["trial_count"] == 8
    assert evidence["condition_count"] == 2
    assert evidence["warmups_per_condition"] == 1
    assert evidence["measured_runs_per_condition"] == 3
    assert evidence["cache_enabled"] is False
    assert evidence["shuffle_partitions"] == 64
    assert evidence["summary"]["conditions"]["single_core"]["median_wall_seconds"] == 6.0
    assert evidence["summary"]["conditions"]["bounded_multi_core"]["median_wall_seconds"] == 3.0
    assert evidence["summary"]["local_parallel_speedup"] == 2.0
    final = paths.run / "performance"
    assert (final / "_SUCCESS.json").is_file()
    assert (final / "summary.json").is_file()
    assert (final / "protocol.json").is_file()
    assert (final / "junit.xml").read_bytes() == junit.read_bytes()
    assert len(list((final / "trials").iterdir())) == 8
    assert not (paths.temporary / "G11-publish").exists()
    assert all(item["event_log_sha256"] for item in evidence["trial_evidence"].values())
    assert all(item["exchange_node_count"] == 1 for item in evidence["trial_evidence"].values())
    for condition in evidence["summary"]["conditions"].values():
        for trial in condition["trials"]:
            assert all(Path(path).is_file() for path in trial["events"]["event_files"])


def test_g11_interrupted_run_reuses_only_atomically_completed_trials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config, paths = _context(tmp_path)
    junit = _passing_junit(tmp_path / "junit.xml")
    first_calls: list[str] = []

    def interrupted(spec, *, reviews_deduplicated_path, products_path, trial_root):
        name = g11._trial_name(spec)
        first_calls.append(name)
        if len(first_calls) == 4:
            raise RuntimeError("fixture interruption")
        return _fake_trial(spec, trial_root)

    monkeypatch.setattr(g11, "logical_core_count", lambda: 8)
    monkeypatch.setattr(g11, "run_performance_trial", interrupted)
    with pytest.raises(RuntimeError, match="fixture interruption"):
        g11.run_g11(config, paths, junit)

    completed = paths.temporary / "G11-publish" / "trials"
    assert len([path for path in completed.iterdir() if not path.name.startswith(".")]) == 3
    assert not any(path.name.endswith(".tmp") for path in completed.iterdir())

    resumed_calls: list[str] = []

    def resumed(spec, *, reviews_deduplicated_path, products_path, trial_root):
        resumed_calls.append(g11._trial_name(spec))
        return _fake_trial(spec, trial_root)

    monkeypatch.setattr(g11, "run_performance_trial", resumed)
    evidence = g11.run_g11(config, paths, junit)

    assert len(resumed_calls) == 5
    assert len(evidence["trials_reused"]) == 3
    assert evidence["trial_count"] == 8


def test_g11_rejects_changed_experiment_budget_before_any_trial() -> None:
    class ChangedConfig:
        def get(self, *path):
            values = {
                ("performance", "masters"): ["local[1]", "local[8]"],
                ("performance", "warmups"): 1,
                ("performance", "measured_runs"): 3,
                ("performance", "cache_enabled"): False,
            }
            return values[path]

    with pytest.raises(RuntimeError, match="configuration changed"):
        g11._validate_binding_config(ChangedConfig())
