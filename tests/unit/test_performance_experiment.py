from __future__ import annotations

import json

import pytest

from amazon_recommender.performance import experiment
from amazon_recommender.performance.experiment import (
    PerformanceCondition,
    SparkEventMetrics,
    TrialResult,
    TrialSpec,
    parse_spark_event_metrics,
    performance_conditions,
    summarize_trials,
    trial_schedule,
)
from amazon_recommender.performance.workload import (
    PartitionEvidence,
    PlanEvidence,
    WorkloadMeasurement,
)


def _events() -> SparkEventMetrics:
    return SparkEventMetrics(
        event_files=("events",),
        event_log_sha256="0" * 64,
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


def _trial(spec: TrialSpec, seconds: float) -> TrialResult:
    workload = WorkloadMeasurement(
        wall_seconds=seconds,
        output_rows=9,
        output_schema_json="schema",
        output_schema_sha256="a" * 64,
        plan=PlanEvidence("plan", "plan", "b" * 64, "b" * 64, 1, ("Exchange",), True),
        partitions=PartitionEvidence(2, 1, 1, 2, 1, 1, 100),
        cache_enabled=False,
    )
    return TrialResult(
        spec,
        workload,
        _events(),
        {
            "spark.master": spec.condition.master,
            "spark.task.cpus": "1",
            "spark.sql.shuffle.partitions": "64",
            "spark.sql.adaptive.enabled": "true",
            "spark.eventLog.enabled": "true",
            "spark.eventLog.compress": "false",
        },
        "app",
    )


@pytest.mark.unit
def test_conditions_and_schedule_are_exact_for_available_logical_cores() -> None:
    assert performance_conditions(12) == (
        PerformanceCondition("single_core", "local[1]", 1),
        PerformanceCondition("bounded_multi_core", "local[4]", 4),
    )
    assert performance_conditions(2)[1].master == "local[2]"
    assert performance_conditions(1)[1].master == "local[1]"
    schedule = trial_schedule(12)
    assert len(schedule) == 8
    for condition in performance_conditions(12):
        selected = [item for item in schedule if item.condition == condition]
        assert [item.is_warmup for item in selected] == [True, False, False, False]
        assert [item.ordinal for item in selected] == [0, 1, 2, 3]


@pytest.mark.unit
def test_performance_session_overrides_pipeline_task_width_for_local_one(
    tmp_path, monkeypatch
) -> None:
    class Builder:
        def __init__(self):
            self.values = {}

        def master(self, value):
            self.values["spark.master"] = value
            return self

        def appName(self, value):
            self.values["spark.app.name"] = value
            return self

        def config(self, key, value):
            self.values[key] = value
            return self

        def getOrCreate(self):
            return self

    builder = Builder()
    monkeypatch.setattr(
        experiment.SparkSession, "builder", builder
    )
    monkeypatch.setattr(experiment.SparkContext, "_active_spark_context", None)
    spec = TrialSpec(
        PerformanceCondition("single_core", "local[1]", 1),
        ordinal=0,
        is_warmup=True,
    )

    session = experiment.create_performance_session(
        spec, tmp_path / "events", app_name="fixture-g11"
    )

    assert session.values["spark.master"] == "local[1]"
    assert session.values["spark.task.cpus"] == "1"
    assert session.values["spark.sql.shuffle.partitions"] == "64"
    assert session.values["spark.sql.adaptive.enabled"] == "true"


@pytest.mark.unit
def test_summary_excludes_warmup_uses_median_and_preserves_trials() -> None:
    schedule = trial_schedule(8)
    timings = {
        "single_core": [99.0, 9.0, 3.0, 6.0],
        "bounded_multi_core": [77.0, 3.0, 1.0, 2.0],
    }
    counters = {name: 0 for name in timings}
    trials = []
    for spec in schedule:
        index = counters[spec.condition.name]
        trials.append(_trial(spec, timings[spec.condition.name][index]))
        counters[spec.condition.name] += 1
    summary = summarize_trials(trials, logical_cores=8)
    assert summary["conditions"]["single_core"]["median_wall_seconds"] == 6.0
    assert summary["conditions"]["bounded_multi_core"]["median_wall_seconds"] == 2.0
    assert summary["local_parallel_speedup"] == 3.0
    assert len(summary["conditions"]["single_core"]["trials"]) == 4
    assert summary["protocol"] == {
        "warmups_per_condition": 1,
        "measured_runs_per_condition": 3,
        "shuffle_partitions": 64,
        "aqe_enabled": True,
        "cache_enabled": False,
        "comparison": "local multi-core parallelism; not horizontal scaling",
    }


@pytest.mark.unit
def test_summary_rejects_incomplete_budget_or_nonreconciling_output() -> None:
    schedule = trial_schedule(4)
    trials = [_trial(spec, 1.0) for spec in schedule]
    with pytest.raises(ValueError, match="exactly 1 warmup and 3 measured"):
        summarize_trials(trials[:-1], logical_cores=4)

    changed = trials[-1]
    mismatched_workload = WorkloadMeasurement(
        wall_seconds=changed.workload.wall_seconds,
        output_rows=10,
        output_schema_json=changed.workload.output_schema_json,
        output_schema_sha256=changed.workload.output_schema_sha256,
        plan=changed.workload.plan,
        partitions=changed.workload.partitions,
    )
    trials[-1] = TrialResult(
        changed.spec,
        mismatched_workload,
        changed.events,
        changed.spark_conf,
        changed.application_id,
    )
    with pytest.raises(ValueError, match="do not reconcile"):
        summarize_trials(trials, logical_cores=4)


@pytest.mark.unit
def test_event_log_parser_captures_shuffle_io_and_spill(tmp_path) -> None:
    events = [
        {"Event": "SparkListenerApplicationStart"},
        {"Event": "SparkListenerStageCompleted"},
        {"Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart"},
        {
            "Event": "SparkListenerTaskEnd",
            "Task End Reason": {"Reason": "Success"},
            "Task Metrics": {
                "Executor Run Time": 101,
                "Executor CPU Time": 202,
                "Executor Deserialize Time": 3,
                "JVM GC Time": 4,
                "Memory Bytes Spilled": 5,
                "Disk Bytes Spilled": 6,
                "Input Metrics": {"Bytes Read": 7},
                "Output Metrics": {"Bytes Written": 8},
                "Shuffle Read Metrics": {
                    "Remote Bytes Read": 9,
                    "Local Bytes Read": 10,
                    "Fetch Wait Time": 11,
                },
                "Shuffle Write Metrics": {"Shuffle Bytes Written": 12},
            },
        },
        {"Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd"},
        {"Event": "SparkListenerApplicationEnd"},
    ]
    event_file = tmp_path / "events_1"
    event_file.write_text(
        "".join(json.dumps(event) + "\n" for event in events), encoding="utf-8"
    )
    metrics = parse_spark_event_metrics(tmp_path)
    assert metrics.applications_started == metrics.applications_ended == 1
    assert metrics.stages_completed == 1
    assert metrics.task_attempts == 1
    assert metrics.shuffle_read_bytes == 19
    assert metrics.shuffle_write_bytes == 12
    assert metrics.memory_bytes_spilled == 5
    assert metrics.disk_bytes_spilled == 6
    assert metrics.input_bytes_read == 7
    assert metrics.output_bytes_written == 8
    assert len(metrics.event_log_sha256) == 64
