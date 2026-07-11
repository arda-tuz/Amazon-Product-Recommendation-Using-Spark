"""Exact G11 trial scheduling, Spark setup, event evidence, and medians."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import statistics
from typing import Any, Iterable, Sequence

from pyspark import SparkContext
from pyspark.sql import SparkSession

from amazon_recommender.performance.workload import (
    SHUFFLE_PARTITIONS,
    WorkloadMeasurement,
    execute_fixed_workload,
)


WARMUP_RUNS = 1
MEASURED_RUNS = 3
MAX_COMPARISON_CORES = 4


@dataclass(frozen=True)
class PerformanceCondition:
    name: str
    master: str
    worker_threads: int


@dataclass(frozen=True)
class TrialSpec:
    condition: PerformanceCondition
    ordinal: int
    is_warmup: bool


@dataclass(frozen=True)
class SparkEventMetrics:
    event_files: tuple[str, ...]
    event_log_sha256: str
    applications_started: int
    applications_ended: int
    stages_completed: int
    task_attempts: int
    failed_task_attempts: int
    executor_run_time_ms: int
    executor_cpu_time_ns: int
    executor_deserialize_time_ms: int
    jvm_gc_time_ms: int
    input_bytes_read: int
    output_bytes_written: int
    shuffle_read_bytes: int
    shuffle_write_bytes: int
    shuffle_fetch_wait_time_ms: int
    memory_bytes_spilled: int
    disk_bytes_spilled: int
    sql_executions_started: int
    sql_executions_ended: int


@dataclass(frozen=True)
class TrialResult:
    spec: TrialSpec
    workload: WorkloadMeasurement
    events: SparkEventMetrics
    spark_conf: dict[str, str]
    application_id: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def logical_core_count() -> int:
    """Return the host logical-core count used to resolve the binding master."""

    return max(1, int(os.cpu_count() or 1))


def performance_conditions(logical_cores: int | None = None) -> tuple[PerformanceCondition, ...]:
    cores = logical_core_count() if logical_cores is None else int(logical_cores)
    if cores < 1:
        raise ValueError("logical_cores must be at least one")
    parallel_threads = min(MAX_COMPARISON_CORES, cores)
    return (
        PerformanceCondition("single_core", "local[1]", 1),
        PerformanceCondition(
            "bounded_multi_core",
            f"local[{parallel_threads}]",
            parallel_threads,
        ),
    )


def trial_schedule(logical_cores: int | None = None) -> tuple[TrialSpec, ...]:
    """Create exactly one warmup followed by three measured runs per condition."""

    schedule: list[TrialSpec] = []
    for condition in performance_conditions(logical_cores):
        schedule.append(TrialSpec(condition, ordinal=0, is_warmup=True))
        schedule.extend(
            TrialSpec(condition, ordinal=index, is_warmup=False)
            for index in range(1, MEASURED_RUNS + 1)
        )
    return tuple(schedule)


def create_performance_session(
    spec: TrialSpec, event_log_directory: Path, *, app_name: str
) -> SparkSession:
    """Create a fresh exact-config Spark application for one isolated trial."""

    if SparkContext._active_spark_context is not None:
        raise RuntimeError("performance trials require a fresh SparkContext per trial")
    event_log_directory.mkdir(parents=True, exist_ok=False)
    return (
        SparkSession.builder.master(spec.condition.master)
        .appName(app_name)
        # The main pipeline launcher raises spark.task.cpus to constrain
        # memory-heavy model stages. G11 must override that operational setting:
        # local[1] cannot schedule a four-CPU task, and task width must remain
        # fixed while only local worker parallelism changes.
        .config("spark.task.cpus", "1")
        .config("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.compress", "false")
        .config("spark.eventLog.logStageExecutorMetrics", "true")
        .config("spark.eventLog.dir", event_log_directory.resolve().as_uri())
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )


def _event_files(root: Path) -> list[Path]:
    return sorted(
        path
        for path in root.rglob("*")
        if path.is_file()
        and not path.name.startswith(".")
        and not path.name.endswith(".crc")
        and not path.name.startswith("appstatus_")
    )


def _event_lines(path: Path) -> Iterable[str]:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as handle:
        yield from handle


def _integer(mapping: dict[str, Any], key: str) -> int:
    value = mapping.get(key, 0)
    return int(value or 0)


def parse_spark_event_metrics(event_log_directory: Path) -> SparkEventMetrics:
    """Aggregate task/stage evidence from an uncompressed Spark JSON event log."""

    files = _event_files(event_log_directory)
    if not files:
        raise FileNotFoundError(f"no Spark event files found in {event_log_directory}")
    digest = hashlib.sha256()
    counters = {
        "applications_started": 0,
        "applications_ended": 0,
        "stages_completed": 0,
        "task_attempts": 0,
        "failed_task_attempts": 0,
        "executor_run_time_ms": 0,
        "executor_cpu_time_ns": 0,
        "executor_deserialize_time_ms": 0,
        "jvm_gc_time_ms": 0,
        "input_bytes_read": 0,
        "output_bytes_written": 0,
        "shuffle_read_bytes": 0,
        "shuffle_write_bytes": 0,
        "shuffle_fetch_wait_time_ms": 0,
        "memory_bytes_spilled": 0,
        "disk_bytes_spilled": 0,
        "sql_executions_started": 0,
        "sql_executions_ended": 0,
    }
    for path in files:
        digest.update(path.relative_to(event_log_directory).as_posix().encode("utf-8"))
        for line in _event_lines(path):
            encoded = line.encode("utf-8")
            digest.update(encoded)
            stripped = line.strip()
            if not stripped:
                continue
            try:
                event = json.loads(stripped)
            except json.JSONDecodeError:
                # V2 event directories can contain non-event metadata.  The event
                # file names and combined digest still make that evidence auditable.
                continue
            event_type = str(event.get("Event", ""))
            if event_type == "SparkListenerApplicationStart":
                counters["applications_started"] += 1
            elif event_type == "SparkListenerApplicationEnd":
                counters["applications_ended"] += 1
            elif event_type == "SparkListenerStageCompleted":
                counters["stages_completed"] += 1
            elif event_type.endswith("SparkListenerSQLExecutionStart"):
                counters["sql_executions_started"] += 1
            elif event_type.endswith("SparkListenerSQLExecutionEnd"):
                counters["sql_executions_ended"] += 1
            elif event_type == "SparkListenerTaskEnd":
                counters["task_attempts"] += 1
                reason = event.get("Task End Reason", {})
                reason_name = (
                    reason.get("Reason", "") if isinstance(reason, dict) else str(reason)
                )
                if reason_name and reason_name != "Success":
                    counters["failed_task_attempts"] += 1
                metrics = event.get("Task Metrics", {}) or {}
                counters["executor_run_time_ms"] += _integer(metrics, "Executor Run Time")
                counters["executor_cpu_time_ns"] += _integer(metrics, "Executor CPU Time")
                counters["executor_deserialize_time_ms"] += _integer(
                    metrics, "Executor Deserialize Time"
                )
                counters["jvm_gc_time_ms"] += _integer(metrics, "JVM GC Time")
                counters["memory_bytes_spilled"] += _integer(
                    metrics, "Memory Bytes Spilled"
                )
                counters["disk_bytes_spilled"] += _integer(metrics, "Disk Bytes Spilled")
                counters["input_bytes_read"] += _integer(
                    metrics.get("Input Metrics", {}) or {}, "Bytes Read"
                )
                counters["output_bytes_written"] += _integer(
                    metrics.get("Output Metrics", {}) or {}, "Bytes Written"
                )
                shuffle_read = metrics.get("Shuffle Read Metrics", {}) or {}
                counters["shuffle_read_bytes"] += _integer(
                    shuffle_read, "Remote Bytes Read"
                ) + _integer(shuffle_read, "Local Bytes Read")
                counters["shuffle_fetch_wait_time_ms"] += _integer(
                    shuffle_read, "Fetch Wait Time"
                )
                counters["shuffle_write_bytes"] += _integer(
                    metrics.get("Shuffle Write Metrics", {}) or {},
                    "Shuffle Bytes Written",
                )
    return SparkEventMetrics(
        event_files=tuple(str(path.resolve()) for path in files),
        event_log_sha256=digest.hexdigest(),
        **counters,
    )


def run_performance_trial(
    spec: TrialSpec,
    *,
    reviews_deduplicated_path: Path,
    products_path: Path,
    trial_root: Path,
) -> TrialResult:
    """Run one isolated trial; callers iterate the immutable schedule externally."""

    trial_root.mkdir(parents=True, exist_ok=False)
    events_path = trial_root / "spark-events"
    output_path = trial_root / "temporary-output"
    app_name = f"amazon-g11-{spec.condition.name}-{spec.ordinal}"
    spark = create_performance_session(spec, events_path, app_name=app_name)
    application_id = spark.sparkContext.applicationId
    conf = {
        "spark.master": spark.sparkContext.master,
        "spark.task.cpus": spark.sparkContext.getConf().get("spark.task.cpus"),
        "spark.sql.shuffle.partitions": spark.conf.get(
            "spark.sql.shuffle.partitions"
        ),
        "spark.sql.adaptive.enabled": spark.conf.get("spark.sql.adaptive.enabled"),
        "spark.eventLog.enabled": spark.sparkContext.getConf().get(
            "spark.eventLog.enabled"
        ),
        "spark.eventLog.compress": spark.sparkContext.getConf().get(
            "spark.eventLog.compress"
        ),
    }
    try:
        workload = execute_fixed_workload(
            spark,
            reviews_deduplicated_path,
            products_path,
            output_path,
        )
    finally:
        spark.stop()
    events = parse_spark_event_metrics(events_path)
    shutil.rmtree(output_path, ignore_errors=True)
    return TrialResult(spec, workload, events, conf, application_id)


def summarize_trials(
    trials: Sequence[TrialResult], *, logical_cores: int | None = None
) -> dict[str, Any]:
    """Validate the 1+3 budget and return medians without discarding raw trials."""

    expected_conditions = performance_conditions(logical_cores)
    summaries: dict[str, dict[str, Any]] = {}
    all_output_rows = {trial.workload.output_rows for trial in trials}
    all_schema_hashes = {trial.workload.output_schema_sha256 for trial in trials}
    if len(all_output_rows) != 1 or len(all_schema_hashes) != 1:
        raise ValueError("performance trial outputs do not reconcile")
    for condition in expected_conditions:
        condition_trials = [
            trial for trial in trials if trial.spec.condition.name == condition.name
        ]
        warmups = [trial for trial in condition_trials if trial.spec.is_warmup]
        measured = [trial for trial in condition_trials if not trial.spec.is_warmup]
        if len(warmups) != WARMUP_RUNS or len(measured) != MEASURED_RUNS:
            raise ValueError(
                f"{condition.name} requires exactly 1 warmup and 3 measured runs"
            )
        if any(trial.spec.condition != condition for trial in condition_trials):
            raise ValueError(f"{condition.name} trial master/thread contract changed")
        if [trial.spec.ordinal for trial in warmups] != [0] or sorted(
            trial.spec.ordinal for trial in measured
        ) != [1, 2, 3]:
            raise ValueError(f"{condition.name} trial ordinals must be warmup 0 and measured 1..3")
        expected_conf = {
            "spark.master": condition.master,
            "spark.task.cpus": "1",
            "spark.sql.shuffle.partitions": str(SHUFFLE_PARTITIONS),
            "spark.sql.adaptive.enabled": "true",
            "spark.eventLog.enabled": "true",
            "spark.eventLog.compress": "false",
        }
        for trial in condition_trials:
            if trial.workload.cache_enabled:
                raise ValueError("performance workload cache must remain disabled")
            if any(trial.spark_conf.get(key) != value for key, value in expected_conf.items()):
                raise ValueError(f"{condition.name} Spark configuration contract changed")
            if (
                trial.events.applications_started != 1
                or trial.events.applications_ended != 1
                or not trial.events.event_files
            ):
                raise ValueError(f"{condition.name} Spark event evidence is incomplete")
        measured_seconds = [trial.workload.wall_seconds for trial in measured]
        if any(seconds <= 0.0 for seconds in measured_seconds):
            raise ValueError("measured wall times must be positive")
        summaries[condition.name] = {
            "master": condition.master,
            "worker_threads": condition.worker_threads,
            "warmup_wall_seconds": warmups[0].workload.wall_seconds,
            "measured_wall_seconds": measured_seconds,
            "median_wall_seconds": statistics.median(measured_seconds),
            "trials": [trial.to_dict() for trial in condition_trials],
        }
    single = summaries["single_core"]["median_wall_seconds"]
    parallel = summaries["bounded_multi_core"]["median_wall_seconds"]
    return {
        "protocol": {
            "warmups_per_condition": WARMUP_RUNS,
            "measured_runs_per_condition": MEASURED_RUNS,
            "shuffle_partitions": SHUFFLE_PARTITIONS,
            "aqe_enabled": True,
            "cache_enabled": False,
            "comparison": "local multi-core parallelism; not horizontal scaling",
        },
        "conditions": summaries,
        "local_parallel_speedup": single / parallel,
        "output_rows": next(iter(all_output_rows)),
        "output_schema_sha256": next(iter(all_schema_hashes)),
    }
