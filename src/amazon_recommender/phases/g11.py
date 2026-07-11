"""G11's single controlled local-parallelism performance experiment.

The phase deliberately does not share a Spark session with another gate.  It runs
the immutable Silver workload in eight fresh Spark applications: one warm-up and
three measured trials for each of ``local[1]`` and
``local[min(4, logical_cores)]``.  Every completed trial is atomically published so
an interrupted long run can resume without repeating valid evidence.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import xml.etree.ElementTree as ET
import uuid
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any, Mapping

from amazon_recommender.core.manifest import atomic_write_json
from amazon_recommender.gate_handlers import register
from amazon_recommender.performance.experiment import (
    MEASURED_RUNS,
    WARMUP_RUNS,
    PerformanceCondition,
    SparkEventMetrics,
    TrialResult,
    TrialSpec,
    logical_core_count,
    parse_spark_event_metrics,
    performance_conditions,
    run_performance_trial,
    summarize_trials,
    trial_schedule,
)
from amazon_recommender.performance.workload import (
    SHUFFLE_PARTITIONS,
    PartitionEvidence,
    PlanEvidence,
    WorkloadMeasurement,
)
from amazon_recommender.pipelines.storage import directory_size, table_fingerprint


G11_CONTRACT_VERSION = 1
EXPECTED_CONDITION_COUNT = 2
EXPECTED_TRIAL_COUNT = EXPECTED_CONDITION_COUNT * (WARMUP_RUNS + MEASURED_RUNS)
FULL_SILVER_INPUTS = ("reviews_deduplicated", "products")


def _junit(path: Path) -> dict[str, Any]:
    root = ET.parse(path).getroot()
    suites = [root] if root.tag == "testsuite" else list(root.findall("testsuite"))
    summary = {
        key: sum(int(suite.attrib.get(key, "0")) for suite in suites)
        for key in ("tests", "failures", "errors", "skipped")
    }
    if not summary["tests"] or summary["failures"] or summary["errors"]:
        raise RuntimeError(f"G11 JUnit evidence is not passing: {summary}")
    summary["path"] = str(path.resolve())
    return summary


def _implementation_signature(config_sha256: str) -> str:
    digest = hashlib.sha256()
    digest.update(f"g11-contract-{G11_CONTRACT_VERSION}".encode("ascii"))
    digest.update(config_sha256.encode("ascii"))
    performance_root = Path(__file__).parents[1] / "performance"
    for path in (
        Path(__file__),
        performance_root / "workload.py",
        performance_root / "experiment.py",
    ):
        digest.update(path.name.encode("utf-8"))
        digest.update(path.read_bytes())
    return digest.hexdigest()


def _validate_binding_config(config: Any) -> None:
    expected = {
        "masters": ["local[1]", "local[min(4,logical_cores)]"],
        "warmups": WARMUP_RUNS,
        "measured_runs": MEASURED_RUNS,
        "cache_enabled": False,
    }
    actual = {key: config.get("performance", key) for key in expected}
    if actual != expected:
        raise RuntimeError(
            f"G11 binding performance configuration changed: {actual!r}; "
            f"expected {expected!r}"
        )
    if SHUFFLE_PARTITIONS != 64:
        raise RuntimeError("G11 requires exactly 64 shuffle partitions")


def _trial_name(spec: TrialSpec) -> str:
    kind = "warmup" if spec.is_warmup else "measured"
    return f"{spec.condition.name}-{kind}-{spec.ordinal}"


def _trial_contract(
    spec: TrialSpec,
    *,
    implementation_sha256: str,
    config_sha256: str,
    source_sha256: str,
) -> dict[str, Any]:
    return {
        "gate": "G11",
        "contract_version": G11_CONTRACT_VERSION,
        "implementation_sha256": implementation_sha256,
        "config_sha256": config_sha256,
        "source_sha256": source_sha256,
        "trial": asdict(spec),
    }


def _prepare_workspace(
    working: Path,
    *,
    implementation_sha256: str,
    config_sha256: str,
    source_sha256: str,
) -> list[str]:
    """Keep only complete trials belonging to the exact same G11 contract."""

    marker = working / "_checkpoint_contract.json"
    expected = {
        "gate": "G11",
        "contract_version": G11_CONTRACT_VERSION,
        "implementation_sha256": implementation_sha256,
        "config_sha256": config_sha256,
        "source_sha256": source_sha256,
    }
    if working.exists():
        try:
            existing = json.loads(marker.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError):
            existing = {}
        if existing != expected:
            shutil.rmtree(working)
    working.mkdir(parents=True, exist_ok=True)
    removed: list[str] = []
    trials_root = working / "trials"
    if trials_root.exists():
        for candidate in sorted(trials_root.iterdir()):
            if (
                candidate.is_dir()
                and candidate.name.startswith(".")
                and candidate.name.endswith(".tmp")
            ):
                removed.append(str(candidate))
                shutil.rmtree(candidate)
    trials_root.mkdir(parents=True, exist_ok=True)
    atomic_write_json(marker, expected)
    return removed


def _deserialize_trial(payload: Mapping[str, Any]) -> TrialResult:
    spec_payload = payload["spec"]
    condition = PerformanceCondition(**spec_payload["condition"])
    spec = TrialSpec(
        condition=condition,
        ordinal=int(spec_payload["ordinal"]),
        is_warmup=bool(spec_payload["is_warmup"]),
    )
    workload_payload = payload["workload"]
    plan_payload = workload_payload["plan"]
    partition_payload = workload_payload["partitions"]
    plan = PlanEvidence(
        formatted_plan=str(plan_payload["formatted_plan"]),
        executed_plan=str(plan_payload["executed_plan"]),
        formatted_plan_sha256=str(plan_payload["formatted_plan_sha256"]),
        executed_plan_sha256=str(plan_payload["executed_plan_sha256"]),
        exchange_node_count=int(plan_payload["exchange_node_count"]),
        exchange_node_lines=tuple(
            str(value) for value in plan_payload["exchange_node_lines"]
        ),
        adaptive_plan_present=bool(plan_payload["adaptive_plan_present"]),
    )
    partitions = PartitionEvidence(
        **{key: int(value) for key, value in partition_payload.items()}
    )
    workload = WorkloadMeasurement(
        wall_seconds=float(workload_payload["wall_seconds"]),
        output_rows=int(workload_payload["output_rows"]),
        output_schema_json=str(workload_payload["output_schema_json"]),
        output_schema_sha256=str(workload_payload["output_schema_sha256"]),
        plan=plan,
        partitions=partitions,
        cache_enabled=bool(workload_payload.get("cache_enabled", False)),
    )
    events_payload = dict(payload["events"])
    events_payload["event_files"] = tuple(
        str(value) for value in events_payload["event_files"]
    )
    events = SparkEventMetrics(**events_payload)
    return TrialResult(
        spec=spec,
        workload=workload,
        events=events,
        spark_conf={str(key): str(value) for key, value in payload["spark_conf"].items()},
        application_id=str(payload["application_id"]),
    )


def _rebase_trial_paths(
    trial: TrialResult, temporary_root: Path, published_root: Path
) -> TrialResult:
    def rebase(path: str) -> str:
        candidate = Path(path)
        try:
            relative = candidate.relative_to(temporary_root)
        except ValueError:
            try:
                candidate.relative_to(published_root)
            except ValueError as error:
                raise RuntimeError(
                    f"G11 event file is outside its trial artifact: {candidate}"
                ) from error
            return str(candidate)
        return str(published_root / relative)

    events = replace(
        trial.events,
        event_files=tuple(rebase(path) for path in trial.events.event_files),
    )
    return replace(trial, events=events)


def _validate_trial_result(
    trial: TrialResult,
    expected_spec: TrialSpec,
    trial_root: Path,
) -> None:
    if trial.spec != expected_spec:
        raise RuntimeError(f"G11 trial spec changed for {_trial_name(expected_spec)}")
    if not trial.application_id:
        raise RuntimeError("G11 trial is missing its Spark application id")
    if trial.workload.wall_seconds <= 0.0 or trial.workload.output_rows <= 0:
        raise RuntimeError("G11 trial must materialize a non-empty timed output")
    if trial.workload.cache_enabled:
        raise RuntimeError("G11 trial cache must remain disabled")
    if (
        hashlib.sha256(trial.workload.output_schema_json.encode("utf-8")).hexdigest()
        != trial.workload.output_schema_sha256
    ):
        raise RuntimeError("G11 output schema digest does not reconcile")
    plan = trial.workload.plan
    if (
        hashlib.sha256(plan.formatted_plan.encode("utf-8")).hexdigest()
        != plan.formatted_plan_sha256
    ):
        raise RuntimeError("G11 formatted plan digest does not reconcile")
    if (
        hashlib.sha256(plan.executed_plan.encode("utf-8")).hexdigest()
        != plan.executed_plan_sha256
    ):
        raise RuntimeError("G11 executed plan digest does not reconcile")
    if plan.exchange_node_count != len(plan.exchange_node_lines):
        raise RuntimeError("G11 exchange-node evidence does not reconcile")
    if plan.exchange_node_count <= 0:
        raise RuntimeError("G11 fixed join/aggregation plan has no Exchange evidence")
    if not plan.adaptive_plan_present:
        raise RuntimeError("G11 trial is missing AdaptiveSparkPlan evidence")
    partitions = trial.workload.partitions
    positive_partition_evidence = (
        partitions.reviews_input_partitions,
        partitions.products_input_partitions,
        partitions.aggregate_output_partitions,
        partitions.reviews_parquet_files,
        partitions.products_parquet_files,
        partitions.output_parquet_files,
        partitions.output_parquet_bytes,
    )
    if any(value <= 0 for value in positive_partition_evidence):
        raise RuntimeError("G11 partition/file evidence must be positive")
    expected_conf = {
        "spark.master": expected_spec.condition.master,
        "spark.task.cpus": "1",
        "spark.sql.shuffle.partitions": "64",
        "spark.sql.adaptive.enabled": "true",
        "spark.eventLog.enabled": "true",
        "spark.eventLog.compress": "false",
    }
    if any(trial.spark_conf.get(key) != value for key, value in expected_conf.items()):
        raise RuntimeError("G11 Spark configuration evidence changed")
    parsed_events = parse_spark_event_metrics(trial_root / "spark-events")
    if parsed_events.event_log_sha256 != trial.events.event_log_sha256:
        raise RuntimeError("G11 Spark event-log digest does not reconcile")
    comparable = (
        "applications_started",
        "applications_ended",
        "stages_completed",
        "task_attempts",
        "failed_task_attempts",
        "executor_run_time_ms",
        "executor_cpu_time_ns",
        "executor_deserialize_time_ms",
        "jvm_gc_time_ms",
        "input_bytes_read",
        "output_bytes_written",
        "shuffle_read_bytes",
        "shuffle_write_bytes",
        "shuffle_fetch_wait_time_ms",
        "memory_bytes_spilled",
        "disk_bytes_spilled",
        "sql_executions_started",
        "sql_executions_ended",
    )
    mismatches = {
        field: (getattr(trial.events, field), getattr(parsed_events, field))
        for field in comparable
        if getattr(trial.events, field) != getattr(parsed_events, field)
    }
    if mismatches:
        raise RuntimeError(f"G11 event metrics do not reconcile: {mismatches}")


def _load_completed_trial(
    trial_root: Path,
    expected_spec: TrialSpec,
    expected_contract: Mapping[str, Any],
) -> TrialResult | None:
    try:
        contract = json.loads(
            (trial_root / "_trial_contract.json").read_text(encoding="utf-8")
        )
        if contract != expected_contract:
            return None
        payload = json.loads((trial_root / "result.json").read_text(encoding="utf-8"))
        trial = _deserialize_trial(payload)
        _validate_trial_result(trial, expected_spec, trial_root)
        return trial
    except (
        FileNotFoundError,
        json.JSONDecodeError,
        KeyError,
        TypeError,
        ValueError,
        RuntimeError,
    ):
        return None


def _publish_trial(
    spec: TrialSpec,
    *,
    reviews_deduplicated_path: Path,
    products_path: Path,
    trials_root: Path,
    contract: Mapping[str, Any],
) -> tuple[TrialResult, bool]:
    name = _trial_name(spec)
    published = trials_root / name
    reused = _load_completed_trial(published, spec, contract)
    if reused is not None:
        return reused, True
    if published.exists():
        shutil.rmtree(published)
    temporary = trials_root / f".{name}.{uuid.uuid4().hex}.tmp"
    try:
        raw = run_performance_trial(
            spec,
            reviews_deduplicated_path=reviews_deduplicated_path,
            products_path=products_path,
            trial_root=temporary,
        )
        trial = _rebase_trial_paths(raw, temporary, published)
        _validate_trial_result(raw, spec, temporary)
        atomic_write_json(temporary / "result.json", trial.to_dict())
        atomic_write_json(temporary / "_trial_contract.json", contract)
        os.replace(temporary, published)
        _validate_trial_result(trial, spec, published)
        return trial, False
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise


def _copy_junit(source: Path, destination: Path) -> None:
    temporary = destination.with_name(f".{destination.name}.tmp")
    shutil.copy2(source, temporary)
    os.replace(temporary, destination)


def _artifact_evidence(root: Path) -> dict[str, Any]:
    file_count, size_bytes = directory_size(root)
    return {
        "path": str(root.resolve()),
        "files": file_count,
        "size_bytes": size_bytes,
        "sha256": table_fingerprint(root),
    }


@register("G11")
def run_g11(config: Any, paths: Any, evidence_file: Path | None) -> dict[str, Any]:
    if evidence_file is None:
        raise RuntimeError("G11 requires passing JUnit XML evidence")
    evidence_file = evidence_file.resolve()
    junit = _junit(evidence_file)
    _validate_binding_config(config)

    silver = paths.data / "full" / "silver"
    inputs = {name: silver / name for name in FULL_SILVER_INPUTS}
    missing = [
        str(path)
        for path in inputs.values()
        if not path.is_dir() or not (path / "_SUCCESS").is_file()
    ]
    if missing:
        raise FileNotFoundError(
            f"G11 requires complete full Silver Parquet inputs: {missing}"
        )

    implementation_sha256 = _implementation_signature(config.sha256)
    final = paths.run / "performance"
    if final.exists():
        raise FileExistsError(
            f"G11 output exists without reusable manifest: {final}"
        )
    working = paths.temporary / "G11-publish"
    removed_scratch = _prepare_workspace(
        working,
        implementation_sha256=implementation_sha256,
        config_sha256=config.sha256,
        source_sha256=config.get("source", "sha256"),
    )

    logical_cores = logical_core_count()
    conditions = performance_conditions(logical_cores)
    schedule = trial_schedule(logical_cores)
    if (
        len(conditions) != EXPECTED_CONDITION_COUNT
        or len(schedule) != EXPECTED_TRIAL_COUNT
    ):
        raise RuntimeError(
            "G11 experiment budget must contain exactly 2 conditions and 8 trials"
        )

    trials: list[TrialResult] = []
    reused_trials: list[str] = []
    for index, spec in enumerate(schedule, start=1):
        name = _trial_name(spec)
        print(
            json.dumps(
                {
                    "gate": "G11",
                    "status": "running",
                    "trial": name,
                    "trial_index": index,
                    "trial_total": EXPECTED_TRIAL_COUNT,
                    "master": spec.condition.master,
                }
            ),
            flush=True,
        )
        contract = _trial_contract(
            spec,
            implementation_sha256=implementation_sha256,
            config_sha256=config.sha256,
            source_sha256=config.get("source", "sha256"),
        )
        trial, reused = _publish_trial(
            spec,
            reviews_deduplicated_path=inputs["reviews_deduplicated"],
            products_path=inputs["products"],
            trials_root=working / "trials",
            contract=contract,
        )
        trials.append(trial)
        if reused:
            reused_trials.append(name)
        print(
            json.dumps(
                {
                    "gate": "G11",
                    "status": "trial_passed",
                    "trial": name,
                    "reused": reused,
                    "wall_seconds": trial.workload.wall_seconds,
                }
            ),
            flush=True,
        )

    # Trial directories first move into the resume workspace and then the whole
    # workspace moves into its final run artifact. Rebase every stored absolute
    # event path for that second atomic rename before writing the summary.
    trials = [_rebase_trial_paths(trial, working, final) for trial in trials]
    for trial in trials:
        atomic_write_json(
            working / "trials" / _trial_name(trial.spec) / "result.json",
            trial.to_dict(),
        )

    summary = summarize_trials(trials, logical_cores=logical_cores)
    if len(trials) != EXPECTED_TRIAL_COUNT:
        raise RuntimeError("G11 did not complete the exact eight-trial budget")
    if any(trial.workload.cache_enabled for trial in trials):
        raise RuntimeError("G11 cache contract changed")
    if len({trial.workload.output_rows for trial in trials}) != 1:
        raise RuntimeError("G11 workload row counts do not reconcile")
    if len({trial.workload.output_schema_sha256 for trial in trials}) != 1:
        raise RuntimeError("G11 workload schemas do not reconcile")

    atomic_write_json(working / "summary.json", summary)
    atomic_write_json(
        working / "protocol.json",
        {
            "gate": "G11",
            "contract_version": G11_CONTRACT_VERSION,
            "implementation_sha256": implementation_sha256,
            "logical_cores": logical_cores,
            "conditions": [asdict(condition) for condition in conditions],
            "schedule": [asdict(spec) for spec in schedule],
            "full_silver_inputs": {
                name: str(path.resolve()) for name, path in inputs.items()
            },
            "workload": [
                "scan reviews_deduplicated Parquet",
                "inner join products on product_id",
                "aggregate by year(review_date), product_group",
                "write temporary Snappy Parquet and count rows",
            ],
            "interpretation": "local multi-core parallelism; not horizontal scaling",
        },
    )
    _copy_junit(evidence_file, working / "junit.xml")
    atomic_write_json(
        working / "_SUCCESS.json",
        {
            "gate": "G11",
            "implementation_sha256": implementation_sha256,
            "trial_count": len(trials),
            "output_rows": summary["output_rows"],
            "output_schema_sha256": summary["output_schema_sha256"],
        },
    )
    final.parent.mkdir(parents=True, exist_ok=True)
    os.replace(working, final)

    artifact = _artifact_evidence(final)
    trial_evidence = {
        _trial_name(trial.spec): {
            "application_id": trial.application_id,
            "is_warmup": trial.spec.is_warmup,
            "wall_seconds": trial.workload.wall_seconds,
            "formatted_plan_sha256": trial.workload.plan.formatted_plan_sha256,
            "executed_plan_sha256": trial.workload.plan.executed_plan_sha256,
            "exchange_node_count": trial.workload.plan.exchange_node_count,
            "partitions": asdict(trial.workload.partitions),
            "event_log_sha256": trial.events.event_log_sha256,
            "task_attempts": trial.events.task_attempts,
            "shuffle_read_bytes": trial.events.shuffle_read_bytes,
            "shuffle_write_bytes": trial.events.shuffle_write_bytes,
            "memory_bytes_spilled": trial.events.memory_bytes_spilled,
            "disk_bytes_spilled": trial.events.disk_bytes_spilled,
            "result_path": str(
                (final / "trials" / _trial_name(trial.spec) / "result.json").resolve()
            ),
        }
        for trial in trials
    }
    return {
        "junit": {**junit, "artifact_path": str((final / "junit.xml").resolve())},
        "implementation_sha256": implementation_sha256,
        "artifact": artifact,
        "scratch_directories_removed": removed_scratch,
        "trials_reused": reused_trials,
        "logical_cores": logical_cores,
        "condition_count": len(conditions),
        "trial_count": len(trials),
        "warmups_per_condition": WARMUP_RUNS,
        "measured_runs_per_condition": MEASURED_RUNS,
        "cache_enabled": False,
        "shuffle_partitions": SHUFFLE_PARTITIONS,
        "aqe_enabled": True,
        "comparison_scope": "local multi-core parallelism; not horizontal scaling",
        "summary": summary,
        "trial_evidence": trial_evidence,
        "full_silver_inputs": {
            name: str(path.resolve()) for name, path in inputs.items()
        },
    }
