"""Atomic local Parquet publication and inspectable table evidence."""

from __future__ import annotations

import hashlib
import json
import math
import os
import shutil
import statistics
import uuid
from pathlib import Path
from typing import Any, Literal

from pyspark.sql import DataFrame, SparkSession


TARGET_BYTES = 134_217_728


def recommended_files(size_bytes: int, kind: Literal["fact", "dimension"]) -> int:
    estimate = max(1, (size_bytes + TARGET_BYTES - 1) // TARGET_BYTES)
    if kind == "fact":
        return max(8, min(64, estimate))
    return max(1, min(8, estimate))


def directory_size(path: Path) -> tuple[int, int]:
    files = [item for item in path.rglob("*") if item.is_file()]
    return len(files), sum(item.stat().st_size for item in files)


def table_fingerprint(path: Path) -> str:
    digest = hashlib.sha256()
    for file in sorted(item for item in path.rglob("*") if item.is_file()):
        relative = file.relative_to(path).as_posix().encode("utf-8")
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        digest.update(file.stat().st_size.to_bytes(8, "big"))
        with file.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    return digest.hexdigest()


def cleanup_incomplete_publications(root: Path) -> list[str]:
    """Remove only atomic-publication scratch directories below ``root``.

    A completed table is a normal directory containing ``_SUCCESS``.  Spark
    writes are first directed to hidden ``.<table>.<uuid>.tmp`` directories,
    so an interrupted process can safely discard those without touching a
    completed ingestion envelope or any already-published table.
    """

    removed: list[str] = []
    if not root.exists():
        return removed
    for candidate in sorted(root.rglob(".*.tmp")):
        if not candidate.is_dir():
            continue
        removed.append(str(candidate))
        shutil.rmtree(candidate)
    return removed


def published_parquet_evidence(
    spark: SparkSession, path: Path
) -> dict[str, Any]:
    """Reconstruct evidence for an atomically published Parquet table."""

    if not path.is_dir() or not (path / "_SUCCESS").is_file():
        raise FileNotFoundError(f"Parquet publication is incomplete: {path}")
    frame = spark.read.parquet(str(path))
    row_count = frame.count()
    file_count, size_bytes = directory_size(path)
    parquet_sizes = [item.stat().st_size for item in path.glob("*.parquet")]
    return {
        "path": str(path),
        "rows": row_count,
        "schema_json": json.loads(frame.schema.json()),
        "files": file_count,
        "parquet_files": len(list(path.glob("*.parquet"))),
        "median_parquet_file_bytes": (
            int(statistics.median(parquet_sizes)) if parquet_sizes else 0
        ),
        "size_bytes": size_bytes,
        "sha256": table_fingerprint(path),
    }


def publish_parquet(
    frame: DataFrame,
    final_path: Path,
    *,
    partitions: int,
    sort_columns: tuple[str, ...] = (),
) -> dict[str, Any]:
    if final_path.exists():
        raise FileExistsError(f"Refusing to overwrite published table: {final_path}")
    final_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = final_path.parent / f".{final_path.name}.{uuid.uuid4().hex}.tmp"
    writer = frame
    if sort_columns:
        writer = writer.sortWithinPartitions(*sort_columns)
    writer = writer.repartition(partitions) if partitions > 1 else writer.coalesce(1)
    try:
        writer.write.mode("error").option("compression", "snappy").parquet(str(temporary))
        # Count the materialized output, not the potentially expensive upstream
        # lineage a second time. This also proves every published Parquet file is
        # readable before the atomic rename.
        row_count = frame.sparkSession.read.parquet(str(temporary)).count()
        os.replace(temporary, final_path)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    file_count, size_bytes = directory_size(final_path)
    data_files = len(list(final_path.glob("*.parquet")))
    parquet_sizes = [item.stat().st_size for item in final_path.glob("*.parquet")]
    return {
        "path": str(final_path),
        "rows": row_count,
        "schema_json": json.loads(frame.schema.json()),
        "files": file_count,
        "parquet_files": data_files,
        "median_parquet_file_bytes": (
            int(statistics.median(parquet_sizes)) if parquet_sizes else 0
        ),
        "size_bytes": size_bytes,
        "sha256": table_fingerprint(final_path),
    }


def publish_or_reuse_parquet(
    frame: DataFrame,
    final_path: Path,
    *,
    partitions: int,
    sort_columns: tuple[str, ...] = (),
) -> tuple[dict[str, Any], bool]:
    """Publish a table or reuse a completed atomic publication.

    Returns ``(evidence, reused)``.  A non-empty path without ``_SUCCESS`` is
    never accepted as evidence; because this helper is only used inside the
    run-scoped temporary workspace, such a path is removed and rebuilt.
    """

    if (final_path / "_SUCCESS").is_file():
        return published_parquet_evidence(frame.sparkSession, final_path), True
    if final_path.exists():
        shutil.rmtree(final_path)
    return (
        publish_parquet(
            frame,
            final_path,
            partitions=partitions,
            sort_columns=sort_columns,
        ),
        False,
    )


def publish_sized_parquet(
    frame: DataFrame,
    final_path: Path,
    *,
    kind: Literal["fact", "dimension"],
    sort_columns: tuple[str, ...] = (),
    initial_partitions: int = 64,
) -> dict[str, Any]:
    """Apply the binding 64-part measure-then-size publication protocol."""

    if final_path.exists():
        raise FileExistsError(f"Refusing to overwrite published table: {final_path}")
    final_path.parent.mkdir(parents=True, exist_ok=True)
    token = uuid.uuid4().hex
    measurement = final_path.parent / f".{final_path.name}.{token}.measure.tmp"
    durable = final_path.parent / f".{final_path.name}.{token}.tmp"
    measured_size = 0
    target_partitions = 0
    row_count = 0
    try:
        frame.repartition(initial_partitions).write.mode("error").option(
            "compression", "snappy"
        ).parquet(str(measurement))
        _, measured_size = directory_size(measurement)
        target_partitions = recommended_files(measured_size, kind)
        materialized = frame.sparkSession.read.parquet(str(measurement))
        measured_input_partitions = materialized.rdd.getNumPartitions()
        materialized = (
            materialized.coalesce(target_partitions)
            if target_partitions < measured_input_partitions
            else materialized.repartition(target_partitions)
        )
        if sort_columns:
            materialized = materialized.sortWithinPartitions(*sort_columns)
        materialized.write.mode("error").option("compression", "snappy").parquet(
            str(durable)
        )
        row_count = frame.sparkSession.read.parquet(str(durable)).count()
        os.replace(durable, final_path)
    except Exception:
        shutil.rmtree(durable, ignore_errors=True)
        raise
    finally:
        shutil.rmtree(measurement, ignore_errors=True)
    evidence = published_parquet_evidence(frame.sparkSession, final_path)
    evidence.update(
        {
            "rows": row_count,
            "initial_partitions": initial_partitions,
            "measurement_size_bytes": measured_size,
            "measurement_input_partitions": measured_input_partitions,
            "target_kind": kind,
            "target_partitions": target_partitions,
            "target_formula": (
                f"max({8 if kind == 'fact' else 1}, "
                f"min({64 if kind == 'fact' else 8}, "
                "ceil(S/134217728)))"
            ),
            "measurement_ceiling": math.ceil(measured_size / TARGET_BYTES),
        }
    )
    return evidence


def publish_or_reuse_sized_parquet(
    frame: DataFrame,
    final_path: Path,
    *,
    kind: Literal["fact", "dimension"],
    sort_columns: tuple[str, ...] = (),
    initial_partitions: int = 64,
) -> tuple[dict[str, Any], bool]:
    """Resume-aware wrapper for the binding sized publication protocol."""

    if (final_path / "_SUCCESS").is_file():
        evidence = published_parquet_evidence(frame.sparkSession, final_path)
        evidence.update(
            {
                "initial_partitions": initial_partitions,
                "measurement_size_bytes": int(evidence["size_bytes"]),
                "target_kind": kind,
                "target_partitions": int(evidence["parquet_files"]),
                "target_formula": (
                    f"max({8 if kind == 'fact' else 1}, "
                    f"min({64 if kind == 'fact' else 8}, "
                    "ceil(S/134217728)))"
                ),
                "measurement_reconstructed_on_resume": True,
            }
        )
        return evidence, True
    if final_path.exists():
        shutil.rmtree(final_path)
    return (
        publish_sized_parquet(
            frame,
            final_path,
            kind=kind,
            sort_columns=sort_columns,
            initial_partitions=initial_partitions,
        ),
        False,
    )
