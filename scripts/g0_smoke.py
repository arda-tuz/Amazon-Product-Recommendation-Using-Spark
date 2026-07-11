#!/usr/bin/env python3
"""Execute the binding G0 environment smoke tests and emit JSON evidence."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import math
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psutil
from graphframes import GraphFrame
from pyspark.sql import SparkSession


EXPECTED_PACKAGES = (
    "pyspark",
    "py4j",
    "graphframes-py",
    "pandas",
    "numpy",
    "PyYAML",
    "networkx",
    "pyarrow",
    "streamlit",
    "duckdb",
    "plotly",
    "pytest",
    "pytest-cov",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def package_versions() -> dict[str, str]:
    result: dict[str, str] = {}
    for package in EXPECTED_PACKAGES:
        try:
            result[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            result[package] = "missing"
    return result


def java_version() -> str:
    completed = subprocess.run(
        ["java", "-version"], check=True, capture_output=True, text=True
    )
    return (completed.stderr or completed.stdout).splitlines()[0]


def git_identity(root: Path) -> dict[str, Any]:
    try:
        commit = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        dirty = bool(
            subprocess.run(
                ["git", "status", "--porcelain"],
                cwd=root,
                check=True,
                capture_output=True,
                text=True,
            ).stdout
        )
        return {"commit": commit, "dirty": dirty}
    except (OSError, subprocess.CalledProcessError):
        return {"commit": "unavailable", "dirty": None}


def hardware(root: Path) -> dict[str, Any]:
    memory = psutil.virtual_memory()
    swap = psutil.swap_memory()
    disk = shutil.disk_usage(root)
    return {
        "platform": platform.platform(),
        "cpu_model": platform.processor(),
        "physical_cores": psutil.cpu_count(logical=False),
        "logical_cores": psutil.cpu_count(logical=True),
        "memory_total_bytes": memory.total,
        "memory_available_bytes": memory.available,
        "swap_total_bytes": swap.total,
        "swap_free_bytes": swap.free,
        "disk_total_bytes": disk.total,
        "disk_free_bytes": disk.free,
    }


def run_smoke(root: Path, jars: list[Path]) -> dict[str, Any]:
    jar_csv = ",".join(str(path.resolve()) for path in jars)
    spark = (
        SparkSession.builder.appName("amazon-recommender-g0")
        .config("spark.jars", jar_csv)
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    tests: dict[str, Any] = {}
    try:
        with tempfile.TemporaryDirectory(prefix="amazon-g0-") as temp_dir:
            temp_path = Path(temp_dir)
            checkpoint_path = temp_path / "checkpoints"
            spark.sparkContext.setCheckpointDir(str(checkpoint_path))

            expected = spark.createDataFrame(
                [(1, "alpha"), (2, "İstanbul")], "id int, label string"
            )
            parquet_path = temp_path / "parquet"
            expected.write.mode("error").option("compression", "snappy").parquet(
                str(parquet_path)
            )
            actual = spark.read.parquet(str(parquet_path))
            assert actual.schema == expected.schema
            assert actual.orderBy("id").collect() == expected.orderBy("id").collect()
            codec_values = {
                row["codec"]
                for row in spark.read.format("binaryFile")
                .load(str(parquet_path / "*.parquet"))
                .selectExpr(
                    "regexp_extract(path, '\\.([^.]+)\\.parquet$', 1) AS codec"
                )
                .collect()
            }
            assert "snappy" in codec_values
            tests["parquet_round_trip"] = {
                "status": "passed",
                "rows": actual.count(),
                "schema": actual.schema.simpleString(),
                "codecs": sorted(codec_values),
            }

            vertices = spark.createDataFrame(
                [("a",), ("b",), ("c",), ("d",), ("e",)], ["id"]
            )
            edges = spark.createDataFrame(
                [("a", "b"), ("b", "c"), ("c", "a"), ("d", "e")],
                ["src", "dst"],
            )
            graph = GraphFrame(vertices, edges)
            ranks = graph.pageRank(resetProbability=0.15, maxIter=2).vertices.select(
                "id", "pagerank"
            )
            rank_rows = ranks.collect()
            assert len(rank_rows) == 5
            assert all(
                math.isfinite(row["pagerank"]) and row["pagerank"] > 0
                for row in rank_rows
            )
            tests["graphframes_pagerank"] = {
                "status": "passed",
                "vertices": len(rank_rows),
                "reset_probability": 0.15,
                "max_iter": 2,
            }

            components = graph.connectedComponents(
                algorithm="two_phase",
                checkpointInterval=1,
                broadcastThreshold=-1,
                use_local_checkpoints=False,
            )
            component_rows = components.select("id", "component").collect()
            by_id = {row["id"]: row["component"] for row in component_rows}
            assert by_id["a"] == by_id["b"] == by_id["c"]
            assert by_id["d"] == by_id["e"]
            assert by_id["a"] != by_id["d"]
            checkpoint_files = [path for path in checkpoint_path.rglob("*") if path.is_file()]
            assert checkpoint_files
            components.unpersist()
            tests["graphframes_wcc"] = {
                "status": "passed",
                "components": len(set(by_id.values())),
                "algorithm": "two_phase",
            }
            tests["durable_checkpoint"] = {
                "status": "passed",
                "file_count": len(checkpoint_files),
            }

        return {
            "spark_version": spark.version,
            "scala_version": spark.sparkContext._jvm.scala.util.Properties.versionNumberString(),
            "java_version": spark.sparkContext._jvm.java.lang.System.getProperty(
                "java.version"
            ),
            "master": spark.sparkContext.master,
            "spark_conf": {
                key: spark.sparkContext.getConf().get(key, "")
                for key in (
                    "spark.driver.memory",
                    "spark.driver.maxResultSize",
                    "spark.sql.shuffle.partitions",
                    "spark.sql.adaptive.enabled",
                    "spark.sql.files.maxPartitionBytes",
                    "spark.sql.parquet.compression.codec",
                    "spark.sql.session.timeZone",
                )
            },
            "tests": tests,
        }
    finally:
        spark.stop()


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--jar-dir", type=Path, default=Path(".cache/ivy/jars"))
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = args.project_root.resolve()
    started = utc_now()
    started_clock = time.perf_counter()
    jars = sorted(args.jar_dir.resolve().glob("*.jar"))
    payload: dict[str, Any] = {
        "schema_version": 1,
        "gate": "G0",
        "started_at": started,
        "status": "running",
        "project_root": str(root),
        "python": {"version": platform.python_version(), "executable": sys.executable},
        "java_home": os.environ.get("JAVA_HOME"),
        "java_command_version": None,
        "packages": package_versions(),
        "jars": [
            {"path": str(path), "size_bytes": path.stat().st_size, "sha256": sha256_file(path)}
            for path in jars
        ],
        "hardware": hardware(root),
        "git": git_identity(root),
    }
    try:
        if not jars:
            raise RuntimeError(f"No GraphFrames runtime JARs found under {args.jar_dir}")
        payload["java_command_version"] = java_version()
        missing = [name for name, version in payload["packages"].items() if version == "missing"]
        if missing:
            raise RuntimeError(f"Missing required packages: {', '.join(missing)}")
        payload["runtime"] = run_smoke(root, jars)
        payload["status"] = "passed"
        return_code = 0
    except Exception as error:  # evidence must survive every G0 failure
        payload["status"] = "failed"
        payload["error"] = {
            "type": type(error).__name__,
            "message": str(error),
            "traceback": traceback.format_exc(),
        }
        return_code = 1
    finally:
        payload["finished_at"] = utc_now()
        payload["duration_seconds"] = round(time.perf_counter() - started_clock, 6)
        atomic_json(args.output.resolve(), payload)
        print(json.dumps(payload, indent=2, ensure_ascii=False))
    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
