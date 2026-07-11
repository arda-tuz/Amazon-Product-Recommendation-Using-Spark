#!/usr/bin/env python3
"""Run the complete test suite in sequential, JVM-isolated pytest shards.

Every shard is a fresh Python process.  Spark's Java gateway therefore cannot retain
cache, code generation, or native memory across unrelated test families.  The runner
also starts each shard in its own process group and terminates only that group after
pytest exits, preventing an orphaned test JVM from overlapping the next shard.
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import shutil
import signal
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXPECTED_PYTHON = Path(
    "/home/arda-tuz/.pyenv/versions/bil401_env_1/bin/python"
)
EXPECTED_JAVA_HOME = Path("/usr/lib/jvm/java-21-openjdk-amd64")
MERGER = PROJECT_ROOT / "scripts" / "merge_junit.py"
MODEL_UNIT_FILES = {
    "test_als_model.py",
    "test_hybrid_model.py",
    "test_popularity_model.py",
}
PYTEST_BASE_ARGS = ("-q", "-ra", "--strict-markers", "--strict-config")


@dataclass(frozen=True)
class Shard:
    name: str
    pytest_args: tuple[str, ...]
    uses_spark: bool


def _relative(path: Path, project_root: Path = PROJECT_ROOT) -> str:
    return path.relative_to(project_root).as_posix()


def _test_uses_spark_fixture(path: Path) -> bool:
    """Detect direct use of the session-scoped ``spark`` pytest fixture."""

    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    functions = (ast.FunctionDef, ast.AsyncFunctionDef)
    for node in ast.walk(tree):
        if not isinstance(node, functions) or not node.name.startswith("test_"):
            continue
        arguments = [*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs]
        if any(argument.arg == "spark" for argument in arguments):
            return True
    return False


def _contains_marker(path: Path, marker: str) -> bool:
    return f"pytest.mark.{marker}" in path.read_text(encoding="utf-8")


def build_shards(project_root: Path = PROJECT_ROOT) -> tuple[Shard, ...]:
    """Partition every discovered test file without silently omitting new files."""

    unit_root = project_root / "tests" / "unit"
    integration_root = project_root / "tests" / "integration"
    unit_files = sorted(unit_root.glob("test_*.py"))
    integration_files = sorted(integration_root.glob("test_*.py"))
    if not unit_files or not integration_files:
        raise RuntimeError("unit and integration test trees must both be present")

    spark_unit = {path for path in unit_files if _test_uses_spark_fixture(path)}
    model_unit = {path for path in spark_unit if path.name in MODEL_UNIT_FILES}
    phase_unit = spark_unit - model_unit
    core_unit = set(unit_files) - spark_unit

    shards: list[Shard] = []

    def add(
        name: str,
        paths: Iterable[Path | str],
        *,
        uses_spark: bool,
        marker_expression: str | None = None,
    ) -> None:
        selected = tuple(
            _relative(path, project_root) if isinstance(path, Path) else path
            for path in paths
        )
        if not selected:
            return
        marker_args = ("-m", marker_expression) if marker_expression else ()
        shards.append(
            Shard(
                name=name,
                pytest_args=(*marker_args, *selected),
                uses_spark=uses_spark,
            )
        )

    # Slow/boundary and embedded integration nodes are deliberately excluded here.
    unit_expression = "not integration and not slow"
    add(
        "unit-core",
        sorted(core_unit),
        uses_spark=False,
        marker_expression=unit_expression,
    )
    add(
        "unit-spark-model-contracts",
        sorted(model_unit),
        uses_spark=True,
        marker_expression=unit_expression,
    )
    add(
        "unit-spark-phase-contracts",
        sorted(phase_unit),
        uses_spark=True,
        marker_expression=unit_expression,
    )

    # A unit file may intentionally contain an integration smoke (currently ALS).
    for path in unit_files:
        if _contains_marker(path, "integration"):
            add(
                f"embedded-integration-{path.stem.removeprefix('test_')}",
                (path,),
                uses_spark=_test_uses_spark_fixture(path),
                marker_expression="integration and not slow",
            )

    reserved: set[Path] = set()

    def reserve_exact(name: str, filename: str, *, uses_spark: bool = True) -> None:
        path = integration_root / filename
        if not path.is_file():
            raise RuntimeError(f"required integration test file is missing: {path}")
        reserved.add(path)
        add(name, (path,), uses_spark=uses_spark)

    model_files = sorted(
        path for path in integration_files if "model" in path.stem
    )
    for path in model_files:
        reserved.add(path)
        add(
            f"model-integration-{path.stem.removeprefix('test_').removesuffix('_model')}",
            (path,),
            uses_spark=True,
        )

    reserve_exact("evaluation-integration", "test_evaluation_metrics.py")
    reserve_exact("performance-integration", "test_performance_workload.py")
    reserve_exact("smoke-integration", "test_smoke_transformations.py")

    hadoop_boundary = integration_root / "test_hadoop_boundary.py"
    delimiter_boundary = (
        unit_root
        / "test_delimiter_framer.py"
    )
    boundary_node = (
        f"{_relative(delimiter_boundary, project_root)}::"
        "test_streaming_framer_accepts_record_larger_than_128_mib"
    )
    if not hadoop_boundary.is_file() or not delimiter_boundary.is_file():
        raise RuntimeError("128 MiB boundary test inputs are missing")
    reserved.add(hadoop_boundary)
    add(
        "boundary-streaming-128mib",
        (boundary_node,),
        uses_spark=False,
    )
    add(
        "boundary-hadoop-128mib",
        (hadoop_boundary,),
        uses_spark=True,
    )

    # Any future integration file receives its own process instead of disappearing.
    for path in sorted(set(integration_files) - reserved):
        add(
            f"integration-{path.stem.removeprefix('test_')}",
            (path,),
            uses_spark=_test_uses_spark_fixture(path),
        )

    names = [shard.name for shard in shards]
    if len(names) != len(set(names)):
        raise RuntimeError(f"duplicate shard names: {names}")
    return tuple(shards)


def _normalise_exit_code(returncode: int) -> int:
    return 128 + abs(returncode) if returncode < 0 else returncode


def _write_process_error_junit(
    path: Path, *, shard: str, exit_code: int, detail: str
) -> None:
    suite = ET.Element(
        "testsuite",
        {
            "name": f"shard.{shard}",
            "tests": "1",
            "failures": "0",
            "errors": "1",
            "skipped": "0",
        },
    )
    case = ET.SubElement(
        suite,
        "testcase",
        {"classname": "scripts.run_tests_sharded", "name": shard},
    )
    error = ET.SubElement(
        case,
        "error",
        {
            "type": "ShardProcessError",
            "message": f"pytest shard exited with code {exit_code}",
        },
    )
    error.text = detail
    path.parent.mkdir(parents=True, exist_ok=True)
    ET.ElementTree(suite).write(path, encoding="utf-8", xml_declaration=True)


def _junit_is_readable(path: Path) -> bool:
    try:
        root = ET.parse(path).getroot()
    except (FileNotFoundError, ET.ParseError, OSError):
        return False
    return root.tag in {"testsuite", "testsuites"}


def _junit_records_nonpassing(path: Path) -> bool:
    root = ET.parse(path).getroot()
    suites = [root] if root.tag == "testsuite" else list(root.findall("testsuite"))
    return any(
        int(suite.attrib.get("failures", "0"))
        or int(suite.attrib.get("errors", "0"))
        for suite in suites
    )


def _external_project_spark_processes() -> list[int]:
    """Find an already-running project SparkSubmit without launching commands."""

    matches: list[int] = []
    proc = Path("/proc")
    if not proc.is_dir():
        return matches
    project = str(PROJECT_ROOT.resolve())
    for entry in proc.iterdir():
        if not entry.name.isdigit() or int(entry.name) == os.getpid():
            continue
        try:
            command = (entry / "cmdline").read_bytes().replace(b"\0", b" ").decode(
                "utf-8", errors="replace"
            )
        except (FileNotFoundError, PermissionError, ProcessLookupError, OSError):
            continue
        if "org.apache.spark.deploy.SparkSubmit" in command and project in command:
            matches.append(int(entry.name))
    return sorted(matches)


def _terminate_owned_process_group(process_group: int) -> None:
    """Terminate only descendants in the shard's dedicated process group."""

    try:
        os.killpg(process_group, signal.SIGTERM)
    except ProcessLookupError:
        return
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        try:
            os.killpg(process_group, 0)
        except ProcessLookupError:
            return
        time.sleep(0.1)
    try:
        os.killpg(process_group, signal.SIGKILL)
    except ProcessLookupError:
        pass


def _test_environment(temp_root: Path) -> dict[str, str]:
    env = os.environ.copy()
    java_bin = EXPECTED_JAVA_HOME / "bin"
    python_bin = EXPECTED_PYTHON.parent
    env.update(
        {
            "JAVA_HOME": str(EXPECTED_JAVA_HOME),
            "PATH": f"{java_bin}:{python_bin}:{env.get('PATH', '')}",
            "PYTHONPATH": (
                f"{PROJECT_ROOT / 'src'}"
                + (f":{env['PYTHONPATH']}" if env.get("PYTHONPATH") else "")
            ),
            "PYSPARK_PYTHON": str(EXPECTED_PYTHON),
            "PYSPARK_DRIVER_PYTHON": str(EXPECTED_PYTHON),
            "SPARK_LOCAL_IP": "127.0.0.1",
            "TMPDIR": str(temp_root),
            "PYTHONUNBUFFERED": "1",
        }
    )
    jars = sorted((PROJECT_ROOT / ".cache" / "ivy" / "jars").glob("*.jar"))
    jar_argument = ",".join(str(path.resolve()) for path in jars)
    submit = [
        "--master local[2]",
        "--driver-memory 8g",
        "--conf spark.driver.maxResultSize=1g",
        "--conf spark.ui.enabled=false",
        "--conf spark.ui.showConsoleProgress=false",
        "--conf spark.task.cpus=1",
        "--conf spark.memory.fraction=0.25",
        "--conf spark.memory.storageFraction=0.30",
        f"--conf spark.local.dir={temp_root / 'spark-local'}",
    ]
    if jar_argument:
        submit.append(f"--jars {jar_argument}")
    submit.append("pyspark-shell")
    env["PYSPARK_SUBMIT_ARGS"] = " ".join(submit)
    return env


def _merge_fragments(
    output: Path, fragments: Sequence[Path], *, allow_failures: bool
) -> int:
    command = [str(EXPECTED_PYTHON), str(MERGER)]
    if allow_failures:
        command.append("--allow-failures")
    command.extend((str(output), *(str(path) for path in fragments)))
    return subprocess.run(command, cwd=PROJECT_ROOT, check=False).returncode


def _preflight() -> None:
    if not EXPECTED_PYTHON.is_file() or not os.access(EXPECTED_PYTHON, os.X_OK):
        raise RuntimeError(f"bil401_env_1 Python is missing: {EXPECTED_PYTHON}")
    if not (EXPECTED_JAVA_HOME / "bin" / "java").is_file():
        raise RuntimeError(f"Java 21 is missing: {EXPECTED_JAVA_HOME}")
    try:
        java_release = (EXPECTED_JAVA_HOME / "release").read_text(encoding="utf-8")
    except OSError as error:
        raise RuntimeError("Java 21 release metadata is unreadable") from error
    if 'JAVA_VERSION="21.' not in java_release:
        raise RuntimeError(f"JAVA_HOME is not Java 21: {EXPECTED_JAVA_HOME}")
    if not MERGER.is_file():
        raise RuntimeError(f"JUnit merger is missing: {MERGER}")
    if Path(sys.executable).resolve() != EXPECTED_PYTHON.resolve():
        raise RuntimeError(
            "runner must use bil401_env_1 Python: "
            f"expected={EXPECTED_PYTHON}, observed={sys.executable}"
        )
    if sys.version_info[:3] != (3, 13, 1):
        raise RuntimeError(
            "bil401_env_1 Python version drifted: "
            f"observed={sys.version_info.major}.{sys.version_info.minor}."
            f"{sys.version_info.micro}"
        )
    if not any((PROJECT_ROOT / ".cache" / "ivy" / "jars").glob("*.jar")):
        raise RuntimeError("GraphFrames runtime JARs are missing; run make bootstrap")


def run(
    shards: Sequence[Shard],
    *,
    output: Path,
    shard_dir: Path,
    allow_concurrent_spark: bool,
) -> int:
    output = output if output.is_absolute() else PROJECT_ROOT / output
    shard_dir = shard_dir if shard_dir.is_absolute() else PROJECT_ROOT / shard_dir
    output.parent.mkdir(parents=True, exist_ok=True)
    shard_dir.mkdir(parents=True, exist_ok=True)
    output.unlink(missing_ok=True)
    for stale in shard_dir.glob("*.xml"):
        stale.unlink()
    for stale in shard_dir.glob("*.tmp"):
        if stale.is_dir():
            shutil.rmtree(stale)

    fragments: list[Path] = []
    try:
        _preflight()
    except RuntimeError as error:
        fragment = shard_dir / "00-preflight.xml"
        _write_process_error_junit(
            fragment,
            shard="preflight-environment",
            exit_code=2,
            detail=str(error),
        )
        if EXPECTED_PYTHON.is_file() and os.access(EXPECTED_PYTHON, os.X_OK):
            _merge_fragments(output, [fragment], allow_failures=True)
        else:
            shutil.copyfile(fragment, output)
        print(str(error), file=sys.stderr, flush=True)
        return 2

    if not allow_concurrent_spark:
        active = _external_project_spark_processes()
        if active:
            fragment = shard_dir / "00-preflight.xml"
            _write_process_error_junit(
                fragment,
                shard="preflight-concurrent-spark",
                exit_code=75,
                detail=f"active project SparkSubmit PIDs: {active}",
            )
            _merge_fragments(output, [fragment], allow_failures=True)
            print(
                "Refusing to overlap test JVMs with active project Spark: "
                f"{active}",
                file=sys.stderr,
                flush=True,
            )
            return 75

    for index, shard in enumerate(shards, start=1):
        if not allow_concurrent_spark:
            active = _external_project_spark_processes()
            if active:
                fragment = shard_dir / f"{index:02d}-{shard.name}.xml"
                _write_process_error_junit(
                    fragment,
                    shard=shard.name,
                    exit_code=75,
                    detail=f"active project SparkSubmit PIDs before shard: {active}",
                )
                fragments.append(fragment)
                _merge_fragments(output, fragments, allow_failures=True)
                return 75

        fragment = shard_dir / f"{index:02d}-{shard.name}.xml"
        temp_root = shard_dir / f"{index:02d}-{shard.name}.tmp"
        shutil.rmtree(temp_root, ignore_errors=True)
        temp_root.mkdir(parents=True)
        command = [
            str(EXPECTED_PYTHON),
            "-m",
            "pytest",
            *PYTEST_BASE_ARGS,
            f"--junitxml={fragment}",
            f"--basetemp={temp_root / 'pytest'}",
            *shard.pytest_args,
        ]
        print(
            json.dumps(
                {
                    "event": "test_shard_started",
                    "index": index,
                    "total": len(shards),
                    "name": shard.name,
                    "uses_spark": shard.uses_spark,
                }
            ),
            flush=True,
        )
        process = subprocess.Popen(
            command,
            cwd=PROJECT_ROOT,
            env=_test_environment(temp_root),
            start_new_session=True,
        )
        try:
            raw_returncode = process.wait()
        except KeyboardInterrupt:
            process.send_signal(signal.SIGTERM)
            raw_returncode = -signal.SIGINT
        finally:
            _terminate_owned_process_group(process.pid)
            try:
                process.wait(timeout=1.0)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            shutil.rmtree(temp_root, ignore_errors=True)

        returncode = _normalise_exit_code(raw_returncode)
        if not _junit_is_readable(fragment):
            _write_process_error_junit(
                fragment,
                shard=shard.name,
                exit_code=returncode or 2,
                detail="pytest did not produce a readable JUnit fragment",
            )
            returncode = returncode or 2
        fragments.append(fragment)
        if returncode and not _junit_records_nonpassing(fragment):
            process_error = shard_dir / f"{index:02d}-{shard.name}-process-error.xml"
            _write_process_error_junit(
                process_error,
                shard=f"{shard.name}-process-error",
                exit_code=returncode,
                detail=(
                    "pytest exited non-zero but its JUnit fragment contained no "
                    "failure/error node"
                ),
            )
            fragments.append(process_error)
        print(
            json.dumps(
                {
                    "event": "test_shard_finished",
                    "index": index,
                    "total": len(shards),
                    "name": shard.name,
                    "exit_code": returncode,
                    "junit": str(fragment),
                }
            ),
            flush=True,
        )
        if returncode:
            merge_code = _merge_fragments(
                output, fragments, allow_failures=True
            )
            if merge_code:
                print(
                    f"JUnit merge failed with code {merge_code}; fragments remain in "
                    f"{shard_dir}",
                    file=sys.stderr,
                )
            return returncode

    merge_code = _merge_fragments(output, fragments, allow_failures=False)
    if merge_code:
        return _normalise_exit_code(merge_code)
    print(
        json.dumps(
            {
                "event": "test_suite_finished",
                "shards": len(shards),
                "exit_code": 0,
                "junit": str(output),
            }
        ),
        flush=True,
    )
    return 0


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument(
        "--output",
        type=Path,
        default=Path("artifacts/test-results/junit.xml"),
        help="merged JUnit XML path",
    )
    result.add_argument(
        "--shard-dir",
        type=Path,
        default=Path("artifacts/test-results/shards"),
        help="durable per-shard JUnit directory",
    )
    result.add_argument(
        "--list",
        action="store_true",
        help="print the deterministic shard plan without running pytest or Spark",
    )
    result.add_argument(
        "--allow-concurrent-spark",
        action="store_true",
        help="explicitly allow overlap with an existing project SparkSubmit",
    )
    return result


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    shards = build_shards()
    if args.list:
        print(
            json.dumps(
                [
                    {
                        "name": shard.name,
                        "uses_spark": shard.uses_spark,
                        "pytest_args": list(shard.pytest_args),
                    }
                    for shard in shards
                ],
                indent=2,
            )
        )
        return 0
    return run(
        shards,
        output=args.output,
        shard_dir=args.shard_dir,
        allow_concurrent_spark=args.allow_concurrent_spark,
    )


if __name__ == "__main__":
    raise SystemExit(main())
