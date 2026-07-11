from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

from scripts.merge_junit import merge
from scripts.run_tests_sharded import (
    PROJECT_ROOT,
    _junit_records_nonpassing,
    _normalise_exit_code,
    _preflight,
    _write_process_error_junit,
    build_shards,
)


pytestmark = pytest.mark.unit


def test_runner_preflight_locks_bil401_python_java21_and_graphframes() -> None:
    _preflight()


def _suite(path: Path, *, name: str, failures: int = 0) -> None:
    root = ET.Element(
        "testsuite",
        {
            "name": name,
            "tests": "1",
            "failures": str(failures),
            "errors": "0",
            "skipped": "0",
        },
    )
    case = ET.SubElement(root, "testcase", {"classname": "fixture", "name": name})
    if failures:
        ET.SubElement(case, "failure", {"message": "expected fixture failure"})
    ET.ElementTree(root).write(path, encoding="utf-8", xml_declaration=True)


def test_shard_plan_is_unique_complete_and_resource_isolated() -> None:
    shards = build_shards(PROJECT_ROOT)
    by_name = {shard.name: shard for shard in shards}
    assert len(by_name) == len(shards)
    assert {
        "unit-core",
        "unit-spark-model-contracts",
        "unit-spark-phase-contracts",
        "embedded-integration-als_model",
        "model-integration-category",
        "model-integration-fp_growth",
        "model-integration-graph",
        "evaluation-integration",
        "performance-integration",
        "smoke-integration",
        "boundary-streaming-128mib",
        "boundary-hadoop-128mib",
    }.issubset(by_name)
    assert by_name["unit-core"].uses_spark is False
    assert all(
        by_name[name].uses_spark
        for name in (
            "unit-spark-model-contracts",
            "unit-spark-phase-contracts",
            "model-integration-category",
            "evaluation-integration",
            "performance-integration",
            "smoke-integration",
            "boundary-hadoop-128mib",
        )
    )
    streaming_args = by_name["boundary-streaming-128mib"].pytest_args
    hadoop_args = by_name["boundary-hadoop-128mib"].pytest_args
    assert any("larger_than_128_mib" in value for value in streaming_args)
    assert "tests/integration/test_hadoop_boundary.py" in hadoop_args
    assert by_name["boundary-streaming-128mib"].uses_spark is False


def test_merge_can_preserve_failure_evidence_but_rejects_it_by_default(
    tmp_path: Path,
) -> None:
    passed = tmp_path / "passed.xml"
    failed = tmp_path / "failed.xml"
    output = tmp_path / "merged.xml"
    _suite(passed, name="passed")
    _suite(failed, name="failed", failures=1)

    with pytest.raises(RuntimeError, match="non-passing"):
        merge(output, [passed, failed])
    merge(output, [passed, failed], allow_failures=True)

    root = ET.parse(output).getroot()
    assert root.attrib["tests"] == "2"
    assert root.attrib["failures"] == "1"
    assert [suite.attrib["name"] for suite in root.findall("testsuite")] == [
        "passed",
        "failed",
    ]


def test_process_error_junit_and_signal_exit_codes_are_truthful(tmp_path: Path) -> None:
    fragment = tmp_path / "process-error.xml"
    _write_process_error_junit(
        fragment,
        shard="killed-shard",
        exit_code=137,
        detail="process ended before pytest wrote JUnit",
    )

    assert _junit_records_nonpassing(fragment)
    assert _normalise_exit_code(-9) == 137
    assert _normalise_exit_code(1) == 1
    error = ET.parse(fragment).getroot().find("testcase/error")
    assert error is not None
    assert error.attrib["type"] == "ShardProcessError"
    assert "137" in error.attrib["message"]
