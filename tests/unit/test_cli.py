from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest
import yaml

from amazon_recommender.cli import main


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _temporary_project(tmp_path: Path) -> Path:
    data = yaml.safe_load((PROJECT_ROOT / "configs/project.yaml").read_text())
    data["paths"]["artifacts"] = "artifacts"
    config_dir = tmp_path / "configs"
    config_dir.mkdir()
    (config_dir / "project.yaml").write_text(yaml.safe_dump(data), encoding="utf-8")
    return tmp_path


@pytest.mark.unit
def test_cli_status_reports_all_gates(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    root = _temporary_project(tmp_path)
    assert main(["--project-root", str(root), "--run-id", "test-run", "status"]) == 0
    rows = json.loads(capsys.readouterr().out)
    assert len(rows) == 13
    assert rows[0] == {"gate": "G0", "status": "missing"}


@pytest.mark.unit
def test_cli_blocks_unimplemented_gate_and_records_attempt(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    root = _temporary_project(tmp_path)
    run = root / "artifacts/runs/test-run/manifests"
    run.mkdir(parents=True)
    (run / "G0.json").write_text(json.dumps({"gate": "G0", "status": "passed"}))
    result = main(["--project-root", str(root), "--run-id", "test-run", "gate", "G2"])
    assert result == 3
    assert "missing G1" in capsys.readouterr().err
    assert list((run / "attempts").glob("G2-*.json"))


@pytest.mark.unit
def test_cli_passes_g1_with_junit_evidence(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    root = _temporary_project(tmp_path)
    manifests = root / "artifacts/runs/test-run/manifests"
    manifests.mkdir(parents=True)
    (manifests / "G0.json").write_text(
        json.dumps({"gate": "G0", "status": "passed"}), encoding="utf-8"
    )
    junit = tmp_path / "junit.xml"
    junit.write_text(
        '<testsuites tests="2" failures="0" errors="0" skipped="0">'
        '<testsuite tests="2" failures="0" errors="0" skipped="0" />'
        "</testsuites>",
        encoding="utf-8",
    )
    result = main(
        [
            "--project-root",
            str(root),
            "--run-id",
            "test-run",
            "gate",
            "G1",
            "--evidence-file",
            str(junit),
        ]
    )
    assert result == 0
    manifest = json.loads((manifests / "G1.json").read_text())
    assert manifest["status"] == "passed"
    assert manifest["evidence"]["pytest"]["tests"] == 2
    capsys.readouterr()


@pytest.mark.unit
def test_cli_rejects_failing_junit(tmp_path: Path) -> None:
    root = _temporary_project(tmp_path)
    manifests = root / "artifacts/runs/test-run/manifests"
    manifests.mkdir(parents=True)
    (manifests / "G0.json").write_text(json.dumps({"gate": "G0", "status": "passed"}))
    junit = tmp_path / "failed.xml"
    junit.write_text('<testsuite tests="1" failures="1" errors="0" skipped="0" />')
    assert (
        main(
            [
                "--project-root",
                str(root),
                "--run-id",
                "test-run",
                "gate",
                "G1",
                "--evidence-file",
                str(junit),
            ]
        )
        == 1
    )


@pytest.mark.unit
def test_launcher_uses_portable_bil401_python_override() -> None:
    environment = os.environ.copy()
    environment["AMAZON_REC_PYTHON"] = sys.executable
    completed = subprocess.run(
        [str(PROJECT_ROOT / "bin" / "amazon-rec"), "--help"],
        cwd=PROJECT_ROOT,
        env=environment,
        text=True,
        capture_output=True,
        timeout=15,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert "strict phase gate" in completed.stdout


@pytest.mark.unit
def test_launcher_rejects_override_outside_bil401_env(tmp_path: Path) -> None:
    fake_python = tmp_path / "python"
    fake_python.write_text(
        "#!/usr/bin/env bash\n"
        "printf '3.13.1\\n/usr/bin/python3\\n/tmp/not-the-required-env\\n'\n",
        encoding="utf-8",
    )
    fake_python.chmod(0o755)
    environment = os.environ.copy()
    environment["AMAZON_REC_PYTHON"] = str(fake_python)
    completed = subprocess.run(
        [str(PROJECT_ROOT / "bin" / "amazon-rec"), "--help"],
        cwd=PROJECT_ROOT,
        env=environment,
        text=True,
        capture_output=True,
        timeout=15,
        check=False,
    )
    assert completed.returncode == 2
    assert "prefix is not bil401_env_1" in completed.stderr
