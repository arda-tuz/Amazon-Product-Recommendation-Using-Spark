from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from amazon_recommender.core.config import ConfigError, load_config
from amazon_recommender.core.gates import GateBlocked, GateStore
from amazon_recommender.core.manifest import (
    atomic_write_json,
    build_manifest,
    content_sha256,
    read_manifest,
)
from amazon_recommender.core.paths import RunPaths


PROJECT_ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.unit
def test_binding_config_loads_with_stable_fingerprint() -> None:
    first = load_config(PROJECT_ROOT / "configs/project.yaml", project_root=PROJECT_ROOT)
    second = load_config(PROJECT_ROOT / "configs/project.yaml", project_root=PROJECT_ROOT)
    assert first.sha256 == second.sha256
    assert first.get("models", "als", "rank") == 20
    assert first.get("cleaning", "avg_rating_rounding") == "nearest-0.5-half-up"


@pytest.mark.unit
def test_binding_config_rejects_parameter_change(tmp_path: Path) -> None:
    data = yaml.safe_load((PROJECT_ROOT / "configs/project.yaml").read_text())
    data["models"]["als"]["rank"] = 21
    path = tmp_path / "project.yaml"
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    with pytest.raises(ConfigError, match="models.als.rank"):
        load_config(path, project_root=tmp_path)


@pytest.mark.unit
def test_binding_config_rejects_different_hybrid_even_if_weights_sum_to_one(
    tmp_path: Path,
) -> None:
    data = yaml.safe_load((PROJECT_ROOT / "configs/project.yaml").read_text())
    data["hybrid"]["h_a"]["als"] = 0.36
    data["hybrid"]["h_a"]["graph"] = 0.19
    path = tmp_path / "project.yaml"
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    with pytest.raises(ConfigError, match="hybrid.h_a"):
        load_config(path, project_root=tmp_path)


@pytest.mark.unit
@pytest.mark.parametrize("run_id", ["../escape", "/absolute", "with space", ""])
def test_run_paths_reject_unsafe_identifiers(tmp_path: Path, run_id: str) -> None:
    with pytest.raises(ValueError, match="Unsafe run id"):
        RunPaths.create(tmp_path, tmp_path / "artifacts", run_id)


@pytest.mark.unit
def test_atomic_manifest_round_trip_and_hash(tmp_path: Path) -> None:
    manifest = build_manifest(
        gate="G1",
        run_id="test-run",
        status="passed",
        config_sha256="c" * 64,
        source_sha256="s" * 64,
        previous_evidence={"G0": "g0-hash"},
        evidence={"tests": 4},
    )
    path = tmp_path / "G1.json"
    atomic_write_json(path, manifest)
    assert read_manifest(path) == manifest
    assert len(content_sha256(manifest)) == 64
    assert not list(tmp_path.glob("*.tmp"))


@pytest.mark.unit
def test_manifest_can_record_gate_timing() -> None:
    manifest = build_manifest(
        gate="G7",
        run_id="test-run",
        status="passed",
        config_sha256="c" * 64,
        source_sha256="s" * 64,
        previous_evidence={},
        evidence={},
        started_at="2026-07-11T00:00:00Z",
        finished_at="2026-07-11T00:00:03Z",
        duration_seconds=3.0,
    )
    assert manifest["duration_seconds"] == 3.0
    assert manifest["started_at"] < manifest["finished_at"]


@pytest.mark.unit
def test_gate_store_requires_passed_chain(tmp_path: Path) -> None:
    paths = RunPaths.create(tmp_path, tmp_path / "artifacts", "test-run")
    paths.ensure_control_dirs()
    store = GateStore(paths, "c" * 64, "s" * 64)
    with pytest.raises(GateBlocked, match="missing G0"):
        store.require_prerequisites("G1")
    atomic_write_json(store.path("G0"), {"status": "passed", "gate": "G0"})
    assert "G0" in store.require_prerequisites("G1")
    with pytest.raises(GateBlocked, match="missing G1"):
        store.require_prerequisites("G2")


@pytest.mark.unit
def test_manifest_reader_rejects_unknown_status(tmp_path: Path) -> None:
    path = tmp_path / "bad.json"
    path.write_text(json.dumps({"status": "invented"}), encoding="utf-8")
    with pytest.raises(ValueError, match="Invalid manifest status"):
        read_manifest(path)
