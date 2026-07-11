"""Canonical, atomically-published gate evidence manifests."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


VALID_STATUSES = {"running", "passed", "failed", "blocked"}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def canonical_json(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def content_sha256(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json(payload)).hexdigest()


def atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def read_manifest(path: Path) -> dict[str, Any]:
    parsed = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(parsed, dict):
        raise ValueError(f"Manifest is not an object: {path}")
    if parsed.get("status") not in VALID_STATUSES:
        raise ValueError(f"Invalid manifest status in {path}")
    return parsed


def build_manifest(
    *,
    gate: str,
    run_id: str,
    status: str,
    config_sha256: str,
    source_sha256: str,
    previous_evidence: Mapping[str, str],
    evidence: Mapping[str, Any],
    error: Mapping[str, Any] | None = None,
    started_at: str | None = None,
    finished_at: str | None = None,
    duration_seconds: float | None = None,
) -> dict[str, Any]:
    if status not in VALID_STATUSES:
        raise ValueError(f"Unsupported manifest status: {status}")
    payload: dict[str, Any] = {
        "schema_version": 1,
        "run_id": run_id,
        "gate": gate,
        "status": status,
        "recorded_at": utc_now(),
        "config_sha256": config_sha256,
        "source_sha256": source_sha256,
        "previous_evidence": dict(sorted(previous_evidence.items())),
        "evidence": dict(evidence),
    }
    if error is not None:
        payload["error"] = dict(error)
    if started_at is not None:
        payload["started_at"] = started_at
    if finished_at is not None:
        payload["finished_at"] = finished_at
    if duration_seconds is not None:
        if duration_seconds < 0:
            raise ValueError("duration_seconds cannot be negative")
        payload["duration_seconds"] = float(duration_seconds)
    payload["evidence_sha256"] = content_sha256(payload)
    return payload
