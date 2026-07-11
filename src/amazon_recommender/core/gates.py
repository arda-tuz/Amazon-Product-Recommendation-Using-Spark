"""Strict G0-G12 prerequisite enforcement."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from amazon_recommender.core.manifest import content_sha256, read_manifest
from amazon_recommender.core.paths import RunPaths


GATES = tuple(f"G{index}" for index in range(13))


class GateBlocked(RuntimeError):
    """Raised before work starts when prior evidence is missing or incompatible."""


@dataclass(frozen=True)
class GateStore:
    paths: RunPaths
    config_sha256: str
    source_sha256: str

    def path(self, gate: str) -> Path:
        if gate not in GATES:
            raise ValueError(f"Unknown gate: {gate}")
        return self.paths.manifests / f"{gate}.json"

    def read(self, gate: str) -> dict[str, Any]:
        return read_manifest(self.path(gate))

    def passed(self, gate: str) -> bool:
        path = self.path(gate)
        if not path.exists():
            return False
        manifest = self.read(gate)
        return manifest.get("status") == "passed"

    def require_prerequisites(self, gate: str) -> dict[str, str]:
        if gate not in GATES:
            raise ValueError(f"Unknown gate: {gate}")
        prior = GATES[: GATES.index(gate)]
        evidence: dict[str, str] = {}
        for required in prior:
            path = self.path(required)
            if not path.exists():
                raise GateBlocked(f"{gate} requires missing {required} evidence: {path}")
            manifest = self.read(required)
            if manifest.get("status") != "passed":
                raise GateBlocked(f"{gate} requires {required}=passed")
            if required != "G0":
                if manifest.get("config_sha256") != self.config_sha256:
                    raise GateBlocked(f"{required} config fingerprint differs")
                if manifest.get("source_sha256") != self.source_sha256:
                    raise GateBlocked(f"{required} source fingerprint differs")
            evidence[required] = str(
                manifest.get("evidence_sha256") or content_sha256(manifest)
            )
        return evidence
