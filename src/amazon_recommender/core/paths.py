"""Run-scoped filesystem paths with traversal-safe identifiers."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path


RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


@dataclass(frozen=True)
class RunPaths:
    project_root: Path
    artifacts_root: Path
    run_id: str

    @classmethod
    def create(cls, project_root: Path, artifacts_root: Path, run_id: str) -> "RunPaths":
        if not RUN_ID_PATTERN.fullmatch(run_id):
            raise ValueError(f"Unsafe run id: {run_id!r}")
        return cls(project_root.resolve(), artifacts_root.resolve(), run_id)

    @property
    def run(self) -> Path:
        return self.artifacts_root / "runs" / self.run_id

    @property
    def manifests(self) -> Path:
        return self.run / "manifests"

    @property
    def logs(self) -> Path:
        return self.run / "logs"

    @property
    def data(self) -> Path:
        return self.run / "data"

    @property
    def checkpoints(self) -> Path:
        return self.run / "checkpoints"

    @property
    def temporary(self) -> Path:
        return self.artifacts_root / ".tmp" / self.run_id

    def ensure_control_dirs(self) -> None:
        for path in (self.manifests, self.logs, self.checkpoints, self.temporary):
            path.mkdir(parents=True, exist_ok=True)
