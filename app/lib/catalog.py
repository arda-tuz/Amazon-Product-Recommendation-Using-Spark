"""Run and Parquet catalog discovery for the presentation layer.

This module intentionally has no Spark dependency.  It accepts only run identifiers
and logical table names from fixed allowlists, and it considers a Parquet data set
servable only when Spark's ``_SUCCESS`` marker is present.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Mapping


RUN_ID_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$"
)

# New dashboard-specific exports take precedence.  The canonical gate tables are
# fallbacks, all within the selected immutable run directory.
TABLE_PATHS: Final[Mapping[str, tuple[str, ...]]] = {
    "overview_metrics": (
        "data/g10/dashboard_overview",
        "data/g9/dashboard_overview",
        "data/g5/profile_metrics",
    ),
    "quality_summary": (
        "data/g10/dashboard_quality_summary",
        "data/g9/dashboard_quality_summary",
        "data/g5/data_quality_summary",
    ),
    "quality_samples": ("data/g5/data_quality_samples",),
    "product_quality": ("data/g5/product_quality_profile",),
    "products": (
        "data/g10/product_search_index",
        "data/g9/product_search_index",
        "data/full/silver/products",
    ),
    "category_paths": (
        "data/g10/product_search_index",
        "data/full/silver/category_paths",
    ),
    "category_nodes": (
        "data/g10/category_search_index",
        "data/full/silver/category_nodes",
    ),
    "reviews": ("data/full/silver/reviews_deduplicated",),
    "interactions": ("data/full/silver/user_item_interactions",),
    "group_distribution": ("data/g10/dashboard_group_distribution",),
    "rating_distribution": ("data/g10/dashboard_rating_distribution",),
    "review_year_distribution": ("data/g10/dashboard_review_year_distribution",),
    "activity_quantiles": ("data/g10/dashboard_activity_quantiles",),
    "active_catalog": ("data/g6/active_catalog",),
    "evaluation_users": ("data/g6/evaluation_users",),
    "seen_items": ("data/g6/stage_seen_items",),
    "graph_edges": ("data/g7/graph_internal_edges",),
    "graph_pagerank": ("data/g7/graph_pagerank",),
    "graph_degrees": ("data/g7/graph_degrees",),
    "graph_components": ("data/g7/graph_weak_components",),
    "graph_summary": ("data/g7/graph_structural_summary",),
    "popularity_scores": ("data/g7/popularity_scores",),
    "popularity_catalog": ("data/g7/popularity_global_catalog",),
    "category_top_products": ("data/g7/category_top_products",),
    "popularity_recommendations": ("data/g7/popularity_recommendations",),
    "als_recommendations": ("data/g7/als_recommendations",),
    "fp_recommendations": ("data/g7/fp_recommendations",),
    "graph_recommendations": ("data/g7/graph_recommendations",),
    "category_recommendations": ("data/g7/category_recommendations",),
    "model_runtime": ("data/g9/model_runtime_summary", "data/g7/model_runtime_summary"),
    "recommendation_validation": ("data/g7/recommendation_validation_summary",),
    "hybrid_candidates": ("data/g8/hybrid_candidates",),
    "h_a_recommendations": ("data/g8/hybrid_a_recommendations",),
    "h_b_recommendations": ("data/g8/hybrid_b_recommendations",),
    "evaluation_summary": ("data/g9/evaluation_summary",),
    "evaluation_per_user": ("data/g9/evaluation_per_user",),
    "als_prediction_summary": ("data/g9/als_prediction_summary",),
    "selected_hybrid": ("data/g9/selected_hybrid",),
    "selected_hybrid_recommendations": (
        "data/g9/selected_hybrid_recommendations",
        "data/g9/selected_hybrid_candidates",
    ),
    "official_test_comparison": ("data/g9/official_test_comparison",),
    "validation_hybrid_comparison": ("data/g9/validation_hybrid_comparison",),
    "servable_customers": ("data/g10/servable_customers", "data/g9/servable_customers"),
    "demo_users": ("data/g10/demo_users", "data/g9/demo_users"),
    "recommendation_evidence": (
        "data/g10/recommendation_evidence",
        "data/g9/recommendation_evidence",
    ),
    "seed_recommendations": ("data/g10/seed_recommendations",),
    "category_onboarding_recommendations": (
        "data/g10/category_onboarding_recommendations",
    ),
}


def project_root() -> Path:
    """Return the repository root without relying on the launch directory."""

    return Path(__file__).resolve().parents[2]


def artifacts_root() -> Path:
    override = os.environ.get("AMAZON_REC_ARTIFACTS_ROOT")
    if override:
        candidate = Path(override).expanduser().resolve()
        return candidate
    return project_root() / "artifacts"


def _read_json(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return value if isinstance(value, dict) else {}


def _gate_number(name: str) -> int:
    match = re.fullmatch(r"G(\d+)\.json", name)
    return int(match.group(1)) if match else -1


@dataclass(frozen=True)
class RunContext:
    """A selected, traversal-safe artifact run."""

    run_id: str
    run_dir: Path
    last_passed_gate: int
    source_sha256: str | None
    recorded_at: str | None
    manifests: Mapping[int, Path]

    @property
    def manifest_path(self) -> Path | None:
        return self.manifests.get(self.last_passed_gate)

    @property
    def is_evaluation_ready(self) -> bool:
        return self.last_passed_gate >= 9 and self.has_table("evaluation_summary")

    def has_table(self, logical_name: str) -> bool:
        return self.table_path(logical_name) is not None

    def table_path(self, logical_name: str) -> Path | None:
        if logical_name not in TABLE_PATHS:
            raise KeyError(f"Unknown dashboard table: {logical_name}")
        root = self.run_dir.resolve()
        for relative in TABLE_PATHS[logical_name]:
            # A passed G10 run must fail closed if a serving export is missing;
            # silently dropping back to Silver would violate the presentation
            # contract and conceal artifact corruption.
            if self.last_passed_gate >= 10 and relative.startswith("data/full/silver/"):
                continue
            candidate = (root / relative).resolve()
            if not candidate.is_relative_to(root):
                continue
            if is_complete_parquet_dir(candidate):
                return candidate
        return None


def is_complete_parquet_dir(path: Path) -> bool:
    """Require an atomic-success marker and at least one data file."""

    return (
        path.is_dir()
        and (path / "_SUCCESS").is_file()
        and any(path.glob("*.parquet"))
    )


def discover_runs(root: Path | None = None) -> list[RunContext]:
    runs_root = (root or artifacts_root()) / "runs"
    if not runs_root.is_dir():
        return []
    contexts: list[RunContext] = []
    for run_dir in runs_root.iterdir():
        if not run_dir.is_dir() or not RUN_ID_PATTERN.fullmatch(run_dir.name):
            continue
        passed: dict[int, Path] = {}
        payloads: dict[int, dict] = {}
        manifest_dir = run_dir / "manifests"
        for path in manifest_dir.glob("G*.json") if manifest_dir.is_dir() else ():
            gate = _gate_number(path.name)
            payload = _read_json(path)
            if gate >= 0 and payload.get("status") == "passed":
                passed[gate] = path.resolve()
                payloads[gate] = payload
        if not passed:
            continue
        last = max(passed)
        latest = payloads[last]
        source_sha = next(
            (
                payloads[g].get("source_sha256")
                for g in sorted(payloads, reverse=True)
                if payloads[g].get("source_sha256")
            ),
            None,
        )
        contexts.append(
            RunContext(
                run_id=run_dir.name,
                run_dir=run_dir.resolve(),
                last_passed_gate=last,
                source_sha256=source_sha,
                recorded_at=latest.get("finished_at") or latest.get("recorded_at"),
                manifests=dict(passed),
            )
        )
    return sorted(
        contexts,
        key=lambda item: (
            item.recorded_at or "",
            item.last_passed_gate,
            item.run_id,
        ),
        reverse=True,
    )


def select_run(run_id: str | None = None, root: Path | None = None) -> RunContext | None:
    contexts = discover_runs(root)
    requested = run_id or os.environ.get("AMAZON_REC_RUN_ID")
    if requested:
        if not RUN_ID_PATTERN.fullmatch(requested):
            raise ValueError(f"Unsafe run id: {requested!r}")
        return next((item for item in contexts if item.run_id == requested), None)
    return contexts[0] if contexts else None
