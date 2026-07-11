"""Load and strictly validate the binding project configuration."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping

import yaml


class ConfigError(ValueError):
    """Raised when a binding configuration invariant is violated."""


IMMUTABLE_VALUES: Mapping[tuple[str, ...], Any] = MappingProxyType(
    {
        ("project", "seed"): 42,
        ("source", "sha256"): "600135116a05b7ce2dcb7e842e892d663c6190a0567d00373e0c5c4f3c908f02",
        ("source", "size_bytes"): 977_506_331,
        ("source", "line_count"): 15_010_574,
        ("source", "record_delimiter_hex"): "0d0a0d0a",
        ("spark", "shuffle_partitions"): 64,
        ("spark", "master"): "local[*]",
        ("spark", "driver_memory"): "8g",
        ("spark", "driver_max_result_size"): "1g",
        ("spark", "adaptive_enabled"): True,
        ("spark", "max_partition_bytes"): 134_217_728,
        ("spark", "parquet_compression"): "snappy",
        ("spark", "session_timezone"): "UTC",
        ("storage", "parquet_target_bytes"): 134_217_728,
        ("storage", "fact_min_files"): 8,
        ("storage", "fact_max_files"): 64,
        ("storage", "dimension_min_files"): 1,
        ("storage", "dimension_max_files"): 8,
        ("storage", "fallback_shards"): 64,
        ("sampling", "cohort_limit"): 20_000,
        ("sampling", "hash_separator"): "\x1f",
        ("cleaning", "positive_rating_min"): 4.0,
        ("cleaning", "avg_rating_rounding"): "nearest-0.5-half-up",
        ("cleaning", "expected_avg_mismatches"): 487,
        ("split", "evaluation_min_distinct_items"): 5,
        ("split", "order"): ["interaction_date_asc", "product_id_asc"],
        ("split", "validation_position_from_end"): 2,
        ("split", "test_position_from_end"): 1,
        ("split", "test_seen_includes_validation"): True,
        ("als_k_core", "min_user_items"): 3,
        ("als_k_core", "min_item_users"): 5,
        ("models", "popularity", "m"): 20,
        ("models", "popularity", "group_min_train_interactions"): 100,
        ("models", "popularity", "global_catalog_depth"): 1_000,
        ("models", "popularity", "candidate_depth"): 100,
        ("models", "als", "rank"): 20,
        ("models", "als", "reg_param"): 0.10,
        ("models", "als", "max_iter"): 10,
        ("models", "als", "implicit_prefs"): False,
        ("models", "als", "nonnegative"): False,
        ("models", "als", "cold_start_strategy"): "drop",
        ("models", "als", "raw_candidate_depth"): 200,
        ("models", "als", "candidate_depth"): 100,
        ("models", "fp_growth", "min_basket_size"): 2,
        ("models", "fp_growth", "max_basket_size"): 50,
        ("models", "fp_growth", "min_fraction"): 0.001,
        ("models", "fp_growth", "min_count"): 200,
        ("models", "fp_growth", "min_confidence"): 0.05,
        ("models", "fp_growth", "min_lift"): 1.10,
        ("models", "fp_growth", "num_partitions"): 64,
        ("models", "fp_growth", "max_rules_per_antecedent"): 20,
        ("models", "fp_growth", "candidate_depth"): 50,
        ("models", "graph", "max_positive_seeds"): 20,
        ("models", "graph", "direct_weight"): 1.0,
        ("models", "graph", "reciprocal_bonus"): 0.25,
        ("models", "graph", "two_hop_weight"): 0.50,
        ("models", "graph", "pagerank_reset_probability"): 0.15,
        ("models", "graph", "pagerank_max_iter"): 10,
        ("models", "graph", "candidate_depth"): 50,
        ("models", "category", "similarity_weight"): 0.80,
        ("models", "category", "group_affinity_weight"): 0.10,
        ("models", "category", "popularity_percentile_weight"): 0.10,
        ("models", "category", "max_profile_categories"): 20,
        ("models", "category", "generic_category_ratio"): 0.10,
        ("models", "category", "products_per_category"): 200,
        ("models", "category", "max_candidate_pool"): 5_000,
        ("models", "category", "candidate_depth"): 50,
        ("hybrid", "rrf_c"): 60,
        ("hybrid", "stored_depth"): 100,
        (
            "hybrid",
            "h_a",
        ): {"als": 0.35, "graph": 0.20, "category": 0.20, "fp": 0.15, "popularity": 0.10},
        (
            "hybrid",
            "h_b",
        ): {"als": 0.50, "graph": 0.20, "category": 0.10, "fp": 0.15, "popularity": 0.05},
        ("hybrid", "selection_cohort"): "common_warm",
        ("hybrid", "ndcg_tie_threshold"): 0.001,
        ("hybrid", "final_tie_break"): "h_a",
        ("evaluation", "k"): 10,
        ("evaluation", "metrics"): ["ndcg", "hit_rate", "mrr"],
        ("evaluation", "coverage"): [
            "user_coverage",
            "fill_rate",
            "catalog_coverage",
        ],
        ("evaluation", "slices"): ["overall", "Book", "non-Book"],
        ("performance", "masters"): [
            "local[1]",
            "local[min(4,logical_cores)]",
        ],
        ("performance", "warmups"): 1,
        ("performance", "measured_runs"): 3,
        ("performance", "cache_enabled"): False,
    }
)


def _lookup(data: Mapping[str, Any], path: tuple[str, ...]) -> Any:
    current: Any = data
    for key in path:
        if not isinstance(current, Mapping) or key not in current:
            raise ConfigError(f"Missing binding configuration key: {'.'.join(path)}")
        current = current[key]
    return current


def _canonical_bytes(data: Mapping[str, Any]) -> bytes:
    return json.dumps(
        data, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


@dataclass(frozen=True)
class ProjectConfig:
    """Immutable loaded configuration plus its content fingerprint."""

    path: Path
    project_root: Path
    data: Mapping[str, Any]
    sha256: str

    def get(self, *path: str) -> Any:
        return _lookup(self.data, tuple(path))

    def resolve(self, *path: str) -> Path:
        value = self.get(*path)
        if not isinstance(value, str):
            raise ConfigError(f"Path value {'.'.join(path)} must be a string")
        candidate = Path(value)
        return candidate if candidate.is_absolute() else (self.project_root / candidate)


def validate_config(data: Mapping[str, Any]) -> None:
    if data.get("schema_version") != 1:
        raise ConfigError("Only config schema_version=1 is supported")
    for path, expected in IMMUTABLE_VALUES.items():
        actual = _lookup(data, path)
        if actual != expected:
            raise ConfigError(
                f"Binding value {'.'.join(path)}={actual!r}; expected {expected!r}"
            )
    weights = (data["hybrid"]["h_a"], data["hybrid"]["h_b"])
    for name, weight_map in zip(("h_a", "h_b"), weights, strict=True):
        if set(weight_map) != {"als", "graph", "category", "fp", "popularity"}:
            raise ConfigError(f"{name} must contain exactly the five binding models")
        if abs(sum(float(value) for value in weight_map.values()) - 1.0) > 1e-12:
            raise ConfigError(f"{name} weights must sum to one")


def load_config(
    path: str | Path = "configs/project.yaml", *, project_root: str | Path | None = None
) -> ProjectConfig:
    config_path = Path(path).resolve()
    root = Path(project_root).resolve() if project_root else config_path.parent.parent
    parsed = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(parsed, dict):
        raise ConfigError("Project configuration must be a YAML mapping")
    validate_config(parsed)
    frozen = MappingProxyType(parsed)
    return ProjectConfig(
        path=config_path,
        project_root=root,
        data=frozen,
        sha256=hashlib.sha256(_canonical_bytes(parsed)).hexdigest(),
    )
