from __future__ import annotations

import ast
import copy
from itertools import combinations
from pathlib import Path
import struct

import pytest

from amazon_recommender.models.hybrid import (
    H_A_WEIGHTS,
    H_B_WEIGHTS,
    MODEL_NAMES,
)


pytestmark = pytest.mark.unit

PROJECT_ROOT = Path(__file__).resolve().parents[2]
HYBRID_SOURCE = PROJECT_ROOT / "src/amazon_recommender/models/hybrid.py"
G8_SOURCE = PROJECT_ROOT / "src/amazon_recommender/phases/g8.py"


def _function(path: Path, name: str) -> ast.FunctionDef:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError(f"function {name!r} not found in {path}")


def _assigned_expression(function: ast.FunctionDef, name: str) -> ast.expr:
    for node in ast.walk(function):
        if not isinstance(node, ast.Assign):
            continue
        if any(isinstance(target, ast.Name) and target.id == name for target in node.targets):
            return node.value
    raise AssertionError(f"assignment {name!r} not found in {function.name}")


def _with_column_expression(function: ast.FunctionDef, column: str) -> ast.expr:
    for node in ast.walk(function):
        if not isinstance(node, ast.Call):
            continue
        if not isinstance(node.func, ast.Attribute) or node.func.attr != "withColumn":
            continue
        if (
            len(node.args) >= 2
            and isinstance(node.args[0], ast.Constant)
            and node.args[0].value == column
        ):
            return node.args[1]
    raise AssertionError(f"withColumn({column!r}, ...) not found in {function.name}")


def _alias_expression(function: ast.FunctionDef, alias: str) -> ast.expr:
    for node in ast.walk(function):
        if not isinstance(node, ast.Call):
            continue
        if not isinstance(node.func, ast.Attribute) or node.func.attr != "alias":
            continue
        if (
            node.args
            and isinstance(node.args[0], ast.Constant)
            and node.args[0].value == alias
        ):
            return node.func.value
    raise AssertionError(f"alias({alias!r}) not found in {function.name}")


class _NormalizeConstants(ast.NodeTransformer):
    def __init__(self, replacements: dict[str, str]) -> None:
        self.replacements = replacements

    def visit_Constant(self, node: ast.Constant) -> ast.Constant:
        if isinstance(node.value, str) and node.value in self.replacements:
            return ast.copy_location(
                ast.Constant(self.replacements[node.value]),
                node,
            )
        return node


def _normalized_dump(node: ast.expr, **replacements: str) -> str:
    normalized = _NormalizeConstants(replacements).visit(copy.deepcopy(node))
    ast.fix_missing_locations(normalized)
    return ast.dump(normalized, include_attributes=False)


def _call_name(node: ast.expr) -> str | None:
    if not isinstance(node, ast.Call):
        return None
    if isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name):
        return f"{node.func.value.id}.{node.func.attr}"
    return None


def _fold_in_canonical_model_order(
    active_models: tuple[str, ...],
    weight_lookup: dict[str, float],
) -> float:
    total = 0.0
    for model in sorted(set(active_models)):
        total = total + weight_lookup[model]
    return total


def test_g8_expected_top_k_uses_the_production_arithmetic_tree() -> None:
    production = _function(HYBRID_SOURCE, "_score_variant")
    validator = _function(G8_SOURCE, "validate_hybrid_recommendations")

    production_models = _alias_expression(production, "active_models")
    validator_models = _alias_expression(validator, "_expected_active_models")
    assert _call_name(production_models) == "F.sort_array"
    assert ast.dump(production_models) == ast.dump(validator_models)

    production_weight = _with_column_expression(production, "active_weight_sum")
    validator_weight = _with_column_expression(
        validator, "_expected_active_weight_sum"
    )
    assert _call_name(production_weight) == "F.aggregate"
    assert _normalized_dump(production_weight) == _normalized_dump(
        validator_weight,
        _expected_active_models="active_models",
    )

    production_score = _assigned_expression(production, "deterministic_score")
    validator_score = _assigned_expression(validator, "candidate_expected_score")
    assert _call_name(production_score) == "F.aggregate"
    assert _normalized_dump(production_score) == _normalized_dump(
        validator_score,
        _expected_active_weight_sum="active_weight_sum",
    )

    production_contributions = _assigned_expression(production, "contribution_map")
    validator_contributions = _with_column_expression(
        validator, "_candidate_expected_contributions"
    )
    assert _call_name(production_contributions) == "F.transform_values"
    assert _normalized_dump(production_contributions) == _normalized_dump(
        validator_contributions,
        _expected_active_weight_sum="active_weight_sum",
    )

    production_order = _assigned_expression(production, "order")
    validator_order = _assigned_expression(validator, "candidate_order")
    assert _normalized_dump(production_order) == _normalized_dump(
        validator_order,
        _candidate_expected_score="hybrid_score",
    )


def test_active_weight_sum_is_bit_identical_for_every_approved_model_subset() -> None:
    checked_subsets = 0
    for weights in (H_A_WEIGHTS, H_B_WEIGHTS):
        production_lookup = {
            model: float(weights[model]) for model in MODEL_NAMES
        }
        validator_lookup = {
            model: float(weight) for model, weight in weights.items()
        }
        assert production_lookup.keys() == validator_lookup.keys()

        for size in range(1, len(MODEL_NAMES) + 1):
            for active_models in combinations(MODEL_NAMES, size):
                production_sum = _fold_in_canonical_model_order(
                    active_models, production_lookup
                )
                validator_sum = _fold_in_canonical_model_order(
                    active_models, validator_lookup
                )
                assert struct.pack(">d", production_sum) == struct.pack(
                    ">d", validator_sum
                ), active_models
                checked_subsets += 1

    assert checked_subsets == 2 * (2 ** len(MODEL_NAMES) - 1) == 62

    # H-A's canonical fold is deliberately one ULP above 1.0; the contract is
    # bit-for-bit agreement between producer and validator, not decimal rounding.
    assert _fold_in_canonical_model_order(
        MODEL_NAMES, {model: float(H_A_WEIGHTS[model]) for model in MODEL_NAMES}
    ).hex() == "0x1.0000000000001p+0"
    assert _fold_in_canonical_model_order(
        MODEL_NAMES, {model: float(H_B_WEIGHTS[model]) for model in MODEL_NAMES}
    ).hex() == "0x1.0000000000000p+0"
