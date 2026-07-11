"""Deterministic implementations of the project's binding model formulas.

No helper in this module depends on Spark, NumPy, or mutable process state.  Constants
that the implementation specification fixes are intentionally not exposed as function
arguments: callers cannot silently turn a production run into another experiment.
"""

from __future__ import annotations

import math
from collections.abc import Hashable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from numbers import Integral, Real
from types import MappingProxyType
from typing import Final

BAYES_PRIOR_COUNT: Final[int] = 20
FP_MINIMUM_SUPPORT_FRACTION: Final[float] = 0.001
FP_MINIMUM_COUNT_FLOOR: Final[int] = 200
GRAPH_RECIPROCAL_BONUS: Final[float] = 0.25
GRAPH_TWO_STEP_FACTOR: Final[float] = 0.50
RRF_C: Final[int] = 60
RRF_MAX_ITEMS: Final[int] = 100
RANKING_CUTOFF: Final[int] = 10

H_A_WEIGHTS: Final[Mapping[str, float]] = MappingProxyType(
    {"als": 0.35, "graph": 0.20, "category": 0.20, "fp": 0.15, "popularity": 0.10}
)
H_B_WEIGHTS: Final[Mapping[str, float]] = MappingProxyType(
    {"als": 0.50, "graph": 0.20, "category": 0.10, "fp": 0.15, "popularity": 0.05}
)


@dataclass(frozen=True, slots=True)
class AssociationRuleStatistics:
    """The three derived statistics for a singleton association rule."""

    confidence: float
    lift: float
    strength: float


@dataclass(frozen=True, slots=True)
class RRFRankedItem:
    """One product in a deterministic weighted-RRF result."""

    product_id: int
    score: float
    contributing_model_count: int
    bayesian_score: float
    model_ranks: tuple[tuple[str, int], ...]

    def rank_from(self, model: str) -> int | None:
        """Return the one-based rank supplied by ``model``, if any."""

        return next((rank for name, rank in self.model_ranks if name == model), None)


@dataclass(frozen=True, slots=True)
class SinglePositiveMetrics:
    """Per-user ranking metrics for one held-out positive target."""

    ndcg_at_10: float
    hit_rate_at_10: float
    mrr_at_10: float


def _finite_real(value: Real, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise TypeError(f"{name} must be a real number")
    converted = float(value)
    if not math.isfinite(converted):
        raise ValueError(f"{name} must be finite")
    return converted


def _integer(value: int, name: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, Integral):
        raise TypeError(f"{name} must be an integer")
    converted = int(value)
    if converted < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    return converted


def preference_weight(rating: Real) -> float:
    """Return ``q_ui = clip((rating - 3) / 2, 0, 1)``."""

    value = _finite_real(rating, "rating")
    return min(1.0, max(0.0, (value - 3.0) / 2.0))


def bayesian_weighted_rating(
    global_mean: Real,
    item_mean: Real,
    unique_rater_count: int,
) -> float:
    """Return the fixed-``m=20`` Bayesian weighted item rating."""

    global_value = _finite_real(global_mean, "global_mean")
    item_value = _finite_real(item_mean, "item_mean")
    if not 1.0 <= global_value <= 5.0:
        raise ValueError("global_mean must be in the inclusive range [1, 5]")
    if not 1.0 <= item_value <= 5.0:
        raise ValueError("item_mean must be in the inclusive range [1, 5]")
    count = _integer(unique_rater_count, "unique_rater_count")
    denominator = count + BAYES_PRIOR_COUNT
    return (count * item_value + BAYES_PRIOR_COUNT * global_value) / denominator


def fp_minimum_count(basket_count: int) -> int:
    """Return ``max(ceil(0.001 * B), 200)`` without float drift."""

    baskets = _integer(basket_count, "basket_count", minimum=1)
    fractional_count = (baskets + 999) // 1000
    return max(fractional_count, FP_MINIMUM_COUNT_FLOOR)


def fp_minimum_support(basket_count: int) -> float:
    """Return the binding FP-Growth minimum support for ``basket_count``."""

    baskets = _integer(basket_count, "basket_count", minimum=1)
    return fp_minimum_count(baskets) / baskets


def association_confidence(pair_count: int, antecedent_count: int) -> float:
    """Return ``count(A,B) / count(A)`` for a singleton rule."""

    antecedent = _integer(antecedent_count, "antecedent_count", minimum=1)
    pair = _integer(pair_count, "pair_count")
    if pair > antecedent:
        raise ValueError("pair_count cannot exceed antecedent_count")
    return pair / antecedent


def association_lift(
    confidence: Real,
    consequent_count: int,
    basket_count: int,
) -> float:
    """Return ``confidence / (count(B) / basket_count)``."""

    confidence_value = _finite_real(confidence, "confidence")
    if not 0.0 <= confidence_value <= 1.0:
        raise ValueError("confidence must be in the inclusive range [0, 1]")
    baskets = _integer(basket_count, "basket_count", minimum=1)
    consequent = _integer(consequent_count, "consequent_count", minimum=1)
    if consequent > baskets:
        raise ValueError("consequent_count cannot exceed basket_count")
    return confidence_value / (consequent / baskets)


def rule_strength(confidence: Real, lift: Real) -> float:
    """Return ``confidence * log2(lift)``."""

    confidence_value = _finite_real(confidence, "confidence")
    lift_value = _finite_real(lift, "lift")
    if not 0.0 <= confidence_value <= 1.0:
        raise ValueError("confidence must be in the inclusive range [0, 1]")
    if lift_value <= 0.0:
        raise ValueError("lift must be > 0")
    return confidence_value * math.log2(lift_value)


def association_rule_statistics(
    pair_count: int,
    antecedent_count: int,
    consequent_count: int,
    basket_count: int,
) -> AssociationRuleStatistics:
    """Compute confidence, lift, and strength from exact rule counts."""

    baskets = _integer(basket_count, "basket_count", minimum=1)
    antecedent = _integer(antecedent_count, "antecedent_count", minimum=1)
    consequent = _integer(consequent_count, "consequent_count", minimum=1)
    pair = _integer(pair_count, "pair_count")
    if antecedent > baskets:
        raise ValueError("antecedent_count cannot exceed basket_count")
    if consequent > baskets:
        raise ValueError("consequent_count cannot exceed basket_count")
    if pair > min(antecedent, consequent):
        raise ValueError("pair_count cannot exceed either marginal count")
    confidence = association_confidence(pair, antecedent)
    lift = association_lift(confidence, consequent, baskets)
    return AssociationRuleStatistics(
        confidence=confidence,
        lift=lift,
        strength=rule_strength(confidence, lift),
    )


def graph_position_decay(position: int) -> float:
    """Return ``1 / log2(position + 1)`` for a physical similar rank 1..5."""

    rank = _integer(position, "position", minimum=1)
    if rank > 5:
        raise ValueError("position must be in the inclusive range [1, 5]")
    return 1.0 / math.log2(rank + 1.0)


def graph_seed_contribution(
    preference: Real,
    *,
    direct_position: int | None = None,
    reciprocal: bool = False,
    two_step_positions: Iterable[tuple[int, int]] = (),
) -> float:
    """Return one seed's direct, reciprocal, and two-step graph contribution."""

    q_value = _finite_real(preference, "preference")
    if not 0.0 <= q_value <= 1.0:
        raise ValueError("preference must be in the inclusive range [0, 1]")
    if not isinstance(reciprocal, bool):
        raise TypeError("reciprocal must be a bool")
    if reciprocal and direct_position is None:
        raise ValueError("reciprocal bonus requires a direct edge")

    direct = 0.0
    if direct_position is not None:
        direct = graph_position_decay(direct_position)
        if reciprocal:
            direct *= 1.0 + GRAPH_RECIPROCAL_BONUS

    path_terms: list[float] = []
    for positions in two_step_positions:
        try:
            first_position, second_position = positions
        except (TypeError, ValueError) as error:
            raise ValueError("each two-step path must contain exactly two positions") from error
        path_terms.append(
            graph_position_decay(first_position) * graph_position_decay(second_position)
        )
    two_step = GRAPH_TWO_STEP_FACTOR * math.fsum(path_terms)
    return q_value * (direct + two_step)


def category_idf(product_count: int, document_frequency: int) -> float:
    """Return ``ln((N + 1) / (df + 1)) + 1`` for a category node."""

    products = _integer(product_count, "product_count", minimum=1)
    frequency = _integer(document_frequency, "document_frequency")
    if frequency > products:
        raise ValueError("document_frequency cannot exceed product_count")
    return math.log((products + 1.0) / (frequency + 1.0)) + 1.0


def category_depth_weight(depth: int, path_length: int) -> float:
    """Return one path's ``depth / path_length`` category weight."""

    length = _integer(path_length, "path_length", minimum=1)
    depth_value = _integer(depth, "depth", minimum=1)
    if depth_value > length:
        raise ValueError("depth cannot exceed path_length")
    return depth_value / length


def cosine_similarity(
    left: Mapping[Hashable, Real],
    right: Mapping[Hashable, Real],
) -> float | None:
    """Return sparse cosine similarity, or ``None`` for a zero-norm vector.

    ``None`` deliberately distinguishes the specification's coverage loss from a
    genuine zero cosine score.
    """

    if not isinstance(left, Mapping) or not isinstance(right, Mapping):
        raise TypeError("left and right must be mappings")
    left_values = {key: _finite_real(value, f"left[{key!r}]") for key, value in left.items()}
    right_values = {
        key: _finite_real(value, f"right[{key!r}]") for key, value in right.items()
    }
    left_norm_squared = math.fsum(value * value for value in left_values.values())
    right_norm_squared = math.fsum(value * value for value in right_values.values())
    if left_norm_squared == 0.0 or right_norm_squared == 0.0:
        return None
    dot = math.fsum(
        left_values[key] * right_values[key]
        for key in left_values.keys() & right_values.keys()
    )
    similarity = dot / math.sqrt(left_norm_squared * right_norm_squared)
    return min(1.0, max(-1.0, similarity))


def weighted_rrf(
    candidate_rankings: Mapping[str, Sequence[int]],
    weights: Mapping[str, Real],
    bayesian_scores: Mapping[int, Real],
) -> tuple[RRFRankedItem, ...]:
    """Fuse one user's candidate lists using active-model-normalized weighted RRF.

    The result is limited to the binding top 100 and ordered by score, contributing
    model count, Bayesian score, then ascending product ID.
    """

    if not isinstance(candidate_rankings, Mapping):
        raise TypeError("candidate_rankings must be a mapping")
    if not isinstance(weights, Mapping):
        raise TypeError("weights must be a mapping")
    if not isinstance(bayesian_scores, Mapping):
        raise TypeError("bayesian_scores must be a mapping")

    checked_weights: dict[str, float] = {}
    for model, weight in weights.items():
        if not isinstance(model, str) or not model:
            raise ValueError("model names must be non-empty strings")
        checked = _finite_real(weight, f"weights[{model!r}]")
        if checked < 0.0:
            raise ValueError("model weights must be non-negative")
        checked_weights[model] = checked

    checked_rankings: dict[str, tuple[int, ...]] = {}
    for model, products in candidate_rankings.items():
        if not isinstance(model, str) or not model:
            raise ValueError("model names must be non-empty strings")
        if model not in checked_weights:
            raise ValueError(f"candidate model {model!r} has no configured weight")
        if isinstance(products, (str, bytes)) or not isinstance(products, Sequence):
            raise TypeError(f"candidate list for {model!r} must be a sequence")
        checked_products = tuple(_integer(product, "product_id") for product in products)
        if len(set(checked_products)) != len(checked_products):
            raise ValueError(f"candidate list for {model!r} contains duplicate products")
        checked_rankings[model] = checked_products

    active_models = tuple(sorted(model for model, products in checked_rankings.items() if products))
    if not active_models:
        return ()
    active_weight_sum = math.fsum(checked_weights[model] for model in active_models)
    if active_weight_sum <= 0.0:
        raise ValueError("active model weights must have a positive sum")
    normalized_weights = {
        model: checked_weights[model] / active_weight_sum for model in active_models
    }

    contributions: dict[int, list[float]] = {}
    model_ranks: dict[int, list[tuple[str, int]]] = {}
    for model in active_models:
        for rank, product_id in enumerate(checked_rankings[model], start=1):
            contributions.setdefault(product_id, []).append(
                normalized_weights[model] / (RRF_C + rank)
            )
            model_ranks.setdefault(product_id, []).append((model, rank))

    missing_scores = sorted(product for product in contributions if product not in bayesian_scores)
    if missing_scores:
        preview = ", ".join(map(str, missing_scores[:5]))
        raise ValueError(f"missing Bayesian tie-break score for product(s): {preview}")

    ranked_items: list[RRFRankedItem] = []
    for product_id, parts in contributions.items():
        bayesian_score = _finite_real(
            bayesian_scores[product_id], f"bayesian_scores[{product_id}]"
        )
        ranks = tuple(sorted(model_ranks[product_id]))
        ranked_items.append(
            RRFRankedItem(
                product_id=product_id,
                score=math.fsum(parts),
                contributing_model_count=len(ranks),
                bayesian_score=bayesian_score,
                model_ranks=ranks,
            )
        )

    ranked_items.sort(
        key=lambda item: (
            -item.score,
            -item.contributing_model_count,
            -item.bayesian_score,
            item.product_id,
        )
    )
    return tuple(ranked_items[:RRF_MAX_ITEMS])


def single_positive_metrics_at_10(rank: int | None) -> SinglePositiveMetrics:
    """Return per-user HitRate, MRR, and NDCG at 10 for one target rank."""

    if rank is None:
        return SinglePositiveMetrics(ndcg_at_10=0.0, hit_rate_at_10=0.0, mrr_at_10=0.0)
    rank_value = _integer(rank, "rank", minimum=1)
    if rank_value > RANKING_CUTOFF:
        return SinglePositiveMetrics(ndcg_at_10=0.0, hit_rate_at_10=0.0, mrr_at_10=0.0)
    return SinglePositiveMetrics(
        ndcg_at_10=1.0 / math.log2(rank_value + 1.0),
        hit_rate_at_10=1.0,
        mrr_at_10=1.0 / rank_value,
    )
