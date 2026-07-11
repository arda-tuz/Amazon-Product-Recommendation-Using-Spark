"""Weighted reciprocal-rank fusion for the two binding hybrid variants.

Independent recommenders enter this module through one deliberately narrow schema:
``stage, customer_id, product_id, rank``.  Their incomparable raw scores are never
used.  Both H-A and H-B are derived from the exact same canonical candidate rows.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Mapping

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegralType, NumericType


MODEL_NAMES = ("popularity", "als", "fp", "graph", "category")
MODEL_CANDIDATE_DEPTHS: Mapping[str, int] = {
    "popularity": 100,
    "als": 100,
    "fp": 50,
    "graph": 50,
    "category": 50,
}

RRF_C = 60
HYBRID_STORED_DEPTH = 100
EVALUATION_CUTOFF = 10
NDCG_TIE_THRESHOLD = 0.001

H_A_WEIGHTS: Mapping[str, float] = {
    "als": 0.35,
    "graph": 0.20,
    "category": 0.20,
    "fp": 0.15,
    "popularity": 0.10,
}
H_B_WEIGHTS: Mapping[str, float] = {
    "als": 0.50,
    "graph": 0.20,
    "category": 0.10,
    "fp": 0.15,
    "popularity": 0.05,
}


@dataclass(frozen=True)
class HybridFrames:
    """Candidate evidence and the only two permitted hybrid recommendation tables."""

    candidates: DataFrame
    h_a_recommendations: DataFrame
    h_b_recommendations: DataFrame

    def as_dict(self) -> dict[str, DataFrame]:
        return {
            "hybrid_candidates": self.candidates,
            "hybrid_a_recommendations": self.h_a_recommendations,
            "hybrid_b_recommendations": self.h_b_recommendations,
        }


@dataclass(frozen=True)
class HybridSelection:
    """Frozen validation-only choice between H-A and H-B."""

    selected_variant: str
    reason: str
    ndcg_difference: float
    coverage_difference: float


def _require_candidate_frames(candidate_frames: Mapping[str, DataFrame]) -> None:
    if not isinstance(candidate_frames, Mapping):
        raise TypeError("candidate_frames must be a mapping")
    actual = set(candidate_frames)
    expected = set(MODEL_NAMES)
    if actual != expected:
        raise ValueError(
            "hybrid requires exactly popularity, als, fp, graph, and category; "
            f"missing={sorted(expected - actual)}, extra={sorted(actual - expected)}"
        )
    for model in MODEL_NAMES:
        frame = candidate_frames[model]
        required = {"stage", "customer_id", "product_id", "rank"}
        missing = sorted(required.difference(frame.columns))
        if missing:
            raise ValueError(f"{model} candidates are missing columns: {missing}")
        if not isinstance(frame.schema["rank"].dataType, IntegralType):
            raise TypeError(f"{model} rank must be an integral Spark type")


def _candidate_union(candidate_frames: Mapping[str, DataFrame]) -> DataFrame:
    _require_candidate_frames(candidate_frames)
    union: DataFrame | None = None
    for model in MODEL_NAMES:
        maximum_rank = MODEL_CANDIDATE_DEPTHS[model]
        frame = candidate_frames[model].select(
            "stage",
            "customer_id",
            "product_id",
            F.col("rank").cast("int").alias("rank"),
        )
        checked = frame.withColumn(
            "rank",
            F.when(
                F.col("rank").isNull()
                | (F.col("rank") < F.lit(1))
                | (F.col("rank") > F.lit(maximum_rank)),
                F.raise_error(
                    F.lit(
                        f"RRF rank for {model} must be within 1..{maximum_rank}"
                    )
                ),
            ).otherwise(F.col("rank")),
        ).withColumn("model", F.lit(model))
        union = checked if union is None else union.unionByName(checked)
    assert union is not None

    # A model may contribute to a user-item pair only once.  Failing loudly keeps
    # upstream contract bugs from silently increasing an item's RRF contribution.
    return (
        union.groupBy("stage", "customer_id", "product_id", "model")
        .agg(
            F.min("rank").cast("int").alias("rank"),
            F.count(F.lit(1)).alias("_source_occurrences"),
        )
        .withColumn(
            "rank",
            F.when(
                F.col("_source_occurrences") != F.lit(1),
                F.raise_error(
                    F.lit("duplicate model candidate for one stage/user/product")
                ),
            ).otherwise(F.col("rank")),
        )
        .drop("_source_occurrences")
    )


def _weight_expression(weights: Mapping[str, float]):
    entries = []
    for model in MODEL_NAMES:
        entries.extend((F.lit(model), F.lit(float(weights[model]))))
    return F.element_at(F.create_map(*entries), F.col("model"))


def _candidate_evidence(long_candidates: DataFrame, bayesian_scores: DataFrame) -> DataFrame:
    required = {"product_id", "global_bayesian_score"}
    missing = sorted(required.difference(bayesian_scores.columns))
    if missing:
        raise ValueError(f"bayesian_scores are missing columns: {missing}")
    if not isinstance(
        bayesian_scores.schema["global_bayesian_score"].dataType, NumericType
    ):
        raise TypeError("global_bayesian_score must be numeric")

    by_product = bayesian_scores.groupBy("product_id").agg(
        F.max(F.col("global_bayesian_score").cast("double")).alias(
            "global_bayesian_score"
        ),
        F.countDistinct(F.col("global_bayesian_score")).alias("_bayesian_values"),
    )
    ranks = long_candidates.groupBy("stage", "customer_id", "product_id").agg(
        F.countDistinct("model").cast("int").alias("contributing_model_count"),
        F.min(F.when(F.col("model") == "als", F.col("rank"))).cast("int").alias(
            "als_rank"
        ),
        F.min(F.when(F.col("model") == "graph", F.col("rank"))).cast("int").alias(
            "graph_rank"
        ),
        F.min(F.when(F.col("model") == "category", F.col("rank")))
        .cast("int")
        .alias("category_rank"),
        F.min(F.when(F.col("model") == "fp", F.col("rank"))).cast("int").alias(
            "fp_rank"
        ),
        F.min(F.when(F.col("model") == "popularity", F.col("rank")))
        .cast("int")
        .alias("popularity_rank"),
        F.map_from_entries(
            F.sort_array(
                F.collect_list(F.struct(F.col("model"), F.col("rank")))
            )
        ).alias("model_ranks"),
    )
    return (
        ranks.join(by_product, "product_id", "left")
        .withColumn(
            "global_bayesian_score",
            F.when(
                F.isnan(F.col("global_bayesian_score"))
                | (F.coalesce(F.col("_bayesian_values"), F.lit(0)) > F.lit(1)),
                F.raise_error(
                    F.lit("hybrid Bayesian tie evidence must be finite and unique")
                ),
            ).otherwise(F.col("global_bayesian_score")),
        )
        .withColumn(
            "has_bayesian_score", F.col("global_bayesian_score").isNotNull()
        )
        .drop("_bayesian_values")
    )


def _score_variant(
    long_candidates: DataFrame,
    evidence: DataFrame,
    *,
    variant: str,
    weights: Mapping[str, float],
) -> DataFrame:
    weight_entries = []
    for model in MODEL_NAMES:
        weight_entries.extend((F.lit(model), F.lit(float(weights[model]))))
    weight_map = F.create_map(*weight_entries)

    weighted = long_candidates.withColumn("_base_weight", _weight_expression(weights))
    active = weighted.select(
        "stage", "customer_id", "model", "_base_weight"
    ).dropDuplicates()
    active_by_user = (
        active.groupBy("stage", "customer_id")
        .agg(
            F.count(F.lit(1)).cast("int").alias("active_model_count"),
            F.sort_array(F.collect_set("model")).alias("active_models"),
        )
        .withColumn(
            "active_weight_sum",
            F.aggregate(
                F.col("active_models"),
                F.lit(0.0),
                lambda total, model: total + F.element_at(weight_map, model),
            ),
        )
    )

    # Compute every item score from the canonical, sorted ``model_ranks`` map.
    # A distributed ``groupBy().sum(double)`` may add the same contributions in
    # different orders and differ by one ULP.  On exact RRF ties that is enough to
    # swap the item at the top-100 boundary.  The map aggregate below has one
    # deterministic order and is also the independent expression used by G8's
    # full-data contract validator.
    with_active = evidence.join(active_by_user, ["stage", "customer_id"], "inner")
    contribution_map = F.transform_values(
        F.col("model_ranks"),
        lambda model, rank: (
            F.element_at(weight_map, model) / F.col("active_weight_sum")
        )
        / (F.lit(float(RRF_C)) + rank.cast("double")),
    )
    deterministic_score = F.aggregate(
        F.map_entries(F.col("model_ranks")),
        F.lit(0.0),
        lambda total, entry: total
        + (
            F.element_at(weight_map, entry["key"])
            / F.col("active_weight_sum")
        )
        / (F.lit(float(RRF_C)) + entry["value"].cast("double")),
    )
    scored = (
        with_active.withColumn("model_contributions", contribution_map)
        .withColumn("hybrid_score", deterministic_score)
        .withColumn("hybrid_variant", F.lit(variant))
    )
    order = Window.partitionBy("stage", "customer_id").orderBy(
        F.col("hybrid_score").desc(),
        F.col("contributing_model_count").desc(),
        F.col("global_bayesian_score").desc_nulls_last(),
        F.col("product_id").asc(),
    )
    return (
        scored.withColumn("rank", F.row_number().over(order))
        .filter(F.col("rank") <= F.lit(HYBRID_STORED_DEPTH))
        .select(
            "stage",
            "customer_id",
            "product_id",
            "rank",
            "hybrid_variant",
            "hybrid_score",
            "contributing_model_count",
            "global_bayesian_score",
            "has_bayesian_score",
            "active_model_count",
            "active_models",
            "active_weight_sum",
            "als_rank",
            "graph_rank",
            "category_rank",
            "fp_rank",
            "popularity_rank",
            "model_ranks",
            "model_contributions",
        )
    )


def score_materialized_hybrid_candidates(
    candidates: DataFrame,
    *,
    variant: str,
    weights: Mapping[str, float],
) -> DataFrame:
    """Score one approved variant from the durable canonical evidence table.

    Reading the materialized G8 candidate table cuts the five G7 source lineages
    before H-A/H-B scoring.  Expanding the canonical rank map is lossless and keeps
    both variants on exactly the same candidate evidence.
    """

    required = {
        "stage",
        "customer_id",
        "product_id",
        "model_ranks",
        "contributing_model_count",
        "global_bayesian_score",
        "has_bayesian_score",
    }
    missing = sorted(required.difference(candidates.columns))
    if missing:
        raise ValueError(f"materialized hybrid candidates are missing: {missing}")
    if variant not in {"h_a", "h_b"} or set(weights) != set(MODEL_NAMES):
        raise ValueError(f"unsupported hybrid variant: {variant}")
    long_candidates = candidates.select(
        "stage",
        "customer_id",
        "product_id",
        F.explode(F.map_entries("model_ranks")).alias("entry"),
    ).select(
        "stage",
        "customer_id",
        "product_id",
        F.col("entry.key").alias("model"),
        F.col("entry.value").cast("int").alias("rank"),
    )
    return _score_variant(
        long_candidates,
        candidates,
        variant=variant,
        weights=weights,
    )


def build_hybrid_frames(
    candidate_frames: Mapping[str, DataFrame], bayesian_scores: DataFrame
) -> HybridFrames:
    """Build H-A and H-B once from a shared, rank-only candidate universe."""

    long_candidates = _candidate_union(candidate_frames)
    evidence = _candidate_evidence(long_candidates, bayesian_scores)
    h_a = _score_variant(
        long_candidates, evidence, variant="h_a", weights=H_A_WEIGHTS
    )
    h_b = _score_variant(
        long_candidates, evidence, variant="h_b", weights=H_B_WEIGHTS
    )
    return HybridFrames(evidence, h_a, h_b)


def select_hybrid_configuration(
    *,
    h_a_ndcg_at_10: float,
    h_a_user_coverage: float,
    h_b_ndcg_at_10: float,
    h_b_user_coverage: float,
) -> HybridSelection:
    """Apply the binding validation-only winner rule; test metrics are not inputs."""

    values = {
        "h_a_ndcg_at_10": h_a_ndcg_at_10,
        "h_a_user_coverage": h_a_user_coverage,
        "h_b_ndcg_at_10": h_b_ndcg_at_10,
        "h_b_user_coverage": h_b_user_coverage,
    }
    checked: dict[str, float] = {}
    for name, value in values.items():
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TypeError(f"{name} must be numeric")
        checked[name] = float(value)
        if not math.isfinite(checked[name]) or not 0.0 <= checked[name] <= 1.0:
            raise ValueError(f"{name} must be finite and within [0, 1]")

    ndcg_difference = checked["h_a_ndcg_at_10"] - checked["h_b_ndcg_at_10"]
    coverage_difference = (
        checked["h_a_user_coverage"] - checked["h_b_user_coverage"]
    )
    if abs(ndcg_difference) >= NDCG_TIE_THRESHOLD:
        selected = "h_a" if ndcg_difference > 0.0 else "h_b"
        reason = "higher_validation_ndcg_at_10"
    elif coverage_difference != 0.0:
        selected = "h_a" if coverage_difference > 0.0 else "h_b"
        reason = "ndcg_tie_higher_user_coverage"
    else:
        selected = "h_a"
        reason = "ndcg_and_coverage_tie_default_h_a"
    return HybridSelection(
        selected_variant=selected,
        reason=reason,
        ndcg_difference=ndcg_difference,
        coverage_difference=coverage_difference,
    )
