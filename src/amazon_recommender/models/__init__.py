"""Pure recommendation-model mathematics.

The Spark-facing model implementations build on these deterministic helpers.  Keeping
the formulas here free of Spark makes the binding mathematical contract cheap to test
before any full-data job is allowed to run.
"""

from .math import (
    BAYES_PRIOR_COUNT,
    FP_MINIMUM_COUNT_FLOOR,
    FP_MINIMUM_SUPPORT_FRACTION,
    H_A_WEIGHTS,
    H_B_WEIGHTS,
    RANKING_CUTOFF,
    RRF_C,
    RRF_MAX_ITEMS,
    AssociationRuleStatistics,
    RRFRankedItem,
    SinglePositiveMetrics,
    association_confidence,
    association_lift,
    association_rule_statistics,
    bayesian_weighted_rating,
    category_depth_weight,
    category_idf,
    cosine_similarity,
    fp_minimum_count,
    fp_minimum_support,
    graph_position_decay,
    graph_seed_contribution,
    preference_weight,
    rule_strength,
    single_positive_metrics_at_10,
    weighted_rrf,
)

__all__ = [
    "BAYES_PRIOR_COUNT",
    "FP_MINIMUM_COUNT_FLOOR",
    "FP_MINIMUM_SUPPORT_FRACTION",
    "H_A_WEIGHTS",
    "H_B_WEIGHTS",
    "RANKING_CUTOFF",
    "RRF_C",
    "RRF_MAX_ITEMS",
    "AssociationRuleStatistics",
    "RRFRankedItem",
    "SinglePositiveMetrics",
    "association_confidence",
    "association_lift",
    "association_rule_statistics",
    "bayesian_weighted_rating",
    "category_depth_weight",
    "category_idf",
    "cosine_similarity",
    "fp_minimum_count",
    "fp_minimum_support",
    "graph_position_decay",
    "graph_seed_contribution",
    "preference_weight",
    "rule_strength",
    "single_positive_metrics_at_10",
    "weighted_rrf",
]
