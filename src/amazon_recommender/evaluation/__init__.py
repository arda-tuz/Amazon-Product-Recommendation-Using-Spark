"""Offline evaluation contracts for ranking, coverage, and ALS prediction."""

from .metrics import (
    BOOK_GROUP,
    EVALUATION_K,
    NON_BOOK_SLICE,
    OVERALL_SLICE,
    ALSPredictionEvaluationFrames,
    RankingEvaluationFrames,
    build_evaluation_population,
    evaluate_als_predictions,
    evaluate_ranking_recommendations,
)

__all__ = [
    "BOOK_GROUP",
    "EVALUATION_K",
    "NON_BOOK_SLICE",
    "OVERALL_SLICE",
    "ALSPredictionEvaluationFrames",
    "RankingEvaluationFrames",
    "build_evaluation_population",
    "evaluate_als_predictions",
    "evaluate_ranking_recommendations",
]
