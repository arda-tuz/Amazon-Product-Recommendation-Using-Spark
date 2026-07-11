"""Small, auditable presentation-only recommendation transforms."""

from __future__ import annotations

from collections.abc import Mapping

import pandas as pd


MODEL_NAMES = ("als", "graph", "category", "fp", "popularity")
RRF_C = 60


def normalize_weights(weights: Mapping[str, float], active: set[str]) -> dict[str, float]:
    """Renormalize only across models with evidence for a candidate universe."""

    unknown = set(weights).difference(MODEL_NAMES)
    if unknown:
        raise ValueError(f"Unknown recommendation models: {sorted(unknown)}")
    denominator = sum(max(0.0, float(weights.get(name, 0.0))) for name in active)
    if denominator <= 0:
        return {}
    return {
        name: max(0.0, float(weights.get(name, 0.0))) / denominator
        for name in active
    }


def compute_custom_rrf(
    evidence: pd.DataFrame,
    weights: Mapping[str, float],
    *,
    depth: int = 100,
    c: int = RRF_C,
) -> pd.DataFrame:
    """Compute explicitly unofficial, rank-only RRF over a bounded evidence frame.

    This helper is used only by the exploratory sliders.  Official H-A/H-B and the
    selected hybrid are always read verbatim from completed Parquet outputs.
    """

    if not 1 <= depth <= 100 or c != RRF_C:
        raise ValueError("Exploratory RRF uses c=60 and a stored depth in [1,100]")
    if "product_id" not in evidence.columns:
        raise ValueError("evidence must include product_id")
    frame = evidence.copy()
    active = {
        model
        for model in MODEL_NAMES
        if f"{model}_rank" in frame.columns and frame[f"{model}_rank"].notna().any()
    }
    normalized = normalize_weights(weights, active)
    frame["exploratory_rrf_score"] = 0.0
    frame["contributing_model_count"] = 0
    if not normalized:
        frame["rank"] = pd.Series(dtype="int64")
        return frame.iloc[0:0]
    for model in MODEL_NAMES:
        rank_column = f"{model}_rank"
        if model not in normalized or rank_column not in frame:
            continue
        valid = frame[rank_column].notna() & (frame[rank_column] > 0)
        frame.loc[valid, "exploratory_rrf_score"] += normalized[model] / (
            c + frame.loc[valid, rank_column].astype(float)
        )
        frame.loc[valid, "contributing_model_count"] += 1
    bayes = (
        frame["global_bayesian_score"]
        if "global_bayesian_score" in frame
        else pd.Series(float("nan"), index=frame.index)
    )
    frame["_bayes"] = bayes
    frame = frame.sort_values(
        ["exploratory_rrf_score", "contributing_model_count", "_bayes", "product_id"],
        ascending=[False, False, False, True],
        na_position="last",
        kind="mergesort",
    ).head(depth)
    frame["rank"] = range(1, len(frame) + 1)
    return frame.drop(columns="_bayes")


def evidence_explanation(row: Mapping[str, object]) -> str:
    """Return a conservative explanation tied only to present evidence columns."""

    fragments: list[str] = []
    if pd.notna(row.get("als_rank")):
        fragments.append("ALS, geçmiş puan örüntüsünde güçlü bir eşleşme buldu")
    if pd.notna(row.get("graph_rank")):
        fragments.append("ürün benzerlik grafında yakın bir bağlantı var")
    if pd.notna(row.get("category_rank")):
        fragments.append("kategori profiliyle içerik benzerliği yüksek")
    if pd.notna(row.get("fp_rank")):
        fragments.append("aynı kullanıcılar tarafından birlikte olumlu değerlendirilmiştir")
    if pd.notna(row.get("popularity_rank")):
        fragments.append("Bayesçi popülerlik kanıtı güçlü")
    if not fragments:
        return "Bu sıra için ayrıntılı model kanıtı dışa aktarılmamış."
    sentence = "; ".join(fragments)
    return sentence[0].upper() + sentence[1:] + "."
