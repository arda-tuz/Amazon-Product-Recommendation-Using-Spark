"""Streamlit caching and run selection glue."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Final

import streamlit as st

from app.lib.catalog import RunContext, artifacts_root, discover_runs, select_run
from app.lib.data import DashboardStore


PUBLIC_READS: Final[set[str]] = {
    "overview_metrics",
    "quality_summary",
    "catalog_rollup",
    "quality_samples",
    "product_group_distribution",
    "rating_distribution",
    "review_year_distribution",
    "activity_quantiles",
    "search_products",
    "product_detail",
    "product_categories",
    "graph_neighbors",
    "search_customers",
    "demo_users",
    "customer_stages",
    "selected_variant",
    "available_models",
    "recommendation_evidence",
    "recommendations",
    "search_categories",
    "category_onboarding",
    "seed_graph_recommendations",
    "evaluation_summary",
    "als_prediction_summary",
    "model_runtime",
    "performance_summary",
    "software_versions",
}


@st.cache_resource(show_spinner=False)
def _store(run_id: str, artifacts_path: str) -> DashboardStore:
    context = select_run(run_id, Path(artifacts_path))
    if context is None:
        raise FileNotFoundError(f"Artifact run not found: {run_id}")
    return DashboardStore(context)


@st.cache_data(ttl=600, show_spinner=False, max_entries=512)
def _cached_read(
    run_id: str,
    artifacts_path: str,
    snapshot_token: str,
    method: str,
    args: tuple[Any, ...],
    kwargs_json: str,
) -> Any:
    del snapshot_token  # cache-key-only freshness token
    if method not in PUBLIC_READS:
        raise ValueError(f"Unsupported dashboard read: {method}")
    kwargs = json.loads(kwargs_json)
    return getattr(_store(run_id, artifacts_path), method)(*args, **kwargs)


def read(context: RunContext, method: str, *args: Any, **kwargs: Any) -> Any:
    """Cache one deterministic bounded read for ten minutes."""

    normalized_args = tuple(tuple(value) if isinstance(value, list) else value for value in args)
    return _cached_read(
        context.run_id,
        str(artifacts_root()),
        f"G{context.last_passed_gate}:{context.recorded_at or ''}",
        method,
        normalized_args,
        json.dumps(kwargs, sort_keys=True, ensure_ascii=False),
    )


def choose_run() -> RunContext | None:
    """Render a compact shared run selector and return its context."""

    root = artifacts_root()
    contexts = discover_runs(root)
    if not contexts:
        st.sidebar.error("Başarılı koşum manifestosu bulunamadı.")
        return None
    labels = [context.run_id for context in contexts]
    default = st.session_state.get("dashboard_run_id")
    index = labels.index(default) if default in labels else 0
    selected_id = st.sidebar.selectbox(
        "Artefakt koşumu",
        labels,
        index=index,
        key="dashboard_run_selector",
        help="Yalnız başarılı geçit manifestosu bulunan yerel koşumlar listelenir.",
    )
    st.session_state["dashboard_run_id"] = selected_id
    context = next(item for item in contexts if item.run_id == selected_id)
    readiness = "Değerlendirme hazır" if context.is_evaluation_ready else "G9 bekleniyor"
    st.sidebar.markdown(
        f"<div class='run-stamp' role='status' aria-label='Koşum durumu'><span>SON GEÇİT</span><b>G{context.last_passed_gate}</b>"
        f"<em>{readiness}</em></div>",
        unsafe_allow_html=True,
    )
    if context.source_sha256:
        st.sidebar.caption(f"Veri izi · {context.source_sha256[:12]}…")
    return context
