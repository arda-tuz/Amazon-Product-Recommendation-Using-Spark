"""Page 4 — official model, hybrid, runtime and performance comparison."""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from app.lib.charts import AMBER, BRICK, MOSS, metric_bars, polish
from app.lib.components import (
    MODEL_LABELS,
    configure_page,
    empty_state,
    hero,
    notice,
    safe_dataframe,
    section,
    source_note,
)
from app.lib.runtime import choose_run, read


INDEPENDENT = {"popularity", "als", "fp", "graph", "category"}


def label_models(frame: pd.DataFrame, selected: str | None) -> pd.DataFrame:
    result = frame.copy()
    if "model" in result:
        result["model_label"] = result["model"].map(MODEL_LABELS).fillna(result["model"])
        if selected:
            result.loc[result["model"] == selected, "model_label"] += " ★ seçilen"
    return result


configure_page("Model ve Deney Karşılaştırması", icon="▥")
context = choose_run()
hero(
    "Sayfa 04 · Resmî karşılaştırma",
    "Tek protokol. Görünür ödünleşimler.",
    "Doğrulama seçimini, dondurulmuş test sonucunu, kapsama metriklerini ve gerçek çalışma sürelerini aynı koşum kimliği altında okuyun.",
    stamp="K=10 · common_warm + operational",
)
if context is None:
    empty_state("Koşum bulunamadı", "Resmî metrikler yalnız tamamlanmış koşumdan okunur.")
    st.stop()

summary = read(context, "evaluation_summary")
selected = read(context, "selected_variant")
if summary.empty:
    notice(
        "G9 değerlendirmesi henüz hazır değil",
        "NDCG, HitRate, MRR ve kapsam hücreleri sahte değerle doldurulmadı. G9 evaluation_summary başarıyla yayımlandığında bu sayfa otomatik olarak dolar.",
    )
else:
    controls = st.columns(2)
    cohort_values = sorted(summary["cohort"].dropna().astype(str).unique()) if "cohort" in summary else []
    slice_values = sorted(summary["slice"].dropna().astype(str).unique()) if "slice" in summary else []
    with controls[0]:
        cohort = st.selectbox(
            "Kohort",
            cohort_values,
            index=cohort_values.index("common_warm") if "common_warm" in cohort_values else 0,
        ) if cohort_values else None
    with controls[1]:
        target_slice = st.selectbox(
            "Hedef dilimi",
            slice_values,
            index=slice_values.index("overall") if "overall" in slice_values else 0,
        ) if slice_values else None
    filtered = summary.copy()
    if cohort is not None:
        filtered = filtered[filtered["cohort"].astype(str) == cohort]
    if target_slice is not None:
        filtered = filtered[filtered["slice"].astype(str) == target_slice]

    section("Doğrulama", "01", "5 bağımsız + H-A + H-B")
    validation = filtered[filtered["stage"].astype(str) == "validation"] if "stage" in filtered else pd.DataFrame()
    validation = label_models(validation, selected)
    if validation.empty:
        empty_state("Doğrulama satırı yok", "Seçili kohort ve dilim için metrik yayımlanmamış.")
    else:
        st.plotly_chart(
            metric_bars(
                validation,
                x="model_label",
                metrics=["ndcg_at_10", "hit_rate_at_10", "mrr_at_10"],
                title="Doğrulama sıralama kalitesi",
            ),
            width="stretch",
        )
        safe_dataframe(validation, height=330)
        st.caption(
            f"Doğrulamada seçilen hibrit: {MODEL_LABELS.get(selected or '', selected or 'seçim kanıtı yok')} · "
            "eşik |ΔNDCG@10| < 0.001 ise doluluk, ardından H-A bağlayıcı tie-break uygulanır."
        )

    section("Test", "02", "5 bağımsız + doğrulamada seçilen hibrit")
    test = filtered[filtered["stage"].astype(str) == "test"] if "stage" in filtered else pd.DataFrame()
    if "model" in test and selected:
        test = test[test["model"].isin([*INDEPENDENT, selected])]
    test = label_models(test, selected)
    if test.empty:
        empty_state("Test satırı yok", "Seçili kohort ve dilim için dondurulmuş test sonucu bulunamadı.")
    else:
        st.plotly_chart(
            metric_bars(
                test,
                x="model_label",
                metrics=["ndcg_at_10", "hit_rate_at_10", "mrr_at_10"],
                title="Dondurulmuş test sıralama kalitesi",
            ),
            width="stretch",
        )
        coverage_columns = [
            column
            for column in (
                "model_label",
                "evaluated_users",
                "users_with_output",
                "user_coverage",
                "fill_rate_at_10",
                "catalog_coverage_at_10",
                "distinct_recommended_products_at_10",
                "active_catalog_size",
            )
            if column in test
        ]
        safe_dataframe(test[coverage_columns], height=300)

section("ALS puan tahmini", "03", "Bütün held-out puanlar · ham tahmin")
als = read(context, "als_prediction_summary")
if als.empty:
    empty_state("ALS RMSE/MAE kanıtı yok", "G9 als_prediction_summary henüz yayımlanmamış.")
else:
    safe_dataframe(als, height=210)

section("Model süreleri", "04", "Eğitim ve aday üretimi")
runtimes = read(context, "model_runtime")
if runtimes.empty:
    empty_state("Süre özeti yok", "G7 model_runtime_summary tamamlanmamış.")
else:
    runtime = runtimes.copy()
    runtime["model_label"] = runtime["model"].map(MODEL_LABELS).fillna(runtime["model"])
    figure = go.Figure()
    if "training_seconds" in runtime:
        figure.add_bar(name="Eğitim", x=runtime["model_label"], y=runtime["training_seconds"], marker_color=AMBER)
    if "candidate_generation_seconds" in runtime:
        figure.add_bar(name="Aday üretimi", x=runtime["model_label"], y=runtime["candidate_generation_seconds"], marker_color=MOSS)
    figure.update_layout(title="Gerçek duvar saati", barmode="stack", yaxis_title="Saniye")
    st.plotly_chart(polish(figure, height=390), width="stretch")
    safe_dataframe(runtime, height=250)

section("Yerel çok çekirdek deneyi", "05", "local[1] ↔ local[min(4, logical cores)]")
performance = read(context, "performance_summary")
if performance.empty:
    notice(
        "G11 performans deneyi bekleniyor",
        "Bu eksiklik yatay ölçekleme sonucu olarak yorumlanmadı. G11 tamamlandığında 1 ısınma + 3 ölçüm, medyan ve tekil süreler burada gösterilir.",
    )
else:
    safe_dataframe(performance, height=280)

section("Tekrarlanabilirlik kaydı", "06")
versions = read(context, "software_versions")
left, right = st.columns([1.2, 1])
with left:
    st.code(
        f"run_id={context.run_id}\nsource_sha256={context.source_sha256 or 'yok'}\n"
        f"last_passed_gate=G{context.last_passed_gate}\nrecorded_at={context.recorded_at or 'yok'}",
        language=None,
    )
with right:
    safe_dataframe(
        pd.DataFrame([{"bileşen": key, "sürüm": value} for key, value in versions.items()]),
        height=240,
    )
if context.manifest_path:
    st.caption(f"Manifest: {context.manifest_path}")

source_note(
    "Karşılaştırma yalnız yayımlanmış metrik satırlarını gösterir. Book / non-Book ayrımı hedef ürün grubuna göre, aynı dondurulmuş aday listeleri üzerinde hesaplanır."
)
