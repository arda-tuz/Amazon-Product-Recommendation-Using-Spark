"""Page 1 — Catalog Observatory overview and data quality."""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from app.lib.charts import AMBER, MOSS, horizontal_bars, polish
from app.lib.components import (
    configure_page,
    empty_state,
    hero,
    kpis,
    safe_dataframe,
    section,
    source_note,
)
from app.lib.runtime import choose_run, read


configure_page("Genel Bakış ve Veri Kalitesi", icon="◫")
context = choose_run()
hero(
    "Sayfa 01 · Katalog anatomisi",
    "Ölçeği gör, kusuru saklama.",
    "Katalog hacmi, değerlendirme davranışı ve temizleme kararları aynı kaynak izi altında uzlaştırılır.",
    stamp="Tam veri profili · DuckDB + Parquet",
)
if context is None:
    empty_state("Koşum bulunamadı", "Başarılı bir artefakt koşumu seçilmeden metrik gösterilemez.")
    st.stop()

metrics = read(context, "overview_metrics")
metric_map = (
    {str(row.metric): row.value for row in metrics.itertuples()}
    if {"metric", "value"}.issubset(metrics.columns)
    else {}
)
rollup = read(context, "catalog_rollup")
catalog = rollup.iloc[0].to_dict() if not rollup.empty else {}

kpis(
    [
        ("Ürün", catalog.get("products", metric_map.get("products")), "Katalog kayıtları"),
        ("Aktif", catalog.get("active_products"), "Önerilebilir normal ürün"),
        ("Durdurulmuş", catalog.get("discontinued_products"), "Meta verisi korunan ürün"),
        ("Bildirilen yorum", catalog.get("declared_reviews"), "Ürün bloklarındaki total"),
        ("İndirilen yorum", catalog.get("downloaded_reviews"), "Ürün bloklarındaki downloaded"),
        (
            "Fiziksel yorum",
            catalog.get("physical_reviews", metric_map.get("profile_physical_reviews")),
            "Gerçek yorum satırları",
        ),
        ("Müşteri", metric_map.get("distinct_customers"), "Tekil kimlik"),
        ("Kategori", metric_map.get("category_nodes"), "Tekil kategori düğümü"),
        ("İç graf kenarı", metric_map.get("internal_graph_edges"), "Katalog içi yönlü kenar"),
    ]
)

section("Katalog bileşimi", "01", "Nadir gruplar log ölçekte")
groups = read(context, "product_group_distribution")
if groups.empty:
    empty_state("Grup dağılımı yok", "Ürün kataloğu henüz tamamlanmış Parquet olarak bulunamadı.")
else:
    st.plotly_chart(
        horizontal_bars(
            groups,
            label="product_group",
            value="product_count",
            title="Ürün grupları · logaritmik ürün sayısı",
            log_x=True,
            height=max(360, min(720, 42 * len(groups))),
        ),
        width="stretch",
    )

section("Puan ve zaman", "02", "Tekilleştirilmiş fiziksel yorumlar")
ratings = read(context, "rating_distribution")
years = read(context, "review_year_distribution")
left, right = st.columns(2)
with left:
    if ratings.empty:
        empty_state("Puan dağılımı yok", "Yorum Gold görünümü erişilebilir değil.")
    else:
        figure = go.Figure(
            go.Bar(
                x=ratings["rating"],
                y=ratings["review_count"],
                marker_color=AMBER,
                hovertemplate="%{x} yıldız<br>%{y:,.0f} yorum<extra></extra>",
            )
        )
        figure.update_layout(title="Puan dağılımı", xaxis_title="Puan", yaxis_title="Yorum")
        st.plotly_chart(polish(figure), width="stretch")
with right:
    if years.empty:
        empty_state("Yıl dağılımı yok", "Geçerli tarih içeren yorum görünümü erişilebilir değil.")
    else:
        figure = go.Figure(
            go.Scatter(
                x=years["review_year"],
                y=years["review_count"],
                mode="lines",
                line=dict(color=MOSS, width=2),
                fill="tozeroy",
                fillcolor="rgba(61,89,71,.12)",
                hovertemplate="%{x}<br>%{y:,.0f} yorum<extra></extra>",
            )
        )
        figure.update_layout(title="Yıllara göre yorum", xaxis_title="Yıl", yaxis_title="Yorum")
        st.plotly_chart(polish(figure), width="stretch")

section("Uzun kuyruk ve seyrek matris", "03", "Yaklaşık yüzdelikler")
activity = read(context, "activity_quantiles")
if activity.empty:
    empty_state("Aktivite özeti yok", "Kullanıcı–ürün etkileşim görünümü bulunamadı.")
else:
    safe_dataframe(
        activity.rename(
            columns={
                "entity_type": "Varlık",
                "entities": "Sayı",
                "p50": "P50",
                "p90": "P90",
                "p99": "P99",
                "maximum": "Maksimum",
            }
        ),
        height=145,
    )
    products = float(metric_map.get("products") or 0)
    customers = float(metric_map.get("distinct_customers") or 0)
    interactions = float(metric_map.get("user_item_interactions") or 0)
    sparsity = 1.0 - interactions / (products * customers) if products and customers else None
    if sparsity is not None:
        st.metric("Kullanıcı–ürün matris seyrekliği", f"{sparsity:.8%}")

section("Kalite olayları", "04", "Kesin sayımlar")
quality = read(context, "quality_summary")
if quality.empty:
    empty_state("Kalite özeti yok", "G5 veri kalite görünümü tamamlanmamış.")
else:
    chart_data = (
        quality[quality["event_count"] > 0].copy()
        if "event_count" in quality
        else pd.DataFrame()
    )
    if not chart_data.empty:
        st.plotly_chart(
            horizontal_bars(
                chart_data,
                label="event_type",
                value="event_count",
                title="Sıfırdan büyük kalite olayları",
                log_x=True,
                height=max(360, min(680, len(chart_data) * 38)),
            ),
            width="stretch",
        )
    safe_dataframe(quality, height=430)

event_options = quality["event_type"].tolist() if "event_type" in quality else []
if event_options:
    selected_event = st.selectbox("Kanıt örneklerini incele", event_options)
    samples = read(context, "quality_samples", selected_event)
    safe_dataframe(samples, height=250)

section("Graf sınırı ve kaynak izi", "05")
kpis(
    [
        ("İç katalog kenarı", metric_map.get("internal_graph_edges"), "İki uç da katalogda"),
        ("Yetim hedef", metric_map.get("orphan_graph_targets"), "Tekil ASIN, meta verisi yok"),
        ("Yetim oluşumu", metric_map.get("orphan_graph_target_occurrences"), "Referans oluşumları"),
        ("Ham → tekil fark", metric_map.get("duplicate_review_extra"), "Birebir fazla yorum oluşumu"),
        ("Ortalama uyuşmazlığı", metric_map.get("avg_rating_mismatches"), "nearest-0.5 HALF_UP"),
    ]
)
source_note(
    f"Koşum {context.run_id} · SHA-256 {context.source_sha256 or 'yok'} · "
    "Dağılımlar tamamlanmış run-scoped Gold Parquet tablolarından DuckDB ile okunur; "
    "bu sayfa Spark oturumu başlatmaz."
)
