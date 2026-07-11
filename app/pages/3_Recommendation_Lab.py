"""Page 3 — recommendation laboratory."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from app.lib.components import (
    MODEL_LABELS,
    configure_page,
    empty_state,
    hero,
    notice,
    recommendation_card,
    safe_dataframe,
    section,
    source_note,
)
from app.lib.recommendation import compute_custom_rrf, evidence_explanation
from app.lib.runtime import choose_run, read


H_A = {"als": 0.35, "graph": 0.20, "category": 0.20, "fp": 0.15, "popularity": 0.10}
H_B = {"als": 0.50, "graph": 0.20, "category": 0.10, "fp": 0.15, "popularity": 0.05}


def render_cards(frame: pd.DataFrame, *, explanation_override: str | None = None) -> None:
    if frame.empty:
        empty_state(
            "Öneri listesi yok",
            "Bu kullanıcı, aşama ve model birleşimi için tamamlanmış aday listesi bulunamadı; boşluk fallback ile gizlenmedi.",
        )
        return
    for row in frame.to_dict("records"):
        recommendation_card(row, explanation_override or evidence_explanation(row))


configure_page("Öneri Laboratuvarı", icon="✦")
context = choose_run()
hero(
    "Sayfa 03 · Kanıt laboratuvarı",
    "Sırayı gör. Nedenini de gör.",
    "Önceden hesaplanmış adayları model, kullanıcı ve bağlam düzeyinde inceleyin. Resmî sonuç ile etkileşimli keşif birbirine karıştırılmaz.",
    stamp="5 model · H-A · H-B · seçilen",
)
if context is None:
    empty_state("Koşum bulunamadı", "Öneri artefaktlarını okumak için başarılı bir koşum gerekir.")
    st.stop()

mode = st.radio(
    "Kullanım modu",
    ["Mevcut müşteri", "Başlangıç ürünü / sepeti", "Yeni kullanıcı kategorisi"],
    horizontal=True,
)

if mode == "Mevcut müşteri":
    section("Sunulabilir müşteri seçimi", "01", "1,55 milyon kimlik açılır listeye yüklenmez")
    demos = read(context, "demo_users")
    demo_values = demos["customer_id"].astype(str).tolist() if "customer_id" in demos else []
    if demo_values:
        demo = st.selectbox("Hazır demo müşterisi", ["—"] + demo_values)
    else:
        demo = "—"
        st.caption("Bu koşumda demo_users dışa aktarımı henüz yok; arama kullanılabilir.")
    search = st.text_input("Müşteri kimliğinde ara", value="" if demo == "—" else demo)
    customers = read(context, "search_customers", search, limit=20) if search else pd.DataFrame()
    candidates = customers["customer_id"].astype(str).tolist() if "customer_id" in customers else []
    if demo != "—" and demo not in candidates:
        candidates.insert(0, demo)
    if not candidates:
        empty_state(
            "Sunulabilir müşteri seçin",
            "Arama yalnız evaluation/demo evreninde yapılır; rastgele müşteri için çevrimiçi öneri üretilmez.",
        )
        st.stop()
    customer_id = st.selectbox("Müşteri", candidates)
    stages = read(context, "customer_stages", customer_id) or ["test", "validation"]
    stage = st.selectbox("Aşama", stages, index=stages.index("test") if "test" in stages else 0)

    models = read(context, "available_models")
    if not models:
        empty_state("Model çıktıları yok", "G7 ve G8 aday Parquet tabloları tamamlandığında modeller burada görünür.")
        st.stop()
    model = st.selectbox(
        "Model / hibrit",
        models,
        format_func=lambda value: MODEL_LABELS.get(value, value),
    )
    controls = st.columns([1, 1, 1])
    with controls[0]:
        top_k = st.slider("İlk-K", 1, 50, 10)
    with controls[1]:
        hide_seen = st.toggle("Görülmüş ürünleri gizle", value=True)
    raw = read(
        context,
        "recommendations",
        model=model,
        stage=stage,
        customer_id=customer_id,
        top_k=100,
        product_group=None,
        hide_seen=hide_seen,
    )
    groups = sorted(raw["group"].dropna().astype(str).unique()) if "group" in raw else []
    with controls[2]:
        group = st.selectbox("Ürün grubu", ["Tümü", *groups])
    shown = raw if group == "Tümü" or "group" not in raw else raw[raw["group"] == group]
    shown = shown.head(top_k)

    if model in {"h_a", "h_b", "selected"}:
        selected_variant = read(context, "selected_variant")
        st.caption(
            f"H-A ağırlıkları: {H_A} · H-B ağırlıkları: {H_B} · "
            f"doğrulamada seçilen: {selected_variant or 'G9 bekleniyor'} · RRF c=60"
        )
    section("Sıralı öneriler", "02", MODEL_LABELS.get(model, model))
    render_cards(shown)

    section("Etkileşimli RRF keşfi", "03", "Resmî deney değildir")
    with st.expander("Özel ağırlıkları aç"):
        notice(
            "Yalnız keşif",
            "Bu slider sonuçları H-A/H-B ya da resmî test sonucu değildir; yeni model eğitmez ve manifestoya yazılmaz. Aday kanıtı G7'nin dondurulmuş sıralarıdır.",
        )
        weight_columns = st.columns(5)
        weights = {}
        for column, name in zip(weight_columns, ("als", "graph", "category", "fp", "popularity"), strict=True):
            with column:
                weights[name] = st.slider(name.upper(), 0.0, 1.0, float(H_A[name]), 0.05)
        evidence = read(context, "recommendation_evidence", stage, customer_id)
        if evidence.empty or sum(weights.values()) <= 0:
            empty_state("RRF kanıtı yok", "En az bir model sırası ve pozitif ağırlık gerekir.")
        else:
            exploratory = compute_custom_rrf(evidence, weights, depth=min(top_k, 100))
            safe_dataframe(exploratory, height=360)

elif mode == "Başlangıç ürünü / sepeti":
    section("Küçük başlangıç sepeti", "01", "En fazla 5 ürün")
    query = st.text_input("Başlangıç ürünü ara", placeholder="ASIN veya başlık")
    matches = read(context, "search_products", query, page=1, page_size=20) if query else pd.DataFrame()
    if matches.empty:
        empty_state("Başlangıç ürünü seçin", "Arama sonucundan en fazla beş katalog ürünü seçebilirsiniz.")
    else:
        options = {
            f"{row.asin} · {str(row.title)[:70]} · #{int(row.product_id)}": int(row.product_id)
            for row in matches.itertuples()
        }
        selected = st.multiselect("Başlangıç ürünleri", list(options), max_selections=5)
        seed_ids = [options[label] for label in selected]
        top_k = st.slider("İlk-K", 1, 30, 10, key="seed_top_k")
        recs = read(context, "seed_graph_recommendations", seed_ids, top_k=top_k) if seed_ids else pd.DataFrame()
        notice(
            "Graf keşif modu",
            "Bu liste seçilen ürünlerin önceden yayımlanmış birinci derece graf komşularını birleştirir; resmî kullanıcı-kohort metriği veya hibrit sonucu değildir.",
        )
        render_cards(
            recs,
            explanation_override="Seçilen başlangıç ürünlerinden en az biri bu ürünü doğrudan benzer ürün olarak işaretliyor.",
        )

else:
    section("Kategoriyle ilk temas", "01", "Yeni kullanıcı · önceden hesaplanmış kategori üst listesi")
    query = st.text_input("Kategori ara", placeholder="Örn. Science, Music, Software")
    categories = read(context, "search_categories", query, limit=30) if query else pd.DataFrame()
    if categories.empty:
        empty_state("Kategori seçin", "Etikette arama yaparak yeni kullanıcı başlangıç listesini açın.")
    else:
        options = {
            f"{row.category_label} · {int(row.product_count):,} ürün · #{int(row.category_id)}": int(row.category_id)
            for row in categories.itertuples()
        }
        selected = st.selectbox("Kategori", list(options))
        top_k = st.slider("İlk-K", 1, 30, 10, key="category_top_k")
        recs = read(context, "category_onboarding", options[selected], top_k=top_k)
        notice(
            "Soğuk başlangıç kapsamı",
            "Liste, eğitimdeki kişisel geçmiş yerine seçili kategori için önceden hesaplanmış aktif ve Bayesçi popüler ürünleri kullanır; kişiselleştirilmiş test metriği değildir.",
        )
        render_cards(
            recs.rename(columns={"category_product_rank": "rank"}),
            explanation_override="Seçili kategorinin önceden hesaplanmış üst ürünleri arasında ve aktif katalogda.",
        )

source_note(
    f"Koşum {context.run_id} · Kullanıcı listeleri yalnız sunulabilir evrenden gelir. "
    "FP kanıtı satın alma sepeti anlamına gelmez; ifade birlikte olumlu değerlendirme davranışını anlatır."
)
