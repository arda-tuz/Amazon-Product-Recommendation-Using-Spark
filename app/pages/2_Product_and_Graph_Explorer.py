"""Page 2 — bounded product and graph explorer."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from app.lib.charts import ego_figure
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


def display(value: object, fallback: str = "—") -> str:
    try:
        missing = bool(pd.isna(value))
    except (TypeError, ValueError):
        missing = False
    if value is None or missing:
        return fallback
    return str(value)


configure_page("Ürün ve Graf Gezgini", icon="⌘")
context = choose_run()
hero(
    "Sayfa 02 · Ürün merceği",
    "Bir üründen kataloğun topolojisine.",
    "Başlığı ve taksonomiyi okuyun; ardından aynı kaydın PageRank, derece, bileşen ve doğrudan komşuluk kanıtına geçin.",
    stamp="NetworkX ≤ 50 düğüm",
)
if context is None:
    empty_state("Koşum bulunamadı", "Ürün araması için başarılı bir artefakt koşumu gerekir.")
    st.stop()

section("Katalog araması", "01", "ASIN · başlık · grup · kategori")
search_col, page_col = st.columns([4, 1])
with search_col:
    search_term = st.text_input(
        "Arama",
        placeholder="Örn. 0805047905, Python, Book veya kategori etiketi",
        label_visibility="collapsed",
    )
with page_col:
    page = st.number_input("Sayfa", min_value=1, max_value=999, value=1, step=1)

results = read(context, "search_products", search_term, page=int(page), page_size=20)
if results.empty:
    empty_state(
        "Eşleşen ürün yok",
        "Aramayı kısaltın veya farklı bir ASIN, başlık, grup ya da kategori etiketi deneyin.",
    )
    st.stop()

labels: dict[str, int] = {}
for row in results.itertuples(index=False):
    product_id = int(row.product_id)
    asin = display(getattr(row, "asin", None), "ASIN yok")
    title = display(getattr(row, "title", None), "Başlık yok")
    label = f"{asin} · {title[:84]} · #{product_id}"
    labels[label] = product_id
selected_label = st.selectbox("Sonuçtan ürün seçin", list(labels))
selected_id = labels[selected_label]
with st.expander("Bu sayfadaki 20 sonucu tablo olarak gör"):
    safe_dataframe(results, height=420)

detail = read(context, "product_detail", selected_id)
if detail is None:
    empty_state(
        "Bu ürünün meta verisi veri kümesinde bulunmuyor",
        "Kimlik graf hedefi olarak gözlenmiş olabilir; başlık veya grup uydurulmadı.",
    )
    st.stop()

section("Ürün kartı", "02", display(detail.get("asin"), f"product_id={selected_id}"))
st.markdown(f"## {display(detail.get('title'), 'Başlık bulunamadı')}")
st.caption(
    f"{display(detail.get('asin'), 'ASIN yok')} · {display(detail.get('group'), 'Grup yok')} · "
    f"durum: {display(detail.get('status'), 'bilinmiyor')}"
)
kpis(
    [
        ("Kaynak ortalama", detail.get("avg_rating_raw"), "Dosyada bildirilen"),
        ("Hesaplanan ortalama", detail.get("avg_rating_computed"), "Fiziksel yorumlardan"),
        ("İndirilen yorum", detail.get("reviews_downloaded"), "Bildirilen downloaded"),
        ("Fiziksel yorum", detail.get("physical_review_count"), "Gerçek alt kayıt"),
        ("Satış sırası", detail.get("salesrank_clean"), "Geçersizse null"),
        ("PageRank", detail.get("pagerank"), "İç katalog grafı"),
        ("Giriş / çıkış", f"{display(detail.get('in_degree'))} / {display(detail.get('out_degree'))}", "Yönlü derece"),
        ("Bileşen", detail.get("component_id"), "Zayıf bağlı bileşen"),
    ]
)

paths = detail.get("category_paths") or []
if paths:
    st.markdown("#### Kategori yolları")
    for path in paths:
        st.code(str(path), language=None)
else:
    st.caption("Bu ürün için kategori yolu yok.")

section("Graf komşuluğu", "03", "Yalnız birinci derece")
neighbors = read(context, "graph_neighbors", selected_id, limit=49)
if neighbors.empty:
    empty_state(
        "İç katalog komşusu yok",
        "Ürün graf dışında olabilir veya G7 graf çıktıları bu koşumda henüz tamamlanmamıştır.",
    )
else:
    labels_by_id: dict[int, str] = {selected_id: display(detail.get("title"), str(selected_id))}
    for row in neighbors.itertuples(index=False):
        source = int(row.source_product_id)
        target = int(row.target_product_id)
        source_title = getattr(row, "source_title", None)
        target_title = getattr(row, "target_title", None)
        labels_by_id[source] = display(
            source_title,
            "Bu ürünün meta verisi veri kümesinde bulunmuyor",
        )
        labels_by_id[target] = display(
            target_title,
            "Bu ürünün meta verisi veri kümesinde bulunmuyor",
        )
    st.plotly_chart(
        ego_figure(selected_id, neighbors, labels_by_id),
        width="stretch",
    )
    with st.expander("Yönlü kenar kanıtı"):
        safe_dataframe(neighbors, height=360)

source_note(
    f"Koşum {context.run_id} · Ego düzeni yalnız bu seçim için ve en fazla 50 düğümle NetworkX üzerinde hesaplanır. "
    "PageRank, bileşen ve dereceler önceden yayımlanmış G7 Parquet kanıtıdır."
)
