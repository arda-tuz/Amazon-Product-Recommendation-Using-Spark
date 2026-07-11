"""Modern Light Lab visual system and reusable Streamlit fragments."""

from __future__ import annotations

import html
from collections.abc import Iterable, Mapping

import pandas as pd
import streamlit as st


MODEL_LABELS = {
    "popularity": "Bayesçi Popülerlik",
    "als": "ALS",
    "fp": "FP-Growth",
    "graph": "Graf",
    "category": "Kategori",
    "h_a": "H-A · Dengeli",
    "h_b": "H-B · ALS ağırlıklı",
    "selected": "Doğrulamada seçilen hibrit",
}


def configure_page(title: str, *, icon: str = "◈") -> None:
    st.set_page_config(
        page_title=f"{title} · Catalog Observatory",
        page_icon=icon,
        layout="wide",
        initial_sidebar_state="expanded",
    )
    inject_styles()


def inject_styles() -> None:
    st.markdown(
        """
<style>
:root {
  --lab-canvas: #f4f7fb;
  --lab-surface: #ffffff;
  --lab-surface-soft: #edf3f9;
  --lab-ink: #102033;
  --lab-muted: #5b6878;
  --lab-line: #d8e0ea;
  --lab-line-strong: #bdcad9;
  --lab-navy: #0b1220;
  --lab-navy-soft: #142238;
  --lab-cobalt: #3567d6;
  --lab-cobalt-dark: #244da8;
  --lab-teal: #0f766e;
  --lab-teal-bright: #2dd4bf;
  --lab-amber: #d97706;
  --lab-coral: #b5523d;
  --lab-focus: #0b8f83;
  --lab-shadow-sm: 0 8px 24px rgba(34, 55, 83, .08);
  --lab-shadow-lg: 0 24px 60px rgba(20, 40, 72, .16);
  --lab-radius-sm: 10px;
  --lab-radius-md: 16px;
  --lab-radius-lg: 26px;
  --lab-display: "Noto Sans Display", "Avenir Next", "Segoe UI", sans-serif;
  --lab-body: "Lato", "Noto Sans", "Segoe UI", sans-serif;
  --lab-mono: "DejaVu Sans Mono", "Noto Sans Mono", monospace;
}

html, body, [data-testid="stAppViewContainer"] { overflow-x: clip; }
.stApp {
  color: var(--lab-ink);
  background:
    radial-gradient(circle at 10% 3%, rgba(53,103,214,.10), transparent 31rem),
    radial-gradient(circle at 92% 24%, rgba(15,118,110,.08), transparent 32rem),
    linear-gradient(rgba(16,32,51,.025) 1px, transparent 1px),
    linear-gradient(90deg, rgba(16,32,51,.025) 1px, transparent 1px),
    var(--lab-canvas);
  background-size: auto, auto, 28px 28px, 28px 28px, auto;
}

[data-testid="stHeader"] {
  background: rgba(244,247,251,.76);
  border-bottom: 1px solid rgba(216,224,234,.74);
  backdrop-filter: blur(16px) saturate(150%);
}
[data-testid="stToolbar"] button { border-radius: 10px; }
[data-testid="stSidebar"] {
  background:
    radial-gradient(circle at 18% 4%, rgba(45,212,191,.14), transparent 15rem),
    linear-gradient(165deg, #0b1220 0%, #101c2f 58%, #0c1727 100%);
  border-right: 1px solid rgba(255,255,255,.09);
  box-shadow: 16px 0 44px rgba(11,18,32,.12);
}
[data-testid="stSidebar"] [data-testid="stSidebarContent"] { padding-top: 1.1rem; }
[data-testid="stSidebar"] p,
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] [data-testid="stCaptionContainer"] { color: #c9d5e5 !important; }
[data-testid="stSidebar"] .stSelectbox label {
  color: #9fb0c6 !important;
  font-family: var(--lab-body);
  letter-spacing: .12em;
  text-transform: uppercase;
  font-size: .68rem;
  font-weight: 800;
}
[data-testid="stSidebar"] [data-baseweb="select"] > div {
  background: rgba(255,255,255,.08);
  border-color: rgba(255,255,255,.14);
  color: #f4f8ff;
}
[data-testid="stSidebar"] [data-baseweb="select"] svg { fill: #c9d5e5; }
[data-testid="stSidebarNav"] { padding-top: .55rem; }
[data-testid="stSidebarNav"]::before {
  content: "CATALOG INTELLIGENCE";
  display: block;
  margin: .45rem 1rem .8rem;
  color: #7f93ad;
  font: 800 .62rem/1 var(--lab-body);
  letter-spacing: .16em;
}
[data-testid="stSidebarNav"] a {
  border: 1px solid transparent;
  border-radius: 11px;
  margin: .26rem .65rem;
  min-height: 44px;
  transition: background-color .18s ease, border-color .18s ease, transform .18s ease;
}
[data-testid="stSidebarNav"] a span { color: #dbe5f2 !important; font-family: var(--lab-body); }
[data-testid="stSidebarNav"] a:hover {
  background: rgba(45,212,191,.10);
  border-color: rgba(45,212,191,.18);
  transform: translateX(2px);
}
[data-testid="stSidebarNav"] a[aria-current="page"] {
  background: linear-gradient(100deg, rgba(53,103,214,.92), rgba(15,118,110,.88));
  border-color: rgba(255,255,255,.16);
  box-shadow: 0 8px 22px rgba(0,0,0,.18);
}
[data-testid="stSidebarNav"] a[aria-current="page"] span { color: #fff !important; font-weight: 800; }

.block-container {
  width: min(100%, 1500px);
  max-width: 1500px;
  padding-top: clamp(2.3rem, 4vw, 4rem);
  padding-bottom: 5.5rem;
}
h1, h2, h3, .editorial-title {
  font-family: var(--lab-display) !important;
  color: var(--lab-ink);
  letter-spacing: -.035em;
}
p, label, .stMarkdown, [data-testid="stMetric"] {
  font-family: var(--lab-body);
}
code, pre, [data-testid="stCode"] { font-family: var(--lab-mono) !important; }

.editorial-hero {
  isolation: isolate;
  overflow: hidden;
  position: relative;
  min-height: 286px;
  padding: clamp(1.7rem, 4vw, 3.5rem);
  margin: 0 0 1.65rem;
  border: 1px solid rgba(255,255,255,.10);
  border-radius: var(--lab-radius-lg);
  color: #f7fbff;
  background:
    radial-gradient(circle at 82% 18%, rgba(45,212,191,.24), transparent 22rem),
    radial-gradient(circle at 16% 105%, rgba(53,103,214,.34), transparent 26rem),
    linear-gradient(135deg, #0b1220 0%, #12243c 54%, #0d2f39 100%);
  box-shadow: var(--lab-shadow-lg);
  animation: lab-reveal .56s cubic-bezier(.2,.75,.25,1) both;
}
.editorial-hero::before {
  content: "";
  position: absolute;
  inset: 0;
  z-index: -1;
  pointer-events: none;
  opacity: .24;
  background-image:
    linear-gradient(rgba(255,255,255,.12) 1px, transparent 1px),
    linear-gradient(90deg, rgba(255,255,255,.12) 1px, transparent 1px);
  background-size: 34px 34px;
  mask-image: linear-gradient(90deg, transparent, #000 46%, #000);
}
.editorial-hero::after {
  content: "";
  position: absolute;
  right: 7%;
  bottom: -86px;
  width: 260px;
  aspect-ratio: 1;
  border: 1px solid rgba(45,212,191,.24);
  border-radius: 50%;
  box-shadow: 0 0 0 30px rgba(45,212,191,.035), 0 0 0 74px rgba(53,103,214,.035);
  pointer-events: none;
}
.eyebrow {
  display: inline-flex;
  align-items: center;
  gap: .5rem;
  min-height: 30px;
  padding: .38rem .72rem;
  border: 1px solid rgba(45,212,191,.25);
  border-radius: 999px;
  background: rgba(45,212,191,.08);
  color: #7de7da;
  font: 800 .68rem/1 var(--lab-body);
  letter-spacing: .16em;
  text-transform: uppercase;
  margin-bottom: 1rem;
}
.eyebrow::before {
  content: "";
  width: 7px;
  height: 7px;
  border-radius: 50%;
  background: var(--lab-teal-bright);
  box-shadow: 0 0 0 5px rgba(45,212,191,.10);
}
.editorial-title {
  position: relative;
  max-width: 960px;
  margin: 0;
  color: #f7fbff !important;
  font-size: clamp(2.25rem, 5.6vw, 5.35rem);
  font-weight: 780;
  line-height: .96;
  text-wrap: balance;
}
.hero-deck {
  position: relative;
  max-width: 760px;
  margin: 1rem 0 0;
  color: #bdcce0;
  font-size: clamp(.98rem, 1.4vw, 1.12rem);
  line-height: 1.65;
}
.hero-stamp {
  position: absolute;
  z-index: 1;
  right: clamp(1.4rem, 3vw, 3rem);
  top: clamp(1.4rem, 3vw, 2.5rem);
  max-width: 260px;
  padding: .65rem .82rem;
  border: 1px solid rgba(255,255,255,.17);
  border-radius: 10px;
  background: rgba(7,15,27,.34);
  color: #d8e5f5;
  font: 800 .64rem/1.35 var(--lab-mono);
  letter-spacing: .08em;
  text-transform: uppercase;
  backdrop-filter: blur(10px);
}

.section-kicker {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  gap: .32rem;
  margin: 2.65rem 0 1rem;
  padding-bottom: .8rem;
  border-bottom: 1px solid var(--lab-line);
  position: relative;
}
.section-kicker::after {
  content: "";
  position: absolute;
  left: 0;
  bottom: -1px;
  width: 72px;
  height: 2px;
  background: linear-gradient(90deg, var(--lab-cobalt), var(--lab-teal));
}
.section-kicker h2 {
  margin: 0;
  color: var(--lab-ink) !important;
  font: 760 clamp(1.32rem, 2vw, 1.72rem)/1.12 var(--lab-display) !important;
}
.section-kicker span {
  color: var(--lab-cobalt-dark);
  font: 800 .64rem/1.25 var(--lab-mono);
  letter-spacing: .12em;
  text-transform: uppercase;
}

.kpi-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(min(185px, 100%), 1fr));
  gap: .8rem;
  margin: .6rem 0 1.65rem;
  animation: lab-reveal .58s .08s cubic-bezier(.2,.75,.25,1) both;
}
.kpi-card {
  overflow: hidden;
  position: relative;
  min-width: 0;
  min-height: 126px;
  padding: 1.05rem 1.1rem 1rem;
  border: 1px solid var(--lab-line);
  border-radius: var(--lab-radius-md);
  background: rgba(255,255,255,.91);
  box-shadow: var(--lab-shadow-sm);
  transition: transform .18s ease, border-color .18s ease, box-shadow .18s ease;
}
.kpi-card::before {
  content: "";
  position: absolute;
  inset: 0 auto 0 0;
  width: 4px;
  background: linear-gradient(180deg, var(--lab-cobalt), var(--lab-teal));
}
.kpi-card::after {
  content: "";
  position: absolute;
  width: 62px;
  height: 62px;
  right: -24px;
  top: -26px;
  border: 1px solid rgba(53,103,214,.15);
  border-radius: 50%;
}
.kpi-card:hover {
  transform: translateY(-3px);
  border-color: #b8c7dc;
  box-shadow: 0 15px 34px rgba(34,55,83,.12);
}
.kpi-label {
  min-height: 2.1em;
  color: var(--lab-muted);
  font: 800 .64rem/1.35 var(--lab-body);
  letter-spacing: .105em;
  text-transform: uppercase;
}
.kpi-value {
  overflow: hidden;
  margin-top: .55rem;
  color: var(--lab-ink);
  font: 800 clamp(1.45rem, 2.35vw, 2.12rem)/1.08 var(--lab-display);
  font-variant-numeric: tabular-nums;
  letter-spacing: -.035em;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.kpi-note { color: var(--lab-muted); font-size: .72rem; line-height: 1.35; margin-top: .38rem; }

.empty-panel, .notice-panel {
  position: relative;
  padding: 1.18rem 1.3rem 1.18rem 1.45rem;
  margin: .8rem 0;
  border: 1px solid var(--lab-line);
  border-radius: var(--lab-radius-md);
  background: rgba(255,255,255,.86);
  box-shadow: var(--lab-shadow-sm);
}
.empty-panel::before, .notice-panel::before {
  content: "";
  position: absolute;
  left: 0;
  top: 12px;
  bottom: 12px;
  width: 4px;
  border-radius: 0 5px 5px 0;
  background: var(--lab-cobalt);
}
.notice-panel::before { background: var(--lab-amber); }
.empty-panel b, .notice-panel b { font: 760 1.02rem/1.3 var(--lab-display); }
.empty-panel p, .notice-panel p { color: var(--lab-muted); line-height: 1.55; margin: .32rem 0 0; }

.evidence-card {
  position: relative;
  overflow: hidden;
  min-height: 146px;
  padding: 1.12rem 1.18rem 1.05rem 1.3rem;
  margin: .6rem 0;
  border: 1px solid var(--lab-line);
  border-radius: var(--lab-radius-md);
  background: rgba(255,255,255,.93);
  box-shadow: var(--lab-shadow-sm);
  transition: transform .18s ease, border-color .18s ease, box-shadow .18s ease;
}
.evidence-card::before {
  content: "";
  position: absolute;
  left: 0;
  top: 0;
  bottom: 0;
  width: 4px;
  background: linear-gradient(180deg, var(--lab-cobalt), var(--lab-teal));
}
.evidence-card:hover {
  transform: translateY(-3px);
  border-color: #b5c7de;
  box-shadow: 0 15px 36px rgba(34,55,83,.12);
}
.evidence-rank {
  float: right;
  min-width: 42px;
  padding: .34rem .48rem;
  border: 1px solid rgba(53,103,214,.16);
  border-radius: 9px;
  background: rgba(53,103,214,.08);
  color: var(--lab-cobalt-dark);
  font: 800 1rem/1 var(--lab-mono);
  text-align: center;
}
.evidence-title {
  padding-right: 3.2rem;
  color: var(--lab-ink);
  font: 760 1.05rem/1.4 var(--lab-display);
}
.evidence-meta {
  margin: .32rem 0 .72rem;
  color: var(--lab-muted);
  font: 700 .69rem/1.45 var(--lab-mono);
  letter-spacing: .02em;
  overflow-wrap: anywhere;
}
.evidence-why {
  border-top: 1px solid var(--lab-line);
  padding-top: .65rem;
  color: #34465a;
  font-size: .84rem;
  line-height: 1.55;
}

.run-stamp {
  display: grid;
  grid-template-columns: 1fr auto;
  gap: .2rem .7rem;
  margin: .7rem .65rem 1.05rem;
  padding: .82rem .9rem;
  border: 1px solid rgba(255,255,255,.13);
  border-radius: 12px;
  background: rgba(255,255,255,.055);
  box-shadow: inset 0 1px 0 rgba(255,255,255,.04);
}
.run-stamp span { color: #91a4bc !important; font: 800 .58rem/1.5 var(--lab-body); letter-spacing: .14em; }
.run-stamp b { color: #71e1d5 !important; font: 800 1.02rem/1.2 var(--lab-mono); }
.run-stamp em { grid-column: 1/-1; color: #d5e1ef !important; font: 700 .7rem/1.4 var(--lab-body); font-style: normal; }
.source-note {
  color: var(--lab-muted);
  font-size: .73rem;
  line-height: 1.55;
  padding: .72rem .1rem 0;
  border-top: 1px dashed var(--lab-line-strong);
}

/* Native Streamlit surfaces */
div[data-testid="stMetric"],
div[data-testid="stDataFrame"],
[data-testid="stPlotlyChart"],
[data-testid="stCode"] {
  border: 1px solid var(--lab-line);
  border-radius: var(--lab-radius-md);
  background: rgba(255,255,255,.88);
  box-shadow: var(--lab-shadow-sm);
}
div[data-testid="stMetric"] { padding: .82rem 1rem; }
div[data-testid="stDataFrame"] { overflow: hidden; }
[data-testid="stPlotlyChart"] { padding: .35rem; overflow: hidden; }
[data-testid="stCode"] { padding: .25rem; }
[data-testid="stExpander"] {
  overflow: hidden;
  border: 1px solid var(--lab-line) !important;
  border-radius: var(--lab-radius-md) !important;
  background: rgba(255,255,255,.82);
  box-shadow: var(--lab-shadow-sm);
}
[data-testid="stExpander"] summary { min-height: 48px; }

.stButton > button, .stDownloadButton > button {
  min-height: 44px;
  border: 1px solid var(--lab-cobalt) !important;
  border-radius: 11px !important;
  background: var(--lab-surface) !important;
  color: var(--lab-cobalt-dark) !important;
  font-family: var(--lab-body);
  font-weight: 800;
  box-shadow: 0 5px 14px rgba(53,103,214,.09);
  transition: transform .15s ease, background-color .15s ease, color .15s ease, box-shadow .15s ease;
}
.stButton > button:hover, .stDownloadButton > button:hover {
  transform: translateY(-1px);
  background: var(--lab-cobalt) !important;
  color: #fff !important;
  box-shadow: 0 9px 20px rgba(53,103,214,.20);
}
[data-baseweb="input"] > div,
[data-baseweb="textarea"] > div,
[data-baseweb="select"] > div {
  min-height: 44px;
  border-color: var(--lab-line-strong) !important;
  border-radius: 11px !important;
  background: rgba(255,255,255,.96);
  transition: border-color .15s ease, box-shadow .15s ease;
}
[data-baseweb="input"] > div:focus-within,
[data-baseweb="textarea"] > div:focus-within,
[data-baseweb="select"] > div:focus-within {
  border-color: var(--lab-focus) !important;
  box-shadow: 0 0 0 3px rgba(15,118,110,.13);
}
div[data-testid="stRadio"] > div[role="radiogroup"] {
  gap: .3rem;
  padding: .3rem;
  border: 1px solid var(--lab-line);
  border-radius: 13px;
  background: rgba(232,239,247,.74);
}
div[data-testid="stRadio"] > div[role="radiogroup"] label {
  min-height: 40px;
  padding: .38rem .6rem;
  border-radius: 9px;
}
[data-testid="stSlider"] [role="slider"] { box-shadow: 0 0 0 3px #fff, 0 0 0 5px rgba(15,118,110,.22); }
[data-testid="stToggle"] label { min-height: 44px; align-items: center; }

a { color: var(--lab-cobalt-dark); text-underline-offset: 3px; }
a:hover { color: var(--lab-teal); }
:where(button, a, input, textarea, select, summary, [tabindex]):focus-visible {
  outline: 3px solid var(--lab-focus) !important;
  outline-offset: 3px !important;
  border-radius: 8px;
}

@keyframes lab-reveal {
  from { opacity: 0; transform: translateY(10px); }
  to { opacity: 1; transform: translateY(0); }
}

@media (max-width: 1100px) {
  .editorial-hero { min-height: 260px; }
  .hero-stamp { position: static; display: inline-block; margin-top: 1.1rem; }
  .editorial-title { max-width: 100%; font-size: clamp(2.35rem, 7vw, 4.3rem); }
  .hero-deck { max-width: 92%; }
  .kpi-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
}

@media (max-width: 700px) {
  .block-container { padding: 2.15rem .85rem 4rem; }
  .editorial-hero { min-height: 0; padding: 1.5rem 1.25rem 1.55rem; border-radius: 20px; }
  .editorial-hero::after { width: 190px; right: -40px; bottom: -100px; }
  .editorial-title { font-size: clamp(2.12rem, 10vw, 3.2rem); line-height: 1; }
  .hero-deck { max-width: 100%; font-size: .96rem; }
  .section-kicker { gap: .3rem; margin-top: 2.15rem; }
  .kpi-grid { gap: .62rem; }
  .kpi-card { min-height: 112px; padding: .88rem .82rem .85rem .95rem; }
  .kpi-value { font-size: clamp(1.28rem, 6.2vw, 1.72rem); }
  .evidence-card { padding: 1rem; }
  [data-testid="stHorizontalBlock"] { gap: .7rem; }
}

@media (max-width: 480px) {
  .eyebrow { max-width: 100%; letter-spacing: .1em; }
  .hero-stamp { max-width: 100%; font-size: .58rem; }
  .kpi-label { letter-spacing: .075em; font-size: .59rem; }
  .kpi-value { font-size: clamp(1.18rem, 6vw, 1.55rem); }
  .evidence-rank { float: none; display: inline-block; margin-bottom: .6rem; }
  .evidence-title { padding-right: 0; }
  div[data-testid="stRadio"] > div[role="radiogroup"] { align-items: stretch; }
}

@media (max-width: 340px) {
  .block-container { padding-inline: .65rem; }
  .kpi-grid { grid-template-columns: 1fr; }
  .editorial-hero { padding-inline: 1rem; }
}

@media (prefers-reduced-motion: reduce) {
  *, *::before, *::after {
    scroll-behavior: auto !important;
    animation-duration: .01ms !important;
    animation-iteration-count: 1 !important;
    transition-duration: .01ms !important;
  }
  .kpi-card:hover, .evidence-card:hover,
  .stButton > button:hover, .stDownloadButton > button:hover,
  [data-testid="stSidebarNav"] a:hover { transform: none !important; }
}
</style>
""",
        unsafe_allow_html=True,
    )


def hero(eyebrow: str, title: str, deck: str, *, stamp: str | None = None) -> None:
    stamp_html = (
        f"<div class='hero-stamp' role='status'>{html.escape(stamp)}</div>" if stamp else ""
    )
    st.markdown(
        f"<section class='editorial-hero' aria-label='{html.escape(title)}'>"
        f"<div class='eyebrow'>{html.escape(eyebrow)}</div>"
        f"<h1 class='editorial-title'>{html.escape(title)}</h1>"
        f"<p class='hero-deck'>{html.escape(deck)}</p>{stamp_html}</section>",
        unsafe_allow_html=True,
    )


def section(title: str, index: str, note: str | None = None) -> None:
    label = f"{index}{' · ' + note if note else ''}"
    st.markdown(
        f"<div class='section-kicker'><span>{html.escape(label)}</span>"
        f"<h2>{html.escape(title)}</h2></div>",
        unsafe_allow_html=True,
    )


def format_number(value: object, *, percent: bool = False, decimals: int = 1) -> str:
    try:
        missing = bool(pd.isna(value))
    except (TypeError, ValueError):
        missing = False
    if value is None or missing:
        return "—"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    if percent:
        return f"{number * 100:.{decimals}f}%"
    if number.is_integer():
        return f"{int(number):,}".replace(",", ".")
    return f"{number:,.{decimals}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def kpis(items: Iterable[tuple[str, object, str | None]]) -> None:
    cards = []
    for label, value, note in items:
        cards.append(
            "<article class='kpi-card'>"
            f"<div class='kpi-label'>{html.escape(label)}</div>"
            f"<div class='kpi-value'>{html.escape(format_number(value))}</div>"
            f"<div class='kpi-note'>{html.escape(note or '')}</div></article>"
        )
    st.markdown(
        f"<section class='kpi-grid' aria-label='Temel göstergeler'>{''.join(cards)}</section>",
        unsafe_allow_html=True,
    )


def empty_state(title: str, detail: str) -> None:
    st.markdown(
        f"<div class='empty-panel' role='status'><b>{html.escape(title)}</b>"
        f"<p>{html.escape(detail)}</p></div>",
        unsafe_allow_html=True,
    )


def notice(title: str, detail: str) -> None:
    st.markdown(
        f"<aside class='notice-panel' role='note'><b>{html.escape(title)}</b>"
        f"<p>{html.escape(detail)}</p></aside>",
        unsafe_allow_html=True,
    )


def source_note(text: str) -> None:
    st.markdown(f"<p class='source-note'>{html.escape(text)}</p>", unsafe_allow_html=True)


def recommendation_card(row: Mapping[str, object], explanation: str) -> None:
    def clean(key: str, fallback: str) -> object:
        value = row.get(key)
        try:
            return fallback if value is None or bool(pd.isna(value)) else value
        except (TypeError, ValueError):
            return value if value is not None else fallback

    title = clean("title", "Başlık bulunamadı")
    asin = clean("asin", "ASIN yok")
    group = clean("group", "Grup bilinmiyor")
    leaf = clean("leaf_category", "Yaprak kategori yok")
    rank = clean("rank", "—")
    st.markdown(
        "<article class='evidence-card'>"
        f"<div class='evidence-rank'>#{html.escape(str(rank))}</div>"
        f"<div class='evidence-title'>{html.escape(str(title))}</div>"
        f"<div class='evidence-meta'>{html.escape(str(asin))} · {html.escape(str(group))} · {html.escape(str(leaf))}</div>"
        f"<div class='evidence-why'>{html.escape(explanation)}</div></article>",
        unsafe_allow_html=True,
    )


def safe_dataframe(frame: pd.DataFrame, *, height: int = 360) -> None:
    if frame.empty:
        empty_state("Gösterilecek satır yok", "Seçili filtreler için tamamlanmış bir kayıt bulunamadı.")
        return
    st.dataframe(frame, width="stretch", hide_index=True, height=height)
