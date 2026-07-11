"""Plotly figures in the Modern Light Lab visual language."""

from __future__ import annotations

from collections.abc import Mapping

import pandas as pd
import plotly.graph_objects as go

from app.lib.graph import bounded_ego_graph, deterministic_layout


INK = "#102033"
MUTED = "#5b6878"
AMBER = "#d97706"
MOSS = "#0f766e"
BRICK = "#b5523d"
COBALT = "#3567d6"
PAPER = "rgba(0,0,0,0)"


def polish(figure: go.Figure, *, height: int = 350) -> go.Figure:
    figure.update_layout(
        height=height,
        paper_bgcolor=PAPER,
        plot_bgcolor=PAPER,
        margin=dict(l=26, r=36, t=72, b=96),
        font=dict(family="Lato, Noto Sans, Segoe UI, sans-serif", color=INK, size=12),
        title=dict(
            x=.01,
            xanchor="left",
            pad=dict(b=12),
            font=dict(
                family="Noto Sans Display, Avenir Next, Segoe UI, sans-serif",
                size=20,
                color=INK,
            ),
        ),
        legend=dict(
            orientation="h",
            yanchor="top",
            y=-.22,
            xanchor="left",
            x=0,
            bgcolor="rgba(255,255,255,.82)",
            bordercolor="rgba(216,224,234,.9)",
            borderwidth=1,
            font=dict(size=11, color=MUTED),
        ),
        hoverlabel=dict(
            bgcolor="#ffffff",
            bordercolor="#bdcad9",
            font_color=INK,
            font_family="Lato, Noto Sans, sans-serif",
        ),
        hovermode="closest",
        transition=dict(duration=220, easing="cubic-in-out"),
    )
    figure.update_xaxes(
        showgrid=False,
        linecolor="rgba(16,32,51,.16)",
        tickfont=dict(color=MUTED),
        automargin=True,
    )
    figure.update_yaxes(
        gridcolor="rgba(16,32,51,.075)",
        linecolor="rgba(16,32,51,.12)",
        tickfont=dict(color=MUTED),
        zeroline=False,
        automargin=True,
    )
    return figure


def horizontal_bars(
    frame: pd.DataFrame,
    *,
    label: str,
    value: str,
    title: str,
    log_x: bool = False,
    height: int = 380,
) -> go.Figure:
    ordered = frame.sort_values(value, ascending=True)
    figure = go.Figure(
        go.Bar(
            x=ordered[value],
            y=ordered[label],
            orientation="h",
            marker=dict(
                color=COBALT,
                line=dict(color="#244da8", width=.5),
                cornerradius=5,
            ),
            hovertemplate="%{y}<br>%{x:,.0f}<extra></extra>",
        )
    )
    figure.update_layout(
        title=title,
        xaxis_type="log" if log_x else "linear",
        showlegend=False,
        margin=dict(b=34),
    )
    return polish(figure, height=height)


def metric_bars(
    frame: pd.DataFrame,
    *,
    x: str,
    metrics: list[str],
    title: str,
) -> go.Figure:
    colors = [COBALT, MOSS, AMBER, BRICK, "#718096"]
    figure = go.Figure()
    for metric, color in zip(metrics, colors, strict=False):
        if metric not in frame:
            continue
        figure.add_bar(
            name=metric.replace("_at_10", "@10").replace("_", " ").title(),
            x=frame[x],
            y=frame[metric],
            marker_color=color,
            hovertemplate="%{x}<br>%{y:.4f}<extra>%{fullData.name}</extra>",
        )
    figure.update_layout(
        title=title,
        barmode="group",
        bargap=.2,
        bargroupgap=.08,
        yaxis_tickformat=".1%",
    )
    figure.update_xaxes(tickangle=-32, tickfont=dict(size=9.5))
    return polish(figure, height=410)


def ego_figure(
    center: int,
    edges: pd.DataFrame,
    labels: Mapping[int, str] | None = None,
) -> go.Figure:
    edge_pairs = list(
        zip(edges["source_product_id"], edges["target_product_id"], strict=False)
    ) if not edges.empty else []
    graph = bounded_ego_graph(center, edge_pairs, max_nodes=50)
    positions = deterministic_layout(graph)
    edge_x: list[float | None] = []
    edge_y: list[float | None] = []
    for source, target in graph.edges:
        x0, y0 = positions[source]
        x1, y1 = positions[target]
        edge_x.extend((x0, x1, None))
        edge_y.extend((y0, y1, None))
    figure = go.Figure()
    figure.add_trace(
        go.Scatter(
            x=edge_x,
            y=edge_y,
            mode="lines",
            line=dict(width=1, color="rgba(15,118,110,.38)"),
            hoverinfo="skip",
        )
    )
    nodes = list(graph.nodes)
    figure.add_trace(
        go.Scatter(
            x=[positions[node][0] for node in nodes],
            y=[positions[node][1] for node in nodes],
            mode="markers",
            marker=dict(
                size=[18 if node == center else 9 for node in nodes],
                color=[AMBER if node == center else COBALT for node in nodes],
                line=dict(color="#ffffff", width=1.2),
            ),
            text=[(labels or {}).get(node, str(node)) for node in nodes],
            hovertemplate="%{text}<extra></extra>",
        )
    )
    figure.update_layout(
        title=f"Birinci derece ego grafı · {len(nodes)} düğüm",
        showlegend=False,
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
    )
    return polish(figure, height=520)
