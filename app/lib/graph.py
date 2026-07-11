"""Bounded NetworkX layout helpers for the product explorer."""

from __future__ import annotations

from collections.abc import Iterable

import networkx as nx


MAX_EGO_NODES = 50


def bounded_ego_graph(
    center: int,
    edges: Iterable[tuple[int, int]],
    *,
    max_nodes: int = MAX_EGO_NODES,
) -> nx.DiGraph:
    if not 1 <= max_nodes <= MAX_EGO_NODES:
        raise ValueError("Ego graph node limit must be between 1 and 50")
    graph = nx.DiGraph()
    graph.add_node(int(center))
    ordered = sorted(
        {(int(source), int(target)) for source, target in edges},
        key=lambda edge: (
            0 if center in edge else 1,
            edge[0],
            edge[1],
        ),
    )
    for source, target in ordered:
        missing = {source, target}.difference(graph.nodes)
        if len(graph) + len(missing) > max_nodes:
            continue
        graph.add_edge(source, target)
    return graph


def deterministic_layout(graph: nx.DiGraph, *, seed: int = 42) -> dict[int, tuple[float, float]]:
    if len(graph) > MAX_EGO_NODES:
        raise ValueError("NetworkX layout is restricted to at most 50 nodes")
    if len(graph) == 1:
        node = next(iter(graph.nodes))
        return {node: (0.0, 0.0)}
    positions = nx.spring_layout(graph, seed=seed, k=0.8)
    return {int(node): (float(x), float(y)) for node, (x, y) in positions.items()}
