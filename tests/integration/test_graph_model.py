from __future__ import annotations

from datetime import date, timedelta

import pytest
from pyspark.sql import Row

from amazon_recommender.models.graph_model import (
    PAGERANK_MAX_ITERATIONS,
    PAGERANK_RESET_PROBABILITY,
    build_extended_graph_inputs,
    build_graph_vertices,
    build_internal_graph_edges,
    generate_graph_recommendations,
    run_extended_graph_pagerank,
    run_graphframes_structural_metrics,
    select_graph_seeds,
)


pytestmark = pytest.mark.integration


def _train(spark, rows):
    return spark.createDataFrame(
        rows,
        "customer_id string, product_id int, interaction_date date, "
        "is_positive boolean, q_ui double",
    )


def _requests(spark, rows):
    return spark.createDataFrame(rows, "stage string, cohort string, customer_id string")


def _edges(spark, rows):
    return spark.createDataFrame(
        rows,
        "source_product_id int, target_product_id int, similar_position int, "
        "is_internal boolean",
    )


def test_internal_edges_filter_and_keep_minimum_physical_position(spark) -> None:
    physical = _edges(
        spark,
        [
            (1, 2, 4, True),
            (1, 2, 2, True),
            (1, 1, 1, True),
            (1, None, 3, False),
            (2, 3, 5, True),
        ],
    )

    rows = {
        (row.source_product_id, row.target_product_id): row.similar_position
        for row in build_internal_graph_edges(physical).collect()
    }

    assert rows == {(1, 2): 2, (2, 3): 5}


def test_extended_graph_keeps_orphan_targets_and_deduplicates_physical_edges(
    spark,
) -> None:
    products = spark.createDataFrame(
        [(1, "A"), (2, "B"), (3, "C")], "product_id int, asin string"
    )
    physical = spark.createDataFrame(
        [
            ("A", "B", 4),
            ("A", "B", 2),
            ("A", "ORPHAN", 1),
            ("B", "B", 1),
            ("MISSING_SOURCE", "OUTSIDE", 1),
            ("A", None, 3),
        ],
        "source_asin string, target_asin string, similar_position int",
    )

    graph = build_extended_graph_inputs(products, physical)
    vertices = {
        row.id: (row.product_id, row.is_catalog) for row in graph.vertices.collect()
    }
    edges = {
        (row.src, row.dst): (row.similar_position, row.physical_occurrence_count)
        for row in graph.edges.collect()
    }

    assert vertices == {
        "A": (1, True),
        "B": (2, True),
        "C": (3, True),
        "ORPHAN": (None, False),
    }
    assert edges == {("A", "B"): (2, 2), ("A", "ORPHAN"): (1, 1)}


def test_extended_graph_pagerank_uses_exact_parameters_and_catalog_flag(
    spark, monkeypatch
) -> None:
    from graphframes import GraphFrame

    products = spark.createDataFrame(
        [(1, "A"), (2, "B"), (3, "ISOLATED")],
        "product_id int, asin string",
    )
    physical = spark.createDataFrame(
        [("A", "B", 1), ("B", "ORPHAN", 1), ("ORPHAN", "A", 1)],
        "source_asin string, target_asin string, similar_position int",
    )
    graph = build_extended_graph_inputs(products, physical)
    observed: dict[str, float | int] = {}
    original_page_rank = GraphFrame.pageRank

    def capture_page_rank(self, *args, **kwargs):
        observed.update(kwargs)
        return original_page_rank(self, *args, **kwargs)

    monkeypatch.setattr(GraphFrame, "pageRank", capture_page_rank)
    rows = {
        row.asin: (row.product_id, row.is_catalog, row.pagerank)
        for row in run_extended_graph_pagerank(graph.vertices, graph.edges).collect()
    }

    assert observed == {
        "resetProbability": PAGERANK_RESET_PROBABILITY,
        "maxIter": PAGERANK_MAX_ITERATIONS,
    }
    assert PAGERANK_RESET_PROBABILITY == 0.15
    assert PAGERANK_MAX_ITERATIONS == 10
    assert set(rows) == {"A", "B", "ISOLATED", "ORPHAN"}
    assert rows["A"][:2] == (1, True)
    assert rows["ORPHAN"][:2] == (None, False)
    assert all(pagerank > 0.0 for _, _, pagerank in rows.values())


def test_seed_selection_is_train_only_latest_twenty_and_request_distinct(spark) -> None:
    start = date(2000, 1, 1)
    rows = [
        ("u", product_id, start + timedelta(days=product_id), True, 1.0)
        for product_id in range(1, 23)
    ]
    rows.append(("u", 100, start + timedelta(days=100), False, 0.0))
    train = _train(spark, rows)
    requests = _requests(
        spark,
        [
            ("validation", "operational", "u"),
            ("validation", "common_warm", "u"),
        ],
    )

    seeds = select_graph_seeds(train, requests).orderBy("seed_rank").collect()

    assert len(seeds) == 20
    assert [row.seed_product_id for row in seeds[:3]] == [22, 21, 20]
    assert {row.seed_product_id for row in seeds}.isdisjoint({1, 2, 100})
    assert {(row.stage, row.customer_id) for row in seeds} == {("validation", "u")}


def test_personal_score_sums_direct_reciprocal_and_distinct_two_hop_paths(spark) -> None:
    train = _train(spark, [("u", 1, date(2005, 1, 1), True, 1.0)])
    requests = _requests(
        spark,
        [
            ("validation", "operational", "u"),
            ("validation", "common_warm", "u"),
        ],
    )
    # 1->2 is reciprocal; product 4 is reached through both 2 and 3.
    edges = _edges(
        spark,
        [
            (1, 2, 1, True),
            (2, 1, 2, True),
            (1, 3, 3, True),
            (2, 4, 1, True),
            (3, 4, 1, True),
        ],
    )
    active = spark.createDataFrame([(1,), (2,), (3,), (4,)], "product_id int")
    seen = spark.createDataFrame(
        [("validation", "u", 1)], "stage string, customer_id string, product_id int"
    )
    pagerank = spark.createDataFrame(
        [(1, 0.1), (2, 0.2), (3, 0.3), (4, 0.4)],
        "product_id int, pagerank double",
    )
    bayesian = spark.createDataFrame(
        [(1, 4.0), (2, 4.0), (3, 4.0), (4, 4.0)],
        "product_id int, bayesian_score double",
    )

    recommendations = generate_graph_recommendations(
        train, requests, edges, active, seen, pagerank, bayesian
    ).orderBy("rank")
    rows = recommendations.collect()
    by_product = {row.product_id: row for row in rows}

    assert [row.product_id for row in rows] == [2, 4, 3]
    assert by_product[2].direct_score == pytest.approx(1.0)
    assert by_product[2].reciprocal_bonus_score == pytest.approx(0.25)
    assert by_product[2].two_hop_score == pytest.approx(0.0)
    assert by_product[2].graph_score == pytest.approx(1.25)
    assert by_product[4].direct_score == pytest.approx(0.0)
    assert by_product[4].two_hop_score == pytest.approx(0.75)
    assert len({(row.stage, row.customer_id, row.product_id) for row in rows}) == 3


def test_candidates_filter_seen_and_inactive_then_use_binding_tie_breaks(spark) -> None:
    train = _train(spark, [("u", 10, date(2005, 1, 1), True, 1.0)])
    requests = _requests(spark, [("test", "operational", "u")])
    edges = _edges(
        spark,
        [(10, 2, 1, True), (10, 3, 1, True), (10, 4, 1, True), (10, 5, 1, True)],
    )
    # Product 2 is stage-seen and product 3 is not in the active catalog.
    active = spark.createDataFrame([(4,), (5,), (10,)], "product_id int")
    seen = spark.createDataFrame(
        [("test", "u", 2), ("test", "u", 10)],
        "stage string, customer_id string, product_id int",
    )
    # Products 4 and 5 tie through PageRank and Bayesian score, so id 4 wins.
    pagerank = spark.createDataFrame(
        [(2, 0.9), (3, 0.9), (4, 0.7), (5, 0.7)],
        "product_id int, pagerank double",
    )
    bayesian = spark.createDataFrame(
        [(2, 5.0), (3, 5.0), (4, 4.5), (5, 4.5)],
        "product_id int, bayesian_score double",
    )

    rows = generate_graph_recommendations(
        train, requests, edges, active, seen, pagerank, bayesian
    ).orderBy("rank").collect()

    assert [(row.product_id, row.rank) for row in rows] == [(4, 1), (5, 2)]
    assert all(row.graph_score == pytest.approx(1.0) for row in rows)


def test_graphframes_outputs_pagerank_degrees_reciprocity_and_wcc(spark) -> None:
    products = spark.createDataFrame([(1,), (2,), (3,), (4,)], "product_id int")
    edges = build_internal_graph_edges(
        _edges(spark, [(1, 2, 1, True), (2, 1, 2, True), (2, 3, 1, True)])
    )

    frames = run_graphframes_structural_metrics(build_graph_vertices(products), edges)
    ranks = frames.pagerank.collect()
    degrees = {
        row.product_id: (row.in_degree, row.out_degree)
        for row in frames.degrees.collect()
    }
    reciprocity = {
        (row.source_product_id, row.target_product_id): row.is_reciprocal
        for row in frames.edge_reciprocity.collect()
    }
    components = {
        row.product_id: row.component_id for row in frames.weak_components.collect()
    }

    assert len(ranks) == 4
    assert all(row.pagerank > 0.0 for row in ranks)
    assert degrees == {1: (1, 1), 2: (1, 2), 3: (1, 0), 4: (0, 0)}
    assert reciprocity == {(1, 2): True, (2, 1): True, (2, 3): False}
    assert components[1] == components[2] == components[3]
    assert components[4] != components[1]
