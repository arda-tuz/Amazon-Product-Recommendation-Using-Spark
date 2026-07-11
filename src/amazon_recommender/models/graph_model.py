"""Train-only graph recommendations and GraphFrames structural features.

The full catalog graph remains distributed throughout this module.  In particular,
no helper collects vertices or edges into a driver-side graph implementation.
Recommendation requests are deliberately keyed by ``(stage, customer_id)``: G9 may
join the same frozen recommendation list to more than one evaluation cohort later.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from amazon_recommender.models.math import (
    GRAPH_RECIPROCAL_BONUS,
    GRAPH_TWO_STEP_FACTOR,
)


MAX_POSITIVE_SEEDS: Final[int] = 20
GRAPH_CANDIDATE_DEPTH: Final[int] = 50
PAGERANK_RESET_PROBABILITY: Final[float] = 0.15
PAGERANK_MAX_ITERATIONS: Final[int] = 10

_REQUEST_KEYS: Final[tuple[str, str]] = ("stage", "customer_id")
_EDGE_COLUMNS: Final[tuple[str, str, str]] = (
    "source_product_id",
    "target_product_id",
    "similar_position",
)


@dataclass(frozen=True)
class GraphStructuralFrames:
    """Distributed structural outputs for the internal catalog graph."""

    pagerank: DataFrame
    degrees: DataFrame
    weak_components: DataFrame
    edge_reciprocity: DataFrame

    def as_dict(self) -> dict[str, DataFrame]:
        return {name: getattr(self, name) for name in self.__dataclass_fields__}


@dataclass(frozen=True)
class ExtendedGraphInputFrames:
    """GraphFrames inputs for the catalog-plus-orphan ASIN graph.

    ``vertices.id`` and the ``src``/``dst`` edge keys are ASIN strings.  This is
    intentionally separate from :class:`GraphStructuralFrames`, whose internal
    catalog graph uses integer product identifiers for recommendation joins.
    """

    vertices: DataFrame
    edges: DataFrame

    def as_dict(self) -> dict[str, DataFrame]:
        return {name: getattr(self, name) for name in self.__dataclass_fields__}


def _require_columns(frame: DataFrame, required: tuple[str, ...], name: str) -> None:
    missing = sorted(set(required).difference(frame.columns))
    if missing:
        raise ValueError(f"{name} is missing required columns: {missing}")


def build_graph_vertices(products: DataFrame) -> DataFrame:
    """Return one GraphFrames-compatible catalog vertex per product."""

    _require_columns(products, ("product_id",), "products")
    return products.select(F.col("product_id").alias("id")).dropDuplicates(["id"])


def build_extended_graph_inputs(
    products: DataFrame, physical_similar_edges: DataFrame
) -> ExtendedGraphInputFrames:
    """Build the binding extended graph from physical ``similar`` occurrences.

    The vertex universe is every catalog ASIN plus every referenced target ASIN.
    A target absent from the product catalog is retained as an orphan vertex with
    ``is_catalog=false`` and a null ``product_id``.  Edges whose source is not in
    the catalog, null/blank identifiers, and self loops are rejected.  Repeated
    directed pairs are reduced to one edge with the smallest physical list
    position, while ``physical_occurrence_count`` preserves their multiplicity.

    The resulting frames remain distributed and are directly consumable by
    GraphFrames: vertices use ``id`` and edges use ``src``/``dst``.
    """

    _require_columns(products, ("product_id", "asin"), "products")
    _require_columns(
        physical_similar_edges,
        ("source_asin", "target_asin", "similar_position"),
        "physical_similar_edges",
    )

    catalog = (
        products.select(
            F.trim(F.col("asin")).alias("id"),
            F.col("product_id").alias("product_id"),
        )
        .filter(F.col("id").isNotNull() & (F.length(F.col("id")) > F.lit(0)))
        .groupBy("id")
        .agg(F.min("product_id").alias("product_id"))
        .withColumn("is_catalog", F.lit(True))
    )

    normalized_occurrences = physical_similar_edges.select(
        F.trim(F.col("source_asin")).alias("src"),
        F.trim(F.col("target_asin")).alias("dst"),
        F.col("similar_position").cast("int").alias("similar_position"),
    ).filter(
        F.col("src").isNotNull()
        & (F.length(F.col("src")) > F.lit(0))
        & F.col("dst").isNotNull()
        & (F.length(F.col("dst")) > F.lit(0))
        & (F.col("src") != F.col("dst"))
    )

    # The extended graph permits catalog-external *targets*, not external sources.
    catalog_sources = catalog.select(F.col("id").alias("src"))
    edges = (
        normalized_occurrences.join(catalog_sources, "src", "inner")
        .groupBy("src", "dst")
        .agg(
            F.min("similar_position").cast("int").alias("similar_position"),
            F.count(F.lit(1)).cast("long").alias("physical_occurrence_count"),
        )
    )

    vertex_ids = catalog.select("id").unionByName(
        edges.select(F.col("dst").alias("id"))
    ).dropDuplicates(["id"])
    vertices = (
        vertex_ids.join(catalog, "id", "left")
        .withColumn("is_catalog", F.coalesce(F.col("is_catalog"), F.lit(False)))
        .select("id", "product_id", "is_catalog")
    )
    return ExtendedGraphInputFrames(vertices=vertices, edges=edges)


def run_extended_graph_pagerank(vertices: DataFrame, edges: DataFrame) -> DataFrame:
    """Run exact directed PageRank over the catalog-plus-orphan ASIN graph.

    PageRank uses the one binding configuration: reset probability 0.15 and ten
    iterations.  The returned table retains the catalog membership flag so the
    structural effect of orphan targets can be reported without another catalog
    lookup.  Isolated catalog vertices remain part of the result.
    """

    _require_columns(vertices, ("id", "product_id", "is_catalog"), "vertices")
    _require_columns(edges, ("src", "dst"), "edges")

    # Lazy import keeps formula-only and Spark-SQL-only callers lightweight.
    from graphframes import GraphFrame

    graph_vertices = vertices.select(
        F.col("id").cast("string").alias("id"),
        "product_id",
        F.col("is_catalog").cast("boolean").alias("is_catalog"),
    ).dropDuplicates(["id"])
    graph_edges = edges.select(
        F.col("src").cast("string").alias("src"),
        F.col("dst").cast("string").alias("dst"),
    ).dropDuplicates(["src", "dst"])
    ranked = GraphFrame(graph_vertices, graph_edges).pageRank(
        resetProbability=PAGERANK_RESET_PROBABILITY,
        maxIter=PAGERANK_MAX_ITERATIONS,
    ).vertices
    return ranked.select(
        F.col("id").alias("asin"),
        "product_id",
        "is_catalog",
        F.col("pagerank").cast("double").alias("pagerank"),
    )


def build_internal_graph_edges(similar_edges: DataFrame) -> DataFrame:
    """Build the canonical internal directed edge set.

    Orphan targets and self loops are excluded.  A repeated directed pair is stored
    once with its smallest physical similar-list position.  ``is_internal`` is used
    when present; a non-null target identifier remains mandatory in all cases.
    """

    _require_columns(similar_edges, _EDGE_COLUMNS, "similar_edges")
    internal = F.col("target_product_id").isNotNull()
    if "is_internal" in similar_edges.columns:
        internal = internal & F.col("is_internal")
    return (
        similar_edges.filter(
            internal
            & (F.col("source_product_id") != F.col("target_product_id"))
        )
        .groupBy("source_product_id", "target_product_id")
        .agg(F.min("similar_position").cast("int").alias("similar_position"))
    )


def _edge_reciprocity(internal_edges: DataFrame) -> DataFrame:
    reverse = internal_edges.select(
        F.col("source_product_id").alias("_reverse_source"),
        F.col("target_product_id").alias("_reverse_target"),
    ).withColumn("_has_reverse", F.lit(True))
    edge = internal_edges.alias("edge")
    return (
        edge.join(
            reverse,
            (F.col("edge.source_product_id") == F.col("_reverse_target"))
            & (F.col("edge.target_product_id") == F.col("_reverse_source")),
            "left",
        )
        .select(
            F.col("edge.source_product_id"),
            F.col("edge.target_product_id"),
            F.col("edge.similar_position"),
            F.coalesce(F.col("_has_reverse"), F.lit(False)).alias(
                "is_reciprocal"
            ),
        )
    )


def run_graphframes_structural_metrics(
    vertices: DataFrame, internal_edges: DataFrame
) -> GraphStructuralFrames:
    """Run the binding PageRank, degree, reciprocity, and WCC computations.

    PageRank is directed with reset probability 0.15 and exactly ten iterations.
    GraphFrames connected components represents weakly connected components for this
    directed input.  A durable Spark checkpoint directory must already be configured
    by the caller, as it is for every gate Spark session.
    """

    _require_columns(vertices, ("id",), "vertices")
    _require_columns(internal_edges, _EDGE_COLUMNS, "internal_edges")

    # Lazy import keeps formula-only callers independent from GraphFrames startup.
    from graphframes import GraphFrame

    graph_vertices = vertices.select("id").dropDuplicates(["id"])
    graph_edges = internal_edges.select(
        F.col("source_product_id").alias("src"),
        F.col("target_product_id").alias("dst"),
    ).dropDuplicates(["src", "dst"])
    graph = GraphFrame(graph_vertices, graph_edges)

    pagerank = graph.pageRank(
        resetProbability=PAGERANK_RESET_PROBABILITY,
        maxIter=PAGERANK_MAX_ITERATIONS,
    ).vertices.select(
        F.col("id").alias("product_id"),
        F.col("pagerank").cast("double").alias("pagerank"),
    )

    degrees = (
        graph_vertices.join(graph.inDegrees, "id", "left")
        .join(graph.outDegrees, "id", "left")
        .select(
            F.col("id").alias("product_id"),
            F.coalesce(F.col("inDegree"), F.lit(0)).cast("long").alias(
                "in_degree"
            ),
            F.coalesce(F.col("outDegree"), F.lit(0)).cast("long").alias(
                "out_degree"
            ),
        )
    )

    weak_components = graph.connectedComponents(
        algorithm="two_phase",
        checkpointInterval=2,
        broadcastThreshold=-1,
        use_local_checkpoints=False,
    ).select(
        F.col("id").alias("product_id"),
        F.col("component").cast("long").alias("component_id"),
    )

    return GraphStructuralFrames(
        pagerank=pagerank,
        degrees=degrees,
        weak_components=weak_components,
        edge_reciprocity=_edge_reciprocity(internal_edges),
    )


def select_graph_seeds(
    train_interactions: DataFrame, evaluation_users: DataFrame
) -> DataFrame:
    """Select each request user's latest twenty positive *training* products."""

    _require_columns(
        train_interactions,
        (
            "customer_id",
            "product_id",
            "interaction_date",
            "is_positive",
            "q_ui",
        ),
        "train_interactions",
    )
    _require_columns(evaluation_users, _REQUEST_KEYS, "evaluation_users")

    requests = evaluation_users.select(*_REQUEST_KEYS).dropDuplicates(
        list(_REQUEST_KEYS)
    )
    ordering = Window.partitionBy(*_REQUEST_KEYS).orderBy(
        F.col("interaction_date").desc_nulls_last(),
        F.col("product_id").asc(),
    )
    return (
        requests.join(
            train_interactions.filter(F.col("is_positive")).select(
                "customer_id", "product_id", "interaction_date", "q_ui"
            ),
            "customer_id",
            "inner",
        )
        .withColumn("seed_rank", F.row_number().over(ordering))
        .filter(F.col("seed_rank") <= F.lit(MAX_POSITIVE_SEEDS))
        .select(
            *_REQUEST_KEYS,
            F.col("product_id").alias("seed_product_id"),
            F.col("q_ui").cast("double").alias("q_ui"),
            "interaction_date",
            "seed_rank",
        )
    )


def _position_decay(position: F.Column) -> F.Column:
    return F.lit(1.0) / F.log2(position.cast("double") + F.lit(1.0))


def _direct_contributions(seeds: DataFrame, edges: DataFrame) -> DataFrame:
    direct = (
        seeds.alias("seed")
        .join(
            edges.alias("edge"),
            F.col("seed.seed_product_id") == F.col("edge.source_product_id"),
            "inner",
        )
        .select(
            *[F.col(f"seed.{key}").alias(key) for key in _REQUEST_KEYS],
            F.col("seed.seed_product_id"),
            F.col("seed.q_ui"),
            F.col("edge.target_product_id").alias("product_id"),
            F.col("edge.similar_position").alias("direct_position"),
        )
    )
    reverse = edges.select(
        F.col("source_product_id").alias("product_id"),
        F.col("target_product_id").alias("seed_product_id"),
    ).withColumn("_is_reciprocal", F.lit(True))
    base = F.col("q_ui") * _position_decay(F.col("direct_position"))
    return (
        direct.join(reverse, ["seed_product_id", "product_id"], "left")
        .select(
            *_REQUEST_KEYS,
            "product_id",
            base.alias("direct_score"),
            F.when(
                F.coalesce(F.col("_is_reciprocal"), F.lit(False)),
                base * F.lit(GRAPH_RECIPROCAL_BONUS),
            )
            .otherwise(F.lit(0.0))
            .alias("reciprocal_bonus_score"),
            F.lit(0.0).alias("two_hop_score"),
        )
    )


def _two_hop_contributions(seeds: DataFrame, edges: DataFrame) -> DataFrame:
    first_hop = (
        seeds.alias("seed")
        .join(
            edges.alias("first"),
            F.col("seed.seed_product_id") == F.col("first.source_product_id"),
            "inner",
        )
        .select(
            *[F.col(f"seed.{key}").alias(key) for key in _REQUEST_KEYS],
            F.col("seed.q_ui"),
            F.col("first.target_product_id").alias("middle_product_id"),
            F.col("first.similar_position").alias("first_position"),
        )
    )
    return (
        first_hop.alias("path")
        .join(
            edges.alias("second"),
            F.col("path.middle_product_id") == F.col("second.source_product_id"),
            "inner",
        )
        .select(
            *[F.col(f"path.{key}").alias(key) for key in _REQUEST_KEYS],
            F.col("second.target_product_id").alias("product_id"),
            F.lit(0.0).alias("direct_score"),
            F.lit(0.0).alias("reciprocal_bonus_score"),
            (
                F.col("path.q_ui")
                * F.lit(GRAPH_TWO_STEP_FACTOR)
                * _position_decay(F.col("path.first_position"))
                * _position_decay(F.col("second.similar_position"))
            ).alias("two_hop_score"),
        )
    )


def generate_graph_recommendations(
    train_interactions: DataFrame,
    evaluation_users: DataFrame,
    internal_edges: DataFrame,
    active_catalog: DataFrame,
    stage_seen_items: DataFrame,
    pagerank: DataFrame,
    bayesian_scores: DataFrame,
) -> DataFrame:
    """Produce the binding top-50 personalized graph candidates.

    Contributions from every selected seed and every distinct one/two-hop path are
    summed.  Only active catalog products not seen at the requested stage survive.
    Ties on the personal graph score are resolved by PageRank, Bayesian score, then
    ascending product identifier.  PageRank is never added to the personal score.
    """

    _require_columns(internal_edges, _EDGE_COLUMNS, "internal_edges")
    _require_columns(active_catalog, ("product_id",), "active_catalog")
    _require_columns(
        stage_seen_items, ("stage", "customer_id", "product_id"), "stage_seen_items"
    )
    _require_columns(pagerank, ("product_id", "pagerank"), "pagerank")
    _require_columns(
        bayesian_scores, ("product_id", "bayesian_score"), "bayesian_scores"
    )

    # Defensively enforce the graph contract even if a caller supplies the physical
    # similar table rather than G5's already-deduplicated edge table.
    edges = build_internal_graph_edges(internal_edges)
    seeds = select_graph_seeds(train_interactions, evaluation_users)
    contributions = _direct_contributions(seeds, edges).unionByName(
        _two_hop_contributions(seeds, edges)
    )
    scores = contributions.groupBy(*_REQUEST_KEYS, "product_id").agg(
        F.sum("direct_score").alias("direct_score"),
        F.sum("reciprocal_bonus_score").alias("reciprocal_bonus_score"),
        F.sum("two_hop_score").alias("two_hop_score"),
    ).withColumn(
        "graph_score",
        F.col("direct_score")
        + F.col("reciprocal_bonus_score")
        + F.col("two_hop_score"),
    )

    active = active_catalog.select("product_id").dropDuplicates(["product_id"])
    seen = stage_seen_items.select(*_REQUEST_KEYS, "product_id").dropDuplicates(
        [*_REQUEST_KEYS, "product_id"]
    )
    eligible = (
        scores.join(active, "product_id", "inner")
        .join(seen, [*_REQUEST_KEYS, "product_id"], "left_anti")
        .join(
            pagerank.select("product_id", "pagerank").dropDuplicates(
                ["product_id"]
            ),
            "product_id",
            "left",
        )
        .join(
            bayesian_scores.select("product_id", "bayesian_score").dropDuplicates(
                ["product_id"]
            ),
            "product_id",
            "left",
        )
    )
    rank_window = Window.partitionBy(*_REQUEST_KEYS).orderBy(
        F.col("graph_score").desc(),
        F.col("pagerank").desc_nulls_last(),
        F.col("bayesian_score").desc_nulls_last(),
        F.col("product_id").asc(),
    )
    return (
        eligible.withColumn("rank", F.row_number().over(rank_window))
        .filter(F.col("rank") <= F.lit(GRAPH_CANDIDATE_DEPTH))
        .select(
            *_REQUEST_KEYS,
            "product_id",
            "rank",
            "graph_score",
            "direct_score",
            "reciprocal_bonus_score",
            "two_hop_score",
            "pagerank",
            "bayesian_score",
        )
    )
