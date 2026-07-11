"""G7 single-fit independent model training and candidate materialization.

The five model families are deliberately isolated into resume-aware substages.  A
completed substage is represented by atomically published Parquet tables plus a
signature marker.  If the local Spark JVM is interrupted, a rerun reuses every
completed model/table and resumes only the unfinished work.
"""

from __future__ import annotations

import gc
import hashlib
import json
import os
import shutil
import time
import uuid
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Mapping

from pyspark.ml.fpm import FPGrowthModel
from pyspark.ml.recommendation import ALSModel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from amazon_recommender.core.manifest import atomic_write_json
from amazon_recommender.gate_handlers import register
from amazon_recommender.models.als_model import (
    build_als_prediction_table,
    fit_explicit_als,
    generate_als_recommendations,
)
from amazon_recommender.models.category_model import (
    build_category_candidate_pool,
    build_category_item_features,
    build_category_top_products,
    build_category_user_features,
    rank_category_recommendations,
    score_category_candidate_pool,
)
from amazon_recommender.models.fp_growth import (
    build_singleton_rules,
    fit_fp_growth,
    score_fp_recommendations,
)
from amazon_recommender.models.graph_model import (
    build_extended_graph_inputs,
    build_graph_vertices,
    build_internal_graph_edges,
    generate_graph_recommendations,
    run_extended_graph_pagerank,
)
from amazon_recommender.models.math import fp_minimum_count
from amazon_recommender.models.popularity import (
    build_active_global_popularity_catalog,
    build_popularity_scores,
    generate_popularity_recommendations,
)
from amazon_recommender.pipelines.storage import (
    cleanup_incomplete_publications,
    publish_or_reuse_sized_parquet,
)


MODEL_NAMES = ("popularity", "als", "fp", "graph", "category")
MODEL_DEPTHS = {
    "popularity": 100,
    "als": 100,
    "fp": 50,
    "graph": 50,
    "category": 50,
}
SCORE_COLUMNS = {
    "popularity": "model_score",
    "als": "als_prediction",
    "fp": "fp_score",
    "graph": "graph_score",
    "category": "category_score",
}
G7_CONTRACT_VERSION = 2

FACT_TABLES = {
    "popularity_recommendations",
    "als_recommendations",
    "als_predictions",
    "fp_rules",
    "fp_recommendations",
    "graph_internal_edges",
    "graph_edge_reciprocity",
    "graph_recommendations",
    "extended_graph_edges",
    "category_item_vectors",
    "category_profile_item_vectors",
    "category_user_profiles",
    "category_candidate_pool",
    "category_scored_candidates",
    "category_recommendations",
}

STAGE_OUTPUTS: Mapping[str, tuple[str, ...]] = {
    "popularity": (
        "popularity_scores",
        "popularity_global_catalog",
        "popularity_recommendations",
    ),
    "als": (
        "als_recommendations",
        "als_predictions",
        "als_user_factors",
        "als_item_factors",
    ),
    "fp": ("fp_rules", "fp_recommendations", "fp_training_summary"),
    "graph": (
        "graph_vertices",
        "graph_internal_edges",
        "graph_pagerank",
        "graph_degrees",
        "graph_weak_components",
        "graph_edge_reciprocity",
        "extended_graph_vertices",
        "extended_graph_edges",
        "extended_graph_pagerank",
        "graph_pagerank_top",
        "extended_graph_pagerank_top",
        "graph_structural_summary",
        "graph_recommendations",
    ),
    "category": (
        "category_item_vectors",
        "category_statistics",
        "category_item_norms",
        "category_profile_item_vectors",
        "category_user_profiles",
        "category_user_norms",
        "category_group_affinity",
        "category_top_products",
        "category_candidate_pool",
        "category_scored_candidates",
        "category_recommendations",
        "category_training_summary",
    ),
}


def _junit(path: Path) -> dict[str, Any]:
    root = ET.parse(path).getroot()
    suites = [root] if root.tag == "testsuite" else list(root.findall("testsuite"))
    summary = {
        key: sum(int(suite.attrib.get(key, "0")) for suite in suites)
        for key in ("tests", "failures", "errors", "skipped")
    }
    if not summary["tests"] or summary["failures"] or summary["errors"]:
        raise RuntimeError(f"G7 JUnit evidence is not passing: {summary}")
    summary["path"] = str(path.resolve())
    return summary


def _implementation_signature(config_sha256: str) -> str:
    digest = hashlib.sha256()
    digest.update(f"g7-contract-{G7_CONTRACT_VERSION}".encode("ascii"))
    digest.update(config_sha256.encode("ascii"))
    root = Path(__file__).parents[1]
    for name in (
        "models/popularity.py",
        "models/als_model.py",
        "models/fp_growth.py",
        "models/graph_model.py",
        "models/category_model.py",
        "models/math.py",
    ):
        path = root / name
        digest.update(name.encode("utf-8"))
        digest.update(path.read_bytes())
    return digest.hexdigest()


def _prepare_workspace(working: Path, signature: str) -> list[str]:
    marker = working / "_checkpoint_contract.json"
    if working.exists():
        try:
            existing = json.loads(marker.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError):
            existing = {}
        if existing.get("implementation_sha256") != signature:
            shutil.rmtree(working)
    working.mkdir(parents=True, exist_ok=True)
    atomic_write_json(
        marker,
        {
            "gate": "G7",
            "contract_version": G7_CONTRACT_VERSION,
            "implementation_sha256": signature,
        },
    )
    return cleanup_incomplete_publications(working)


def _stage_marker(working: Path, stage: str) -> Path:
    return working / "_substages" / f"{stage}.json"


def _model_is_complete(path: Path) -> bool:
    """Spark ML writers place success markers in model subdirectories."""

    return (path / "metadata" / "_SUCCESS").is_file()


def _reuse_stage(
    working: Path, stage: str, signature: str
) -> dict[str, Any] | None:
    marker = _stage_marker(working, stage)
    try:
        payload = json.loads(marker.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return None
    if payload.get("implementation_sha256") != signature:
        return None
    if any(
        not (working / table / "_SUCCESS").is_file()
        for table in STAGE_OUTPUTS[stage]
    ):
        return None
    model_path = payload.get("model_path")
    if model_path and not _model_is_complete(working / model_path):
        return None
    return payload


def _complete_stage(
    working: Path,
    stage: str,
    signature: str,
    tables: Mapping[str, dict[str, Any]],
    details: Mapping[str, Any],
    *,
    model_path: str | None = None,
) -> None:
    marker = _stage_marker(working, stage)
    marker.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(
        marker,
        {
            "stage": stage,
            "status": "passed",
            "implementation_sha256": signature,
            "outputs": list(STAGE_OUTPUTS[stage]),
            "tables": {name: tables[name] for name in STAGE_OUTPUTS[stage]},
            "details": dict(details),
            "model_path": model_path,
        },
    )


def _model_contract(path: Path, signature: str) -> dict[str, Any] | None:
    marker = path / "_model_contract.json"
    try:
        payload = json.loads(marker.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        payload = None
    if (
        payload is not None
        and payload.get("implementation_sha256") == signature
        and _model_is_complete(path)
    ):
        return payload
    if path.exists():
        shutil.rmtree(path)
    return None


def _save_model(
    model: Any,
    path: Path,
    *,
    signature: str,
    training_seconds: float,
    parameters: Mapping[str, Any],
) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.{uuid.uuid4().hex}.tmp"
    try:
        model.write().save(str(temporary))
        os.replace(temporary, path)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    payload = {
        "implementation_sha256": signature,
        "training_seconds": float(training_seconds),
        "parameters": dict(parameters),
    }
    atomic_write_json(path / "_model_contract.json", payload)
    return payload


def _finite_score_violations(frame: DataFrame, score_column: str) -> int:
    score = F.col(score_column).cast("double")
    return frame.filter(
        score.isNull() | F.isnan(score) | (F.abs(score) > F.lit(1.7976931348623157e308))
    ).limit(1).count()


def validate_recommendation_table(
    frame: DataFrame,
    *,
    model: str,
    requests: DataFrame,
    active_catalog: DataFrame,
    stage_seen_items: DataFrame,
) -> dict[str, Any]:
    """Prove the common G7 recommendation contract for one frozen model list."""

    required = {"stage", "customer_id", "product_id", "rank", SCORE_COLUMNS[model]}
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise RuntimeError(f"{model} recommendation columns missing: {missing}")
    depth = MODEL_DEPTHS[model]
    key = ["stage", "customer_id", "product_id"]
    request_keys = requests.select("stage", "customer_id").dropDuplicates()
    active = active_catalog.select("product_id").dropDuplicates()
    seen = stage_seen_items.select(*key).dropDuplicates()

    row_count = frame.count()
    if row_count <= 0:
        raise RuntimeError(f"{model} produced no full-data recommendations")
    duplicate_key_violations = (
        frame.groupBy(*key).count().filter(F.col("count") != F.lit(1)).limit(1).count()
    )
    rank_violations = frame.filter(
        F.col("rank").isNull()
        | (F.col("rank") < F.lit(1))
        | (F.col("rank") > F.lit(depth))
    ).limit(1).count()
    null_key_violations = frame.filter(
        F.col("stage").isNull()
        | F.col("customer_id").isNull()
        | F.col("product_id").isNull()
    ).limit(1).count()
    score_violations = _finite_score_violations(frame, SCORE_COLUMNS[model])
    request_universe_violations = frame.select("stage", "customer_id").dropDuplicates().join(
        request_keys, ["stage", "customer_id"], "left_anti"
    ).limit(1).count()
    inactive_violations = frame.select("product_id").dropDuplicates().join(
        active, "product_id", "left_anti"
    ).limit(1).count()
    seen_violations = frame.select(*key).join(seen, key, "inner").limit(1).count()
    per_request = frame.groupBy("stage", "customer_id").agg(
        F.count(F.lit(1)).cast("long").alias("candidate_count"),
        F.countDistinct("rank").cast("long").alias("distinct_ranks"),
        F.min("rank").cast("int").alias("min_rank"),
        F.max("rank").cast("int").alias("max_rank"),
    )
    dense_rank_violations = per_request.filter(
        (F.col("min_rank") != F.lit(1))
        | (F.col("candidate_count") != F.col("distinct_ranks"))
        | (F.col("max_rank") != F.col("candidate_count"))
    ).limit(1).count()
    failures = {
        "duplicate_key_violations": duplicate_key_violations,
        "rank_violations": rank_violations,
        "null_key_violations": null_key_violations,
        "score_violations": score_violations,
        "request_universe_violations": request_universe_violations,
        "inactive_violations": inactive_violations,
        "seen_violations": seen_violations,
        "dense_rank_violations": dense_rank_violations,
    }
    nonzero = {name: value for name, value in failures.items() if value != 0}
    if nonzero:
        raise RuntimeError(f"{model} recommendation invariant failure: {nonzero}")

    request_count = request_keys.count()
    distribution = per_request.agg(
        F.count(F.lit(1)).cast("long").alias("users_with_output"),
        F.min("candidate_count").cast("long").alias("min_candidates"),
        F.avg("candidate_count").alias("avg_candidates"),
        F.max("candidate_count").cast("long").alias("max_candidates"),
    ).first().asDict()
    stage_counts = {
        row.stage: int(row["count"])
        for row in frame.groupBy("stage").count().collect()
    }
    users_with_output = int(distribution["users_with_output"])
    return {
        "model": model,
        "depth": depth,
        "rows": int(row_count),
        "request_count": int(request_count),
        "users_with_output": users_with_output,
        "empty_requests": int(request_count - users_with_output),
        "user_coverage": users_with_output / request_count,
        "min_candidates": int(distribution["min_candidates"]),
        "avg_candidates": float(distribution["avg_candidates"]),
        "max_candidates": int(distribution["max_candidates"]),
        "distinct_catalog_products": frame.select("product_id").distinct().count(),
        "stage_rows": stage_counts,
        **failures,
    }


def _release_spark(spark: SparkSession) -> None:
    spark.catalog.clearCache()
    gc.collect()


@register("G7")
def run_g7(config: Any, paths: Any, evidence_file: Path | None) -> dict[str, Any]:
    if evidence_file is None:
        raise RuntimeError("G7 requires passing JUnit XML evidence")
    g6 = paths.data / "g6"
    full = paths.data / "full" / "silver"
    g5 = paths.data / "g5"
    for required in (g6, full, g5):
        if not required.exists():
            raise FileNotFoundError(f"G7 prerequisite data is missing: {required}")
    final = paths.data / "g7"
    if final.exists():
        raise FileExistsError(f"G7 output exists without reusable manifest: {final}")

    signature = _implementation_signature(config.sha256)
    working = paths.temporary / "G7-publish"
    cleaned_scratch = _prepare_workspace(working, signature)
    final.parent.mkdir(parents=True, exist_ok=True)

    spark = SparkSession.builder.appName("amazon-recommender-g7").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    checkpoint = paths.checkpoints / "g7"
    checkpoint.mkdir(parents=True, exist_ok=True)
    spark.sparkContext.setCheckpointDir(str(checkpoint))

    train = spark.read.parquet(str(g6 / "train_interactions"))
    validation = spark.read.parquet(str(g6 / "validation_interactions"))
    test = spark.read.parquet(str(g6 / "test_interactions"))
    als_train = spark.read.parquet(str(g6 / "als_train_interactions"))
    requests = spark.read.parquet(str(g6 / "evaluation_users")).select(
        "stage", "customer_id"
    ).dropDuplicates()
    stage_seen = spark.read.parquet(str(g6 / "stage_seen_items"))
    active_catalog = spark.read.parquet(str(g6 / "active_catalog"))
    baskets = spark.read.parquet(str(g6 / "positive_user_baskets"))
    active_item_features = spark.read.parquet(str(g6 / "item_features"))
    products = spark.read.parquet(str(full / "products"))
    customers = spark.read.parquet(str(full / "customers"))
    physical_similar_edges = spark.read.parquet(str(full / "similar_edges"))
    all_category_nodes = spark.read.parquet(str(full / "product_category_nodes"))
    g5_graph_edges = spark.read.parquet(str(g5 / "graph_edges_deduplicated"))

    tables: dict[str, dict[str, Any]] = {}
    reused_tables: list[str] = []
    reused_stages: list[str] = []
    stage_details: dict[str, dict[str, Any]] = {}

    def publish(name: str, frame: DataFrame) -> dict[str, Any]:
        evidence, reused = publish_or_reuse_sized_parquet(
            frame,
            working / name,
            kind="fact" if name in FACT_TABLES else "dimension",
            sort_columns=tuple(
                column
                for column in (
                    "stage",
                    "customer_id",
                    "rank",
                    "product_id",
                    "category_id",
                    "source_product_id",
                    "target_product_id",
                    "asin",
                )
                if column in frame.columns
            ),
        )
        tables[name] = evidence
        if reused:
            reused_tables.append(name)
        return evidence

    def take_reused(stage: str, marker: Mapping[str, Any]) -> None:
        reused_stages.append(stage)
        reused_tables.extend(STAGE_OUTPUTS[stage])
        tables.update(marker["tables"])
        stage_details[stage] = dict(marker["details"])

    try:
        # 1/5: train-only Bayesian popularity and frozen stage-aware top-100.
        marker = _reuse_stage(working, "popularity", signature)
        if marker is not None:
            take_reused("popularity", marker)
        else:
            started = time.perf_counter()
            raw_scores = build_popularity_scores(
                train,
                products.select("product_id", "group"),
                m=config.get("models", "popularity", "m"),
                group_min_train_interactions=config.get(
                    "models", "popularity", "group_min_train_interactions"
                ),
            )
            active_percentiles = (
                raw_scores.join(active_catalog.select("product_id"), "product_id", "inner")
                .withColumn(
                    "popularity_percentile",
                    F.percent_rank().over(
                        Window.orderBy(
                            F.col("global_bayesian_score").asc(),
                            F.col("unique_reviewers").asc(),
                            F.col("product_id").desc(),
                        )
                    ),
                )
                .select("product_id", "popularity_percentile")
            )
            popularity_scores = (
                raw_scores.join(active_percentiles, "product_id", "left")
                .withColumn("bayesian_score", F.col("global_bayesian_score"))
                .withColumn("rater_count", F.col("unique_reviewers"))
            )
            publish("popularity_scores", popularity_scores)
            popularity_scores = spark.read.parquet(str(working / "popularity_scores"))
            popularity_catalog = build_active_global_popularity_catalog(
                popularity_scores,
                active_catalog,
                catalog_depth=config.get(
                    "models", "popularity", "global_catalog_depth"
                ),
            )
            publish("popularity_global_catalog", popularity_catalog)
            training_seconds = time.perf_counter() - started

            candidate_started = time.perf_counter()
            popularity_recommendations = generate_popularity_recommendations(
                spark.read.parquet(str(working / "popularity_global_catalog")),
                requests,
                stage_seen,
                candidate_depth=config.get(
                    "models", "popularity", "candidate_depth"
                ),
            ).withColumnRenamed("recommendation_rank", "rank")
            publish("popularity_recommendations", popularity_recommendations)
            candidate_seconds = time.perf_counter() - candidate_started
            details = {
                "fit_count": 1,
                "training_seconds": training_seconds,
                "candidate_generation_seconds": candidate_seconds,
                "active_scored_percentile_products": popularity_scores.filter(
                    F.col("popularity_percentile").isNotNull()
                ).count(),
                "parameters": dict(config.get("models", "popularity")),
            }
            stage_details["popularity"] = details
            _complete_stage(working, "popularity", signature, tables, details)
        _release_spark(spark)

        # 2/5: the sole explicit ALS fit and raw held-out predictions.
        marker = _reuse_stage(working, "als", signature)
        if marker is not None:
            take_reused("als", marker)
        else:
            model_path = working / "_models" / "als"
            contract = _model_contract(model_path, signature)
            als_parameters = dict(config.get("models", "als"))
            if contract is None:
                started = time.perf_counter()
                model = fit_explicit_als(als_train)
                training_seconds = time.perf_counter() - started
                contract = _save_model(
                    model,
                    model_path,
                    signature=signature,
                    training_seconds=training_seconds,
                    parameters={**als_parameters, "fit_count": 1, "seed": 42},
                )
            else:
                model = ALSModel.load(str(model_path))
                training_seconds = float(contract["training_seconds"])

            candidate_started = time.perf_counter()
            request_mapping = requests.join(
                customers.select("customer_id", "customer_int_id"),
                "customer_id",
                "inner",
            )
            als_recommendations = generate_als_recommendations(
                model,
                request_mapping,
                stage_seen,
                active_catalog,
                raw_candidate_depth=config.get(
                    "models", "als", "raw_candidate_depth"
                ),
                candidate_depth=config.get("models", "als", "candidate_depth"),
            ).withColumnRenamed("recommendation_rank", "rank")
            publish("als_recommendations", als_recommendations)
            candidate_seconds = time.perf_counter() - candidate_started

            prediction_started = time.perf_counter()
            held_out = validation.withColumn("stage", F.lit("validation")).unionByName(
                test.withColumn("stage", F.lit("test"))
            )
            predictions = build_als_prediction_table(model, held_out, customers)
            publish("als_predictions", predictions)
            publish("als_user_factors", model.userFactors)
            publish("als_item_factors", model.itemFactors)
            prediction_seconds = time.perf_counter() - prediction_started
            prediction_status = {
                row.prediction_status: int(row["count"])
                for row in spark.read.parquet(str(working / "als_predictions"))
                .groupBy("prediction_status")
                .count()
                .collect()
            }
            details = {
                "fit_count": 1,
                "training_seconds": training_seconds,
                "candidate_generation_seconds": candidate_seconds,
                "heldout_prediction_seconds": prediction_seconds,
                "training_rows": als_train.count(),
                "user_factor_count": tables["als_user_factors"]["rows"],
                "item_factor_count": tables["als_item_factors"]["rows"],
                "prediction_status": prediction_status,
                "parameters": {**als_parameters, "seed": 42},
            }
            stage_details["als"] = details
            _complete_stage(
                working,
                "als",
                signature,
                tables,
                details,
                model_path="_models/als",
            )
        _release_spark(spark)

        # 3/5: one FP-Growth fit over positive-review baskets; join-based scoring.
        marker = _reuse_stage(working, "fp", signature)
        if marker is not None:
            take_reused("fp", marker)
        else:
            model_path = working / "_models" / "fp_growth"
            contract = _model_contract(model_path, signature)
            basket_count = baskets.count()
            truncated_users = (
                train.filter(F.col("is_positive"))
                .groupBy("customer_id")
                .agg(F.countDistinct("product_id").alias("positive_items"))
                .filter(F.col("positive_items") > F.lit(50))
                .count()
            )
            fp_parameters = dict(config.get("models", "fp_growth"))
            if contract is None:
                started = time.perf_counter()
                artifacts = fit_fp_growth(
                    baskets,
                    train.filter(F.col("is_positive")),
                    requests,
                    active_catalog,
                    stage_seen,
                    spark.read.parquet(str(working / "popularity_scores")),
                )
                model = artifacts.model
                rules = artifacts.rules
                parameters = dict(artifacts.parameters)
                training_seconds = time.perf_counter() - started
                contract = _save_model(
                    model,
                    model_path,
                    signature=signature,
                    training_seconds=training_seconds,
                    parameters=parameters,
                )
            else:
                model = FPGrowthModel.load(str(model_path))
                training_seconds = float(contract["training_seconds"])
                parameters = dict(contract["parameters"])
                rules = build_singleton_rules(
                    model.associationRules,
                    model.freqItemsets,
                    basket_count=basket_count,
                )
            publish("fp_rules", rules)
            candidate_started = time.perf_counter()
            # Score only from the bounded, materialized singleton-rule table.  This
            # cuts the MLlib model lineage and makes an interrupted candidate pass
            # independently resumable without another FP fit.
            fp_recommendations = score_fp_recommendations(
                spark.read.parquet(str(working / "fp_rules")),
                train.filter(F.col("is_positive")),
                requests,
                active_catalog,
                stage_seen,
                spark.read.parquet(str(working / "popularity_scores")),
            )
            publish("fp_recommendations", fp_recommendations)
            candidate_seconds = time.perf_counter() - candidate_started
            minimum_count = fp_minimum_count(basket_count)
            training_summary = spark.createDataFrame(
                [
                    (
                        basket_count,
                        truncated_users,
                        minimum_count,
                        minimum_count / basket_count,
                        int(tables["fp_rules"]["rows"]),
                    )
                ],
                "basket_count long, truncated_user_count long, minimum_count long, "
                "min_support double, retained_rule_count long",
            )
            publish("fp_training_summary", training_summary)
            details = {
                "fit_count": 1,
                "training_seconds": training_seconds,
                "candidate_generation_seconds": candidate_seconds,
                "basket_count": basket_count,
                "truncated_user_count": truncated_users,
                "minimum_count": minimum_count,
                "min_support": minimum_count / basket_count,
                "retained_rule_count": tables["fp_rules"]["rows"],
                "parameters": {**fp_parameters, **parameters},
            }
            stage_details["fp"] = details
            _complete_stage(
                working,
                "fp",
                signature,
                tables,
                details,
                model_path="_models/fp_growth",
            )
        _release_spark(spark)

        # 4/5: internal recommendation graph plus catalog+orphan structural graph.
        marker = _reuse_stage(working, "graph", signature)
        if marker is not None:
            take_reused("graph", marker)
        else:
            feature_started = time.perf_counter()
            graph_vertices = build_graph_vertices(products)
            internal_edges = build_internal_graph_edges(g5_graph_edges)
            publish("graph_vertices", graph_vertices)
            publish("graph_internal_edges", internal_edges)
            graph_vertices = spark.read.parquet(str(working / "graph_vertices"))
            internal_edges = spark.read.parquet(str(working / "graph_internal_edges"))

            # Keep structural algorithms independently resumable.  In particular,
            # a full-graph WCC failure must not discard a completed PageRank run.
            from graphframes import GraphFrame

            graph_edges = internal_edges.select(
                F.col("source_product_id").alias("src"),
                F.col("target_product_id").alias("dst"),
            ).dropDuplicates(["src", "dst"])
            internal_graph = GraphFrame(graph_vertices.select("id"), graph_edges)

            if (working / "graph_pagerank" / "_SUCCESS").is_file():
                publish(
                    "graph_pagerank",
                    spark.read.parquet(str(working / "graph_pagerank")),
                )
            else:
                pagerank = internal_graph.pageRank(
                    resetProbability=0.15, maxIter=10
                ).vertices.select(
                    F.col("id").alias("product_id"),
                    F.col("pagerank").cast("double").alias("pagerank"),
                )
                publish("graph_pagerank", pagerank)

            if (working / "graph_degrees" / "_SUCCESS").is_file():
                publish(
                    "graph_degrees",
                    spark.read.parquet(str(working / "graph_degrees")),
                )
            else:
                in_degree = graph_edges.groupBy("dst").agg(
                    F.count(F.lit(1)).cast("long").alias("in_degree")
                )
                out_degree = graph_edges.groupBy("src").agg(
                    F.count(F.lit(1)).cast("long").alias("out_degree")
                )
                degrees = (
                    graph_vertices.select("id")
                    .join(in_degree, F.col("id") == F.col("dst"), "left")
                    .join(out_degree, F.col("id") == F.col("src"), "left")
                    .select(
                        F.col("id").alias("product_id"),
                        F.coalesce(F.col("in_degree"), F.lit(0)).alias("in_degree"),
                        F.coalesce(F.col("out_degree"), F.lit(0)).alias("out_degree"),
                    )
                )
                publish("graph_degrees", degrees)

            if (working / "graph_edge_reciprocity" / "_SUCCESS").is_file():
                publish(
                    "graph_edge_reciprocity",
                    spark.read.parquet(str(working / "graph_edge_reciprocity")),
                )
            else:
                reverse = internal_edges.select(
                    F.col("target_product_id").alias("source_product_id"),
                    F.col("source_product_id").alias("target_product_id"),
                ).withColumn("is_reciprocal", F.lit(True))
                reciprocity_frame = (
                    internal_edges.join(
                        reverse,
                        ["source_product_id", "target_product_id"],
                        "left",
                    )
                    .withColumn(
                        "is_reciprocal",
                        F.coalesce(F.col("is_reciprocal"), F.lit(False)),
                    )
                )
                publish("graph_edge_reciprocity", reciprocity_frame)

            if (working / "graph_weak_components" / "_SUCCESS").is_file():
                publish(
                    "graph_weak_components",
                    spark.read.parquet(str(working / "graph_weak_components")),
                )
            else:
                # GraphFrames' GraphX implementation has the same weak-component
                # semantics and is substantially more stable for this 548k-vertex
                # local graph than two_phase's iterative SQL plan.
                weak_components = internal_graph.connectedComponents(
                    algorithm="graphx"
                ).select(
                    F.col("id").alias("product_id"),
                    F.col("component").cast("long").alias("component_id"),
                )
                publish("graph_weak_components", weak_components)

            extended_inputs = build_extended_graph_inputs(products, physical_similar_edges)
            publish("extended_graph_vertices", extended_inputs.vertices)
            publish("extended_graph_edges", extended_inputs.edges)
            extended_pagerank = run_extended_graph_pagerank(
                spark.read.parquet(str(working / "extended_graph_vertices")),
                spark.read.parquet(str(working / "extended_graph_edges")),
            )
            publish("extended_graph_pagerank", extended_pagerank)

            internal_rank = spark.read.parquet(str(working / "graph_pagerank"))
            extended_rank = spark.read.parquet(str(working / "extended_graph_pagerank"))
            publish(
                "graph_pagerank_top",
                internal_rank.orderBy(F.col("pagerank").desc(), F.col("product_id").asc()).limit(100),
            )
            publish(
                "extended_graph_pagerank_top",
                extended_rank.orderBy(
                    F.col("pagerank").desc(), F.col("asin").asc()
                ).limit(100),
            )

            reciprocity = spark.read.parquet(str(working / "graph_edge_reciprocity")).agg(
                F.sum(F.col("is_reciprocal").cast("long")).alias("reciprocal_edges"),
                F.count(F.lit(1)).alias("edge_count"),
            ).first().asDict()
            components = (
                spark.read.parquet(str(working / "graph_weak_components"))
                .groupBy("component_id")
                .count()
                .agg(
                    F.count(F.lit(1)).alias("component_count"),
                    F.max("count").alias("largest_component_size"),
                )
                .first()
                .asDict()
            )
            orphan_rank = extended_rank.agg(
                F.sum((~F.col("is_catalog")).cast("long")).alias("orphan_vertices"),
                F.sum(
                    F.when(~F.col("is_catalog"), F.col("pagerank")).otherwise(F.lit(0.0))
                ).alias("orphan_pagerank_sum"),
                F.max(F.when(~F.col("is_catalog"), F.col("pagerank"))).alias(
                    "max_orphan_pagerank"
                ),
            ).first().asDict()
            edge_count = int(reciprocity["edge_count"])
            structural_row = (
                int(tables["graph_vertices"]["rows"]),
                edge_count,
                int(reciprocity["reciprocal_edges"] or 0),
                float((reciprocity["reciprocal_edges"] or 0) / edge_count),
                int(components["component_count"]),
                int(components["largest_component_size"]),
                int(tables["extended_graph_vertices"]["rows"]),
                int(tables["extended_graph_edges"]["rows"]),
                int(orphan_rank["orphan_vertices"] or 0),
                float(orphan_rank["orphan_pagerank_sum"] or 0.0),
                float(orphan_rank["max_orphan_pagerank"] or 0.0),
            )
            graph_summary = spark.createDataFrame(
                [structural_row],
                "internal_vertices long, internal_edges long, reciprocal_edges long, "
                "reciprocal_edge_ratio double, weak_component_count long, "
                "largest_component_size long, extended_vertices long, extended_edges long, "
                "orphan_vertices long, orphan_pagerank_sum double, max_orphan_pagerank double",
            )
            publish("graph_structural_summary", graph_summary)
            feature_seconds = time.perf_counter() - feature_started

            candidate_started = time.perf_counter()
            graph_recommendations = generate_graph_recommendations(
                train,
                requests,
                internal_edges,
                active_catalog,
                stage_seen,
                internal_rank,
                spark.read.parquet(str(working / "popularity_scores")),
            )
            publish("graph_recommendations", graph_recommendations)
            candidate_seconds = time.perf_counter() - candidate_started
            graph_null_bayes = spark.read.parquet(
                str(working / "graph_recommendations")
            ).filter(F.col("bayesian_score").isNull()).count()
            details = {
                "fit_count": 1,
                "training_seconds": feature_seconds,
                "candidate_generation_seconds": candidate_seconds,
                "pagerank_reset_probability": 0.15,
                "pagerank_max_iter": 10,
                "page_rank_used_only_as_tie_break": True,
                "recommendations_missing_bayesian_tie_score": graph_null_bayes,
                "structural": {
                    name: value
                    for name, value in zip(
                        (
                            "internal_vertices",
                            "internal_edges",
                            "reciprocal_edges",
                            "reciprocal_edge_ratio",
                            "weak_component_count",
                            "largest_component_size",
                            "extended_vertices",
                            "extended_edges",
                            "orphan_vertices",
                            "orphan_pagerank_sum",
                            "max_orphan_pagerank",
                        ),
                        structural_row,
                        strict=True,
                    )
                },
                "parameters": dict(config.get("models", "graph")),
            }
            stage_details["graph"] = details
            _complete_stage(working, "graph", signature, tables, details)
        _release_spark(spark)

        # 5/5: active-metadata IDF vectors and train-only category personalization.
        marker = _reuse_stage(working, "category", signature)
        if marker is not None:
            take_reused("category", marker)
        else:
            feature_started = time.perf_counter()
            # Binding N: active products that actually have category metadata.
            active_metadata_products = active_item_features.select(
                "product_id"
            ).distinct().count()
            item_frames = build_category_item_features(
                active_item_features, active_metadata_products
            )
            publish("category_item_vectors", item_frames.item_vectors)
            publish("category_statistics", item_frames.category_statistics)
            publish("category_item_norms", item_frames.item_norms)

            # Historical discontinued products can still contribute category signal.
            # Their weights use IDF learned only from the active-metadata universe.
            statistics = spark.read.parquet(str(working / "category_statistics"))
            profile_vectors = (
                all_category_nodes.groupBy("product_id", "category_id")
                .agg(F.max("normalized_depth_weight").alias("depth_weight"))
                .join(
                    statistics.select(
                        "category_id", "idf", "document_frequency", "document_ratio"
                    ),
                    "category_id",
                    "inner",
                )
                .withColumn(
                    "item_category_weight", F.col("idf") * F.col("depth_weight")
                )
            )
            publish("category_profile_item_vectors", profile_vectors)
            item_vectors = spark.read.parquet(str(working / "category_item_vectors"))
            profile_vectors = spark.read.parquet(
                str(working / "category_profile_item_vectors")
            )
            user_frames = build_category_user_features(
                train,
                requests,
                profile_vectors,
                products.select("product_id", "group"),
            )
            publish("category_user_profiles", user_frames.user_category_profiles)
            publish("category_user_norms", user_frames.user_norms)
            publish("category_group_affinity", user_frames.user_group_affinity)
            popularity_scores = spark.read.parquet(str(working / "popularity_scores"))
            category_top = build_category_top_products(
                item_vectors,
                popularity_scores,
                generic_category_ratio=config.get(
                    "models", "category", "generic_category_ratio"
                ),
                products_per_category=config.get(
                    "models", "category", "products_per_category"
                ),
            )
            publish("category_top_products", category_top)
            feature_seconds = time.perf_counter() - feature_started

            candidate_started = time.perf_counter()
            candidate_pool = build_category_candidate_pool(
                spark.read.parquet(str(working / "category_user_profiles")),
                spark.read.parquet(str(working / "category_top_products")),
                popularity_scores,
                max_profile_categories=config.get(
                    "models", "category", "max_profile_categories"
                ),
                max_candidate_pool=config.get(
                    "models", "category", "max_candidate_pool"
                ),
            )
            publish("category_candidate_pool", candidate_pool)
            scored = score_category_candidate_pool(
                spark.read.parquet(str(working / "category_candidate_pool")),
                spark.read.parquet(str(working / "category_user_profiles")),
                spark.read.parquet(str(working / "category_user_norms")),
                item_vectors,
                spark.read.parquet(str(working / "category_item_norms")),
                spark.read.parquet(str(working / "category_group_affinity")),
                active_catalog,
                similarity_weight=config.get(
                    "models", "category", "similarity_weight"
                ),
                group_affinity_weight=config.get(
                    "models", "category", "group_affinity_weight"
                ),
                popularity_percentile_weight=config.get(
                    "models", "category", "popularity_percentile_weight"
                ),
            )
            publish("category_scored_candidates", scored)
            category_recommendations = rank_category_recommendations(
                spark.read.parquet(str(working / "category_scored_candidates")),
                requests,
                stage_seen,
                candidate_depth=config.get("models", "category", "candidate_depth"),
            )
            publish("category_recommendations", category_recommendations)
            candidate_seconds = time.perf_counter() - candidate_started

            user_norms = spark.read.parquet(str(working / "category_user_norms"))
            unique_request_users = requests.select("customer_id").distinct()
            zero_norm_users = unique_request_users.join(
                user_norms.filter(F.col("user_norm") > F.lit(0.0)).select("customer_id"),
                "customer_id",
                "left_anti",
            ).count()
            category_summary = spark.createDataFrame(
                [
                    (
                        active_metadata_products,
                        int(tables["category_statistics"]["rows"]),
                        int(tables["category_user_profiles"]["rows"]),
                        zero_norm_users,
                    )
                ],
                "active_metadata_products long, category_count long, "
                "user_profile_rows long, zero_norm_request_users long",
            )
            publish("category_training_summary", category_summary)
            details = {
                "fit_count": 1,
                "training_seconds": feature_seconds,
                "candidate_generation_seconds": candidate_seconds,
                "idf_denominator_active_metadata_products": active_metadata_products,
                "category_count": tables["category_statistics"]["rows"],
                "zero_norm_request_users": zero_norm_users,
                "historical_profile_items_include_discontinued": True,
                "parameters": dict(config.get("models", "category")),
            }
            stage_details["category"] = details
            _complete_stage(working, "category", signature, tables, details)
        _release_spark(spark)

        # Shared contract proof over the exact frozen lists consumed by G8.
        validations: dict[str, dict[str, Any]] = {}
        for model in MODEL_NAMES:
            recommendations = spark.read.parquet(
                str(working / f"{model}_recommendations")
            )
            validations[model] = validate_recommendation_table(
                recommendations,
                model=model,
                requests=requests,
                active_catalog=active_catalog,
                stage_seen_items=stage_seen,
            )

        runtime_rows = []
        for model in MODEL_NAMES:
            details = stage_details[model]
            runtime_rows.append(
                (
                    model,
                    float(details["training_seconds"]),
                    float(details["candidate_generation_seconds"]),
                    int(details["fit_count"]),
                    json.dumps(details["parameters"], sort_keys=True),
                )
            )
        runtime_summary = spark.createDataFrame(
            runtime_rows,
            "model string, training_seconds double, candidate_generation_seconds double, "
            "fit_count int, parameters_json string",
        )
        publish("model_runtime_summary", runtime_summary)
        validation_summary = spark.createDataFrame(
            [
                (
                    model,
                    int(value["rows"]),
                    int(value["request_count"]),
                    int(value["users_with_output"]),
                    int(value["empty_requests"]),
                    float(value["user_coverage"]),
                    int(value["min_candidates"]),
                    float(value["avg_candidates"]),
                    int(value["max_candidates"]),
                    int(value["distinct_catalog_products"]),
                    int(value["depth"]),
                )
                for model, value in validations.items()
            ],
            "model string, rows long, request_count long, users_with_output long, "
            "empty_requests long, user_coverage double, min_candidates long, "
            "avg_candidates double, max_candidates long, distinct_catalog_products long, "
            "candidate_depth int",
        )
        publish("recommendation_validation_summary", validation_summary)
        budget = spark.createDataFrame(
            [
                (model, 1, MODEL_DEPTHS[model], "train_only_single_fit")
                for model in MODEL_NAMES
            ],
            "model string, fit_count int, candidate_depth int, training_contract string",
        )
        publish("experiment_budget_summary", budget)

        if len(stage_details) != 5 or any(
            int(stage_details[model]["fit_count"]) != 1 for model in MODEL_NAMES
        ):
            raise RuntimeError("G7 experiment budget must contain exactly five single-fit models")
        if tables["experiment_budget_summary"]["rows"] != 5:
            raise RuntimeError("G7 experiment budget table must contain exactly five rows")

        junit = _junit(evidence_file)
        os.replace(working, final)
    except Exception:
        cleanup_incomplete_publications(working)
        raise

    def final_path(value: Any) -> Any:
        if isinstance(value, str):
            return value.replace(str(working), str(final), 1)
        if isinstance(value, dict):
            return {key: final_path(item) for key, item in value.items()}
        if isinstance(value, list):
            return [final_path(item) for item in value]
        return value

    tables = final_path(tables)
    stage_details = final_path(stage_details)
    request_count = requests.count()
    return {
        "junit": junit,
        "implementation_sha256": signature,
        "scratch_directories_removed": cleaned_scratch,
        "tables_reused": sorted(set(reused_tables)),
        "substages_reused": sorted(set(reused_stages)),
        "single_fit_contract": True,
        "train_only_feature_lineage": True,
        "independent_model_count": 5,
        "independent_models": list(MODEL_NAMES),
        "hybrid_models_trained": 0,
        "evaluation_request_count": request_count,
        "request_key": ["stage", "customer_id"],
        "stage_details": stage_details,
        "recommendation_validations": validations,
        "tables": tables,
    }
