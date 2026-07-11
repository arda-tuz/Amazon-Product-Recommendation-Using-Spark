"""G10 Spark-free Streamlit serving contract and compact dashboard exports.

The phase never imports PySpark.  DuckDB reads completed upstream Parquet tables,
materializes compact presentation indexes/aggregates, and publishes the complete G10
directory with one atomic rename.  A synthetic, source-backed AppTest fixture proves
that all four pages execute without a server or browser; static import auditing and a
child-process probe prove that the presentation code cannot start Spark.
"""

from __future__ import annotations

import ast
import hashlib
import json
import os
import shutil
import uuid
import xml.etree.ElementTree as ET
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Final, Iterable, Mapping, Sequence

import duckdb
import psutil
import pyarrow as pa
import pyarrow.parquet as pq

from amazon_recommender.core.manifest import atomic_write_json
from amazon_recommender.gate_handlers import register


G10_CONTRACT_VERSION: Final[int] = 1
G10_DUCKDB_BUILD_THREADS: Final[int] = 1
G10_DUCKDB_BUILD_MEMORY_LIMIT: Final[str] = "2GB"
OUTPUT_TABLES: Final[tuple[str, ...]] = (
    "dashboard_overview",
    "dashboard_quality_summary",
    "dashboard_group_distribution",
    "dashboard_rating_distribution",
    "dashboard_review_year_distribution",
    "dashboard_activity_quantiles",
    "product_search_index",
    "category_search_index",
    "servable_customers",
    "demo_users",
    "dashboard_contract",
)
PAGE_FILES: Final[tuple[str, ...]] = (
    "2_Product_and_Graph_Explorer.py",
    "3_Recommendation_Lab.py",
    "4_Model_and_Experiment_Comparison.py",
)
FORBIDDEN_IMPORT_ROOTS: Final[set[str]] = {
    "pyspark",
    "graphframes",
    "py4j",
    "subprocess",
}
REQUIRED_UPSTREAM: Final[Mapping[str, str]] = {
    "profile_metrics": "data/g5/profile_metrics",
    "data_quality_summary": "data/g5/data_quality_summary",
    "data_quality_samples": "data/g5/data_quality_samples",
    "product_quality_profile": "data/g5/product_quality_profile",
    "evaluation_users": "data/g6/evaluation_users",
    "active_catalog": "data/g6/active_catalog",
    "stage_seen_items": "data/g6/stage_seen_items",
    "popularity_recommendations": "data/g7/popularity_recommendations",
    "als_recommendations": "data/g7/als_recommendations",
    "fp_recommendations": "data/g7/fp_recommendations",
    "graph_recommendations": "data/g7/graph_recommendations",
    "category_recommendations": "data/g7/category_recommendations",
    "popularity_scores": "data/g7/popularity_scores",
    "category_top_products": "data/g7/category_top_products",
    "graph_internal_edges": "data/g7/graph_internal_edges",
    "graph_pagerank": "data/g7/graph_pagerank",
    "graph_degrees": "data/g7/graph_degrees",
    "graph_weak_components": "data/g7/graph_weak_components",
    "graph_structural_summary": "data/g7/graph_structural_summary",
    "g7_model_runtime": "data/g7/model_runtime_summary",
    "hybrid_candidates": "data/g8/hybrid_candidates",
    "hybrid_a_recommendations": "data/g8/hybrid_a_recommendations",
    "hybrid_b_recommendations": "data/g8/hybrid_b_recommendations",
    "evaluation_per_user": "data/g9/evaluation_per_user",
    "evaluation_summary": "data/g9/evaluation_summary",
    "als_prediction_summary": "data/g9/als_prediction_summary",
    "selected_hybrid": "data/g9/selected_hybrid",
    "validation_hybrid_comparison": "data/g9/validation_hybrid_comparison",
    "official_test_comparison": "data/g9/official_test_comparison",
    "g9_model_runtime": "data/g9/model_runtime_summary",
    "products": "data/full/silver/products",
    "category_paths": "data/full/silver/category_paths",
    "category_nodes": "data/full/silver/category_nodes",
    "reviews": "data/full/silver/reviews_deduplicated",
    "interactions": "data/full/silver/user_item_interactions",
}

# Once G10 succeeds, each user-facing path resolves to G5+ Gold or to a compact G10
# export.  The five Silver inputs above are build-time sources only.
GOLD_RUNTIME_PATHS: Final[Mapping[str, str]] = {
    "overview_metrics": "data/g10/dashboard_overview",
    "quality_summary": "data/g10/dashboard_quality_summary",
    "quality_samples": "data/g5/data_quality_samples",
    "product_quality": "data/g5/product_quality_profile",
    "products": "data/g10/product_search_index",
    "category_paths": "data/g10/product_search_index",
    "category_nodes": "data/g10/category_search_index",
    "group_distribution": "data/g10/dashboard_group_distribution",
    "rating_distribution": "data/g10/dashboard_rating_distribution",
    "review_year_distribution": "data/g10/dashboard_review_year_distribution",
    "activity_quantiles": "data/g10/dashboard_activity_quantiles",
    "active_catalog": "data/g6/active_catalog",
    "evaluation_users": "data/g6/evaluation_users",
    "seen_items": "data/g6/stage_seen_items",
    "graph_edges": "data/g7/graph_internal_edges",
    "graph_pagerank": "data/g7/graph_pagerank",
    "graph_degrees": "data/g7/graph_degrees",
    "graph_components": "data/g7/graph_weak_components",
    "graph_summary": "data/g7/graph_structural_summary",
    "popularity_scores": "data/g7/popularity_scores",
    "category_top_products": "data/g7/category_top_products",
    "popularity_recommendations": "data/g7/popularity_recommendations",
    "als_recommendations": "data/g7/als_recommendations",
    "fp_recommendations": "data/g7/fp_recommendations",
    "graph_recommendations": "data/g7/graph_recommendations",
    "category_recommendations": "data/g7/category_recommendations",
    "h_a_recommendations": "data/g8/hybrid_a_recommendations",
    "h_b_recommendations": "data/g8/hybrid_b_recommendations",
    "hybrid_candidates": "data/g8/hybrid_candidates",
    "evaluation_summary": "data/g9/evaluation_summary",
    "evaluation_per_user": "data/g9/evaluation_per_user",
    "als_prediction_summary": "data/g9/als_prediction_summary",
    "selected_hybrid": "data/g9/selected_hybrid",
    "model_runtime": "data/g9/model_runtime_summary",
    "servable_customers": "data/g10/servable_customers",
    "demo_users": "data/g10/demo_users",
}


def _junit(path: Path) -> dict[str, Any]:
    root = ET.parse(path).getroot()
    suites = [root] if root.tag == "testsuite" else list(root.findall("testsuite"))
    summary = {
        key: sum(int(suite.attrib.get(key, "0")) for suite in suites)
        for key in ("tests", "failures", "errors", "skipped")
    }
    if not summary["tests"] or summary["failures"] or summary["errors"]:
        raise RuntimeError(f"G10 JUnit evidence is not passing: {summary}")
    summary["path"] = str(path.resolve())
    return summary


def _implementation_signature(config_sha256: str, project_root: Path) -> str:
    digest = hashlib.sha256()
    digest.update(f"g10-contract-{G10_CONTRACT_VERSION}".encode("ascii"))
    digest.update(config_sha256.encode("ascii"))
    files = [Path(__file__)] + sorted((project_root / "app").rglob("*.py"))
    for path in files:
        relative = path.relative_to(project_root) if path.is_relative_to(project_root) else path.name
        digest.update(str(relative).encode("utf-8"))
        digest.update(path.read_bytes())
    return digest.hexdigest()


def _prepare_workspace(working: Path, signature: str) -> list[str]:
    marker = working / "_checkpoint_contract.json"
    removed: list[str] = []
    if working.exists():
        try:
            existing = json.loads(marker.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError):
            existing = {}
        if existing.get("implementation_sha256") != signature:
            shutil.rmtree(working)
    working.mkdir(parents=True, exist_ok=True)
    for child in working.iterdir():
        if child.name.startswith(".") and child.name.endswith(".tmp"):
            removed.append(str(child))
            shutil.rmtree(child, ignore_errors=True)
        elif child.is_dir() and child.name in OUTPUT_TABLES:
            if not (child / "_SUCCESS").is_file() or not any(child.glob("*.parquet")):
                removed.append(str(child))
                shutil.rmtree(child)
    atomic_write_json(
        marker,
        {
            "gate": "G10",
            "contract_version": G10_CONTRACT_VERSION,
            "implementation_sha256": signature,
        },
    )
    return removed


def _parquet_expr(path: Path) -> str:
    if not (path / "_SUCCESS").is_file() or not any(path.glob("*.parquet")):
        raise FileNotFoundError(f"Incomplete Parquet table: {path}")
    glob = str(path.resolve() / "*.parquet").replace("'", "''")
    return f"read_parquet('{glob}', union_by_name=true)"


def _connect(temp_directory: Path | None = None) -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect(database=":memory:")
    # Building the product search index aggregates 2.5M category paths into
    # product-level lists.  This is an offline Gold build, not the dashboard's
    # bounded serving connection, so give the single worker enough headroom to
    # complete while retaining a hard, workstation-safe cap and spill path.
    connection.execute(f"SET threads = {G10_DUCKDB_BUILD_THREADS}")
    connection.execute(f"SET memory_limit = '{G10_DUCKDB_BUILD_MEMORY_LIMIT}'")
    connection.execute("SET preserve_insertion_order = false")
    if temp_directory is not None:
        temp_directory.mkdir(parents=True, exist_ok=True)
        escaped = str(temp_directory.resolve()).replace("'", "''")
        connection.execute(f"SET temp_directory = '{escaped}'")
    return connection


def _directory_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    for file in sorted(path.glob("*.parquet")):
        digest.update(file.name.encode("utf-8"))
        with file.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
    return digest.hexdigest()


def _table_evidence(connection: duckdb.DuckDBPyConnection, path: Path) -> dict[str, Any]:
    expression = _parquet_expr(path)
    rows = int(connection.execute(f"SELECT COUNT(*) FROM {expression}").fetchone()[0])
    schema_rows = connection.execute(f"DESCRIBE SELECT * FROM {expression}").fetchall()
    files = sorted(path.glob("*.parquet"))
    return {
        "path": str(path.resolve()),
        "rows": rows,
        "parquet_files": len(files),
        "size_bytes": sum(file.stat().st_size for file in files),
        "sha256": _directory_sha256(path),
        "schema": [{"name": row[0], "type": row[1]} for row in schema_rows],
    }


def _publish_query(
    connection: duckdb.DuckDBPyConnection,
    working: Path,
    name: str,
    query: str,
) -> tuple[dict[str, Any], bool]:
    if name not in OUTPUT_TABLES:
        raise ValueError(f"Unknown G10 output: {name}")
    target = working / name
    if (target / "_SUCCESS").is_file() and any(target.glob("*.parquet")):
        return _table_evidence(connection, target), True
    if target.exists():
        shutil.rmtree(target)
    temporary = working / f".{name}.{uuid.uuid4().hex}.tmp"
    temporary.mkdir(parents=True)
    destination = str(temporary / "part-00000.parquet").replace("'", "''")
    try:
        normalized = query.strip().rstrip(";")
        connection.execute(
            f"COPY ({normalized}) TO '{destination}' "
            "(FORMAT PARQUET, COMPRESSION SNAPPY)"
        )
        (temporary / "_SUCCESS").touch()
        os.replace(temporary, target)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return _table_evidence(connection, target), False


def _publish_rows(
    connection: duckdb.DuckDBPyConnection,
    working: Path,
    name: str,
    rows: Sequence[Mapping[str, object]],
) -> tuple[dict[str, Any], bool]:
    target = working / name
    if (target / "_SUCCESS").is_file() and any(target.glob("*.parquet")):
        return _table_evidence(connection, target), True
    if not rows:
        raise ValueError(f"{name} requires at least one row")
    temporary = working / f".{name}.{uuid.uuid4().hex}.tmp"
    temporary.mkdir(parents=True)
    try:
        table = pa.Table.from_pylist([dict(row) for row in rows])
        pq.write_table(table, temporary / "part-00000.parquet", compression="snappy")
        (temporary / "_SUCCESS").touch()
        if target.exists():
            shutil.rmtree(target)
        os.replace(temporary, target)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return _table_evidence(connection, target), False


def audit_dashboard_sources(project_root: Path) -> dict[str, Any]:
    """Prove four pages and a Spark-free, server-free import graph."""

    app_root = project_root / "app"
    home = app_root / "Home.py"
    pages = app_root / "pages"
    expected = [pages / name for name in PAGE_FILES]
    missing = [str(path) for path in [home, *expected] if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"G10 application files are missing: {missing}")
    actual_pages = sorted(path.name for path in pages.glob("[1-4]_*.py"))
    if actual_pages != sorted(PAGE_FILES):
        raise RuntimeError(
            "G10 requires Home as Page 1 and only Pages 2-4 under app/pages: "
            f"{actual_pages}"
        )

    audited: list[dict[str, Any]] = []
    imported_roots: set[str] = set()
    forbidden_calls: list[str] = []
    spark_construction_tokens: list[str] = []
    for path in sorted(app_root.rglob("*.py")):
        source = path.read_text(encoding="utf-8")
        for token in ("SparkSession", "spark-submit", "PYSPARK_SUBMIT_ARGS"):
            if token in source:
                spark_construction_tokens.append(f"{path.name}:{token}")
        tree = ast.parse(source, filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported_roots.add(node.module.split(".", 1)[0])
            elif isinstance(node, ast.Call):
                function = node.func
                if isinstance(function, ast.Attribute) and function.attr in {
                    "system",
                    "popen",
                    "Popen",
                    "run",
                    "call",
                }:
                    if isinstance(function.value, ast.Name) and function.value.id in {
                        "os",
                        "subprocess",
                    }:
                        forbidden_calls.append(f"{path.name}:{function.attr}")
        audited.append(
            {
                "path": str(path.relative_to(project_root)),
                "sha256": hashlib.sha256(source.encode("utf-8")).hexdigest(),
            }
        )
    forbidden_imports = sorted(imported_roots.intersection(FORBIDDEN_IMPORT_ROOTS))
    if forbidden_imports or forbidden_calls or spark_construction_tokens:
        raise RuntimeError(
            f"Dashboard may start an external/Spark process: imports={forbidden_imports}, "
            f"calls={forbidden_calls}, spark_tokens={spark_construction_tokens}"
        )

    required_text = {
        "Home.py": ("product_group_distribution", "rating_distribution", "quality_summary"),
        PAGE_FILES[0]: ("search_products", "ego_figure", "graph_neighbors"),
        PAGE_FILES[1]: ("Mevcut müşteri", "Başlangıç ürünü / sepeti", "Yeni kullanıcı kategorisi"),
        PAGE_FILES[2]: ("ndcg_at_10", "hit_rate_at_10", "mrr_at_10"),
    }
    for name, snippets in required_text.items():
        path = home if name == "Home.py" else pages / name
        source = path.read_text(encoding="utf-8")
        absent = [snippet for snippet in snippets if snippet not in source]
        if absent:
            raise RuntimeError(f"{name} is missing binding UI contracts: {absent}")
    recommendation_source = (app_root / "lib" / "recommendation.py").read_text(
        encoding="utf-8"
    )
    fp_wording = "aynı kullanıcılar tarafından birlikte olumlu değerlendirilmiştir"
    if fp_wording not in recommendation_source:
        raise RuntimeError("FP explanation does not use the binding positive-rating wording")
    return {
        "status": "passed",
        "entrypoint": str(home.relative_to(project_root)),
        "page_count": 1 + len(expected),
        "pages": [str(home.relative_to(project_root))]
        + [str(path.relative_to(project_root)) for path in expected],
        "audited_python_files": audited,
        "forbidden_imports": forbidden_imports,
        "forbidden_process_calls": forbidden_calls,
        "spark_session_construction": False,
        "spark_construction_tokens": spark_construction_tokens,
        "streamlit_server_started": False,
        "fp_explanation_contract": fp_wording,
    }


def _write_fixture_table(run: Path, relative: str, rows: Sequence[Mapping[str, object]]) -> None:
    target = run / relative
    target.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist([dict(row) for row in rows]), target / "part.parquet")
    (target / "_SUCCESS").touch()


def build_app_test_fixture(root: Path, *, source_sha256: str) -> Path:
    """Create a tiny but data-backed four-page fixture under an isolated artifact root."""

    fixture_root = root / "fixture-artifacts"
    run = fixture_root / "runs" / "run-g10-app-test"
    manifests = run / "manifests"
    manifests.mkdir(parents=True, exist_ok=True)
    (manifests / "G9.json").write_text(
        json.dumps(
            {
                "gate": "G9",
                "run_id": run.name,
                "status": "passed",
                "source_sha256": source_sha256,
                "recorded_at": "2026-07-11T00:00:00Z",
            }
        ),
        encoding="utf-8",
    )
    _write_fixture_table(
        run,
        "data/g10/dashboard_overview",
        [
            {"metric": "products", "value": 2},
            {"metric": "distinct_customers", "value": 1},
            {"metric": "user_item_interactions", "value": 2},
            {"metric": "total_quality_events", "value": 1},
            {"metric": "category_nodes", "value": 1},
            {"metric": "internal_graph_edges", "value": 1},
        ],
    )
    _write_fixture_table(
        run,
        "data/g10/dashboard_quality_summary",
        [{"event_type": "AVG_RATING_MISMATCH", "event_count": 1, "distinct_entities": 1}],
    )
    _write_fixture_table(
        run,
        "data/g10/dashboard_group_distribution",
        [{"product_group": "Book", "product_count": 2}],
    )
    _write_fixture_table(
        run,
        "data/g10/dashboard_rating_distribution",
        [{"rating": value, "review_count": value} for value in range(1, 6)],
    )
    _write_fixture_table(
        run,
        "data/g10/dashboard_review_year_distribution",
        [{"review_year": 2004, "review_count": 2}],
    )
    _write_fixture_table(
        run,
        "data/g10/dashboard_activity_quantiles",
        [
            {"entity_type": "Kullanıcı", "entities": 1, "p50": 2.0, "p90": 2.0, "p99": 2.0, "maximum": 2},
            {"entity_type": "Ürün", "entities": 2, "p50": 1.0, "p90": 1.0, "p99": 1.0, "maximum": 1},
        ],
    )
    products = [
        {
            "product_id": 1,
            "asin": "A000000001",
            "title": "The Observable Catalog",
            "group": "Book",
            "status": "active",
            "is_active": True,
            "reviews_total": 1,
            "reviews_downloaded": 1,
            "avg_rating_raw": 5.0,
            "salesrank_clean": 10,
            "category_paths": ["|Books[1]|Data[2]"],
            "category_search_text": "|Books[1]|Data[2]",
        },
        {
            "product_id": 2,
            "asin": "A000000002",
            "title": "Graph Evidence",
            "group": "Book",
            "status": "active",
            "is_active": True,
            "reviews_total": 1,
            "reviews_downloaded": 1,
            "avg_rating_raw": 4.0,
            "salesrank_clean": 20,
            "category_paths": ["|Books[1]|Data[2]"],
            "category_search_text": "|Books[1]|Data[2]",
        },
    ]
    _write_fixture_table(run, "data/g10/product_search_index", products)
    _write_fixture_table(
        run,
        "data/g5/product_quality_profile",
        [
            {
                **{key: value for key, value in row.items() if key not in {"category_paths", "category_search_text"}},
                "physical_review_count": 1,
                "avg_rating_computed": row["avg_rating_raw"],
            }
            for row in products
        ],
    )
    _write_fixture_table(
        run,
        "data/g5/data_quality_samples",
        [{"event_type": "AVG_RATING_MISMATCH", "sample_rank": 1, "product_id": 1}],
    )
    _write_fixture_table(
        run,
        "data/g10/category_search_index",
        [{"category_id": 2, "category_label": "Data", "product_count": 2}],
    )
    _write_fixture_table(
        run,
        "data/g7/graph_internal_edges",
        [{"source_product_id": 1, "target_product_id": 2, "similar_position": 1}],
    )
    _write_fixture_table(run, "data/g7/graph_pagerank", [{"product_id": 1, "pagerank": 0.5}])
    _write_fixture_table(
        run,
        "data/g7/graph_degrees",
        [{"product_id": 1, "in_degree": 0, "out_degree": 1}],
    )
    _write_fixture_table(
        run,
        "data/g7/graph_weak_components",
        [{"product_id": 1, "component_id": 1}],
    )
    _write_fixture_table(
        run,
        "data/g10/servable_customers",
        [{"stage": "test", "cohort": "operational", "customer_id": "U1"}],
    )
    _write_fixture_table(
        run,
        "data/g10/demo_users",
        [{"demo_rank": 1, "customer_id": "U1", "output_model_count": 6}],
    )
    summary_rows = []
    for stage, models in (("validation", ("h_a", "h_b")), ("test", ("h_a",))):
        for model in models:
            summary_rows.append(
                {
                    "model": model,
                    "stage": stage,
                    "cohort": "common_warm",
                    "slice": "overall",
                    "evaluated_users": 1,
                    "users_with_output": 1,
                    "ndcg_at_10": 0.5,
                    "hit_rate_at_10": 1.0,
                    "mrr_at_10": 0.5,
                    "user_coverage": 1.0,
                    "fill_rate_at_10": 1.0,
                    "catalog_coverage_at_10": 0.5,
                    "distinct_recommended_products_at_10": 1,
                    "active_catalog_size": 2,
                }
            )
    _write_fixture_table(run, "data/g9/evaluation_summary", summary_rows)
    _write_fixture_table(
        run,
        "data/g9/selected_hybrid",
        [{"selected_model": "h_a", "selection_reason": "fixture"}],
    )
    _write_fixture_table(
        run,
        "data/g9/als_prediction_summary",
        [{"model": "als", "stage": "test", "heldout_rows": 1, "rmse": 1.0, "mae": 0.8}],
    )
    _write_fixture_table(
        run,
        "data/g9/model_runtime_summary",
        [{"model": "als", "training_seconds": 1.0, "candidate_generation_seconds": 1.0}],
    )
    return fixture_root


@contextmanager
def _temporary_environment(**values: str):
    previous = {key: os.environ.get(key) for key in values}
    os.environ.update(values)
    try:
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def run_four_page_app_test(project_root: Path, fixture_root: Path) -> dict[str, Any]:
    """Execute Home + four pages in-process; never start a Streamlit server."""

    import streamlit as st
    from streamlit.testing.v1 import AppTest

    process = psutil.Process()
    before = {child.pid for child in process.children(recursive=True)}
    results: list[dict[str, Any]] = []
    targets = [project_root / "app" / "Home.py"] + [
        project_root / "app" / "pages" / name for name in PAGE_FILES
    ]
    with _temporary_environment(
        AMAZON_REC_ARTIFACTS_ROOT=str(fixture_root),
        AMAZON_REC_RUN_ID="run-g10-app-test",
    ):
        st.cache_data.clear()
        st.cache_resource.clear()
        for target in targets:
            test = AppTest.from_file(str(target), default_timeout=30).run()
            exceptions = [str(item.value) for item in test.exception]
            if exceptions:
                raise RuntimeError(f"Streamlit AppTest failed for {target.name}: {exceptions}")
            results.append(
                {
                    "file": str(target.relative_to(project_root)),
                    "exceptions": 0,
                    "markdown_elements": len(test.markdown),
                    "status": "passed",
                }
            )
        st.cache_data.clear()
        st.cache_resource.clear()
    new_children = [
        child
        for child in process.children(recursive=True)
        if child.pid not in before and child.is_running()
    ]
    suspicious = [
        {"pid": child.pid, "name": child.name(), "command": " ".join(child.cmdline())}
        for child in new_children
        if "java" in child.name().lower() or "spark" in " ".join(child.cmdline()).lower()
    ]
    if suspicious:
        raise RuntimeError(f"Dashboard AppTest started a Spark/Java child: {suspicious}")
    return {
        "status": "passed",
        "surface": "streamlit.testing.v1.AppTest",
        "server_started": False,
        "browser_started": False,
        "files_executed": len(results),
        "pages_executed": len(results),
        "results": results,
        "new_spark_or_java_children": suspicious,
    }


def _required_inputs(run_dir: Path) -> dict[str, Path]:
    result: dict[str, Path] = {}
    missing: list[str] = []
    for name, relative in REQUIRED_UPSTREAM.items():
        path = run_dir / relative
        if not (path / "_SUCCESS").is_file() or not any(path.glob("*.parquet")):
            missing.append(str(path))
        else:
            result[name] = path
    if missing:
        raise FileNotFoundError(f"G10 prerequisite tables are missing/incomplete: {missing}")
    return result


def _export_queries(inputs: Mapping[str, Path]) -> Mapping[str, str]:
    expression = {name: _parquet_expr(path) for name, path in inputs.items()}
    return {
        "dashboard_overview": f"""
            SELECT metric, value::BIGINT AS value FROM {expression['profile_metrics']}
            UNION ALL
            SELECT 'active_products', COUNT(*) FILTER (WHERE is_active)::BIGINT
              FROM {expression['product_quality_profile']}
            UNION ALL
            SELECT 'discontinued_products', COUNT(*) FILTER (WHERE NOT is_active)::BIGINT
              FROM {expression['product_quality_profile']}
            UNION ALL
            SELECT 'declared_reviews', SUM(reviews_total)::BIGINT
              FROM {expression['product_quality_profile']}
            UNION ALL
            SELECT 'downloaded_reviews', SUM(reviews_downloaded)::BIGINT
              FROM {expression['product_quality_profile']}
        """,
        "dashboard_quality_summary": f"SELECT * FROM {expression['data_quality_summary']} ORDER BY event_type",
        "dashboard_group_distribution": f"""
            SELECT COALESCE(NULLIF(\"group\", ''), 'Bilinmiyor') AS product_group,
                   COUNT(*)::BIGINT AS product_count
              FROM {expression['product_quality_profile']}
             GROUP BY 1 ORDER BY product_count DESC, product_group
        """,
        "dashboard_rating_distribution": f"""
            SELECT rating, COUNT(*)::BIGINT AS review_count
              FROM {expression['reviews']} GROUP BY rating ORDER BY rating
        """,
        "dashboard_review_year_distribution": f"""
            SELECT YEAR(review_date)::INTEGER AS review_year,
                   COUNT(*)::BIGINT AS review_count
              FROM {expression['reviews']}
             WHERE review_date IS NOT NULL GROUP BY 1 ORDER BY 1
        """,
        "dashboard_activity_quantiles": f"""
            WITH degrees AS (
              SELECT 'Kullanıcı' AS entity_type, COUNT(*)::BIGINT AS degree
                FROM {expression['interactions']} GROUP BY customer_id
              UNION ALL
              SELECT 'Ürün' AS entity_type, COUNT(*)::BIGINT AS degree
                FROM {expression['interactions']} GROUP BY product_id
            )
            SELECT entity_type, COUNT(*)::BIGINT AS entities,
                   approx_quantile(degree, 0.50)::DOUBLE AS p50,
                   approx_quantile(degree, 0.90)::DOUBLE AS p90,
                   approx_quantile(degree, 0.99)::DOUBLE AS p99,
                   MAX(degree)::BIGINT AS maximum
              FROM degrees GROUP BY entity_type ORDER BY entity_type
        """,
        "product_search_index": f"""
            WITH category_rollup AS (
              SELECT product_id,
                     list(raw_path ORDER BY path_ordinal) AS category_paths
                FROM {expression['category_paths']} GROUP BY product_id
            )
            SELECT p.*,
                   COALESCE(c.category_paths, []::VARCHAR[]) AS category_paths,
                   array_to_string(c.category_paths, ' || ') AS category_search_text
              FROM {expression['product_quality_profile']} p
              LEFT JOIN category_rollup c USING (product_id)
             ORDER BY p.product_id
        """,
        "category_search_index": f"SELECT * FROM {expression['category_nodes']} ORDER BY product_count DESC, category_id",
        "servable_customers": f"""
            SELECT stage, cohort, customer_id, target_product_id, rating, stable_hash, sample_rank
              FROM {expression['evaluation_users']}
             ORDER BY stage, cohort, sample_rank, customer_id
        """,
        "demo_users": f"""
            WITH selected AS (
              SELECT selected_model
                FROM {expression['selected_hybrid']} LIMIT 1
            ),
            expected AS (
              SELECT COUNT(DISTINCT model)::INTEGER AS model_count
                FROM {expression['evaluation_per_user']}
               WHERE stage = 'test' AND cohort = 'operational' AND slice = 'overall'
            ), scored AS (
              SELECT customer_id,
                     COUNT(DISTINCT model)::INTEGER AS evaluated_model_count,
                     COUNT(DISTINCT CASE WHEN has_output THEN model END)::INTEGER AS output_model_count,
                     MIN(top_k_list_length)::INTEGER AS min_top_k_list_length,
                     AVG(top_k_list_length)::DOUBLE AS avg_top_k_list_length,
                     MAX(target_group) AS target_group,
                     MAX(CASE WHEN model = selected.selected_model AND has_output THEN 1 ELSE 0 END)::INTEGER
                       AS selected_hybrid_has_output,
                     MAX(selected.selected_model) AS selected_hybrid_model
                FROM {expression['evaluation_per_user']} CROSS JOIN selected
               WHERE stage = 'test' AND cohort = 'operational' AND slice = 'overall'
               GROUP BY customer_id
            ), eligible AS (
              SELECT scored.*, expected.model_count AS expected_model_count
                FROM scored CROSS JOIN expected
               WHERE scored.selected_hybrid_has_output = 1
            )
            SELECT row_number() OVER (
                       ORDER BY output_model_count DESC,
                                min_top_k_list_length DESC,
                                avg_top_k_list_length DESC,
                                customer_id
                   )::INTEGER AS demo_rank,
                   customer_id, evaluated_model_count, output_model_count,
                   expected_model_count, min_top_k_list_length,
                   avg_top_k_list_length, target_group, selected_hybrid_model,
                   'test.operational; selected hybrid has output; ranked by multi-model evidence' AS selection_reason
              FROM eligible
             ORDER BY output_model_count DESC,
                      min_top_k_list_length DESC,
                      avg_top_k_list_length DESC,
                      customer_id
             LIMIT 20
        """,
    }


def validate_gold_runtime_contract(
    run_dir: Path, g10_working: Path
) -> dict[str, Any]:
    from app.lib.catalog import TABLE_PATHS

    resolved: dict[str, str] = {}
    missing: list[str] = []
    silver_paths: list[str] = []
    priority_violations: dict[str, dict[str, str]] = {}
    for logical, relative in GOLD_RUNTIME_PATHS.items():
        configured = TABLE_PATHS.get(logical, ())
        if not configured or configured[0] != relative:
            priority_violations[logical] = {
                "expected_first": relative,
                "observed_first": configured[0] if configured else "missing",
            }
        path = g10_working / Path(relative).name if relative.startswith("data/g10/") else run_dir / relative
        if not (path / "_SUCCESS").is_file() or not any(path.glob("*.parquet")):
            missing.append(logical)
            continue
        resolved[logical] = str(path.resolve())
        if "/full/silver/" in str(path):
            silver_paths.append(logical)
    if missing or silver_paths or priority_violations:
        raise RuntimeError(
            f"G10 runtime source contract failed: missing={missing}, silver={silver_paths}, "
            f"resolver_priority={priority_violations}"
        )
    return {
        "status": "passed",
        "logical_table_count": len(resolved),
        "resolved_tables": resolved,
        "silver_runtime_sources": silver_paths,
        "resolver_priority_violations": priority_violations,
        "all_tables_have_success_marker": True,
        "source_policy": "G5-G10 run-scoped Gold Parquet only after G10",
    }


@register("G10")
def run_g10(config: Any, paths: Any, evidence_file: Path | None) -> dict[str, Any]:
    if evidence_file is None:
        raise RuntimeError("G10 requires passing JUnit XML evidence")
    junit = _junit(evidence_file)
    project_root = Path(paths.project_root).resolve()
    app_audit = audit_dashboard_sources(project_root)
    inputs = _required_inputs(paths.run)
    final = paths.data / "g10"
    if final.exists():
        raise FileExistsError(f"G10 output exists without reusable manifest: {final}")
    signature = _implementation_signature(config.sha256, project_root)
    working = paths.temporary / "G10-publish"
    removed = _prepare_workspace(working, signature)
    final.parent.mkdir(parents=True, exist_ok=True)
    spill = paths.temporary / "G10-duckdb-spill"
    shutil.rmtree(spill, ignore_errors=True)
    connection = _connect(spill)
    tables: dict[str, dict[str, Any]] = {}
    reused: list[str] = []
    fixture_parent = paths.temporary / "G10-app-test"
    shutil.rmtree(fixture_parent, ignore_errors=True)

    try:
        for name, query in _export_queries(inputs).items():
            evidence, was_reused = _publish_query(connection, working, name, query)
            tables[name] = evidence
            if was_reused:
                reused.append(name)

        if tables["servable_customers"]["rows"] != 80_000:
            raise RuntimeError(
                "G10 servable customer population must preserve exactly 80,000 G6 rows; "
                f"observed {tables['servable_customers']['rows']}"
            )
        if tables["demo_users"]["rows"] < 20:
            raise RuntimeError(
                "G10 requires at least 20 evidence-backed users with selected-hybrid output"
            )

        gold_contract = validate_gold_runtime_contract(paths.run, working)
        fixture_root = build_app_test_fixture(
            fixture_parent, source_sha256=config.get("source", "sha256")
        )
        app_test = run_four_page_app_test(project_root, fixture_root)
        shutil.rmtree(fixture_parent, ignore_errors=True)

        # Probe the real compact exports with a quote-bearing parameter and reject a
        # write through the same presentation facade used by Streamlit.
        product_expression = _parquet_expr(working / "product_search_index")
        quote_probe = connection.execute(
            f"SELECT product_id FROM {product_expression} "
            "WHERE title ILIKE ? OR asin ILIKE ? LIMIT 5",
            ["%'%", "%'%"],
        ).fetchall()
        from app.lib.catalog import RunContext
        from app.lib.data import DashboardStore

        # The fixture was intentionally removed after AppTest; recreating only for
        # this tiny facade probe keeps no generated test data in the final artifact.
        fixture_root = build_app_test_fixture(
            fixture_parent, source_sha256=config.get("source", "sha256")
        )
        fixture_context = RunContext(
            run_id="run-g10-app-test",
            run_dir=fixture_root / "runs" / "run-g10-app-test",
            last_passed_gate=9,
            source_sha256=config.get("source", "sha256"),
            recorded_at="2026-07-11T00:00:00Z",
            manifests={},
        )
        store = DashboardStore(fixture_context)
        try:
            try:
                store.query("DELETE FROM dashboard_contract")
            except ValueError as error:
                write_rejection = str(error)
            else:
                raise RuntimeError("DashboardStore unexpectedly accepted a write statement")
            bounded_search_rows = len(store.search_products("'", page=1, page_size=5))
        finally:
            store.close()
            shutil.rmtree(fixture_parent, ignore_errors=True)
        duckdb_probe = {
            "status": "passed",
            "engine": duckdb.__version__,
            "threads": G10_DUCKDB_BUILD_THREADS,
            "memory_limit": G10_DUCKDB_BUILD_MEMORY_LIMIT,
            "parameterized_quote_probe_rows": len(quote_probe),
            "bounded_fixture_search_rows": bounded_search_rows,
            "write_statement_rejected": True,
            "write_rejection": write_rejection,
            "spark_imported_by_app": False,
        }

        contract_rows = [
            {"check_name": "four_binding_pages", "status": "passed", "detail_json": json.dumps(app_test, sort_keys=True)},
            {"check_name": "spark_free_static_audit", "status": "passed", "detail_json": json.dumps(app_audit, sort_keys=True)},
            {"check_name": "gold_runtime_sources", "status": "passed", "detail_json": json.dumps(gold_contract, sort_keys=True)},
            {"check_name": "duckdb_read_only", "status": "passed", "detail_json": json.dumps(duckdb_probe, sort_keys=True)},
            {"check_name": "servable_customers", "status": "passed", "detail_json": json.dumps({"rows": tables['servable_customers']['rows']})},
            {"check_name": "demo_users", "status": "passed", "detail_json": json.dumps({"rows": tables['demo_users']['rows']})},
        ]
        evidence, was_reused = _publish_rows(
            connection, working, "dashboard_contract", contract_rows
        )
        tables["dashboard_contract"] = evidence
        if was_reused:
            reused.append("dashboard_contract")
        if set(tables) != set(OUTPUT_TABLES):
            raise RuntimeError(
                f"G10 output contract mismatch: {sorted(tables)} != {sorted(OUTPUT_TABLES)}"
            )
        os.replace(working, final)
    except Exception:
        shutil.rmtree(fixture_parent, ignore_errors=True)
        raise
    finally:
        connection.close()
        shutil.rmtree(spill, ignore_errors=True)

    def final_path(value: Any) -> Any:
        if isinstance(value, str):
            return value.replace(str(working), str(final), 1)
        if isinstance(value, dict):
            return {key: final_path(item) for key, item in value.items()}
        if isinstance(value, list):
            return [final_path(item) for item in value]
        return value

    tables = final_path(tables)
    gold_contract = final_path(gold_contract)
    return {
        "junit": junit,
        "implementation_sha256": signature,
        "scratch_directories_removed": removed,
        "tables_reused": sorted(set(reused)),
        "spark_free_static_audit": app_audit,
        "four_page_app_test": app_test,
        "gold_source_contract": gold_contract,
        "duckdb_read_only_probe": duckdb_probe,
        "dashboard_exports": {
            "compact_aggregate_tables": 6,
            "product_search_index_rows": tables["product_search_index"]["rows"],
            "category_search_index_rows": tables["category_search_index"]["rows"],
        },
        "servable_customer_contract": {
            "rows": tables["servable_customers"]["rows"],
            "population": "G6 evaluation users only",
            "online_arbitrary_user_promise": False,
        },
        "demo_user_contract": {
            "rows": tables["demo_users"]["rows"],
            "minimum_required": 20,
            "criterion": (
                "test operational overall; selected hybrid has output; "
                "ranked by multi-model evidence"
            ),
        },
        "streamlit_server_started": False,
        "browser_started": False,
        "spark_session_started": False,
        "tables": tables,
    }
