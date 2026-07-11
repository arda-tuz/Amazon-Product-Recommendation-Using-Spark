from __future__ import annotations

import shutil
import ast
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from amazon_recommender.phases.g10 import (
    GOLD_RUNTIME_PATHS,
    OUTPUT_TABLES,
    PAGE_FILES,
    _connect,
    _export_queries,
    _junit,
    _prepare_workspace,
    _publish_query,
    _publish_rows,
    audit_dashboard_sources,
    build_app_test_fixture,
    run_four_page_app_test,
    validate_gold_runtime_contract,
)


pytestmark = pytest.mark.unit
PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _table(root: Path, name: str, rows: list[dict]) -> Path:
    target = root / name
    target.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), target / "part.parquet")
    (target / "_SUCCESS").touch()
    return target


def _query_inputs(tmp_path: Path) -> dict[str, Path]:
    source = tmp_path / "source"
    result = {
        "profile_metrics": _table(
            source, "profile_metrics", [{"metric": "products", "value": 2}]
        ),
        "data_quality_summary": _table(
            source,
            "data_quality_summary",
            [{"event_type": "AVG_RATING_MISMATCH", "event_count": 1, "distinct_entities": 1}],
        ),
        "product_quality_profile": _table(
            source,
            "product_quality_profile",
            [
                {
                    "product_id": 1,
                    "asin": "A1",
                    "title": "One",
                    "group": "Book",
                    "is_active": True,
                    "reviews_total": 2,
                    "reviews_downloaded": 2,
                },
                {
                    "product_id": 2,
                    "asin": "A2",
                    "title": "Two",
                    "group": "Music",
                    "is_active": False,
                    "reviews_total": 1,
                    "reviews_downloaded": 1,
                },
            ],
        ),
        "category_paths": _table(
            source,
            "category_paths",
            [
                {"product_id": 1, "path_ordinal": 1, "raw_path": "|Books[1]"},
                {"product_id": 2, "path_ordinal": 1, "raw_path": "|Music[2]"},
            ],
        ),
        "category_nodes": _table(
            source,
            "category_nodes",
            [
                {"category_id": 1, "category_label": "Books", "product_count": 1},
                {"category_id": 2, "category_label": "Music", "product_count": 1},
            ],
        ),
        "reviews": _table(
            source,
            "reviews",
            [
                {"rating": 5, "review_date": date(2004, 1, 1)},
                {"rating": 4, "review_date": date(2004, 1, 2)},
            ],
        ),
        "interactions": _table(
            source,
            "interactions",
            [
                {"customer_id": "U1", "product_id": 1},
                {"customer_id": "U1", "product_id": 2},
            ],
        ),
        "evaluation_users": _table(
            source,
            "evaluation_users",
            [
                {
                    "stage": "test",
                    "cohort": "operational",
                    "customer_id": f"U{index:02d}",
                    "target_product_id": 1,
                    "rating": 5.0,
                    "stable_hash": f"h{index:02d}",
                    "sample_rank": index,
                }
                for index in range(1, 21)
            ],
        ),
    }
    models = ("popularity", "als", "fp", "graph", "category", "h_a")
    result["evaluation_per_user"] = _table(
        source,
        "evaluation_per_user",
        [
            {
                "model": model,
                "stage": "test",
                "cohort": "operational",
                "slice": "overall",
                "customer_id": f"U{index:02d}",
                "has_output": True,
                "top_k_list_length": 10,
                "target_group": "Book",
            }
            for index in range(1, 21)
            for model in models
        ],
    )
    result["selected_hybrid"] = _table(
        source, "selected_hybrid", [{"selected_model": "h_a"}]
    )
    return result


def test_g10_export_queries_are_compact_source_backed_and_atomic(tmp_path: Path) -> None:
    connection = _connect()
    working = tmp_path / "G10-publish"
    working.mkdir()
    try:
        evidence = {}
        for name, query in _export_queries(_query_inputs(tmp_path)).items():
            evidence[name], reused = _publish_query(connection, working, name, query)
            assert reused is False
        assert evidence["dashboard_overview"]["rows"] == 5
        assert evidence["dashboard_activity_quantiles"]["rows"] == 2
        assert evidence["product_search_index"]["rows"] == 2
        assert evidence["servable_customers"]["rows"] == 20
        assert evidence["demo_users"]["rows"] == 20
        category_paths = connection.execute(
            f"SELECT category_paths FROM read_parquet('{working / 'product_search_index' / '*.parquet'}') "
            "WHERE product_id = 1"
        ).fetchone()[0]
        assert category_paths == ["|Books[1]"]

        contract, reused = _publish_rows(
            connection,
            working,
            "dashboard_contract",
            [{"check_name": "fixture", "status": "passed", "detail_json": "{}"}],
        )
        assert contract["rows"] == 1
        assert reused is False
        _, reused = _publish_rows(
            connection,
            working,
            "dashboard_contract",
            [{"check_name": "ignored", "status": "failed", "detail_json": "{}"}],
        )
        assert reused is True
    finally:
        connection.close()


def test_g10_gold_runtime_contract_has_no_silver_serving_path(tmp_path: Path) -> None:
    run = tmp_path / "runs" / "run-test"
    working = tmp_path / "G10-publish"
    for logical, relative in GOLD_RUNTIME_PATHS.items():
        if relative.startswith("data/g10/"):
            target = working / Path(relative).name
        else:
            target = run / relative
        if not target.exists():
            _table(target.parent, target.name, [{"value": logical}])
    evidence = validate_gold_runtime_contract(run, working)
    assert evidence["status"] == "passed"
    assert evidence["silver_runtime_sources"] == []
    assert evidence["logical_table_count"] == len(GOLD_RUNTIME_PATHS)


def test_g10_static_audit_and_four_page_apptest_start_no_spark(tmp_path: Path) -> None:
    audit = audit_dashboard_sources(PROJECT_ROOT)
    assert audit["page_count"] == 4
    assert audit["forbidden_imports"] == []
    assert audit["spark_session_construction"] is False

    fixture = build_app_test_fixture(tmp_path, source_sha256="a" * 64)
    smoke = run_four_page_app_test(PROJECT_ROOT, fixture)
    assert smoke["pages_executed"] == 4
    assert smoke["files_executed"] == 4
    assert smoke["new_spark_or_java_children"] == []
    assert smoke["server_started"] is False


def test_g10_static_audit_rejects_pyspark_import(tmp_path: Path) -> None:
    project = tmp_path / "project"
    shutil.copytree(PROJECT_ROOT / "app", project / "app")
    with (project / "app" / "lib" / "data.py").open("a", encoding="utf-8") as stream:
        stream.write("\nimport pyspark\n")
    with pytest.raises(RuntimeError, match="may start an external/Spark process"):
        audit_dashboard_sources(project)


def test_g10_resume_junit_and_output_budget_contract(tmp_path: Path) -> None:
    working = tmp_path / "G10-publish"
    _prepare_workspace(working, "signature-a")
    complete = _table(working, "dashboard_overview", [{"metric": "x", "value": 1}])
    partial = working / ".demo_users.deadbeef.tmp"
    partial.mkdir()
    removed = _prepare_workspace(working, "signature-a")
    assert (complete / "_SUCCESS").is_file()
    assert str(partial) in removed
    _prepare_workspace(working, "signature-b")
    assert not complete.exists()

    passing = tmp_path / "passing.xml"
    passing.write_text('<testsuite tests="5" failures="0" errors="0" skipped="0"/>', encoding="utf-8")
    assert _junit(passing)["tests"] == 5
    failing = tmp_path / "failing.xml"
    failing.write_text('<testsuite tests="1" failures="1" errors="0" skipped="0"/>', encoding="utf-8")
    with pytest.raises(RuntimeError, match="not passing"):
        _junit(failing)

    assert len(PAGE_FILES) == 3
    assert not (PROJECT_ROOT / "app" / "pages" / "1_Overview_and_Data_Quality.py").exists()
    assert set(OUTPUT_TABLES) == {
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
    }


def test_g10_phase_itself_has_no_spark_import_or_session_constructor() -> None:
    path = PROJECT_ROOT / "src" / "amazon_recommender" / "phases" / "g10.py"
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module)
    assert not any(name == "pyspark" or name.startswith("pyspark.") for name in imports)
    assert "SparkSession.builder" not in source
    assert "streamlit run" not in source
