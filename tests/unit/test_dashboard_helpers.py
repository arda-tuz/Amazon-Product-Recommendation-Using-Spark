from __future__ import annotations

import ast
import json
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from app.lib.catalog import RunContext, discover_runs, select_run
from app.lib.data import DashboardStore
from app.lib.graph import bounded_ego_graph, deterministic_layout
from app.lib.recommendation import compute_custom_rrf, evidence_explanation


def _write_table(run: Path, relative: str, rows: dict[str, list[object]], *, complete: bool = True) -> Path:
    target = run / relative
    target.mkdir(parents=True)
    pq.write_table(pa.table(rows), target / "part-00000.parquet")
    if complete:
        (target / "_SUCCESS").touch()
    return target


def _context(tmp_path: Path) -> RunContext:
    run = tmp_path / "runs" / "run-test"
    run.mkdir(parents=True)
    return RunContext(
        run_id="run-test",
        run_dir=run,
        last_passed_gate=9,
        source_sha256="a" * 64,
        recorded_at="2026-07-11T00:00:00Z",
        manifests={},
    )


def _performance_payload() -> tuple[dict[str, object], dict[str, object]]:
    output_rows = 68
    schema_sha256 = "c" * 64
    timings = {
        "single_core": ("local[1]", 1, [12.0, 9.0, 6.0, 7.5]),
        "bounded_multi_core": ("local[4]", 4, [4.0, 3.0, 2.0, 2.5]),
    }
    conditions: dict[str, object] = {}
    for name, (master, workers, values) in timings.items():
        trials = []
        for ordinal, wall_seconds in enumerate(values):
            trials.append(
                {
                    "spec": {
                        "condition": {
                            "name": name,
                            "master": master,
                            "worker_threads": workers,
                        },
                        "ordinal": ordinal,
                        "is_warmup": ordinal == 0,
                    },
                    "workload": {
                        "wall_seconds": wall_seconds,
                        "output_rows": output_rows,
                    },
                }
            )
        conditions[name] = {
            "master": master,
            "worker_threads": workers,
            "warmup_wall_seconds": values[0],
            "measured_wall_seconds": values[1:],
            "median_wall_seconds": sorted(values[1:])[1],
            "trials": trials,
        }
    summary: dict[str, object] = {
        "protocol": {
            "warmups_per_condition": 1,
            "measured_runs_per_condition": 3,
        },
        "conditions": conditions,
        "local_parallel_speedup": 3.0,
        "output_rows": output_rows,
        "output_schema_sha256": schema_sha256,
    }
    success: dict[str, object] = {
        "gate": "G11",
        "trial_count": 8,
        "output_rows": output_rows,
        "output_schema_sha256": schema_sha256,
    }
    return summary, success


def _write_performance_artifacts(
    context: RunContext,
    summary: object,
    success: object | None,
) -> None:
    performance = context.run_dir / "performance"
    performance.mkdir(parents=True, exist_ok=True)
    (performance / "summary.json").write_text(json.dumps(summary), encoding="utf-8")
    if success is not None:
        (performance / "_SUCCESS.json").write_text(
            json.dumps(success), encoding="utf-8"
        )


@pytest.mark.unit
def test_catalog_selects_latest_passed_run_and_rejects_unsafe_id(tmp_path: Path) -> None:
    artifacts = tmp_path / "artifacts"
    for run_id, gate, recorded in (
        ("run-old", 8, "2026-07-10T00:00:00Z"),
        ("run-new", 9, "2026-07-11T00:00:00Z"),
    ):
        manifests = artifacts / "runs" / run_id / "manifests"
        manifests.mkdir(parents=True)
        (manifests / f"G{gate}.json").write_text(
            json.dumps(
                {
                    "status": "passed",
                    "gate": f"G{gate}",
                    "recorded_at": recorded,
                    "source_sha256": "b" * 64,
                }
            ),
            encoding="utf-8",
        )
    contexts = discover_runs(artifacts)
    assert [item.run_id for item in contexts] == ["run-new", "run-old"]
    assert select_run("run-old", artifacts).last_passed_gate == 8
    with pytest.raises(ValueError, match="Unsafe run id"):
        select_run("../escape", artifacts)


@pytest.mark.unit
def test_table_resolver_requires_success_marker_and_allowlist(tmp_path: Path) -> None:
    context = _context(tmp_path)
    _write_table(
        context.run_dir,
        "data/full/silver/products",
        {"product_id": [1], "asin": ["A"]},
        complete=False,
    )
    assert context.table_path("products") is None
    (context.run_dir / "data/full/silver/products/_SUCCESS").touch()
    assert context.table_path("products") is not None
    with pytest.raises(KeyError, match="Unknown dashboard table"):
        context.table_path("../../etc/passwd")


@pytest.mark.unit
def test_passed_g10_run_fails_closed_instead_of_falling_back_to_silver(tmp_path: Path) -> None:
    context = _context(tmp_path)
    _write_table(
        context.run_dir,
        "data/full/silver/products",
        {"product_id": [1], "asin": ["A"]},
    )
    g10_context = RunContext(
        run_id=context.run_id,
        run_dir=context.run_dir,
        last_passed_gate=10,
        source_sha256=context.source_sha256,
        recorded_at=context.recorded_at,
        manifests={},
    )
    assert context.table_path("products") is not None
    assert g10_context.table_path("products") is None


@pytest.mark.unit
def test_duckdb_search_is_bounded_and_user_text_is_parameterized(tmp_path: Path) -> None:
    context = _context(tmp_path)
    _write_table(
        context.run_dir,
        "data/full/silver/products",
        {
            "product_id": [1, 2, 3],
            "asin": ["A1", "A2", "A3"],
            "title": ["A calm book", "Other", "A title with ' quote"],
            "group": ["Book", "Music", "Book"],
            "status": ["active", "active", "active"],
            "is_active": [True, True, True],
        },
    )
    store = DashboardStore(context)
    try:
        result = store.search_products("' quote", page=1, page_size=1)
        assert result["product_id"].tolist() == [3]
        assert len(store.search_products("", page=1, page_size=999)) == 3
        with pytest.raises(ValueError, match="read-only"):
            store.query("DELETE FROM anything")
    finally:
        store.close()


@pytest.mark.unit
def test_custom_rrf_renormalizes_active_models_and_has_stable_ties() -> None:
    evidence = pd.DataFrame(
        {
            "product_id": [20, 10, 30],
            "als_rank": [1.0, 1.0, None],
            "graph_rank": [None, None, 1.0],
            "global_bayesian_score": [4.0, 4.0, 5.0],
        }
    )
    result = compute_custom_rrf(
        evidence,
        {"als": 0.5, "graph": 0.5, "category": 0, "fp": 0, "popularity": 0},
        depth=3,
    )
    # Equal model weights and equal rank produce equal scores; Bayes then product ID
    # implements the documented deterministic presentation tie chain.
    assert result["product_id"].tolist() == [30, 10, 20]
    assert result["rank"].tolist() == [1, 2, 3]
    assert result["exploratory_rrf_score"].round(12).nunique() == 1
    inactive_only = compute_custom_rrf(
        evidence[["product_id", "als_rank"]],
        {"als": 0, "graph": 1, "category": 0, "fp": 0, "popularity": 0},
    )
    assert inactive_only.empty


@pytest.mark.unit
def test_fp_explanation_uses_positive_rating_language_not_purchase_claim() -> None:
    text = evidence_explanation({"fp_rank": 2})
    assert "aynı kullanıcılar tarafından birlikte olumlu değerlendirilmiştir" in text.lower()
    assert "satın" not in text.lower()


@pytest.mark.unit
def test_networkx_layout_is_deterministic_and_never_exceeds_50_nodes() -> None:
    edges = [(0, value) for value in range(1, 80)]
    graph = bounded_ego_graph(0, edges)
    assert len(graph) == 50
    first = deterministic_layout(graph)
    second = deterministic_layout(graph)
    assert first == second
    with pytest.raises(ValueError, match="between 1 and 50"):
        bounded_ego_graph(0, edges, max_nodes=51)


@pytest.mark.unit
def test_streamlit_app_never_imports_pyspark() -> None:
    root = Path(__file__).resolve().parents[2] / "app"
    imported: set[str] = set()
    for path in root.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module)
    assert not any(name == "pyspark" or name.startswith("pyspark.") for name in imported)


@pytest.mark.unit
def test_performance_summary_reads_valid_published_g11_json_as_two_bounded_rows(
    tmp_path: Path,
) -> None:
    base = _context(tmp_path)
    context = RunContext(**{**base.__dict__, "last_passed_gate": 11})
    summary, success = _performance_payload()
    _write_performance_artifacts(context, summary, success)

    store = DashboardStore(context)
    try:
        result = store.performance_summary()
    finally:
        store.close()

    assert result["condition"].tolist() == ["single_core", "bounded_multi_core"]
    assert result["master"].tolist() == ["local[1]", "local[4]"]
    assert result["worker_threads"].tolist() == [1, 4]
    assert result["warmup_seconds"].tolist() == [12.0, 4.0]
    assert result["median_seconds"].tolist() == [7.5, 2.5]
    assert result["speedup_vs_single_core"].tolist() == [1.0, 3.0]
    assert result["output_rows"].tolist() == [68, 68]
    assert len(result) == 2


@pytest.mark.unit
def test_performance_summary_requires_success_marker_before_reading_summary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    base = _context(tmp_path)
    context = RunContext(**{**base.__dict__, "last_passed_gate": 11})
    summary, _ = _performance_payload()
    _write_performance_artifacts(context, summary, None)
    summary_path = context.run_dir / "performance" / "summary.json"
    original_read_text = Path.read_text

    def guarded_read(path: Path, *args: object, **kwargs: object) -> str:
        if path == summary_path:
            raise AssertionError("unpublished summary.json must not be read")
        return original_read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", guarded_read)
    store = DashboardStore(context)
    try:
        assert store.performance_summary().empty
    finally:
        store.close()


@pytest.mark.unit
@pytest.mark.parametrize(
    "mutation",
    ("wrong_budget", "non_finite", "wrong_median", "wrong_speedup", "wrong_trial"),
)
def test_performance_summary_fails_closed_for_invalid_g11_contract(
    tmp_path: Path, mutation: str
) -> None:
    base = _context(tmp_path)
    context = RunContext(**{**base.__dict__, "last_passed_gate": 11})
    summary, success = _performance_payload()
    conditions = summary["conditions"]
    assert isinstance(conditions, dict)
    single = conditions["single_core"]
    assert isinstance(single, dict)
    if mutation == "wrong_budget":
        single["measured_wall_seconds"] = [9.0, 6.0]
    elif mutation == "non_finite":
        single["warmup_wall_seconds"] = float("nan")
    elif mutation == "wrong_median":
        single["median_wall_seconds"] = 9.0
    elif mutation == "wrong_speedup":
        summary["local_parallel_speedup"] = 99.0
    else:
        trials = single["trials"]
        assert isinstance(trials, list) and isinstance(trials[-1], dict)
        trials[-1]["spec"]["ordinal"] = 2
    _write_performance_artifacts(context, summary, success)

    store = DashboardStore(context)
    try:
        result = store.performance_summary()
    finally:
        store.close()
    assert result.empty
