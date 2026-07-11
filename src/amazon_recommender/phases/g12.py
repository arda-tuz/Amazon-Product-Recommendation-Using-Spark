"""G12 final acceptance audit and atomic delivery publication.

This phase starts no Spark session.  It independently verifies the G0--G11
manifest chain, immutable experiment budgets, source and artifact fingerprints,
JUnit evidence, the official G9 comparison table, and the documentation contract.
Only evidence already materialized by prior gates is rendered into the delivery;
missing or non-finite results are fatal rather than replaced with example values.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import re
import shutil
import statistics
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import pyarrow.dataset as pads

from amazon_recommender.core.manifest import (
    atomic_write_json,
    content_sha256,
)
from amazon_recommender.gate_handlers import register
from amazon_recommender.pipelines.storage import (
    directory_size,
    table_fingerprint,
)


G12_CONTRACT_VERSION = 1
PRIOR_GATES = tuple(f"G{index}" for index in range(12))
INDEPENDENT_MODELS = ("popularity", "als", "fp", "graph", "category")
HYBRID_MODELS = ("h_a", "h_b")
HYBRID_WEIGHTS = {
    "h_a": {
        "als": 0.35,
        "graph": 0.20,
        "category": 0.20,
        "fp": 0.15,
        "popularity": 0.10,
    },
    "h_b": {
        "als": 0.50,
        "graph": 0.20,
        "category": 0.10,
        "fp": 0.15,
        "popularity": 0.05,
    },
}
COHORTS = ("common_warm", "operational")
SLICES = ("overall", "Book", "non-Book")
EXPECTED_HARD_COUNTS = {
    "products": 548_552,
    "active_products": 542_684,
    "discontinued_products": 5_868,
    "distinct_product_ids": 548_552,
    "distinct_asins": 548_552,
    "min_product_id": 0,
    "max_product_id": 548_551,
    "reviews_total_sum": 7_781_990,
    "reviews_downloaded_sum": 7_593_244,
    "physical_reviews": 7_593_244,
    "distinct_customers": 1_555_170,
    "category_path_occurrences": 2_509_699,
    "similar_occurrences": 1_788_725,
}
README_REQUIRED_SNIPPETS = (
    "7.781.990",
    "7.593.244",
    "bil401_env_1",
    "Java",
    "Spark",
    "GraphFrames",
    "gate G4",
    "gate G7",
    "gate G9",
    "gate G11",
    "gate G12",
    "dashboard",
    "H-A",
    "H-B",
    "manifest",
    "20 GiB",
    "official_test_comparison",
)
PLACEHOLDER_PATTERN = re.compile(
    r"\b(?:TODO|FIXME|TBD)\b|lorem\s+ipsum|"
    r"\[\s*(?:to be written|content here|placeholder)\s*\]|"
    r"/path/to(?:/|\b)|run-YYYY",
    re.IGNORECASE,
)
DELIVERY_FINALIZATION_FILES = frozenset(
    {
        "_PENDING_G12_MANIFEST.json",
        "_SUCCESS.json",
        "acceptance-report.json",
        "manifest-index.json",
        "manifests/G12.json",
    }
)
DELIVERY_FINALIZATION_TEMP_FILES = frozenset(
    {
        "._PENDING_G12_MANIFEST.json.tmp",
        "._SUCCESS.json.tmp",
        ".acceptance-report.json.tmp",
        ".manifest-index.json.tmp",
        "manifests/.G12.json.tmp",
    }
)
G7_PARAMETER_SUBSETS = {
    "popularity": {
        "m": 20,
        "group_min_train_interactions": 100,
        "global_catalog_depth": 1_000,
        "candidate_depth": 100,
    },
    "als": {
        "rank": 20,
        "reg_param": 0.10,
        "max_iter": 10,
        "implicit_prefs": False,
        "nonnegative": False,
        "cold_start_strategy": "drop",
        "raw_candidate_depth": 200,
        "candidate_depth": 100,
        "seed": 42,
    },
    "fp": {
        "min_basket_size": 2,
        "max_basket_size": 50,
        "min_fraction": 0.001,
        "min_count": 200,
        "min_confidence": 0.05,
        "min_lift": 1.10,
        "num_partitions": 64,
        "max_rules_per_antecedent": 20,
        "candidate_depth": 50,
    },
    "graph": {
        "max_positive_seeds": 20,
        "direct_weight": 1.0,
        "reciprocal_bonus": 0.25,
        "two_hop_weight": 0.50,
        "pagerank_reset_probability": 0.15,
        "pagerank_max_iter": 10,
        "candidate_depth": 50,
    },
    "category": {
        "similarity_weight": 0.80,
        "group_affinity_weight": 0.10,
        "popularity_percentile_weight": 0.10,
        "max_profile_categories": 20,
        "generic_category_ratio": 0.10,
        "products_per_category": 200,
        "max_candidate_pool": 5_000,
        "candidate_depth": 50,
    },
}


@dataclass(frozen=True)
class VerifiedDeliveryInputs:
    manifests: Mapping[str, Mapping[str, Any]]
    manifest_digests: Mapping[str, str]
    source_identity: Mapping[str, Any]
    contracts: Mapping[str, Any]
    test_summary: Mapping[str, Any]
    test_files: Mapping[str, str]
    artifact_inventory: Sequence[Mapping[str, Any]]
    comparison_columns: Sequence[str]
    comparison_rows: Sequence[Mapping[str, Any]]


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _junit(path: Path) -> dict[str, Any]:
    root = ET.parse(path).getroot()
    suites = [root] if root.tag == "testsuite" else list(root.findall("testsuite"))
    result = {
        key: sum(int(suite.attrib.get(key, "0")) for suite in suites)
        for key in ("tests", "failures", "errors", "skipped")
    }
    _require(result["tests"] > 0, f"JUnit contains no tests: {path}")
    _require(
        result["failures"] == 0 and result["errors"] == 0,
        f"JUnit is not passing: {path}: {result}",
    )
    result["path"] = str(path.resolve())
    return result


def _manifest_digest(manifest: Mapping[str, Any], *, gate: str) -> str:
    if gate == "G0":
        return content_sha256(manifest)
    stored = manifest.get("evidence_sha256")
    _require(isinstance(stored, str) and len(stored) == 64, f"{gate} digest missing")
    unsigned = dict(manifest)
    unsigned.pop("evidence_sha256", None)
    computed = content_sha256(unsigned)
    _require(computed == stored, f"{gate} manifest digest mismatch")
    return stored


def validate_manifest_chain(
    manifest_directory: Path,
    *,
    run_id: str,
    config_sha256: str,
    source_sha256: str,
) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    manifests: dict[str, dict[str, Any]] = {}
    digests: dict[str, str] = {}
    for index, gate in enumerate(PRIOR_GATES):
        path = manifest_directory / f"{gate}.json"
        _require(path.is_file(), f"G12 requires missing {gate} manifest: {path}")
        try:
            manifest = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise RuntimeError(f"invalid {gate} manifest: {path}") from error
        _require(isinstance(manifest, dict), f"{gate} manifest must be an object")
        _require(manifest.get("schema_version") == 1, f"{gate} schema version changed")
        _require(manifest.get("gate") == gate, f"{gate} identity mismatch")
        _require(manifest.get("status") == "passed", f"{gate} is not passed")
        if gate != "G0":
            _require(manifest.get("run_id") == run_id, f"{gate} run id mismatch")
            _require(
                manifest.get("config_sha256") == config_sha256,
                f"{gate} config fingerprint mismatch",
            )
            _require(
                manifest.get("source_sha256") == source_sha256,
                f"{gate} source fingerprint mismatch",
            )
            expected_previous = {prior: digests[prior] for prior in PRIOR_GATES[:index]}
            _require(
                manifest.get("previous_evidence") == expected_previous,
                f"{gate} prerequisite evidence chain mismatch",
            )
            _require(
                isinstance(manifest.get("evidence"), dict),
                f"{gate} evidence object missing",
            )
        digest = _manifest_digest(manifest, gate=gate)
        manifests[gate] = manifest
        digests[gate] = digest
    return manifests, digests


def _validate_g0(manifest: Mapping[str, Any]) -> dict[str, Any]:
    runtime = manifest.get("runtime", {})
    python = manifest.get("python", {})
    packages = manifest.get("packages", {})
    _require(python.get("version") == "3.13.1", "G0 Python version mismatch")
    _require(runtime.get("java_version") == "21.0.11", "G0 Java mismatch")
    _require(
        manifest.get("java_home") == "/usr/lib/jvm/java-21-openjdk-amd64",
        "G0 JAVA_HOME mismatch",
    )
    _require(runtime.get("spark_version") == "4.0.0", "G0 Spark mismatch")
    _require(runtime.get("scala_version") == "2.13.16", "G0 Scala mismatch")
    _require(packages.get("graphframes-py") == "0.12.1", "G0 GraphFrames mismatch")
    _require(packages.get("pyarrow") == "25.0.0", "G0 PyArrow mismatch")
    _require(packages.get("duckdb") == "1.5.4", "G0 DuckDB mismatch")
    _require(packages.get("streamlit") == "1.59.1", "G0 Streamlit mismatch")
    hardware = manifest.get("hardware", {})
    _require(
        int(hardware.get("logical_cores", 0)) >= 1, "G0 logical-core evidence missing"
    )
    _require(
        int(hardware.get("memory_total_bytes", 0)) >= 15 * 1024**3,
        "G0 memory evidence is below documented baseline",
    )
    _require(
        runtime.get("spark_conf", {}).get("spark.driver.memory") == "8g",
        "G0 Spark driver heap differs from README",
    )
    tests = runtime.get("tests", {})
    _require(len(tests) >= 4, "G0 runtime test evidence is incomplete")
    _require(
        all(item.get("status") == "passed" for item in tests.values()),
        "G0 runtime test failed",
    )
    return {
        "python": python.get("version"),
        "java": runtime.get("java_version"),
        "java_home": manifest.get("java_home"),
        "spark": runtime.get("spark_version"),
        "scala": runtime.get("scala_version"),
        "graphframes": packages.get("graphframes-py"),
        "pyarrow": packages.get("pyarrow"),
        "duckdb": packages.get("duckdb"),
        "streamlit": packages.get("streamlit"),
        "logical_cores": hardware.get("logical_cores"),
        "memory_total_bytes": hardware.get("memory_total_bytes"),
        "spark_driver_memory": runtime["spark_conf"]["spark.driver.memory"],
        "runtime_tests": len(tests),
    }


def validate_gate_contracts(
    manifests: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    g0 = _validate_g0(manifests["G0"])
    evidence = {gate: manifests[gate]["evidence"] for gate in PRIOR_GATES[1:]}

    g2 = evidence["G2"]
    _require(g2["delimiter"]["hex"] == "0d0a0d0a", "G2 delimiter changed")
    _require(
        g2["contracts"]["source_offsets"] == "LongWritable uncompressed offsets",
        "G2 uncompressed offset contract missing",
    )
    offsets = g2["hadoop_sample"]["offsets"]
    _require(offsets[:2] == [0, 80], "G2 header/product source offsets changed")

    g4 = evidence["G4"]
    hard_counts = g4.get("hard_counts", {})
    mismatched_counts = {
        key: (hard_counts.get(key), expected)
        for key, expected in EXPECTED_HARD_COUNTS.items()
        if hard_counts.get(key) != expected
    }
    _require(not mismatched_counts, f"G4 hard-count mismatch: {mismatched_counts}")
    _require(g4.get("header_records") == 1, "G4 must retain exactly one header")
    _require(g4.get("quarantine_records") == 0, "G4 quarantine is not empty")
    _require(g4["delimiter"]["hex"] == "0d0a0d0a", "G4 delimiter changed")

    g5 = evidence["G5"]["profile_counts"]
    _require(g5["reviews_raw"] == 7_593_244, "G5 raw-review count changed")
    _require(g5["reviews_deduplicated"] == 7_446_499, "G5 dedup count changed")
    _require(g5["user_item_interactions"] == 6_359_182, "G5 interaction count changed")
    _require(g5["category_nodes"] == 49_732, "G5 category-node count changed")
    _require(
        g5["internal_graph_edges"] == 1_231_439, "G5 internal graph-edge count changed"
    )
    _require(g5["orphan_graph_targets"] == 172_790, "G5 orphan-target count changed")
    _require(
        g5["reviews_raw"] - g5["reviews_deduplicated"]
        == g5["duplicate_review_extra"]
        == 146_745,
        "G5 duplicate reconciliation failed",
    )
    for key in (
        "invalid_dates",
        "invalid_ratings",
        "downloaded_row_count_mismatches",
        "category_count_mismatches",
        "similar_count_mismatches",
        "deduplicated_key_violations",
        "interaction_key_violations",
    ):
        _require(g5[key] == 0, f"G5 invariant is non-zero: {key}")

    g6 = evidence["G6"]["invariants"]
    _require(g6["source_interactions"] == 6_359_182, "G6 source count changed")
    _require(
        g6["train_interactions"]
        + g6["validation_interactions"]
        + g6["test_interactions"]
        == g6["split_total"]
        == g6["source_interactions"],
        "G6 split does not reconcile",
    )
    for key, value in g6.items():
        if key.endswith("_violations") or key in {
            "split_pair_overlap",
            "test_seen_missing_validation",
        }:
            _require(value == 0, f"G6 leakage invariant is non-zero: {key}")
    _require(g6["kcore_converged"] == 1, "G6 ALS k-core did not converge")
    _require(
        evidence["G6"].get("split_order") == ["interaction_date ASC", "product_id ASC"],
        "G6 temporal split order changed",
    )
    _require(
        evidence["G6"].get("validation_seen") == "train only",
        "G6 validation seen-set changed",
    )
    _require(
        evidence["G6"].get("test_seen") == "train plus validation target",
        "G6 test seen-set changed",
    )
    _require(
        evidence["G6"].get("stable_hash") == "SHA256(customer_id + U+001F + '42')",
        "G6 evaluation hash changed",
    )

    g7 = evidence["G7"]
    _require(g7.get("independent_model_count") == 5, "G7 model count changed")
    _require(
        tuple(g7.get("independent_models", ())) == INDEPENDENT_MODELS,
        "G7 model set changed",
    )
    _require(g7.get("hybrid_models_trained") == 0, "G7 trained a hybrid model")
    _require(g7.get("single_fit_contract") is True, "G7 single-fit proof missing")
    _require(
        g7.get("train_only_feature_lineage") is True,
        "G7 train-only feature lineage proof missing",
    )
    details = g7.get("stage_details", {})
    _require(set(details) == set(INDEPENDENT_MODELS), "G7 stage budget is incomplete")
    _require(
        all(int(details[model]["fit_count"]) == 1 for model in INDEPENDENT_MODELS),
        "G7 independent model fit count changed",
    )
    for model, expected_parameters in G7_PARAMETER_SUBSETS.items():
        actual_parameters = details[model].get("parameters", {})
        mismatches = {
            key: (actual_parameters.get(key), expected)
            for key, expected in expected_parameters.items()
            if actual_parameters.get(key) != expected
        }
        _require(not mismatches, f"G7 {model} parameters changed: {mismatches}")

    g8 = evidence["G8"]
    _require(g8.get("hybrid_variants") == ["h_a", "h_b"], "G8 variant set changed")
    _require(g8.get("independent_models_refit") == 0, "G8 refit an independent model")
    _require(g8.get("hybrid_models_fit") == 0, "G8 fit a hybrid estimator")
    _require(
        g8.get("selection_deferred_to_g9_validation") is True,
        "G8 selected a hybrid before G9",
    )
    budget = g8.get("experiment_budget", {})
    _require(
        budget.get("g7_independent_model_count") == 5,
        "G8 upstream model budget changed",
    )
    _require(budget.get("g7_total_fit_count") == 5, "G8 upstream fit budget changed")
    _require(budget.get("hybrid_variant_count") == 2, "G8 hybrid budget changed")
    _require(budget.get("g8_model_refit_count") == 0, "G8 model-refit count changed")
    _require(
        budget.get("variants") == ["h_a", "h_b"], "G8 contains an unapproved variant"
    )
    variant_validations = g8.get("variant_validations", {})
    _require(
        set(variant_validations) == set(HYBRID_MODELS),
        "G8 variant validation set changed",
    )
    for variant, expected_weights in HYBRID_WEIGHTS.items():
        validation = variant_validations[variant]
        _require(
            validation.get("weights") == expected_weights,
            f"G8 {variant} weights changed",
        )
        _require(validation.get("rrf_c") == 60, f"G8 {variant} RRF c changed")
        _require(
            validation.get("stored_depth") == 100, f"G8 {variant} stored depth changed"
        )

    g9 = evidence["G9"]
    selection = g9.get("selection", {})
    selected = selection.get("selected_model")
    _require(selected in HYBRID_MODELS, "G9 selected hybrid is invalid")
    _require(
        selection.get("test_metrics_used") is False, "G9 selection used test metrics"
    )
    _require(
        selection.get("selection_status") == "frozen_before_test_evaluation",
        "G9 selection was not frozen before test",
    )
    _require(
        [
            selection.get("selection_stage"),
            selection.get("selection_cohort"),
            selection.get("selection_slice"),
        ]
        == ["validation", "common_warm", "overall"],
        "G9 selection inputs changed",
    )
    freeze = g9.get("selection_freeze_evidence", {})
    _require(
        freeze.get("test_outputs_present_at_freeze") == [],
        "G9 test output preceded selection",
    )
    _require(freeze.get("selected_model") == selected, "G9 freeze selection mismatch")
    _require(g9.get("selection_test_blind") is True, "G9 test-blind proof missing")
    try:
        selected_weights = json.loads(selection.get("selected_weights_json", ""))
    except json.JSONDecodeError as error:
        raise RuntimeError("G9 selected weights JSON is invalid") from error
    _require(
        selected_weights == HYBRID_WEIGHTS[selected], "G9 selected weights changed"
    )
    _require(selection.get("rrf_c") == 60, "G9 selected RRF c changed")
    _require(selection.get("stored_depth") == 100, "G9 selected stored depth changed")
    _require(selection.get("ndcg_tie_threshold") == 0.001, "G9 tie threshold changed")
    _require(
        g9.get("experiment_budget_rows") == 7,
        "G9 experiment budget must have seven rows",
    )
    validation_models = set(g9.get("official_validation_models", ()))
    test_models = set(g9.get("official_test_models", ()))
    _require(
        validation_models == {*INDEPENDENT_MODELS, *HYBRID_MODELS},
        "G9 validation model set changed",
    )
    _require(
        test_models == {*INDEPENDENT_MODELS, selected},
        "G9 official test model set changed",
    )
    invariants = g9.get("invariants", {})
    _require(
        invariants.get("selection_test_blind") is True,
        "G9 invariant test-blind proof missing",
    )
    _require(
        invariants.get("validation_model_count") == 7, "G9 validation count changed"
    )
    _require(invariants.get("test_model_count") == 6, "G9 test count changed")
    _require(
        invariants.get("experiment_budget_rows") == 7, "G9 budget-row invariant changed"
    )

    g10 = evidence["G10"]
    required_g10 = {
        "spark_free_static_audit",
        "four_page_app_test",
        "gold_source_contract",
        "duckdb_read_only_probe",
        "dashboard_exports",
        "servable_customer_contract",
        "demo_user_contract",
        "tables",
    }
    _require(
        required_g10.issubset(g10),
        f"G10 evidence is incomplete: {sorted(required_g10 - set(g10))}",
    )
    _validate_no_violations(g10["spark_free_static_audit"], "G10 spark-free audit")
    _validate_no_violations(g10["four_page_app_test"], "G10 four-page app")
    _validate_no_violations(g10["gold_source_contract"], "G10 Gold source")
    _validate_no_violations(g10["duckdb_read_only_probe"], "G10 DuckDB read-only probe")
    audit = g10["spark_free_static_audit"]
    app_test = g10["four_page_app_test"]
    gold = g10["gold_source_contract"]
    duckdb_probe = g10["duckdb_read_only_probe"]
    _require(audit.get("status") == "passed", "G10 static audit did not pass")
    _require(audit.get("page_count") == 4, "G10 static audit page count changed")
    _require(
        audit.get("spark_session_construction") is False, "G10 app can construct Spark"
    )
    _require(
        audit.get("streamlit_server_started") is False, "G10 audit started a server"
    )
    _require(app_test.get("status") == "passed", "G10 AppTest did not pass")
    _require(app_test.get("files_executed") == 4, "G10 AppTest file count changed")
    _require(app_test.get("pages_executed") == 4, "G10 AppTest page count changed")
    _require(app_test.get("server_started") is False, "G10 AppTest started a server")
    _require(app_test.get("browser_started") is False, "G10 AppTest started a browser")
    _require(
        app_test.get("new_spark_or_java_children") == [],
        "G10 AppTest started Spark/Java",
    )
    _require(gold.get("status") == "passed", "G10 Gold source contract did not pass")
    _require(gold.get("silver_runtime_sources") == [], "G10 runtime reads Silver")
    _require(
        gold.get("resolver_priority_violations") == {}, "G10 resolver priority changed"
    )
    _require(
        gold.get("all_tables_have_success_marker") is True, "G10 Gold marker missing"
    )
    _require(duckdb_probe.get("status") == "passed", "G10 DuckDB probe did not pass")
    _require(
        duckdb_probe.get("write_statement_rejected") is True,
        "G10 DuckDB accepted a write",
    )
    _require(
        duckdb_probe.get("spark_imported_by_app") is False, "G10 app imported Spark"
    )
    _require(
        g10["dashboard_exports"].get("compact_aggregate_tables") == 6,
        "G10 export budget changed",
    )
    _require(
        g10["servable_customer_contract"].get("rows") == 80_000,
        "G10 servable user count changed",
    )
    _require(
        g10["servable_customer_contract"].get("online_arbitrary_user_promise") is False,
        "G10 promises unsupported arbitrary online users",
    )
    _require(
        g10["demo_user_contract"].get("rows", 0) >= 20,
        "G10 demo-user count is below 20",
    )
    _require(
        g10.get("streamlit_server_started") is False,
        "G10 handler started Streamlit server",
    )
    _require(g10.get("browser_started") is False, "G10 handler started a browser")
    _require(g10.get("spark_session_started") is False, "G10 handler started Spark")
    expected_g10_tables = {
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
    _require(set(g10["tables"]) == expected_g10_tables, "G10 output table set changed")

    g11 = evidence["G11"]
    _require(g11.get("condition_count") == 2, "G11 condition count changed")
    _require(g11.get("trial_count") == 8, "G11 must contain eight trials")
    _require(g11.get("warmups_per_condition") == 1, "G11 warm-up budget changed")
    _require(g11.get("measured_runs_per_condition") == 3, "G11 measured budget changed")
    _require(g11.get("cache_enabled") is False, "G11 cache must remain disabled")
    _require(g11.get("shuffle_partitions") == 64, "G11 shuffle partition count changed")
    _require(g11.get("aqe_enabled") is True, "G11 AQE must be enabled")
    summary = g11.get("summary", {})
    conditions = summary.get("conditions", {})
    _require(
        set(conditions) == {"single_core", "bounded_multi_core"},
        "G11 conditions changed",
    )
    logical_cores = int(manifests["G0"].get("hardware", {}).get("logical_cores", 0))
    _require(logical_cores >= 1, "G0 logical-core evidence missing")
    expected_masters = {
        "single_core": "local[1]",
        "bounded_multi_core": f"local[{min(4, logical_cores)}]",
    }
    expected_threads = {
        "single_core": 1,
        "bounded_multi_core": min(4, logical_cores),
    }
    protocol = summary.get("protocol", {})
    expected_protocol = {
        "warmups_per_condition": 1,
        "measured_runs_per_condition": 3,
        "shuffle_partitions": 64,
        "aqe_enabled": True,
        "cache_enabled": False,
        "comparison": "local multi-core parallelism; not horizontal scaling",
    }
    _require(
        all(protocol.get(key) == value for key, value in expected_protocol.items()),
        "G11 summary protocol changed",
    )
    application_ids: set[str] = set()
    event_log_digests: set[str] = set()
    event_files: set[str] = set()
    output_rows: set[int] = set()
    output_schemas: set[str] = set()
    for name, condition in conditions.items():
        _require(
            condition.get("master") == expected_masters[name],
            f"G11 {name} master changed",
        )
        _require(
            condition.get("worker_threads") == expected_threads[name],
            f"G11 {name} worker width changed",
        )
        trials = condition.get("trials", [])
        measured = condition.get("measured_wall_seconds", [])
        _require(
            len(trials) == 4 and len(measured) == 3, f"G11 {name} trial budget changed"
        )
        warmup_trials = [
            trial for trial in trials if trial.get("spec", {}).get("is_warmup") is True
        ]
        measured_trials = [
            trial for trial in trials if trial.get("spec", {}).get("is_warmup") is False
        ]
        _require(
            len(warmup_trials) == 1 and len(measured_trials) == 3,
            f"G11 {name} warm-up/measured roles changed",
        )
        _require(
            [int(trial["spec"]["ordinal"]) for trial in warmup_trials] == [0]
            and sorted(int(trial["spec"]["ordinal"]) for trial in measured_trials)
            == [1, 2, 3],
            f"G11 {name} trial ordinals changed",
        )
        for trial in trials:
            spec_condition = trial["spec"]["condition"]
            _require(
                spec_condition
                == {
                    "name": name,
                    "master": expected_masters[name],
                    "worker_threads": expected_threads[name],
                },
                f"G11 {name} embedded condition changed",
            )
        measured_trials.sort(key=lambda trial: int(trial["spec"]["ordinal"]))
        derived_measured = [
            float(trial["workload"]["wall_seconds"]) for trial in measured_trials
        ]
        _require(
            len(measured) == len(derived_measured)
            and all(
                math.isclose(float(observed), expected, rel_tol=0.0, abs_tol=1e-12)
                for observed, expected in zip(measured, derived_measured, strict=True)
            ),
            f"G11 {name} measured timings do not reconcile with raw trials",
        )
        _require(
            math.isclose(
                float(condition["warmup_wall_seconds"]),
                float(warmup_trials[0]["workload"]["wall_seconds"]),
                rel_tol=0.0,
                abs_tol=1e-12,
            ),
            f"G11 {name} warm-up timing does not reconcile",
        )
        _require(
            all(float(value) > 0.0 for value in measured),
            f"G11 {name} has invalid timing",
        )
        _require(
            math.isclose(
                float(condition["median_wall_seconds"]),
                statistics.median(measured),
                rel_tol=0.0,
                abs_tol=1e-12,
            ),
            f"G11 {name} median does not reconcile",
        )
        for trial in trials:
            application_id = str(trial.get("application_id", ""))
            _require(application_id, "G11 application id missing")
            _require(application_id not in application_ids, "G11 application id reused")
            application_ids.add(application_id)
            _require(
                trial["workload"]["cache_enabled"] is False, "G11 trial cache changed"
            )
            _require(
                float(trial["workload"]["wall_seconds"]) > 0.0,
                "G11 raw trial timing is invalid",
            )
            _require(
                trial["workload"]["plan"]["exchange_node_count"] > 0,
                "G11 Exchange proof missing",
            )
            _require(
                trial["events"]["applications_started"] == 1,
                "G11 application-start proof missing",
            )
            _require(
                trial["events"]["applications_ended"] == 1,
                "G11 application-end proof missing",
            )
            _require(trial["events"]["event_files"], "G11 event files missing")
            event_digest = str(trial["events"].get("event_log_sha256", ""))
            _require(
                re.fullmatch(r"[0-9a-f]{64}", event_digest) is not None,
                "G11 event-log SHA-256 missing",
            )
            _require(
                event_digest not in event_log_digests, "G11 event-log digest reused"
            )
            event_log_digests.add(event_digest)
            for event_file in trial["events"]["event_files"]:
                event_file = str(event_file)
                _require(event_file not in event_files, "G11 event file reused")
                event_files.add(event_file)
            _require(
                int(trial["events"].get("failed_task_attempts", -1)) == 0,
                "G11 contains failed task attempts",
            )
            _require(
                trial["spark_conf"]["spark.task.cpus"] == "1", "G11 task width changed"
            )
            _require(
                trial["spark_conf"].get("spark.master") == expected_masters[name]
                and trial["spark_conf"].get("spark.sql.shuffle.partitions") == "64"
                and trial["spark_conf"].get("spark.sql.adaptive.enabled") == "true"
                and trial["spark_conf"].get("spark.eventLog.enabled") == "true"
                and trial["spark_conf"].get("spark.eventLog.compress") == "false",
                "G11 Spark configuration changed",
            )
            output_rows.add(int(trial["workload"].get("output_rows", 0)))
            output_schemas.add(str(trial["workload"].get("output_schema_sha256", "")))
    _require(len(application_ids) == 8, "G11 does not contain eight applications")
    _require(len(event_log_digests) == 8, "G11 event-log evidence is not unique")
    _require(
        len(output_rows) == 1 and next(iter(output_rows)) > 0, "G11 output rows differ"
    )
    _require(
        len(output_schemas) == 1
        and re.fullmatch(r"[0-9a-f]{64}", next(iter(output_schemas))) is not None,
        "G11 output schemas differ or are invalid",
    )
    _require(
        int(summary.get("output_rows", 0)) == next(iter(output_rows)),
        "G11 summary output row count does not reconcile",
    )
    _require(
        summary.get("output_schema_sha256") == next(iter(output_schemas)),
        "G11 summary output schema does not reconcile",
    )
    expected_speedup = float(conditions["single_core"]["median_wall_seconds"]) / float(
        conditions["bounded_multi_core"]["median_wall_seconds"]
    )
    _require(
        math.isclose(
            float(summary.get("local_parallel_speedup", 0.0)),
            expected_speedup,
            rel_tol=0.0,
            abs_tol=1e-12,
        ),
        "G11 speedup does not reconcile",
    )

    return {
        "runtime": g0,
        "source_offsets": offsets,
        "hard_counts": dict(hard_counts),
        "cleaning_counts": {
            key: g5[key]
            for key in (
                "reviews_raw",
                "reviews_deduplicated",
                "user_item_interactions",
                "duplicate_review_extra",
            )
        },
        "split_counts": {
            key: g6[key]
            for key in (
                "train_interactions",
                "validation_interactions",
                "test_interactions",
            )
        },
        "independent_models": list(INDEPENDENT_MODELS),
        "hybrid_variants": list(HYBRID_MODELS),
        "selected_hybrid": selected,
        "selection_test_blind": True,
        "performance_trials": 8,
        "dashboard_spark_free": True,
    }


def _validate_no_violations(value: Any, label: str) -> None:
    if isinstance(value, bool):
        _require(value, f"{label} is false")
        return
    _require(isinstance(value, Mapping) and value, f"{label} evidence is empty")
    for key, item in value.items():
        lowered = str(key).lower()
        if isinstance(item, bool) and any(
            token in lowered for token in ("passed", "spark_free", "read_only", "valid")
        ):
            _require(item, f"{label} failed: {key}")
        if isinstance(item, (int, float)) and any(
            token in lowered for token in ("violation", "error", "spark_session")
        ):
            _require(item == 0, f"{label} has non-zero {key}")


def validate_readme(path: Path) -> dict[str, Any]:
    _require(path.is_file(), f"README is missing: {path}")
    text = path.read_text(encoding="utf-8")
    _require(len(text) >= 5_000, "README is too short for the binding run guide")
    missing = [snippet for snippet in README_REQUIRED_SNIPPETS if snippet not in text]
    _require(not missing, f"README required content missing: {missing}")
    match = PLACEHOLDER_PATTERN.search(text)
    _require(
        match is None,
        f"README contains placeholder text: {match.group(0) if match else ''}",
    )
    return {
        "path": str(path.resolve()),
        "bytes": path.stat().st_size,
        "sha256": _sha256_file(path),
        "required_snippets": len(README_REQUIRED_SNIPPETS),
        "placeholder_matches": 0,
    }


def _source_identity(
    config: Any, manifests: Mapping[str, Mapping[str, Any]]
) -> dict[str, Any]:
    source = config.resolve("source", "path")
    _require(source.is_file(), f"source file is missing: {source}")
    size = source.stat().st_size
    _require(size == config.get("source", "size_bytes"), "source size changed")
    digest = _sha256_file(source)
    _require(digest == config.get("source", "sha256"), "source SHA-256 changed")
    g4_source = manifests["G4"]["evidence"]["source"]
    _require(g4_source["size_bytes"] == size, "G4 source size does not reconcile")
    _require(g4_source["sha256"] == digest, "G4 source digest does not reconcile")
    _require(
        g4_source["line_count"] == config.get("source", "line_count"),
        "G4 line count changed",
    )
    return {
        "path": str(source.resolve()),
        "size_bytes": size,
        "line_count": g4_source["line_count"],
        "encoding": config.get("source", "encoding"),
        "eol": config.get("source", "eol"),
        "record_delimiter_hex": config.get("source", "record_delimiter_hex"),
        "sha256": digest,
    }


def _table_evidence_entries(
    manifests: Mapping[str, Mapping[str, Any]],
) -> Iterable[tuple[str, str, Mapping[str, Any]]]:
    for gate in PRIOR_GATES[1:]:
        tables = manifests[gate].get("evidence", {}).get("tables", {})
        if not isinstance(tables, Mapping):
            continue
        for name, evidence in sorted(tables.items()):
            _require(
                isinstance(evidence, Mapping),
                f"{gate}.{name} table evidence is invalid",
            )
            yield gate, str(name), evidence


def _verify_directory_artifact(
    path: Path,
    *,
    expected_sha256: str | None,
    artifact_type: str,
    logical_name: str,
) -> dict[str, Any]:
    _require(path.is_dir(), f"artifact directory is missing: {path}")
    files, size_bytes = directory_size(path)
    digest = table_fingerprint(path)
    if expected_sha256 is not None:
        _require(digest == expected_sha256, f"artifact fingerprint mismatch: {path}")
    return {
        "logical_name": logical_name,
        "artifact_type": artifact_type,
        "path": str(path.resolve()),
        "files": files,
        "size_bytes": size_bytes,
        "sha256": digest,
    }


def _parquet_only_fingerprint(path: Path) -> tuple[int, int, str]:
    """Reproduce G10's DuckDB-export fingerprint contract exactly."""

    digest = hashlib.sha256()
    files = sorted(path.glob("*.parquet"))
    for file in files:
        digest.update(file.name.encode("utf-8"))
        with file.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
    return len(files), sum(file.stat().st_size for file in files), digest.hexdigest()


def verify_artifact_fingerprints(
    project_root: Path,
    run_root: Path,
    manifests: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    inventory: list[dict[str, Any]] = []
    verified_paths: dict[Path, str] = {}
    entries = list(_table_evidence_entries(manifests))
    for index, (gate, name, evidence) in enumerate(entries, start=1):
        path_value = evidence.get("path")
        expected_digest = evidence.get("sha256")
        _require(isinstance(path_value, str), f"{gate}.{name} path missing")
        _require(isinstance(expected_digest, str), f"{gate}.{name} SHA-256 missing")
        path = Path(path_value).resolve()
        _require(
            path.is_relative_to(run_root.resolve()), f"{gate}.{name} escapes run root"
        )
        if path in verified_paths:
            _require(
                verified_paths[path] == expected_digest,
                f"conflicting fingerprints for {path}",
            )
            continue
        print(
            json.dumps(
                {
                    "gate": "G12",
                    "status": "verifying_artifact",
                    "artifact_index": index,
                    "artifact_total": len(entries),
                    "source_gate": gate,
                    "table": name,
                }
            ),
            flush=True,
        )
        _require((path / "_SUCCESS").is_file(), f"incomplete Parquet table: {path}")
        if gate == "G10":
            parquet_files, parquet_bytes, digest = _parquet_only_fingerprint(path)
            _require(
                digest == expected_digest, f"artifact fingerprint mismatch: {path}"
            )
            total_files, total_bytes = directory_size(path)
            item = {
                "logical_name": f"{gate}.{name}",
                "artifact_type": "parquet_table",
                "path": str(path),
                "files": total_files,
                "size_bytes": total_bytes,
                "parquet_files": parquet_files,
                "parquet_bytes": parquet_bytes,
                "sha256": digest,
            }
            _require(
                parquet_files == int(evidence.get("parquet_files", -1)),
                f"{gate}.{name} Parquet file count changed",
            )
            _require(
                parquet_bytes == int(evidence.get("size_bytes", -1)),
                f"{gate}.{name} Parquet byte size changed",
            )
        else:
            item = _verify_directory_artifact(
                path,
                expected_sha256=expected_digest,
                artifact_type="parquet_table",
                logical_name=f"{gate}.{name}",
            )
        if "files" in evidence:
            _require(
                item["files"] == int(evidence["files"]),
                f"{gate}.{name} file count changed",
            )
        if gate != "G10" and "size_bytes" in evidence:
            _require(
                item["size_bytes"] == int(evidence["size_bytes"]),
                f"{gate}.{name} byte size changed",
            )
        _require(
            int(evidence.get("rows", -1)) >= 0, f"{gate}.{name} row evidence missing"
        )
        item["rows"] = int(evidence["rows"])
        inventory.append(item)
        verified_paths[path] = expected_digest

    g7_models = run_root / "data" / "g7" / "_models"
    for directory_name, evidence_name in (("als", "als"), ("fp_growth", "fp")):
        path = g7_models / directory_name
        _require(
            (path / "metadata" / "_SUCCESS").is_file(),
            f"G7 {directory_name} model is incomplete",
        )
        contract_path = path / "_model_contract.json"
        _require(contract_path.is_file(), f"G7 {directory_name} contract is missing")
        contract = json.loads(contract_path.read_text(encoding="utf-8"))
        g7_evidence = manifests["G7"]["evidence"]
        _require(
            contract.get("implementation_sha256")
            == g7_evidence.get("implementation_sha256"),
            f"G7 {directory_name} implementation contract changed",
        )
        _require(
            math.isclose(
                float(contract.get("training_seconds", -1.0)),
                float(g7_evidence["stage_details"][evidence_name]["training_seconds"]),
                rel_tol=0.0,
                abs_tol=1e-12,
            ),
            f"G7 {directory_name} training time contract changed",
        )
        inventory.append(
            _verify_directory_artifact(
                path,
                expected_sha256=None,
                artifact_type="spark_model",
                logical_name=f"G7.model.{directory_name}",
            )
        )

    g11_artifact = manifests["G11"]["evidence"].get("artifact", {})
    g11_artifact_sha256 = g11_artifact.get("sha256")
    _require(
        isinstance(g11_artifact_sha256, str) and len(g11_artifact_sha256) == 64,
        "G11 performance artifact SHA-256 is missing",
    )
    performance = Path(str(g11_artifact.get("path", ""))).resolve()
    _require(
        performance.is_relative_to(run_root.resolve()), "G11 artifact escapes run root"
    )
    _require(
        (performance / "_SUCCESS.json").is_file(),
        "G11 performance artifact is incomplete",
    )
    inventory.append(
        _verify_directory_artifact(
            performance,
            expected_sha256=g11_artifact_sha256,
            artifact_type="performance_evidence",
            logical_name="G11.performance",
        )
    )

    source_patterns = (
        "README.md",
        "requirements.lock",
        "pyproject.toml",
        "Makefile",
        "configs/*.yaml",
        "bin/amazon-rec",
        "scripts/*.py",
        "src/**/*.py",
        "tests/**/*.py",
        "app/**/*.py",
        "Documents/*.md",
    )
    code_files: set[Path] = set()
    for pattern in source_patterns:
        code_files.update(path for path in project_root.glob(pattern) if path.is_file())
    _require(code_files, "project source-file inventory is empty")
    for path in sorted(code_files):
        inventory.append(
            {
                "logical_name": path.relative_to(project_root).as_posix(),
                "artifact_type": "project_file",
                "path": str(path.resolve()),
                "files": 1,
                "size_bytes": path.stat().st_size,
                "sha256": _sha256_file(path),
            }
        )
    return inventory


def collect_test_summary(
    manifests: Mapping[str, Mapping[str, Any]],
    g12_junit: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, str]]:
    per_gate: dict[str, dict[str, Any]] = {}
    test_files: dict[str, str] = {}
    g0_tests = manifests["G0"]["runtime"]["tests"]
    _require(
        all(value.get("status") == "passed" for value in g0_tests.values()),
        "G0 test summary is not passing",
    )
    per_gate["G0"] = {
        "tests": len(g0_tests),
        "failures": 0,
        "errors": 0,
        "skipped": 0,
        "source": "embedded_runtime_checks",
    }
    for gate in PRIOR_GATES[1:]:
        evidence = manifests[gate]["evidence"]
        recorded = evidence.get("junit") or evidence.get("pytest")
        _require(isinstance(recorded, Mapping), f"{gate} JUnit summary missing")
        path_value = recorded.get("artifact_path") or recorded.get("path")
        _require(isinstance(path_value, str), f"{gate} JUnit path missing")
        path = Path(path_value)
        _require(path.is_file(), f"{gate} JUnit artifact missing: {path}")
        parsed = _junit(path)
        for key in ("tests", "failures", "errors", "skipped"):
            _require(
                int(recorded.get(key, -1)) == int(parsed[key]),
                f"{gate} JUnit summary mismatch for {key}",
            )
        per_gate[gate] = {**parsed, "sha256": _sha256_file(path)}
        test_files[gate] = str(path.resolve())

    per_gate["G12"] = {
        key: g12_junit[key]
        for key in ("tests", "failures", "errors", "skipped", "path")
    }
    per_gate["G12"]["sha256"] = _sha256_file(Path(g12_junit["path"]))
    test_files["G12"] = str(Path(g12_junit["path"]).resolve())
    unique_junit: dict[str, Mapping[str, Any]] = {}
    for gate, item in per_gate.items():
        if gate == "G0":
            continue
        unique_junit.setdefault(str(item["sha256"]), item)
    unique_test_count = len(g0_tests) + sum(
        int(item["tests"]) for item in unique_junit.values()
    )
    return (
        {
            "all_passed": True,
            "gate_count": 13,
            "tests": unique_test_count,
            "gate_evidence_test_references": sum(
                int(item["tests"]) for item in per_gate.values()
            ),
            "unique_junit_files": len(unique_junit),
            "failures": 0,
            "errors": 0,
            "skipped": sum(int(item["skipped"]) for item in per_gate.values()),
            "per_gate": per_gate,
        },
        test_files,
    )


def _read_comparison_table(path: Path) -> tuple[list[str], list[dict[str, Any]]]:
    _require(
        path.is_dir() and (path / "_SUCCESS").is_file(),
        "official comparison table is incomplete",
    )
    try:
        table = pads.dataset(str(path), format="parquet").to_table()
    except Exception as error:
        raise RuntimeError(
            f"official comparison Parquet is unreadable: {path}"
        ) from error
    columns = list(table.column_names)
    rows = [dict(row) for row in table.to_pylist()]
    _require(rows, "official comparison table is empty")
    return columns, rows


def validate_official_comparison(
    columns: Sequence[str],
    rows: Sequence[Mapping[str, Any]],
    *,
    selected_hybrid: str,
) -> dict[str, Any]:
    required = {
        "model",
        "stage",
        "cohort",
        "slice",
        "evaluated_users",
        "users_with_output",
        "ndcg_at_10",
        "hit_rate_at_10",
        "mrr_at_10",
        "user_coverage",
        "fill_rate_at_10",
        "catalog_coverage_at_10",
        "selected_hybrid_model",
        "is_selected_hybrid",
        "official_result",
        "rmse",
        "mae",
    }
    missing = sorted(required.difference(columns))
    _require(not missing, f"official comparison columns missing: {missing}")
    expected_models = {*INDEPENDENT_MODELS, selected_hybrid}
    _require(
        len(rows) == len(expected_models) * len(COHORTS) * len(SLICES),
        "official comparison row count changed",
    )
    keys: set[tuple[str, str, str]] = set()
    for row in rows:
        model = row["model"]
        _require(row["stage"] == "test", "official comparison contains non-test row")
        _require(model in expected_models, f"unofficial test model found: {model}")
        _require(
            row["cohort"] in COHORTS and row["slice"] in SLICES,
            "official comparison slice changed",
        )
        key = (str(model), str(row["cohort"]), str(row["slice"]))
        _require(key not in keys, f"duplicate official comparison key: {key}")
        keys.add(key)
        _require(
            row["selected_hybrid_model"] == selected_hybrid,
            "selected hybrid column drifted",
        )
        _require(bool(row["official_result"]), "unofficial row marked in final table")
        _require(
            bool(row["is_selected_hybrid"]) == (model == selected_hybrid),
            "selected-hybrid flag mismatch",
        )
        evaluated = int(row["evaluated_users"])
        with_output = int(row["users_with_output"])
        _require(
            evaluated > 0 and 0 <= with_output <= evaluated,
            "invalid official user counts",
        )
        for metric in (
            "ndcg_at_10",
            "hit_rate_at_10",
            "mrr_at_10",
            "user_coverage",
            "fill_rate_at_10",
            "catalog_coverage_at_10",
        ):
            value = row[metric]
            _require(
                value is not None and math.isfinite(float(value)),
                f"missing/non-finite official metric: {metric}",
            )
            _require(
                0.0 <= float(value) <= 1.0, f"official metric outside [0,1]: {metric}"
            )
        if model == "als":
            _require(
                row["rmse"] is not None and row["mae"] is not None, "ALS errors missing"
            )
            _require(
                math.isfinite(float(row["rmse"])) and math.isfinite(float(row["mae"])),
                "ALS errors non-finite",
            )
        else:
            _require(
                row["rmse"] is None and row["mae"] is None,
                "non-ALS model has prediction errors",
            )
    expected_keys = {
        (model, cohort, slice_name)
        for model in expected_models
        for cohort in COHORTS
        for slice_name in SLICES
    }
    _require(keys == expected_keys, "official comparison matrix is incomplete")
    return {
        "rows": len(rows),
        "models": sorted(expected_models),
        "cohorts": list(COHORTS),
        "slices": list(SLICES),
        "selected_hybrid": selected_hybrid,
        "all_metrics_finite": True,
        "test_only": True,
    }


def verify_g9_selection_artifacts(
    run_root: Path,
    manifest: Mapping[str, Any],
) -> dict[str, Any]:
    evidence = manifest["evidence"]
    selection = evidence["selection"]
    selected = selection["selected_model"]
    g9 = run_root / "data" / "g9"
    marker_path = g9 / "_selection_frozen_before_test.json"
    _require(marker_path.is_file(), "G9 selection freeze marker is missing")
    marker = json.loads(marker_path.read_text(encoding="utf-8"))
    freeze_evidence = evidence.get("selection_freeze_evidence", {})
    _require(
        isinstance(freeze_evidence, Mapping), "G9 manifest freeze evidence is missing"
    )
    for key in (
        "gate",
        "implementation_sha256",
        "selected_model",
        "test_outputs_present_at_freeze",
        "frozen_at_utc",
    ):
        _require(
            marker.get(key) == freeze_evidence.get(key),
            f"G9 freeze marker differs from manifest: {key}",
        )
    _require(
        marker.get("selected_model") == selected, "G9 freeze marker winner changed"
    )
    _require(
        marker.get("implementation_sha256") == evidence.get("implementation_sha256"),
        "G9 freeze marker implementation changed",
    )
    _require(
        marker.get("test_outputs_present_at_freeze") == [],
        "G9 freeze marker contains prior test output",
    )

    selected_columns, selected_rows = _read_comparison_table(g9 / "selected_hybrid")
    _require(len(selected_rows) == 1, "G9 selected_hybrid must contain one row")
    selected_row = selected_rows[0]
    for key, expected in selection.items():
        _require(key in selected_columns, f"G9 selected_hybrid column missing: {key}")
        observed = selected_row[key]
        if isinstance(expected, float):
            _require(
                math.isclose(float(observed), expected, rel_tol=0.0, abs_tol=1e-15),
                f"G9 selected_hybrid value changed: {key}",
            )
        else:
            _require(observed == expected, f"G9 selected_hybrid value changed: {key}")
    _require(selected_row["test_metrics_used"] is False, "G9 selection table used test")
    _require(
        selected_row.get("frozen_at_utc") == marker.get("frozen_at_utc"),
        "G9 freeze timestamps do not reconcile",
    )

    comparison_columns, all_comparison_rows = _read_comparison_table(
        g9 / "validation_hybrid_comparison"
    )
    required_comparison_columns = {
        "model",
        "stage",
        "cohort",
        "slice",
        "evaluated_users",
        "ndcg_at_10",
        "user_coverage",
        "selected",
        "selection_reason",
        "selection_status",
    }
    _require(
        required_comparison_columns.issubset(comparison_columns),
        "G9 validation hybrid comparison schema changed",
    )
    comparison_rows = [
        row
        for row in all_comparison_rows
        if row.get("stage") == "validation"
        and row.get("cohort") == "common_warm"
        and row.get("slice") == "overall"
        and row.get("model") in HYBRID_MODELS
    ]
    _require(
        len(comparison_rows) == 2,
        "G9 validation hybrid comparison must have two selection rows",
    )
    by_model = {row["model"]: row for row in comparison_rows}
    _require(set(by_model) == set(HYBRID_MODELS), "G9 validation hybrid table changed")
    for model in HYBRID_MODELS:
        row = by_model[model]
        prefix = model
        _require(
            [row.get("stage"), row.get("cohort"), row.get("slice")]
            == ["validation", "common_warm", "overall"],
            f"G9 {model} selection dimensions changed",
        )
        _require(
            int(row.get("evaluated_users", 0)) == int(selection["evaluated_users"])
            and int(row.get("evaluated_users", 0)) > 0,
            f"G9 {model} selection denominator changed",
        )
        _require(
            math.isclose(
                float(row["ndcg_at_10"]),
                float(selection[f"{prefix}_ndcg_at_10"]),
                rel_tol=0.0,
                abs_tol=1e-15,
            ),
            f"G9 {model} validation NDCG changed",
        )
        _require(
            math.isclose(
                float(row["user_coverage"]),
                float(selection[f"{prefix}_user_coverage"]),
                rel_tol=0.0,
                abs_tol=1e-15,
            ),
            f"G9 {model} validation coverage changed",
        )
        _require(
            bool(row["selected"]) == (model == selected),
            "G9 validation winner flag changed",
        )
        _require(
            row.get("selection_reason") == selection["selection_reason"]
            and row.get("selection_status") == selection["selection_status"],
            f"G9 {model} frozen selection labels changed",
        )

    h_a = by_model["h_a"]
    h_b = by_model["h_b"]
    ndcg_difference = float(h_a["ndcg_at_10"]) - float(h_b["ndcg_at_10"])
    coverage_difference = float(h_a["user_coverage"]) - float(h_b["user_coverage"])
    if abs(ndcg_difference) >= 0.001:
        derived_selected = "h_a" if ndcg_difference > 0.0 else "h_b"
        derived_reason = "higher_validation_ndcg_at_10"
    elif coverage_difference != 0.0:
        derived_selected = "h_a" if coverage_difference > 0.0 else "h_b"
        derived_reason = "ndcg_tie_higher_user_coverage"
    else:
        derived_selected = "h_a"
        derived_reason = "ndcg_and_coverage_tie_default_h_a"
    _require(
        selected == derived_selected, "G9 selected hybrid violates the frozen rule"
    )
    _require(
        selection.get("selection_reason") == derived_reason,
        "G9 selection reason does not reconcile",
    )
    _require(
        math.isclose(
            float(selection.get("ndcg_difference_h_a_minus_h_b")),
            ndcg_difference,
            rel_tol=0.0,
            abs_tol=1e-15,
        ),
        "G9 selection NDCG difference does not reconcile",
    )
    _require(
        math.isclose(
            float(selection.get("coverage_difference_h_a_minus_h_b")),
            coverage_difference,
            rel_tol=0.0,
            abs_tol=1e-15,
        ),
        "G9 selection coverage difference does not reconcile",
    )

    _, budget_rows = _read_comparison_table(g9 / "experiment_budget")
    _require(len(budget_rows) == 7, "G9 experiment budget table must have seven rows")
    budget_by_model = {row["model"]: row for row in budget_rows}
    _require(
        set(budget_by_model) == {*INDEPENDENT_MODELS, *HYBRID_MODELS},
        "G9 experiment budget model set changed",
    )
    for model in INDEPENDENT_MODELS:
        _require(
            int(budget_by_model[model]["fit_count"]) == 1,
            f"G9 {model} fit count changed",
        )
        _require(
            budget_by_model[model]["test_status"] == "evaluated_official",
            f"G9 {model} test status changed",
        )
    for model in HYBRID_MODELS:
        row = budget_by_model[model]
        _require(int(row["fit_count"]) == 0, f"G9 {model} fit count changed")
        expected_status = (
            "evaluated_official_selected_winner"
            if model == selected
            else "not_evaluated_validation_loser"
        )
        _require(
            row["test_status"] == expected_status, f"G9 {model} test status changed"
        )

    test_markers = [
        g9 / name / "_SUCCESS"
        for name in (
            "test_evaluation_per_user",
            "test_evaluation_summary",
            "official_test_comparison",
        )
    ]
    _require(
        all(path.is_file() for path in test_markers), "G9 test output marker missing"
    )
    _require(
        marker_path.stat().st_mtime_ns
        <= min(path.stat().st_mtime_ns for path in test_markers),
        "G9 freeze marker does not precede test outputs",
    )
    return {
        "selected_hybrid": selected,
        "freeze_marker_path": str(marker_path.resolve()),
        "freeze_marker_sha256": _sha256_file(marker_path),
        "selection_table_rows": 1,
        "validation_comparison_rows": 2,
        "experiment_budget_rows": 7,
        "freeze_precedes_test_markers": True,
    }


def collect_verified_inputs(
    config: Any,
    paths: Any,
    g12_junit: Mapping[str, Any],
) -> VerifiedDeliveryInputs:
    manifests, manifest_digests = validate_manifest_chain(
        paths.manifests,
        run_id=paths.run_id,
        config_sha256=config.sha256,
        source_sha256=config.get("source", "sha256"),
    )
    contracts = validate_gate_contracts(manifests)
    source_identity = _source_identity(config, manifests)
    test_summary, test_files = collect_test_summary(manifests, g12_junit)
    artifact_inventory = verify_artifact_fingerprints(
        paths.project_root, paths.run, manifests
    )
    selection_artifacts = verify_g9_selection_artifacts(paths.run, manifests["G9"])
    official_evidence = manifests["G9"]["evidence"]["tables"].get(
        "official_test_comparison"
    )
    _require(
        isinstance(official_evidence, Mapping),
        "G9 official comparison evidence missing",
    )
    official_path = Path(str(official_evidence.get("path", "")))
    columns, rows = _read_comparison_table(official_path)
    validation = validate_official_comparison(
        columns,
        rows,
        selected_hybrid=str(contracts["selected_hybrid"]),
    )
    contracts = {
        **contracts,
        "g9_selection_artifacts": selection_artifacts,
        "official_comparison": validation,
    }
    return VerifiedDeliveryInputs(
        manifests=manifests,
        manifest_digests=manifest_digests,
        source_identity=source_identity,
        contracts=contracts,
        test_summary=test_summary,
        test_files=test_files,
        artifact_inventory=artifact_inventory,
        comparison_columns=columns,
        comparison_rows=rows,
    )


def _implementation_signature(config_sha256: str) -> str:
    digest = hashlib.sha256()
    digest.update(f"g12-contract-{G12_CONTRACT_VERSION}".encode("ascii"))
    digest.update(config_sha256.encode("ascii"))
    digest.update(Path(__file__).read_bytes())
    return digest.hexdigest()


def _prepare_workspace(working: Path, signature: str) -> None:
    marker = working / "_checkpoint_contract.json"
    if working.exists():
        try:
            existing = json.loads(marker.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            existing = {}
        if existing.get("implementation_sha256") != signature:
            shutil.rmtree(working)
    working.mkdir(parents=True, exist_ok=True)
    for child in working.iterdir():
        if child == marker:
            continue
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()
    atomic_write_json(
        marker,
        {
            "gate": "G12",
            "contract_version": G12_CONTRACT_VERSION,
            "implementation_sha256": signature,
        },
    )


def _format_cell(value: Any) -> str:
    if value is None:
        return "—"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, float):
        return format(value, ".8g")
    return str(value).replace("|", "\\|").replace("\n", " ")


def _write_comparison_csv(
    path: Path,
    columns: Sequence[str],
    rows: Sequence[Mapping[str, Any]],
) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(columns), extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary, path)


def _render_final_results(
    rows: Sequence[Mapping[str, Any]],
    *,
    run_id: str,
    selection: Mapping[str, Any],
    comparison_sha256: str,
    performance_summary: Mapping[str, Any],
) -> str:
    selected = selection["selected_model"]
    display_columns = (
        "model",
        "cohort",
        "slice",
        "evaluated_users",
        "ndcg_at_10",
        "hit_rate_at_10",
        "mrr_at_10",
        "user_coverage",
        "fill_rate_at_10",
        "catalog_coverage_at_10",
        "rmse",
        "mae",
    )
    ordered = sorted(
        rows,
        key=lambda row: (
            str(row["cohort"]),
            str(row["slice"]),
            str(row["model"]),
        ),
    )
    header = "| " + " | ".join(display_columns) + " |"
    separator = "|" + "|".join("---" for _ in display_columns) + "|"
    body = [
        "| "
        + " | ".join(_format_cell(row[column]) for column in display_columns)
        + " |"
        for row in ordered
    ]
    conditions = performance_summary["conditions"]
    single = conditions["single_core"]
    parallel = conditions["bounded_multi_core"]
    lines = [
        "# Doğrulanmış final sonuçları",
        "",
        f"Koşum: `{run_id}`",
        "",
        "Bu belge G12 tarafından fingerprinti doğrulanmış G9 Parquet tablosundan otomatik üretildi. "
        "Değerler elle girilmedi. CSV tam sütun kümesini korur.",
        "",
        "## Validation ile dondurulan hibrit",
        "",
        f"Seçilen hibrit: **{selected}**. Seçim test metriği kullanılmadan "
        f"`{selection['selection_status']}` durumunda donduruldu.",
        "",
        "| validation ölçütü | H-A | H-B |",
        "|---|---:|---:|",
        f"| NDCG@10 | {_format_cell(selection['h_a_ndcg_at_10'])} | {_format_cell(selection['h_b_ndcg_at_10'])} |",
        f"| User coverage | {_format_cell(selection['h_a_user_coverage'])} | {_format_cell(selection['h_b_user_coverage'])} |",
        "",
        "## Resmî test karşılaştırması",
        "",
        f"Kaynak tablo fingerprinti: `{comparison_sha256}`.",
        "",
        header,
        separator,
        *body,
        "",
        "RMSE ve MAE yalnız ham ALS puan tahminleri için tanımlıdır; `—` diğer modellerde bilinçli null değerdir.",
        "",
        "## Tek Spark performans deneyi",
        "",
        "Bu karşılaştırma yatay ölçekleme değil, yerel çok çekirdek paralelliğidir.",
        "",
        "| Koşul | Isınma süresi (s) | Ölçülen süreler (s) | Medyan (s) |",
        "|---|---:|---|---:|",
        f"| `{single['master']}` | {_format_cell(single['warmup_wall_seconds'])} | "
        f"{', '.join(_format_cell(value) for value in single['measured_wall_seconds'])} | "
        f"{_format_cell(single['median_wall_seconds'])} |",
        f"| `{parallel['master']}` | {_format_cell(parallel['warmup_wall_seconds'])} | "
        f"{', '.join(_format_cell(value) for value in parallel['measured_wall_seconds'])} | "
        f"{_format_cell(parallel['median_wall_seconds'])} |",
        "",
        f"Yerel paralellik hızlanma oranı: `{_format_cell(performance_summary['local_parallel_speedup'])}`.",
        "",
        "Ham denemeler, planlar ve Spark olay ölçümleri `../performance/` altındadır.",
        "",
    ]
    text = "\n".join(lines)
    _require(
        PLACEHOLDER_PATTERN.search(text) is None,
        "generated result report contains a placeholder",
    )
    return text


def _render_test_summary(summary: Mapping[str, Any]) -> str:
    lines = [
        "# Test özeti",
        "",
        f"Toplam `{summary['tests']}` test/ortam kontrolü; başarısızlık `{summary['failures']}`, "
        f"hata `{summary['errors']}`, atlanan `{summary['skipped']}`.",
        "",
        "| Geçit | Test | Başarısız | Hata | Atlanan |",
        "|---|---:|---:|---:|---:|",
    ]
    for gate, item in summary["per_gate"].items():
        lines.append(
            f"| {gate} | {item['tests']} | {item['failures']} | {item['errors']} | {item['skipped']} |"
        )
    lines.append("")
    return "\n".join(lines)


def _copy_file_atomic(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.tmp")
    shutil.copy2(source, temporary)
    os.replace(temporary, destination)


def _delivery_payload_fingerprint(root: Path) -> str:
    """Hash a boundary-safe inventory of immutable delivery files."""

    _require(root.is_dir(), f"delivery payload is missing: {root}")
    digest = hashlib.sha256()
    files = sorted(path for path in root.rglob("*") if path.is_file())
    for path in files:
        relative = path.relative_to(root).as_posix()
        if relative in DELIVERY_FINALIZATION_FILES:
            continue
        _require(not path.is_symlink(), f"delivery payload contains symlink: {path}")
        relative_bytes = relative.encode("utf-8")
        file_digest = hashlib.sha256()
        size_bytes = 0
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                file_digest.update(chunk)
                size_bytes += len(chunk)
        digest.update(len(relative_bytes).to_bytes(8, byteorder="big"))
        digest.update(relative_bytes)
        digest.update(size_bytes.to_bytes(8, byteorder="big"))
        digest.update(file_digest.digest())
    return digest.hexdigest()


def _cleanup_delivery_finalization_temps(root: Path) -> list[str]:
    """Remove only reserved atomic-write leftovers before resume verification."""

    removed: list[str] = []
    for relative in sorted(DELIVERY_FINALIZATION_TEMP_FILES):
        path = root / relative
        if path.exists() or path.is_symlink():
            _require(
                not path.is_dir(), f"finalization temp path is a directory: {path}"
            )
            path.unlink()
            removed.append(relative)
    return removed


def _publish_delivery(
    working: Path,
    verified: VerifiedDeliveryInputs,
    *,
    paths: Any,
    readme_evidence: Mapping[str, Any],
    implementation_sha256: str,
) -> None:
    manifest_directory = working / "manifests"
    manifest_directory.mkdir(parents=True, exist_ok=True)
    for gate in PRIOR_GATES:
        _copy_file_atomic(
            paths.manifests / f"{gate}.json", manifest_directory / f"{gate}.json"
        )

    test_directory = working / "test-results"
    for gate, source in verified.test_files.items():
        _copy_file_atomic(Path(source), test_directory / f"{gate}-junit.xml")
    _copy_file_atomic(paths.project_root / "README.md", working / "README.md")

    official_evidence = verified.manifests["G9"]["evidence"]["tables"][
        "official_test_comparison"
    ]
    _write_comparison_csv(
        working / "official-test-comparison.csv",
        verified.comparison_columns,
        verified.comparison_rows,
    )
    results = _render_final_results(
        verified.comparison_rows,
        run_id=paths.run_id,
        selection=verified.manifests["G9"]["evidence"]["selection"],
        comparison_sha256=official_evidence["sha256"],
        performance_summary=verified.manifests["G11"]["evidence"]["summary"],
    )
    (working / "final-results.md").write_text(results, encoding="utf-8")
    (working / "test-summary.md").write_text(
        _render_test_summary(verified.test_summary), encoding="utf-8"
    )

    manifest_index = {
        "run_id": paths.run_id,
        "gates": [
            {
                "gate": gate,
                "status": "passed",
                "canonical_path": str((paths.manifests / f"{gate}.json").resolve()),
                "delivery_path": f"manifests/{gate}.json",
                "evidence_sha256": verified.manifest_digests[gate],
                "file_sha256": _sha256_file(paths.manifests / f"{gate}.json"),
            }
            for gate in PRIOR_GATES
        ],
        "g12_manifest": {
            "status": "pending_cli_atomic_manifest_write",
            "canonical_path": str((paths.manifests / "G12.json").resolve()),
        },
    }
    atomic_write_json(working / "manifest-index.json", manifest_index)
    atomic_write_json(working / "source-identity.json", verified.source_identity)
    atomic_write_json(working / "test-summary.json", verified.test_summary)
    atomic_write_json(
        working / "artifact-inventory.json",
        {
            "verified": True,
            "artifact_count": len(verified.artifact_inventory),
            "artifacts": list(verified.artifact_inventory),
        },
    )
    acceptance = {
        "gate": "G12",
        "run_id": paths.run_id,
        "status": "acceptance_checks_passed_publication_requires_success_marker",
        "passed": False,
        "acceptance_checks_passed": True,
        "publication_authority": "_SUCCESS.json",
        "implementation_sha256": implementation_sha256,
        "prior_manifest_chain_verified": True,
        "source_fingerprint_verified": True,
        "artifact_fingerprints_verified": True,
        "readme_verified": dict(readme_evidence),
        "test_summary": dict(verified.test_summary),
        "contracts": dict(verified.contracts),
        "unexecuted_or_placeholder_results_published": False,
        "official_metric_source": official_evidence,
        "limitations": [
            "Publication remains pending until the CLI writes and verifies the canonical G12 manifest.",
            "Historical G1-G6 manifests predate duration fields; no missing duration was synthesized.",
            "The authoritative metric source remains fingerprint-verified G9 Parquet.",
        ],
    }
    atomic_write_json(working / "acceptance-report.json", acceptance)
    atomic_write_json(
        working / "_PENDING_G12_MANIFEST.json",
        {
            "gate": "G12",
            "run_id": paths.run_id,
            "implementation_sha256": implementation_sha256,
            "prior_gate_count": len(PRIOR_GATES),
            "official_comparison_rows": len(verified.comparison_rows),
            "tests": verified.test_summary["tests"],
        },
    )


def _load_json_object(path: Path, description: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError(f"invalid {description}: {path}") from error
    _require(isinstance(payload, dict), f"{description} must be an object: {path}")
    return payload


def finalize_g12_delivery(paths: Any, manifest_path: Path) -> dict[str, Any]:
    """Verify the canonical G12 manifest and write the success marker last.

    The handler cannot include its own manifest because the CLI creates that
    manifest from the handler evidence.  This explicit second phase closes the
    cycle: no delivery is marked successful until all G0--G12 manifests are
    present in the package and the canonical G12 digest has been recomputed.
    """

    manifest_path = manifest_path.resolve()
    _require(manifest_path.is_file(), "canonical G12 manifest is missing")
    manifest = _load_json_object(manifest_path, "G12 manifest")
    _require(manifest.get("schema_version") == 1, "G12 schema version changed")
    _require(manifest.get("gate") == "G12", "G12 manifest identity mismatch")
    _require(manifest.get("status") == "passed", "G12 manifest is not passed")
    _require(manifest.get("run_id") == paths.run_id, "G12 run id mismatch")
    manifest_digest = _manifest_digest(manifest, gate="G12")
    evidence = manifest.get("evidence", {})
    _require(isinstance(evidence, dict), "G12 evidence object missing")

    final = paths.run / "delivery"
    _require(final.is_dir(), "G12 delivery payload is missing")
    _cleanup_delivery_finalization_temps(final)
    pending_path = final / "_PENDING_G12_MANIFEST.json"
    success_path = final / "_SUCCESS.json"
    copied_manifest = final / "manifests" / "G12.json"
    index_path = final / "manifest-index.json"
    acceptance_path = final / "acceptance-report.json"
    delivery_evidence = evidence.get("delivery", {})
    _require(isinstance(delivery_evidence, Mapping), "G12 delivery evidence missing")
    _require(
        delivery_evidence.get("path") == str(final.resolve()),
        "G12 manifest points to a different delivery",
    )
    _require(
        delivery_evidence.get("status") == "pending_manifest_finalization",
        "G12 delivery was not in the expected pending state",
    )
    expected_payload_sha256 = str(delivery_evidence.get("payload_sha256", ""))
    _require(
        re.fullmatch(r"[0-9a-f]{64}", expected_payload_sha256) is not None,
        "G12 immutable payload fingerprint is missing",
    )
    observed_payload_sha256 = _delivery_payload_fingerprint(final)
    _require(
        observed_payload_sha256 == expected_payload_sha256,
        "G12 immutable delivery payload fingerprint mismatch",
    )
    implementation_sha256 = str(evidence.get("implementation_sha256", ""))
    _require(
        re.fullmatch(r"[0-9a-f]{64}", implementation_sha256) is not None,
        "G12 implementation fingerprint is missing",
    )
    pending_acceptance_sha256 = str(
        delivery_evidence.get("pending_acceptance_sha256", "")
    )
    pending_manifest_index_sha256 = str(
        delivery_evidence.get("pending_manifest_index_sha256", "")
    )
    _require(
        re.fullmatch(r"[0-9a-f]{64}", pending_acceptance_sha256) is not None,
        "G12 pending acceptance fingerprint is missing",
    )
    _require(
        re.fullmatch(r"[0-9a-f]{64}", pending_manifest_index_sha256) is not None,
        "G12 pending manifest-index fingerprint is missing",
    )
    _require(
        _sha256_file(acceptance_path) == pending_acceptance_sha256,
        "G12 acceptance report changed before publication",
    )

    if success_path.is_file():
        success = _load_json_object(success_path, "G12 delivery success marker")
        _require(
            success.get("evidence_sha256") == manifest_digest,
            "G12 success digest changed",
        )
        _require(success.get("status") == "passed", "G12 success status changed")
        _require(success.get("run_id") == paths.run_id, "G12 success run id changed")
        _require(
            success.get("implementation_sha256") == implementation_sha256,
            "G12 success implementation changed",
        )
        _require(
            success.get("payload_sha256") == observed_payload_sha256,
            "G12 success payload digest changed",
        )
        _require(copied_manifest.is_file(), "published G12 manifest copy is missing")
        _require(
            _sha256_file(copied_manifest)
            == success.get("manifest_file_sha256")
            == _sha256_file(manifest_path),
            "published G12 manifest copy changed",
        )
        _require(
            _sha256_file(index_path) == success.get("manifest_index_sha256"),
            "published G12 manifest index changed",
        )
        _require(
            _sha256_file(acceptance_path)
            == success.get("acceptance_report_sha256")
            == pending_acceptance_sha256,
            "published G12 acceptance report changed",
        )
        # Repair only a legacy dual-marker state. New publications remove the
        # pending marker before creating success and perform no later write.
        pending_path.unlink(missing_ok=True)
        return success

    if pending_path.is_file():
        pending = _load_json_object(pending_path, "G12 pending publication marker")
        _require(pending.get("gate") == "G12", "G12 pending marker identity changed")
        _require(pending.get("run_id") == paths.run_id, "G12 pending run id changed")
        _require(
            pending.get("implementation_sha256") == implementation_sha256,
            "G12 implementation fingerprint changed during finalization",
        )
    else:
        # The only supported marker-less state is a crash after removing
        # pending and immediately before the final success write.
        _require(
            copied_manifest.is_file(),
            "G12 pending marker is missing outside recoverable finalization",
        )

    index = _load_json_object(index_path, "G12 manifest index")
    _require(index.get("run_id") == paths.run_id, "G12 manifest index run id changed")
    _require(
        set(index) == {"run_id", "gates", "g12_manifest"},
        "G12 manifest index schema changed",
    )
    indexed_gates = index.get("gates", [])
    _require(len(indexed_gates) == len(PRIOR_GATES), "G12 prior manifest index changed")
    expected_previous = {
        str(item["gate"]): str(item["evidence_sha256"]) for item in indexed_gates
    }
    _require(
        tuple(expected_previous) == PRIOR_GATES,
        "G12 prior manifest index order changed",
    )
    _require(
        manifest.get("previous_evidence") == expected_previous,
        "G12 prerequisite evidence chain mismatch",
    )
    for item in indexed_gates:
        canonical = Path(str(item["canonical_path"]))
        delivered = final / str(item["delivery_path"])
        _require(
            canonical.is_file() and delivered.is_file(),
            "G12 indexed manifest is missing",
        )
        _require(
            _sha256_file(canonical) == item["file_sha256"], "canonical manifest changed"
        )
        _require(
            _sha256_file(delivered) == item["file_sha256"], "delivered manifest changed"
        )

    g12_file_sha256 = _sha256_file(manifest_path)
    expected_g12_index = {
        "gate": "G12",
        "status": "passed",
        "canonical_path": str(manifest_path),
        "delivery_path": "manifests/G12.json",
        "evidence_sha256": manifest_digest,
        "file_sha256": g12_file_sha256,
    }
    current_index_sha256 = _sha256_file(index_path)
    _require(
        current_index_sha256 == pending_manifest_index_sha256
        or index.get("g12_manifest") == expected_g12_index,
        "G12 manifest index changed before publication",
    )

    _copy_file_atomic(manifest_path, copied_manifest)
    index["g12_manifest"] = expected_g12_index
    atomic_write_json(index_path, index)

    acceptance_sha256 = pending_acceptance_sha256
    index_sha256 = _sha256_file(index_path)
    success = {
        "gate": "G12",
        "run_id": paths.run_id,
        "status": "passed",
        "implementation_sha256": implementation_sha256,
        "evidence_sha256": manifest_digest,
        "payload_sha256": observed_payload_sha256,
        "manifest_file_sha256": g12_file_sha256,
        "manifest_index_sha256": index_sha256,
        "acceptance_report_sha256": acceptance_sha256,
        "manifest_count": len(PRIOR_GATES) + 1,
    }
    # Remove pending first. A crash here is recovered from the copied G12
    # manifest plus the ready acceptance/index records. No required write or
    # delete follows the success marker.
    pending_path.unlink(missing_ok=True)
    files, size_bytes = directory_size(final)
    success["files_before_success_marker"] = files
    success["size_bytes_before_success_marker"] = size_bytes
    atomic_write_json(success_path, success)
    return success


@register("G12")
def run_g12(config: Any, paths: Any, evidence_file: Path | None) -> dict[str, Any]:
    if evidence_file is None:
        raise RuntimeError("G12 requires passing JUnit XML evidence")
    evidence_file = evidence_file.resolve()
    g12_junit = _junit(evidence_file)
    readme_evidence = validate_readme(paths.project_root / "README.md")
    implementation_sha256 = _implementation_signature(config.sha256)
    final = paths.run / "delivery"
    if final.exists():
        pending = final / "_PENDING_G12_MANIFEST.json"
        success = final / "_SUCCESS.json"
        if success.exists():
            raise FileExistsError(f"G12 delivery is already published: {final}")
        if pending.is_file():
            existing = _load_json_object(pending, "G12 pending publication marker")
            _require(
                existing.get("implementation_sha256") == implementation_sha256,
                "existing G12 payload uses a different implementation",
            )
            shutil.rmtree(final)
        else:
            raise FileExistsError(
                f"G12 delivery exists without reusable marker: {final}"
            )
    working = paths.temporary / "G12-publish"
    _prepare_workspace(working, implementation_sha256)

    verified = collect_verified_inputs(config, paths, g12_junit)
    try:
        _publish_delivery(
            working,
            verified,
            paths=paths,
            readme_evidence=readme_evidence,
            implementation_sha256=implementation_sha256,
        )
        final.parent.mkdir(parents=True, exist_ok=True)
        os.replace(working, final)
    except Exception:
        # Preserve only the contract marker; every generated delivery file is
        # cheap and must not look published after a failed acceptance check.
        if working.exists():
            for child in working.iterdir():
                if child.name == "_checkpoint_contract.json":
                    continue
                if child.is_dir():
                    shutil.rmtree(child)
                else:
                    child.unlink()
        raise

    files, size_bytes = directory_size(final)
    payload_sha256 = _delivery_payload_fingerprint(final)
    pending_acceptance_sha256 = _sha256_file(final / "acceptance-report.json")
    pending_manifest_index_sha256 = _sha256_file(final / "manifest-index.json")
    return {
        "junit": {
            **g12_junit,
            "artifact_path": str((final / "test-results" / "G12-junit.xml").resolve()),
        },
        "implementation_sha256": implementation_sha256,
        "manifest_chain_verified": True,
        "prior_gate_count": len(PRIOR_GATES),
        "source_identity": dict(verified.source_identity),
        "contracts": dict(verified.contracts),
        "test_summary": dict(verified.test_summary),
        "artifact_fingerprints_verified": len(verified.artifact_inventory),
        "official_comparison_rows": len(verified.comparison_rows),
        "selected_hybrid": verified.contracts["selected_hybrid"],
        "placeholder_results_published": False,
        "delivery": {
            "path": str(final.resolve()),
            "status": "pending_manifest_finalization",
            "files": files,
            "size_bytes": size_bytes,
            "payload_sha256": payload_sha256,
            "pending_acceptance_sha256": pending_acceptance_sha256,
            "pending_manifest_index_sha256": pending_manifest_index_sha256,
        },
        "outputs": {
            name: str((final / name).resolve())
            for name in (
                "README.md",
                "final-results.md",
                "official-test-comparison.csv",
                "acceptance-report.json",
                "artifact-inventory.json",
                "manifest-index.json",
                "source-identity.json",
                "test-summary.json",
                "test-summary.md",
            )
        },
    }
