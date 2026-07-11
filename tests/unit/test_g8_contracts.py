from __future__ import annotations

import json
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from amazon_recommender.models.hybrid import (
    H_A_WEIGHTS,
    H_B_WEIGHTS,
    MODEL_CANDIDATE_DEPTHS,
    MODEL_NAMES,
    build_hybrid_frames,
)
from amazon_recommender.phases.g8 import (
    OUTPUT_TABLES,
    _prepare_workspace,
    validate_experiment_budget,
    validate_hybrid_candidate_evidence,
    validate_hybrid_recommendations,
)


pytestmark = pytest.mark.unit
SCHEMA = "stage string, customer_id string, product_id int, rank int"


def _candidate_frames(spark):
    rows = {
        "popularity": [
            ("validation", "u1", 1, 1),
            ("validation", "u2", 4, 1),
        ],
        "als": [("validation", "u1", 2, 1)],
        "fp": [("validation", "u1", 3, 1)],
        "graph": [("validation", "u1", 2, 1)],
        "category": [("validation", "u1", 3, 1)],
    }
    return {
        model: spark.createDataFrame(rows.get(model, []), SCHEMA)
        for model in MODEL_NAMES
    }


def _hybrid_frames(spark):
    candidates = _candidate_frames(spark)
    bayes = spark.createDataFrame(
        [(1, 4.0), (2, 4.5), (3, 4.2), (4, 3.9)],
        "product_id int, global_bayesian_score double",
    )
    return candidates, build_hybrid_frames(candidates, bayes)


def _evaluation_users(spark):
    return spark.createDataFrame(
        [("validation", "u1"), ("validation", "u2")],
        "stage string, customer_id string",
    )


def _g7_budget(spark):
    return spark.createDataFrame(
        [
            (model, 1, MODEL_CANDIDATE_DEPTHS[model], "train_only_single_fit")
            for model in MODEL_NAMES
        ],
        "model string, fit_count int, candidate_depth int, training_contract string",
    )


def _g8_budget(spark, *, third_variant: bool = False, refits: int = 0):
    variants = [("h_a", H_A_WEIGHTS), ("h_b", H_B_WEIGHTS)]
    if third_variant:
        variants.append(("h_c", H_A_WEIGHTS))
    return spark.createDataFrame(
        [
            (
                variant,
                refits,
                5,
                60,
                100,
                "pending_validation_g9",
                "g7_frozen_rank_only",
                json.dumps(dict(weights), sort_keys=True, separators=(",", ":")),
            )
            for variant, weights in variants
        ],
        "variant string, model_fit_count int, independent_model_count int, "
        "rrf_c int, stored_depth int, selection_status string, "
        "candidate_source string, weights_json string",
    )


def test_g8_contract_accepts_one_shared_candidate_evidence_and_two_variants(
    spark,
) -> None:
    candidate_frames, hybrid = _hybrid_frames(spark)

    candidate_evidence = validate_hybrid_candidate_evidence(
        hybrid.candidates, candidate_frames, _evaluation_users(spark)
    )
    h_a = validate_hybrid_recommendations(
        hybrid.h_a_recommendations,
        hybrid.candidates,
        variant="h_a",
        weights=H_A_WEIGHTS,
    )
    h_b = validate_hybrid_recommendations(
        hybrid.h_b_recommendations,
        hybrid.candidates,
        variant="h_b",
        weights=H_B_WEIGHTS,
    )

    # Six source rows collapse to four unique stage/user/product candidates:
    # products 2 and 3 are each contributed by two independent models.
    assert candidate_evidence["source_occurrences"] == 6
    assert candidate_evidence["rows"] == 4
    assert candidate_evidence["source_minus_evidence"] == 0
    assert h_a["requests_with_output"] == h_b["requests_with_output"] == 2
    assert h_a["stored_depth"] == h_b["stored_depth"] == 100
    assert h_a["rrf_score_violations"] == h_b["rrf_score_violations"] == 0


def test_g8_variant_contract_rejects_unapproved_variant_label(spark) -> None:
    _, hybrid = _hybrid_frames(spark)
    tampered = hybrid.h_a_recommendations.withColumn(
        "hybrid_variant", F.lit("h_c")
    )

    with pytest.raises(RuntimeError, match="variant_label_violations"):
        validate_hybrid_recommendations(
            tampered,
            hybrid.candidates,
            variant="h_a",
            weights=H_A_WEIGHTS,
        )


def test_g8_candidate_contract_rejects_request_outside_g6_population(spark) -> None:
    candidate_frames, hybrid = _hybrid_frames(spark)
    incomplete_population = spark.createDataFrame(
        [("validation", "u1")], "stage string, customer_id string"
    )

    with pytest.raises(RuntimeError, match="source_request_universe_violations"):
        validate_hybrid_candidate_evidence(
            hybrid.candidates, candidate_frames, incomplete_population
        )


def test_g8_variant_contract_recomputes_top_100_from_all_shared_candidates(
    spark,
) -> None:
    empty = spark.createDataFrame([], SCHEMA)
    candidate_frames = {model: empty for model in MODEL_NAMES}
    candidate_frames["popularity"] = spark.createDataFrame(
        [("validation", "u", product_id, product_id) for product_id in range(1, 101)],
        SCHEMA,
    )
    # The 101st distinct item comes from another approved source so every source
    # rank remains inside its binding candidate depth.
    candidate_frames["graph"] = spark.createDataFrame(
        [("validation", "u", 101, 1)],
        SCHEMA,
    )
    bayes = spark.createDataFrame(
        [(product_id, 4.0) for product_id in range(1, 102)],
        "product_id int, global_bayesian_score double",
    )
    hybrid = build_hybrid_frames(candidate_frames, bayes)
    omitted_product = (
        hybrid.candidates.select("product_id")
        .join(
            hybrid.h_a_recommendations.select("product_id"),
            "product_id",
            "left_anti",
        )
        .first()
        .product_id
    )
    forged_last = hybrid.h_a_recommendations.filter("rank = 100").withColumn(
        "product_id", F.lit(omitted_product)
    )
    tampered = hybrid.h_a_recommendations.filter("rank < 100").unionByName(
        forged_last
    )

    with pytest.raises(
        RuntimeError,
        match="expected_top_k_minus_output|candidate_derived_contract_violations",
    ):
        validate_hybrid_recommendations(
            tampered,
            hybrid.candidates,
            variant="h_a",
            weights=H_A_WEIGHTS,
        )


def test_g8_experiment_budget_proves_no_refit_and_rejects_third_variant(
    spark,
) -> None:
    evidence = validate_experiment_budget(_g7_budget(spark), _g8_budget(spark))
    assert evidence == {
        "g7_independent_model_count": 5,
        "g7_total_fit_count": 5,
        "hybrid_variant_count": 2,
        "g8_model_refit_count": 0,
        "variants": ["h_a", "h_b"],
        "selection_status": "pending_validation_g9",
    }

    with pytest.raises(RuntimeError, match="exactly h_a and h_b"):
        validate_experiment_budget(
            _g7_budget(spark), _g8_budget(spark, third_variant=True)
        )
    with pytest.raises(RuntimeError, match="hybrid budget mismatch"):
        validate_experiment_budget(_g7_budget(spark), _g8_budget(spark, refits=1))


def test_g8_resume_workspace_keeps_complete_same_signature_and_resets_changed_code(
    tmp_path: Path,
) -> None:
    working = tmp_path / "G8-publish"
    _prepare_workspace(working, "signature-a")
    complete = working / "hybrid_candidates"
    complete.mkdir()
    (complete / "_SUCCESS").write_bytes(b"")
    partial = working / ".hybrid_a_recommendations.deadbeef.tmp"
    partial.mkdir()

    removed = _prepare_workspace(working, "signature-a")
    assert (complete / "_SUCCESS").is_file()
    assert not partial.exists()
    assert removed == [str(partial)]

    _prepare_workspace(working, "signature-b")
    assert not complete.exists()
    contract = json.loads(
        (working / "_checkpoint_contract.json").read_text(encoding="utf-8")
    )
    assert contract["implementation_sha256"] == "signature-b"
    assert "hybrid_runtime_summary" in OUTPUT_TABLES
