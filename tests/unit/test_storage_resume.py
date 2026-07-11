from __future__ import annotations

from pathlib import Path

import pytest

from amazon_recommender.pipelines.storage import (
    cleanup_incomplete_publications,
    publish_or_reuse_sized_parquet,
    publish_sized_parquet,
    published_parquet_evidence,
    table_fingerprint,
)


@pytest.mark.unit
def test_published_parquet_evidence_reuses_materialized_table_without_mutation(
    spark, tmp_path: Path
) -> None:
    table = tmp_path / "silver" / "reviews_raw"
    frame = spark.createDataFrame(
        [(1, "A", 5.0), (2, "B", 3.5), (3, "C", 4.0)],
        "product_id long, asin string, rating double",
    )
    frame.coalesce(1).write.mode("error").option("compression", "snappy").parquet(
        str(table)
    )
    before = {
        path.relative_to(table).as_posix(): (path.stat().st_size, path.stat().st_mtime_ns)
        for path in table.rglob("*")
        if path.is_file()
    }
    expected_fingerprint = table_fingerprint(table)

    first = published_parquet_evidence(spark, table)
    second = published_parquet_evidence(spark, table)

    after = {
        path.relative_to(table).as_posix(): (path.stat().st_size, path.stat().st_mtime_ns)
        for path in table.rglob("*")
        if path.is_file()
    }
    assert first == second
    assert first["path"] == str(table)
    assert first["rows"] == 3
    assert first["schema_json"] == frame.schema.jsonValue()
    assert first["parquet_files"] == 1
    assert first["files"] == len(before)
    assert first["size_bytes"] == sum(size for size, _ in before.values())
    assert first["sha256"] == expected_fingerprint
    assert before == after
    assert not list(table.parent.glob(".*.tmp"))


@pytest.mark.unit
def test_cleanup_incomplete_publications_only_removes_hidden_temp_directories(
    tmp_path: Path,
) -> None:
    root = tmp_path / "G4-publish"
    completed = root / "bronze" / "product_records"
    completed.mkdir(parents=True)
    (completed / "_SUCCESS").write_bytes(b"")
    (completed / "part-00000.parquet").write_bytes(b"published")

    envelope = root / "_ingestion_envelope"
    envelope.mkdir(parents=True)
    (envelope / "_SUCCESS").write_bytes(b"")
    (envelope / "part-00000.parquet").write_bytes(b"resume-me")

    nested_partial = root / "silver" / ".reviews_raw.abc123.tmp"
    nested_partial.mkdir(parents=True)
    (nested_partial / "part-00000.parquet").write_bytes(b"partial")
    top_level_partial = root / ".bronze.def456.tmp"
    top_level_partial.mkdir(parents=True)
    (top_level_partial / "nested").mkdir()
    (top_level_partial / "nested" / "stale").write_bytes(b"partial")

    non_hidden_suffix = root / "silver" / "reviews_raw.tmp"
    non_hidden_suffix.mkdir(parents=True)
    (non_hidden_suffix / "keep").write_bytes(b"not-a-publication-temp")
    unrelated_hidden = root / "silver" / ".checkpoint"
    unrelated_hidden.mkdir(parents=True)
    (unrelated_hidden / "keep").write_bytes(b"checkpoint")
    hidden_temp_file = root / "silver" / ".manifest.tmp"
    hidden_temp_file.write_bytes(b"ordinary-file")

    cleanup_incomplete_publications(root)

    assert not nested_partial.exists()
    assert not top_level_partial.exists()
    assert (completed / "_SUCCESS").is_file()
    assert (completed / "part-00000.parquet").read_bytes() == b"published"
    assert (envelope / "_SUCCESS").is_file()
    assert (envelope / "part-00000.parquet").read_bytes() == b"resume-me"
    assert (non_hidden_suffix / "keep").read_bytes() == b"not-a-publication-temp"
    assert (unrelated_hidden / "keep").read_bytes() == b"checkpoint"
    assert hidden_temp_file.read_bytes() == b"ordinary-file"


@pytest.mark.unit
def test_cleanup_incomplete_publications_is_idempotent_for_missing_or_clean_roots(
    tmp_path: Path,
) -> None:
    missing = tmp_path / "missing"
    cleanup_incomplete_publications(missing)

    clean = tmp_path / "clean"
    table = clean / "silver" / "customers"
    table.mkdir(parents=True)
    (table / "_SUCCESS").write_bytes(b"")
    cleanup_incomplete_publications(clean)
    cleanup_incomplete_publications(clean)

    assert not missing.exists()
    assert (table / "_SUCCESS").is_file()


@pytest.mark.unit
def test_sized_publication_measures_64_parts_then_applies_fact_floor(
    spark, tmp_path: Path
) -> None:
    table = tmp_path / "silver" / "reviews_raw"
    frame = spark.range(0, 32).withColumnRenamed("id", "product_id")

    evidence = publish_sized_parquet(frame, table, kind="fact")
    before = table_fingerprint(table)
    reused_evidence, reused = publish_or_reuse_sized_parquet(
        frame, table, kind="fact"
    )

    assert evidence["rows"] == 32
    assert evidence["initial_partitions"] == 64
    assert evidence["target_partitions"] == 8
    assert evidence["parquet_files"] == 8
    assert evidence["measurement_size_bytes"] > 0
    assert reused is True
    assert reused_evidence["rows"] == 32
    assert reused_evidence["measurement_reconstructed_on_resume"] is True
    assert table_fingerprint(table) == before
