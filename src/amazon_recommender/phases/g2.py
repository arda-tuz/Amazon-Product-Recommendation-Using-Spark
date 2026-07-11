"""G2 parser and record-boundary acceptance handler."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

from amazon_recommender.gate_handlers import register
from amazon_recommender.ingestion.delimiter import detect_delimiter
from amazon_recommender.ingestion.hadoop import read_hadoop_blocks
from amazon_recommender.ingestion.parser import parse_block


def _passing_junit(path: Path) -> dict[str, Any]:
    root = ET.parse(path).getroot()
    suites = [root] if root.tag == "testsuite" else list(root.findall("testsuite"))
    summary = {
        key: sum(int(suite.attrib.get(key, "0")) for suite in suites)
        for key in ("tests", "failures", "errors", "skipped")
    }
    if summary["tests"] <= 0 or summary["failures"] or summary["errors"]:
        raise RuntimeError(f"G2 JUnit evidence is not passing: {summary}")
    summary["path"] = str(path.resolve())
    return summary


@register("G2")
def run_g2(config: Any, paths: Any, evidence_file: Path | None) -> dict[str, Any]:
    if evidence_file is None:
        raise RuntimeError("G2 requires passing --evidence-file JUnit XML")
    source = config.resolve("source", "path")
    delimiter = detect_delimiter(source)
    expected = bytes.fromhex(config.get("source", "record_delimiter_hex"))
    if delimiter.delimiter != expected:
        raise RuntimeError(
            f"Detected delimiter {delimiter.delimiter!r} differs from binding {expected!r}"
        )
    spark = SparkSession.builder.appName("amazon-recommender-g2").getOrCreate()
    records = read_hadoop_blocks(
        spark,
        source,
        delimiter.delimiter,
        split_max_bytes=config.get("spark", "max_partition_bytes"),
    ).take(8)
    parsed = [
        parse_block(raw, source_path=str(source), source_offset=offset)
        for offset, raw in records
    ]
    headers = [outcome for outcome in parsed if outcome.kind == "header"]
    products = [outcome for outcome in parsed if outcome.kind == "product"]
    quarantined = [outcome for outcome in parsed if outcome.kind == "quarantine"]
    if len(headers) != 1 or headers[0].row["declared_items"] != 548_552:
        raise RuntimeError("Canonical Hadoop sample did not yield the exact header")
    if len(products) < 6 or quarantined:
        raise RuntimeError(
            f"Canonical Hadoop sample parse failed: products={len(products)}, quarantine={len(quarantined)}"
        )
    return {
        "junit": _passing_junit(evidence_file),
        "delimiter": {
            "style": delimiter.style,
            "hex": delimiter.delimiter.hex(),
            "sample_bytes": delimiter.sample_bytes,
            "sample_occurrences": delimiter.delimiter_occurrences,
            "crlf_lines": delimiter.crlf_lines,
            "lf_only_lines": delimiter.lf_only_lines,
        },
        "hadoop_sample": {
            "records": len(records),
            "offsets": [offset for offset, _ in records],
            "header_declared_items": headers[0].row["declared_items"],
            "product_ids": [item.row["product_id"] for item in products],
            "quarantine": 0,
        },
        "contracts": {
            "split_max_bytes": config.get("spark", "max_partition_bytes"),
            "fallback_shards": config.get("storage", "fallback_shards"),
            "source_offsets": "LongWritable uncompressed offsets",
        },
    }
