from __future__ import annotations

from pathlib import Path

import pytest

from amazon_recommender.ingestion.delimiter import CRLF_DELIMITER
from amazon_recommender.ingestion.hadoop import read_hadoop_blocks


@pytest.mark.integration
def test_hadoop_record_delimiter_preserves_uncompressed_offsets(
    spark, tmp_path: Path
) -> None:
    source = tmp_path / "records.txt"
    source.write_bytes(b"first\r\nline\r\n\r\nsecond\r\n\r\nthird")
    rows = read_hadoop_blocks(
        spark, source, CRLF_DELIMITER, split_max_bytes=16
    ).collect()
    assert rows == [(0, "first\r\nline"), (15, "second"), (25, "third")]


@pytest.mark.slow
@pytest.mark.integration
def test_hadoop_record_larger_than_split_is_emitted_exactly_once(
    spark, tmp_path: Path
) -> None:
    source = tmp_path / "large-records.txt"
    unit = b"z" * (1024 * 1024)
    with source.open("wb") as handle:
        for _ in range(129):
            handle.write(unit)
        handle.write(CRLF_DELIMITER)
        handle.write(b"tail")
    rows = read_hadoop_blocks(
        spark, source, CRLF_DELIMITER, split_max_bytes=128 * 1024 * 1024
    ).map(lambda pair: (pair[0], len(pair[1]))).collect()
    assert sorted(rows) == [(0, 129 * 1024 * 1024), (129 * 1024 * 1024 + 4, 4)]
