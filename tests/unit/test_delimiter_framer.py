from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from amazon_recommender.ingestion.delimiter import (
    CRLF_DELIMITER,
    LF_DELIMITER,
    DelimiterError,
    detect_delimiter,
    iter_blocks,
)
from amazon_recommender.ingestion.framer import decode_framed_line, frame_to_jsonl


@pytest.mark.unit
@pytest.mark.parametrize(
    "payload,expected",
    [(b"one\r\n\r\ntwo\r\n\r\n", CRLF_DELIMITER), (b"one\n\ntwo\n\n", LF_DELIMITER)],
)
def test_detect_delimiter(payload: bytes, expected: bytes, tmp_path: Path) -> None:
    source = tmp_path / "source.txt"
    source.write_bytes(payload)
    assert detect_delimiter(source, sample_bytes=len(payload)).delimiter == expected


@pytest.mark.unit
def test_mixed_line_endings_fail_closed(tmp_path: Path) -> None:
    source = tmp_path / "source.txt"
    source.write_bytes(b"one\r\n\r\ntwo\n\n")
    with pytest.raises(DelimiterError, match="Mixed"):
        detect_delimiter(source)


@pytest.mark.unit
def test_streaming_blocks_preserve_offsets_and_eof(tmp_path: Path) -> None:
    source = tmp_path / "source.txt"
    source.write_bytes(b"abc\r\n\r\ndefgh\r\n\r\nlast")
    assert list(iter_blocks(source, CRLF_DELIMITER, chunk_bytes=3)) == [
        (0, 0, b"abc"),
        (7, 1, b"defgh"),
        (16, 2, b"last"),
    ]


@pytest.mark.unit
def test_fallback_writes_exactly_64_integrity_checked_shards(tmp_path: Path) -> None:
    source = tmp_path / "source.txt"
    blocks = [f"record-{index}".encode() for index in range(70)]
    source.write_bytes(CRLF_DELIMITER.join(blocks) + CRLF_DELIMITER)
    destination = tmp_path / "framed"
    summary = frame_to_jsonl(source, destination, CRLF_DELIMITER)
    shard_paths = sorted(destination.glob("part-*.jsonl"))
    assert len(shard_paths) == 64
    decoded = []
    for path in shard_paths:
        for line in path.read_text().splitlines():
            decoded.append(decode_framed_line(line))
    decoded.sort(key=lambda item: item[1])
    assert [raw for _, _, raw in decoded] == blocks
    assert summary.records == len(blocks)
    assert hashlib.sha256(decoded[-1][2]).hexdigest()


@pytest.mark.slow
@pytest.mark.unit
def test_streaming_framer_accepts_record_larger_than_128_mib(tmp_path: Path) -> None:
    source = tmp_path / "large.txt"
    one_mib = b"x" * (1024 * 1024)
    with source.open("wb") as handle:
        for _ in range(129):
            handle.write(one_mib)
        handle.write(CRLF_DELIMITER)
        handle.write(b"tail")
    blocks = iter_blocks(source, CRLF_DELIMITER, chunk_bytes=1024 * 1024)
    first = next(blocks)
    second = next(blocks)
    assert len(first[2]) == 129 * 1024 * 1024
    assert first[:2] == (0, 0)
    assert second[1:] == (1, b"tail")
    with pytest.raises(StopIteration):
        next(blocks)
