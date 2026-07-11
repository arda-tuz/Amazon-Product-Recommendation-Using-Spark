"""Detect and stream exact byte record boundaries without normalizing input."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Iterator


CRLF_DELIMITER = b"\r\n\r\n"
LF_DELIMITER = b"\n\n"
DEFAULT_SAMPLE_BYTES = 1024 * 1024
_LF_ONLY = re.compile(rb"(?<!\r)\n")


class DelimiterError(ValueError):
    """Raised when physical line endings or record delimiters are ambiguous."""


@dataclass(frozen=True)
class DelimiterInfo:
    delimiter: bytes
    style: str
    sample_bytes: int
    delimiter_occurrences: int
    crlf_lines: int
    lf_only_lines: int


def detect_delimiter(path: Path, sample_bytes: int = DEFAULT_SAMPLE_BYTES) -> DelimiterInfo:
    with path.open("rb") as handle:
        sample = handle.read(sample_bytes)
    crlf_delimiters = sample.count(CRLF_DELIMITER)
    lf_delimiters = sample.count(LF_DELIMITER)
    crlf_lines = sample.count(b"\r\n")
    lf_only_lines = len(_LF_ONLY.findall(sample))
    if crlf_lines and lf_only_lines:
        raise DelimiterError("Mixed CRLF and LF-only line endings in delimiter sample")
    if crlf_delimiters and lf_delimiters:
        raise DelimiterError("Mixed CRLFCRLF and LFLF record delimiters")
    if crlf_delimiters:
        return DelimiterInfo(
            CRLF_DELIMITER,
            "CRLFCRLF",
            len(sample),
            crlf_delimiters,
            crlf_lines,
            lf_only_lines,
        )
    if lf_delimiters:
        return DelimiterInfo(
            LF_DELIMITER,
            "LFLF",
            len(sample),
            lf_delimiters,
            crlf_lines,
            lf_only_lines,
        )
    raise DelimiterError("No supported blank-line record delimiter in first sample")


def iter_blocks_from_handle(
    handle: BinaryIO, delimiter: bytes, *, chunk_bytes: int = 1024 * 1024
) -> Iterator[tuple[int, int, bytes]]:
    if not delimiter:
        raise ValueError("delimiter must not be empty")
    buffer = bytearray()
    source_offset = 0
    ordinal = 0
    while True:
        chunk = handle.read(chunk_bytes)
        if chunk:
            buffer.extend(chunk)
        while True:
            boundary = buffer.find(delimiter)
            if boundary < 0:
                break
            raw_block = bytes(buffer[:boundary])
            yield source_offset, ordinal, raw_block
            consumed = boundary + len(delimiter)
            del buffer[:consumed]
            source_offset += consumed
            ordinal += 1
        if not chunk:
            break
    if buffer:
        yield source_offset, ordinal, bytes(buffer)


def iter_blocks(
    path: Path, delimiter: bytes, *, chunk_bytes: int = 1024 * 1024
) -> Iterator[tuple[int, int, bytes]]:
    with path.open("rb") as handle:
        yield from iter_blocks_from_handle(handle, delimiter, chunk_bytes=chunk_bytes)
