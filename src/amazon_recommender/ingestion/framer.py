"""Byte-preserving, semantics-free fallback framing into exactly 64 JSONL shards."""

from __future__ import annotations

import base64
import hashlib
import json
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path

from amazon_recommender.ingestion.delimiter import iter_blocks


@dataclass(frozen=True)
class FramingSummary:
    records: int
    shards: int
    source_bytes: int
    framed_payload_bytes: int
    first_offset: int | None
    last_offset: int | None


def frame_to_jsonl(
    source: Path,
    destination: Path,
    delimiter: bytes,
    *,
    shards: int = 64,
    chunk_bytes: int = 1024 * 1024,
) -> FramingSummary:
    if shards != 64:
        raise ValueError("Binding fallback contract requires exactly 64 shards")
    destination.mkdir(parents=True, exist_ok=False)
    paths = [destination / f"part-{index:05d}.jsonl" for index in range(shards)]
    records = 0
    payload_bytes = 0
    first_offset: int | None = None
    last_offset: int | None = None
    with ExitStack() as stack:
        handles = [stack.enter_context(path.open("w", encoding="utf-8", newline="\n")) for path in paths]
        for offset, ordinal, raw_block in iter_blocks(
            source, delimiter, chunk_bytes=chunk_bytes
        ):
            payload = {
                "source_path": str(source.resolve()),
                "source_offset": offset,
                "record_ordinal": ordinal,
                "raw_block_b64": base64.b64encode(raw_block).decode("ascii"),
                "raw_block_sha256": hashlib.sha256(raw_block).hexdigest(),
            }
            handles[ordinal % shards].write(
                json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n"
            )
            records += 1
            payload_bytes += len(raw_block)
            first_offset = offset if first_offset is None else first_offset
            last_offset = offset
    return FramingSummary(
        records=records,
        shards=shards,
        source_bytes=source.stat().st_size,
        framed_payload_bytes=payload_bytes,
        first_offset=first_offset,
        last_offset=last_offset,
    )


def decode_framed_line(line: str) -> tuple[int, int, bytes]:
    payload = json.loads(line)
    raw = base64.b64decode(payload["raw_block_b64"], validate=True)
    if hashlib.sha256(raw).hexdigest() != payload["raw_block_sha256"]:
        raise ValueError("Framed block SHA-256 mismatch")
    return int(payload["source_offset"]), int(payload["record_ordinal"]), raw
