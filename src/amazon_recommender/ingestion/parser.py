"""Strict deterministic parser for one delimiter-framed Amazon metadata block."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any, Literal


HEADER_DESCRIPTION = "# Full information about Amazon Share the Love products"
ID_RE = re.compile(r"^Id:\s+(\d+)$")
ASIN_RE = re.compile(r"^ASIN:\s+(\S+)$")
TITLE_RE = re.compile(r"^  title:\s?(.*)$")
GROUP_RE = re.compile(r"^  group:\s?(.*)$")
SALESRANK_RE = re.compile(r"^  salesrank:\s+(-?\d+)$")
SIMILAR_RE = re.compile(r"^  similar:\s+(\d+)(?:\s+(.*))?$")
CATEGORIES_RE = re.compile(r"^  categories:\s+(\d+)$")
REVIEWS_RE = re.compile(
    r"^  reviews:\s+total:\s+(\d+)\s+downloaded:\s+(\d+)\s+"
    r"avg rating:\s+([+-]?(?:\d+(?:\.\d*)?|\.\d+))$"
)
REVIEW_RE = re.compile(
    r"^    (\d{4})-(\d{1,2})-(\d{1,2})\s+cutomer:\s*(\S+)\s+"
    r"rating:\s*(-?\d+)\s+votes:\s*(-?\d+)\s+helpful:\s*(-?\d+)\s*$"
)
REVIEW_COMPAT_RE = re.compile(
    r"^    (\d{4})-(\d{1,2})-(\d{1,2})\s+customer:\s*(\S+)\s+"
    r"rating:\s*(-?\d+)\s+votes:\s*(-?\d+)\s+helpful:\s*(-?\d+)\s*$"
)
CATEGORY_SEGMENT_RE = re.compile(r"^(.*)\[(-?\d+)\]$")


class ParseError(ValueError):
    def __init__(self, code: str, detail: str) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail


@dataclass(frozen=True)
class ParseOutcome:
    kind: Literal["header", "product", "quarantine"]
    row: dict[str, Any]


def _expect(pattern: re.Pattern[str], line: str, code: str) -> re.Match[str]:
    match = pattern.fullmatch(line)
    if match is None:
        raise ParseError(code, f"Unexpected line: {line[:240]!r}")
    return match


def _category_path(text: str, path_ordinal: int) -> dict[str, Any]:
    if not text.startswith("   |"):
        raise ParseError("invalid_category_path", f"Category path lacks prefix: {text!r}")
    raw_path = text[3:]
    raw_segments = raw_path.split("|")[1:]
    if not raw_segments:
        raise ParseError("invalid_category_path", f"No category segments: {text!r}")
    segments = []
    for depth, segment in enumerate(raw_segments, 1):
        match = CATEGORY_SEGMENT_RE.fullmatch(segment)
        if match is None:
            raise ParseError(
                "invalid_category_segment", f"Missing final numeric id: {segment!r}"
            )
        segments.append(
            {"depth": depth, "label": match.group(1), "category_id": int(match.group(2))}
        )
    return {"path_ordinal": path_ordinal, "raw_path": raw_path, "segments": segments}


def _review(
    line: str,
    ordinal: int,
    asin: str,
    *,
    allow_customer_alias: bool,
) -> dict[str, Any]:
    match = REVIEW_RE.fullmatch(line)
    quality: list[str] = []
    if match is None and allow_customer_alias:
        match = REVIEW_COMPAT_RE.fullmatch(line)
        if match is not None:
            quality.append("customer_alias_used")
    if match is None:
        raise ParseError("invalid_review_line", f"Malformed physical review: {line[:240]!r}")
    year, month, day = (int(match.group(index)) for index in (1, 2, 3))
    raw_date = f"{year}-{month}-{day}"
    try:
        parsed_date: date | None = date(year, month, day)
    except ValueError:
        parsed_date = None
        quality.append("invalid_review_date")
    customer_id = match.group(4)
    rating, votes, helpful = (int(match.group(index)) for index in (5, 6, 7))
    if rating < 1 or rating > 5:
        quality.append("invalid_rating")
    if votes < 0:
        quality.append("negative_votes")
    if helpful < 0:
        quality.append("negative_helpful")
    if helpful > votes:
        quality.append("helpful_exceeds_votes")
    digest_source = "\x1f".join(
        (asin, customer_id, raw_date, str(rating), str(votes), str(helpful))
    )
    return {
        "review_ordinal": ordinal,
        "review_date_raw": raw_date,
        "review_date": parsed_date,
        "customer_id": customer_id,
        "rating": rating,
        "votes": votes,
        "helpful": helpful,
        "content_hash": hashlib.sha256(digest_source.encode("utf-8")).hexdigest(),
        "quality_codes": quality,
    }


def _parse_header(lines: list[str], source_path: str, offset: int, digest: str) -> dict[str, Any]:
    if len(lines) != 2 or lines[0] != HEADER_DESCRIPTION:
        raise ParseError("invalid_header", "Unexpected non-product block")
    match = re.fullmatch(r"Total items:\s+(\d+)", lines[1])
    if match is None:
        raise ParseError("invalid_header", "Header total item line is invalid")
    return {
        "source_path": source_path,
        "source_offset": offset,
        "description": lines[0],
        "declared_items": int(match.group(1)),
        "source_block_sha256": digest,
    }


def _parse_product(
    lines: list[str],
    *,
    source_path: str,
    offset: int,
    ordinal: int | None,
    digest: str,
    allow_customer_alias: bool,
) -> dict[str, Any]:
    if len(lines) < 3:
        raise ParseError("incomplete_record", "Product record has fewer than three lines")
    product_id = int(_expect(ID_RE, lines[0], "invalid_id").group(1))
    asin = _expect(ASIN_RE, lines[1], "invalid_asin").group(1)
    common = {
        "source_path": source_path,
        "source_offset": offset,
        "record_ordinal": ordinal,
        "source_block_sha256": digest,
        "product_id": product_id,
        "asin": asin,
    }
    if lines[2] == "  discontinued product":
        if len(lines) != 3:
            raise ParseError("extra_discontinued_lines", "Discontinued record has extra fields")
        return {
            **common,
            "title": None,
            "product_group": None,
            "salesrank_raw": None,
            "status": "discontinued",
            "similar_declared": None,
            "similars": [],
            "categories_declared": None,
            "category_paths": [],
            "reviews_total": None,
            "reviews_downloaded": None,
            "avg_rating_raw": None,
            "reviews": [],
            "quality_codes": [],
        }
    index = 2
    title = _expect(TITLE_RE, lines[index], "missing_or_reordered_title").group(1)
    index += 1
    title_continuations: list[str] = []
    while index < len(lines) and GROUP_RE.fullmatch(lines[index]) is None:
        # Ten source records contain a physical line break inside title text.
        # Continuations are unindented; another indented field still means the
        # required group field is missing/reordered and must be quarantined.
        if lines[index].startswith("  "):
            break
        title_continuations.append(lines[index])
        index += 1
    if title_continuations:
        title = "\n".join((title, *title_continuations))
    if index >= len(lines):
        raise ParseError("missing_or_reordered_group", "Group line is absent")
    product_group = _expect(GROUP_RE, lines[index], "missing_or_reordered_group").group(1)
    index += 1
    salesrank = int(
        _expect(SALESRANK_RE, lines[index], "missing_or_reordered_salesrank").group(1)
    )
    index += 1
    similar_match = _expect(SIMILAR_RE, lines[index], "missing_or_reordered_similar")
    similar_declared = int(similar_match.group(1))
    refs = similar_match.group(2).split() if similar_match.group(2) else []
    if len(refs) != similar_declared:
        raise ParseError(
            "similar_count_mismatch",
            f"declared={similar_declared}, parsed={len(refs)}",
        )
    similars = [
        {"target_asin": target, "position": position}
        for position, target in enumerate(refs, 1)
    ]
    index += 1
    category_match = _expect(
        CATEGORIES_RE, lines[index], "missing_or_reordered_categories"
    )
    categories_declared = int(category_match.group(1))
    index += 1
    if index + categories_declared > len(lines):
        raise ParseError("category_count_mismatch", "Category lines are truncated")
    category_paths = [
        _category_path(lines[index + path_index], path_index)
        for path_index in range(categories_declared)
    ]
    index += categories_declared
    if index >= len(lines):
        raise ParseError("missing_reviews_header", "Reviews header is absent")
    reviews_match = _expect(REVIEWS_RE, lines[index], "missing_or_reordered_reviews")
    total = int(reviews_match.group(1))
    downloaded = int(reviews_match.group(2))
    try:
        average = Decimal(reviews_match.group(3))
    except InvalidOperation as error:
        raise ParseError("invalid_average", reviews_match.group(3)) from error
    if not average.is_finite():
        raise ParseError("invalid_average", "Average is not finite")
    index += 1
    if len(lines) - index != downloaded:
        raise ParseError(
            "downloaded_review_count_mismatch",
            f"declared={downloaded}, physical={len(lines) - index}",
        )
    reviews = [
        _review(
            lines[index + review_index],
            review_index,
            asin,
            allow_customer_alias=allow_customer_alias,
        )
        for review_index in range(downloaded)
    ]
    quality: list[str] = []
    if title_continuations:
        quality.append("multiline_title")
    if total > downloaded:
        quality.append("reviews_total_greater_than_downloaded")
    elif total < downloaded:
        quality.append("reviews_total_less_than_downloaded")
    if salesrank in (-1, 0):
        quality.append("nonpositive_salesrank")
    for review in reviews:
        quality.extend(review["quality_codes"])
    return {
        **common,
        "title": title,
        "product_group": product_group,
        "salesrank_raw": salesrank,
        "status": "active",
        "similar_declared": similar_declared,
        "similars": similars,
        "categories_declared": categories_declared,
        "category_paths": category_paths,
        "reviews_total": total,
        "reviews_downloaded": downloaded,
        "avg_rating_raw": average,
        "reviews": reviews,
        "quality_codes": sorted(set(quality)),
    }


def parse_block(
    raw_block: bytes | str,
    *,
    source_path: str,
    source_offset: int,
    record_ordinal: int | None = None,
    allow_customer_alias: bool = False,
) -> ParseOutcome:
    raw_bytes = raw_block.encode("utf-8") if isinstance(raw_block, str) else raw_block
    digest = hashlib.sha256(raw_bytes).hexdigest()
    try:
        text = raw_bytes.decode("utf-8")
    except UnicodeDecodeError as error:
        return ParseOutcome(
            "quarantine",
            {
                "source_path": source_path,
                "source_offset": source_offset,
                "record_ordinal": record_ordinal,
                "raw_block": raw_bytes.decode("utf-8", errors="replace"),
                "raw_block_sha256": digest,
                "error_code": "invalid_utf8",
                "error_detail": str(error),
            },
        )
    lines = text.splitlines()
    try:
        if lines and lines[0].startswith("#"):
            row = _parse_header(lines, source_path, source_offset, digest)
            return ParseOutcome("header", row)
        row = _parse_product(
            lines,
            source_path=source_path,
            offset=source_offset,
            ordinal=record_ordinal,
            digest=digest,
            allow_customer_alias=allow_customer_alias,
        )
        return ParseOutcome("product", row)
    except ParseError as error:
        return ParseOutcome(
            "quarantine",
            {
                "source_path": source_path,
                "source_offset": source_offset,
                "record_ordinal": record_ordinal,
                "raw_block": text,
                "raw_block_sha256": digest,
                "error_code": error.code,
                "error_detail": error.detail,
            },
        )
