#!/usr/bin/env python3
"""Stream and profile the Amazon metadata text file without modifying it.

The output is deterministic JSON intended as evidence for dataset-description.md.
Only Python's standard library is required.
"""

from __future__ import annotations

import argparse
import hashlib
import heapq
import io
import json
import math
import re
import statistics
from collections import Counter, defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import BinaryIO, Iterable


ID_RE = re.compile(r"^Id:\s+(\d+)$")
ASIN_LINE_RE = re.compile(r"^ASIN:\s+(\S+)$")
ASIN_VALUE_RE = re.compile(r"^[A-Z0-9]{10}$")
TITLE_RE = re.compile(r"^  title:\s?(.*)$")
GROUP_RE = re.compile(r"^  group:\s?(.*)$")
SALESRANK_RE = re.compile(r"^  salesrank:\s+(-?\d+)$")
SIMILAR_RE = re.compile(r"^  similar:\s+(\d+)(?:\s+(.*))?$")
CATEGORIES_RE = re.compile(r"^  categories:\s+(\d+)$")
REVIEWS_RE = re.compile(
    r"^  reviews:\s+total:\s+(\d+)\s+downloaded:\s+(\d+)\s+"
    r"avg rating:\s+([+-]?(?:\d+(?:\.\d*)?|\.\d+|nan|inf))$",
    re.IGNORECASE,
)
REVIEW_LINE_RE = re.compile(
    r"^    (\d{4}-\d{1,2}-\d{1,2})\s+cutomer:\s*(\S+)\s+"
    r"rating:\s*(-?\d+)\s+votes:\s*(-?\d+)\s+helpful:\s*(-?\d+)\s*$"
)
CATEGORY_SEGMENT_RE = re.compile(r"^(.*)\[(-?\d+)\]$")
CUSTOMER_RE = re.compile(r"^[A-Z0-9]+$")
CONTROL_RE = re.compile(rb"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")

NORMAL_FIELDS = ("id", "asin", "title", "group", "salesrank", "similar", "categories", "reviews")
DISCONTINUED_FIELDS = ("id", "asin", "discontinued_product")
SUMMARY_PERCENTILES = (0.25, 0.75, 0.90, 0.95, 0.99)
EXAMPLE_LIMIT = 8
TOP_LIMIT = 10


def pct(numerator: int, denominator: int) -> float | None:
    return round(100.0 * numerator / denominator, 6) if denominator else None


def nearest_rank(sorted_values: list[int], probability: float) -> int:
    if not sorted_values:
        raise ValueError("empty distribution")
    return sorted_values[max(0, math.ceil(probability * len(sorted_values)) - 1)]


def summarize_values(values: Iterable[int]) -> dict[str, int | float | None]:
    vals = sorted(values)
    if not vals:
        return {"count": 0, "min": None, "max": None, "mean": None, "median": None}
    result: dict[str, int | float | None] = {
        "count": len(vals),
        "sum": sum(vals),
        "distinct": len(set(vals)),
        "min": vals[0],
        "max": vals[-1],
        "mean": round(math.fsum(vals) / len(vals), 6),
        "median": statistics.median(vals),
        "zero_count": sum(value == 0 for value in vals),
        "negative_count": sum(value < 0 for value in vals),
    }
    for probability in SUMMARY_PERCENTILES:
        result[f"p{int(probability * 100)}"] = nearest_rank(vals, probability)
    q1 = nearest_rank(vals, 0.25)
    q3 = nearest_rank(vals, 0.75)
    iqr = q3 - q1
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    result["iqr_lower_fence"] = lower
    result["iqr_upper_fence"] = upper
    result["below_iqr_fence_count"] = sum(value < lower for value in vals)
    result["above_iqr_fence_count"] = sum(value > upper for value in vals)
    return result


def counter_rank(counter: Counter[int], rank: int) -> int:
    seen = 0
    for value, count in sorted(counter.items()):
        seen += count
        if seen >= rank:
            return value
    raise ValueError("rank outside distribution")


def summarize_counter(counter: Counter[int]) -> dict[str, int | float | None]:
    count = sum(counter.values())
    if not count:
        return {"count": 0, "min": None, "max": None, "mean": None, "median": None}
    keys = sorted(counter)
    middle_left = (count + 1) // 2
    middle_right = (count + 2) // 2
    result: dict[str, int | float | None] = {
        "count": count,
        "sum": sum(value * n for value, n in counter.items()),
        "distinct": len(counter),
        "min": keys[0],
        "max": keys[-1],
        "mean": round(math.fsum(value * n for value, n in counter.items()) / count, 6),
        "median": (counter_rank(counter, middle_left) + counter_rank(counter, middle_right)) / 2,
        "zero_count": counter.get(0, 0),
        "negative_count": sum(n for value, n in counter.items() if value < 0),
    }
    for probability in SUMMARY_PERCENTILES:
        result[f"p{int(probability * 100)}"] = counter_rank(counter, max(1, math.ceil(probability * count)))
    q1 = int(result["p25"])
    q3 = int(result["p75"])
    iqr = q3 - q1
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    result["iqr_lower_fence"] = lower
    result["iqr_upper_fence"] = upper
    result["below_iqr_fence_count"] = sum(n for value, n in counter.items() if value < lower)
    result["above_iqr_fence_count"] = sum(n for value, n in counter.items() if value > upper)
    return result


def top_counter(counter: Counter, limit: int = TOP_LIMIT) -> list[dict[str, object]]:
    return [
        {"value": value, "count": count}
        for value, count in sorted(counter.items(), key=lambda item: (-item[1], str(item[0])))[:limit]
    ]


class Analyzer:
    def __init__(self, source_label: str) -> None:
        self.source_label = source_label
        self.sha256 = hashlib.sha256()
        self.size_bytes = 0
        self.total_lines = 0
        self.blank_lines = 0
        self.whitespace_only_lines = 0
        self.crlf_lines = 0
        self.lf_only_lines = 0
        self.no_terminator_lines = 0
        self.total_cr_bytes = 0
        self.total_lf_bytes = 0
        self.non_ascii_bytes = 0
        self.utf8_decode_error_lines = 0
        self.nul_bytes = 0
        self.control_bytes = 0
        self.max_line_bytes = 0
        self.max_line_number: int | None = None
        self.final_bytes = b""

        self.header_lines: list[str] = []
        self.header_total_items: int | None = None
        self.header_valid = False

        self.records = 0
        self.normal_records = 0
        self.discontinued_records = 0
        self.valid_records = 0
        self.incomplete_records = 0
        self.malformed_records = 0
        self.first_record_line: int | None = None
        self.last_record_line: int | None = None

        self.ids: set[int] = set()
        self.asins: set[str] = set()
        self.titles: set[str] = set()
        self.salesranks: set[int] = set()
        self.groups: Counter[str] = Counter()
        self.record_hashes: set[bytes] = set()
        self.duplicate_ids: Counter[int] = Counter()
        self.duplicate_asins: Counter[str] = Counter()
        self.previous_id: int | None = None

        self.field_presence: Counter[str] = Counter()
        self.active_field_presence: Counter[str] = Counter()
        self.field_empty: Counter[str] = Counter()
        self.field_distinct: defaultdict[str, set] = defaultdict(set)
        self.field_order: Counter[tuple[str, ...]] = Counter()

        self.values: defaultdict[str, list[int]] = defaultdict(list)
        self.rating_values: Counter[int] = Counter()
        self.vote_values: Counter[int] = Counter()
        self.helpful_values: Counter[int] = Counter()
        self.product_avg_ratings: Counter[float] = Counter()
        self.review_dates: set[str] = set()
        self.review_years: Counter[int] = Counter()
        self.earliest_review_date: date | None = None
        self.latest_review_date: date | None = None
        self.review_customers: set[str] = set()
        self.customer_lengths: Counter[int] = Counter()
        self.review_order: Counter[str] = Counter()
        self.review_total_downloaded_relation: Counter[str] = Counter()
        self.total_review_lines = 0
        self.exact_duplicate_review_extra_occurrences = 0
        self.duplicate_reviewer_date_extra_occurrences = 0
        self.exact_avg_matches = 0
        self.half_step_avg_matches = 0
        self.avg_mismatches = 0
        self.avg_recomputable_records = 0
        self.avg_not_recomputable_records = 0

        self.similar_ref_counts: Counter[str] = Counter()
        self.similar_ref_first_source: dict[str, tuple[int, int, str]] = {}
        self.category_paths: Counter[str] = Counter()
        self.category_depths: Counter[int] = Counter()
        self.category_roots: Counter[str] = Counter()
        self.category_node_labels: dict[int, str] = {}
        self.category_node_ids: set[int] = set()
        self.category_labels: set[str] = set()
        self.category_labels_with_brackets: Counter[str] = Counter()

        self.anomaly_counts: Counter[str] = Counter()
        self.anomaly_examples: defaultdict[str, list[dict[str, object]]] = defaultdict(list)
        self.extremes: defaultdict[str, list[tuple[int, int, int, str]]] = defaultdict(list)

    def anomaly(
        self,
        kind: str,
        line: int | None,
        product_id: int | None,
        detail: str,
        count: int = 1,
    ) -> None:
        self.anomaly_counts[kind] += count
        examples = self.anomaly_examples[kind]
        if len(examples) < EXAMPLE_LIMIT:
            examples.append({"line": line, "product_id": product_id, "detail": detail[:240]})

    def extreme(self, name: str, value: int, line: int, product_id: int, detail: str, largest: bool = True) -> None:
        heap = self.extremes[name]
        item = (value if largest else -value, line, product_id, detail[:160])
        if len(heap) < EXAMPLE_LIMIT:
            heapq.heappush(heap, item)
        elif item > heap[0]:
            heapq.heapreplace(heap, item)

    def scan(self, handle: BinaryIO) -> None:
        block: list[tuple[int, bytes, bytes]] = []
        block_index = 0
        for line_number, raw in enumerate(handle, 1):
            self.total_lines += 1
            self.size_bytes += len(raw)
            self.sha256.update(raw)
            self.final_bytes = (self.final_bytes + raw)[-64:]
            self.total_cr_bytes += raw.count(b"\r")
            self.total_lf_bytes += raw.count(b"\n")
            self.nul_bytes += raw.count(b"\x00")
            self.control_bytes += len(CONTROL_RE.findall(raw))
            if not raw.isascii():
                self.non_ascii_bytes += sum(byte >= 128 for byte in raw)
                try:
                    raw.decode("utf-8", errors="strict")
                except UnicodeDecodeError as exc:
                    self.utf8_decode_error_lines += 1
                    self.anomaly("invalid_utf8_line", line_number, None, f"byte_offset={exc.start}, reason={exc.reason}")

            if raw.endswith(b"\r\n"):
                self.crlf_lines += 1
                content = raw[:-2]
            elif raw.endswith(b"\n"):
                self.lf_only_lines += 1
                content = raw[:-1]
            else:
                self.no_terminator_lines += 1
                content = raw

            if len(content) > self.max_line_bytes:
                self.max_line_bytes = len(content)
                self.max_line_number = line_number

            if content == b"":
                self.blank_lines += 1
                if block:
                    self.process_block(block, block_index)
                    block_index += 1
                    block = []
            else:
                if not content.strip():
                    self.whitespace_only_lines += 1
                block.append((line_number, content, raw))
        if block:
            self.process_block(block, block_index)
            self.anomaly("unterminated_final_block", block[-1][0], None, "EOF arrived without a blank-line record terminator")

    def process_block(self, block: list[tuple[int, bytes, bytes]], block_index: int) -> None:
        texts = [content.decode("utf-8", errors="replace") for _, content, _ in block]
        if block_index == 0 and texts and texts[0].startswith("#"):
            self.header_lines = texts
            if len(texts) == 2 and texts[0] == "# Full information about Amazon Share the Love products":
                match = re.fullmatch(r"Total items:\s+(\d+)", texts[1])
                if match:
                    self.header_total_items = int(match.group(1))
                    self.header_valid = True
            if not self.header_valid:
                self.anomaly("invalid_header", block[0][0], None, " | ".join(texts))
            return

        self.records += 1
        start_line = block[0][0]
        end_line = block[-1][0]
        self.first_record_line = self.first_record_line or start_line
        self.last_record_line = end_line
        block_bytes = sum(len(raw) for _, _, raw in block)
        block_hash = hashlib.sha256(b"".join(raw for _, _, raw in block)).digest()

        product_id: int | None = None
        asin: str | None = None
        syntax_bad = False
        missing = False
        presence: Counter[str] = Counter()
        order: list[str] = []

        id_match = ID_RE.fullmatch(texts[0]) if texts else None
        if id_match:
            product_id = int(id_match.group(1))
            presence["id"] += 1
            order.append("id")
            if product_id in self.ids:
                self.duplicate_ids[product_id] += 1
                self.anomaly("duplicate_id", start_line, product_id, str(product_id))
            self.ids.add(product_id)
            if self.previous_id is not None and product_id != self.previous_id + 1:
                self.anomaly("nonsequential_id", start_line, product_id, f"previous={self.previous_id}, current={product_id}")
            self.previous_id = product_id
        else:
            syntax_bad = True
            missing = True
            self.anomaly("invalid_id_line", start_line, None, texts[0] if texts else "empty block")

        if len(texts) > 1:
            asin_match = ASIN_LINE_RE.fullmatch(texts[1])
        else:
            asin_match = None
        if asin_match:
            asin = asin_match.group(1)
            presence["asin"] += 1
            order.append("asin")
            if asin in self.asins:
                self.duplicate_asins[asin] += 1
                self.anomaly("duplicate_asin", block[1][0], product_id, asin)
            self.asins.add(asin)
            if not ASIN_VALUE_RE.fullmatch(asin):
                self.anomaly("invalid_asin_format", block[1][0], product_id, asin)
        else:
            syntax_bad = True
            missing = True
            self.anomaly("invalid_asin_line", block[1][0] if len(block) > 1 else start_line, product_id, texts[1] if len(texts) > 1 else "missing")

        if block_hash in self.record_hashes:
            self.anomaly("exact_duplicate_record_block", start_line, product_id, "SHA-256 duplicate block")
        self.record_hashes.add(block_hash)

        is_discontinued = len(texts) >= 3 and texts[2] == "  discontinued product"
        if is_discontinued:
            self.discontinued_records += 1
            presence["discontinued_product"] += 1
            order.append("discontinued_product")
            if len(texts) != 3:
                syntax_bad = True
                self.anomaly("extra_lines_in_discontinued_record", block[3][0] if len(block) > 3 else end_line, product_id, f"line_count={len(texts)}")
        else:
            self.normal_records += 1
            syntax_bad = self.parse_normal_record(block, texts, product_id, asin, presence, order) or syntax_bad
            absent = [field for field in NORMAL_FIELDS if presence[field] == 0]
            if absent:
                missing = True
                self.anomaly("missing_required_field", start_line, product_id, ", ".join(absent), len(absent))

        expected = DISCONTINUED_FIELDS if is_discontinued else NORMAL_FIELDS
        duplicate_fields = [field for field in expected if presence[field] > 1]
        if duplicate_fields:
            syntax_bad = True
            self.anomaly("duplicate_top_level_field", start_line, product_id, ", ".join(duplicate_fields))

        if tuple(order) != expected:
            self.anomaly("unexpected_field_order", start_line, product_id, " > ".join(order))

        if missing:
            self.incomplete_records += 1
        elif syntax_bad:
            self.malformed_records += 1
        else:
            self.valid_records += 1

        for field in presence:
            if presence[field]:
                self.field_presence[field] += 1
                if not is_discontinued:
                    self.active_field_presence[field] += 1
        self.field_order[tuple(order)] += 1
        self.values["record_line_count"].append(len(block))
        self.values["record_byte_count"].append(block_bytes)
        if product_id is not None:
            self.extreme("largest_records_by_bytes", block_bytes, start_line, product_id, f"lines={len(block)}")
            self.extreme("smallest_records_by_bytes", block_bytes, start_line, product_id, f"lines={len(block)}", largest=False)

    def parse_normal_record(
        self,
        block: list[tuple[int, bytes, bytes]],
        texts: list[str],
        product_id: int | None,
        asin: str | None,
        presence: Counter[str],
        order: list[str],
    ) -> bool:
        syntax_bad = False
        i = 2
        while i < len(texts):
            text = texts[i]
            line_number = block[i][0]
            match = TITLE_RE.fullmatch(text)
            if match:
                title = match.group(1)
                presence["title"] += 1
                order.append("title")
                self.titles.add(title)
                self.field_distinct["title"].add(title)
                self.values["title_length"].append(len(title))
                if title == "":
                    self.field_empty["title"] += 1
                if product_id is not None:
                    self.extreme("longest_titles", len(title), line_number, product_id, title)
                i += 1
                continue

            match = GROUP_RE.fullmatch(text)
            if match:
                group = match.group(1)
                presence["group"] += 1
                order.append("group")
                self.groups[group] += 1
                self.field_distinct["group"].add(group)
                if group == "":
                    self.field_empty["group"] += 1
                i += 1
                continue

            match = SALESRANK_RE.fullmatch(text)
            if match:
                salesrank = int(match.group(1))
                presence["salesrank"] += 1
                order.append("salesrank")
                self.salesranks.add(salesrank)
                self.field_distinct["salesrank"].add(salesrank)
                self.values["salesrank"].append(salesrank)
                if product_id is not None:
                    self.extreme("largest_salesranks", salesrank, line_number, product_id, str(salesrank))
                    self.extreme("smallest_salesranks", salesrank, line_number, product_id, str(salesrank), largest=False)
                i += 1
                continue

            match = SIMILAR_RE.fullmatch(text)
            if match:
                declared = int(match.group(1))
                refs = match.group(2).split() if match.group(2) else []
                presence["similar"] += 1
                order.append("similar")
                self.field_distinct["similar_declared_count"].add(declared)
                self.values["similar_count"].append(len(refs))
                if declared != len(refs):
                    self.anomaly("similar_count_mismatch", line_number, product_id, f"declared={declared}, parsed={len(refs)}")
                if len(set(refs)) != len(refs):
                    self.anomaly("duplicate_similar_reference_in_record", line_number, product_id, "duplicate ASIN in similar list")
                for ref in refs:
                    self.similar_ref_counts[ref] += 1
                    self.similar_ref_first_source.setdefault(ref, (line_number, product_id if product_id is not None else -1, asin or ""))
                    if not ASIN_VALUE_RE.fullmatch(ref):
                        self.anomaly("invalid_similar_asin_format", line_number, product_id, ref)
                    if asin is not None and ref == asin:
                        self.anomaly("self_similar_reference", line_number, product_id, ref)
                if product_id is not None:
                    self.extreme("largest_similar_lists", len(refs), line_number, product_id, f"declared={declared}")
                i += 1
                continue

            match = CATEGORIES_RE.fullmatch(text)
            if match:
                declared = int(match.group(1))
                presence["categories"] += 1
                order.append("categories")
                self.field_distinct["categories_declared_count"].add(declared)
                i += 1
                parsed = 0
                local_paths: set[str] = set()
                while i < len(texts) and texts[i].startswith("   |"):
                    path_text = texts[i][3:]
                    path_line = block[i][0]
                    parsed += 1
                    self.category_paths[path_text] += 1
                    if path_text in local_paths:
                        self.anomaly("duplicate_category_path_in_record", path_line, product_id, path_text)
                    local_paths.add(path_text)
                    segments = path_text.split("|")[1:] if path_text.startswith("|") else []
                    components = [CATEGORY_SEGMENT_RE.fullmatch(segment) for segment in segments]
                    if not components or any(component is None for component in components):
                        syntax_bad = True
                        self.anomaly("invalid_category_path", path_line, product_id, path_text)
                    else:
                        self.category_depths[len(components)] += 1
                        root_label = components[0].group(1)  # type: ignore[union-attr]
                        root_id = int(components[0].group(2))  # type: ignore[union-attr]
                        self.category_roots[f"{root_label}[{root_id}]"] += 1
                        for component in components:
                            label = component.group(1)  # type: ignore[union-attr]
                            node_id = int(component.group(2))  # type: ignore[union-attr]
                            self.category_node_ids.add(node_id)
                            self.category_labels.add(label)
                            if "[" in label or "]" in label:
                                self.category_labels_with_brackets[label] += 1
                            previous = self.category_node_labels.setdefault(node_id, label)
                            if previous != label:
                                self.anomaly("category_node_label_conflict", path_line, product_id, f"id={node_id}: {previous!r} vs {label!r}")
                    i += 1
                self.values["category_count"].append(parsed)
                if declared != parsed:
                    self.anomaly("category_count_mismatch", line_number, product_id, f"declared={declared}, parsed={parsed}")
                if product_id is not None:
                    self.extreme("largest_category_lists", parsed, line_number, product_id, f"declared={declared}")
                continue

            match = REVIEWS_RE.fullmatch(text)
            if match:
                total = int(match.group(1))
                downloaded = int(match.group(2))
                try:
                    avg_rating = float(match.group(3))
                except ValueError:
                    avg_rating = math.nan
                presence["reviews"] += 1
                order.append("reviews")
                self.field_distinct["reviews_total"].add(total)
                self.field_distinct["reviews_downloaded"].add(downloaded)
                self.field_distinct["avg_rating"].add(avg_rating)
                self.values["reviews_total"].append(total)
                self.values["reviews_downloaded"].append(downloaded)
                self.product_avg_ratings[avg_rating] += 1
                if total == downloaded:
                    self.review_total_downloaded_relation["equal"] += 1
                elif total > downloaded:
                    self.review_total_downloaded_relation["total_greater"] += 1
                else:
                    self.review_total_downloaded_relation["total_less"] += 1
                if total < downloaded:
                    self.anomaly("reviews_total_less_than_downloaded", line_number, product_id, f"total={total}, downloaded={downloaded}")
                i += 1
                parsed_reviews = []
                while i < len(texts) and texts[i].startswith("    "):
                    parsed = self.parse_review_line(texts[i], block[i][0], product_id)
                    if parsed is None:
                        syntax_bad = True
                    else:
                        parsed_reviews.append(parsed)
                    i += 1
                self.total_review_lines += len(parsed_reviews)
                if downloaded != len(parsed_reviews):
                    self.anomaly("downloaded_review_count_mismatch", line_number, product_id, f"declared={downloaded}, parsed={len(parsed_reviews)}")
                if total == downloaded == len(parsed_reviews):
                    self.avg_recomputable_records += 1
                    if parsed_reviews:
                        exact_mean = math.fsum(item[2] for item in parsed_reviews) / len(parsed_reviews)
                        half_step = math.floor(exact_mean * 2 + 0.5) / 2
                    else:
                        exact_mean = 0.0
                        half_step = 0.0
                    if math.isfinite(avg_rating) and math.isclose(avg_rating, exact_mean, abs_tol=1e-12):
                        self.exact_avg_matches += 1
                    if math.isfinite(avg_rating) and math.isclose(avg_rating, half_step, abs_tol=1e-12):
                        self.half_step_avg_matches += 1
                    else:
                        self.avg_mismatches += 1
                        self.anomaly("average_rating_mismatch_when_fully_downloaded", line_number, product_id, f"stored={avg_rating}, computed={exact_mean:.6f}, half_step={half_step}")
                else:
                    self.avg_not_recomputable_records += 1
                review_tuples = [(item[0], item[1], item[2], item[3], item[4]) for item in parsed_reviews]
                if len(set(review_tuples)) != len(review_tuples):
                    self.exact_duplicate_review_extra_occurrences += len(review_tuples) - len(set(review_tuples))
                    self.anomaly("exact_duplicate_review_in_record", line_number, product_id, "duplicate parsed review tuple")
                reviewer_dates = [(item[0], item[1]) for item in parsed_reviews]
                if len(set(reviewer_dates)) != len(reviewer_dates):
                    self.duplicate_reviewer_date_extra_occurrences += len(reviewer_dates) - len(set(reviewer_dates))
                    self.anomaly("duplicate_reviewer_date_in_record", line_number, product_id, "same customer and date repeated")
                parsed_dates = [item[5] for item in parsed_reviews]
                if len(parsed_dates) < 2:
                    order_class = "zero_or_one"
                elif all(left <= right for left, right in zip(parsed_dates, parsed_dates[1:])):
                    order_class = "ascending"
                elif all(left >= right for left, right in zip(parsed_dates, parsed_dates[1:])):
                    order_class = "descending"
                else:
                    order_class = "mixed"
                self.review_order[order_class] += 1
                if product_id is not None:
                    self.extreme("largest_downloaded_review_lists", len(parsed_reviews), line_number, product_id, f"total={total}, downloaded={downloaded}")
                continue

            syntax_bad = True
            self.anomaly("unrecognized_record_line", line_number, product_id, text)
            i += 1
        return syntax_bad

    def parse_review_line(
        self, text: str, line_number: int, product_id: int | None
    ) -> tuple[str, str, int, int, int, date] | None:
        match = REVIEW_LINE_RE.fullmatch(text)
        if not match:
            self.anomaly("invalid_review_line", line_number, product_id, text)
            return None
        date_text, customer, rating_text, votes_text, helpful_text = match.groups()
        rating = int(rating_text)
        votes = int(votes_text)
        helpful = int(helpful_text)
        try:
            parsed_date = datetime.strptime(date_text, "%Y-%m-%d").date()
        except ValueError:
            self.anomaly("invalid_review_date", line_number, product_id, date_text)
            return None
        self.review_dates.add(date_text)
        self.review_years[parsed_date.year] += 1
        self.earliest_review_date = parsed_date if self.earliest_review_date is None else min(self.earliest_review_date, parsed_date)
        self.latest_review_date = parsed_date if self.latest_review_date is None else max(self.latest_review_date, parsed_date)
        self.review_customers.add(customer)
        self.customer_lengths[len(customer)] += 1
        if not CUSTOMER_RE.fullmatch(customer):
            self.anomaly("invalid_customer_format", line_number, product_id, customer)
        self.rating_values[rating] += 1
        self.vote_values[votes] += 1
        self.helpful_values[helpful] += 1
        if rating < 1 or rating > 5:
            self.anomaly("rating_out_of_range", line_number, product_id, str(rating))
        if votes < 0:
            self.anomaly("negative_votes", line_number, product_id, str(votes))
        if helpful < 0:
            self.anomaly("negative_helpful", line_number, product_id, str(helpful))
        if helpful > votes:
            self.anomaly("helpful_exceeds_votes", line_number, product_id, f"votes={votes}, helpful={helpful}")
        return date_text, customer, rating, votes, helpful, parsed_date

    def finalize(self, source_path: Path | None = None) -> dict[str, object]:
        orphan_refs = {ref: count for ref, count in self.similar_ref_counts.items() if ref not in self.asins}
        orphan_examples = []
        for ref, count in sorted(orphan_refs.items(), key=lambda item: (-item[1], item[0]))[:EXAMPLE_LIMIT]:
            line, product_id, source_asin = self.similar_ref_first_source[ref]
            orphan_examples.append({"referenced_asin": ref, "occurrences": count, "line": line, "product_id": product_id, "source_asin": source_asin})
        if orphan_refs:
            self.anomaly_counts["orphan_similar_reference_occurrence"] = sum(orphan_refs.values())
            self.anomaly_examples["orphan_similar_reference_occurrence"] = orphan_examples

        missing_ids: list[int] = []
        if self.ids:
            for candidate in range(min(self.ids), max(self.ids) + 1):
                if candidate not in self.ids:
                    if len(missing_ids) < 100:
                        missing_ids.append(candidate)
        expected_missing_id_count = (max(self.ids) - min(self.ids) + 1 - len(self.ids)) if self.ids else 0

        source_stat: dict[str, object] = {}
        if source_path is not None:
            stat = source_path.stat()
            source_stat = {
                "path": str(source_path),
                "size_from_stat": stat.st_size,
                "mtime_local": datetime.fromtimestamp(stat.st_mtime).astimezone().isoformat(),
                "mtime_ns": stat.st_mtime_ns,
            }

        field_profiles: dict[str, object] = {}
        for field in NORMAL_FIELDS + ("discontinued_product",):
            expected_count = self.records if field in ("id", "asin") else self.normal_records
            present = self.field_presence[field]
            if field == "discontinued_product":
                expected_count = self.discontinued_records
            distinct = None
            if field == "id":
                distinct = len(self.ids)
            elif field == "asin":
                distinct = len(self.asins)
            elif field == "title":
                distinct = len(self.titles)
            elif field == "group":
                distinct = len(self.groups)
            elif field == "salesrank":
                distinct = len(self.salesranks)
            elif field == "similar":
                distinct = len(self.field_distinct["similar_declared_count"])
            elif field == "categories":
                distinct = len(self.field_distinct["categories_declared_count"])
            elif field == "reviews":
                distinct = len(self.field_distinct["reviews_total"])
            field_profiles[field] = {
                "present_records": present,
                "expected_records": expected_count,
                "missing_records": max(0, expected_count - present),
                "coverage_percent": pct(present, expected_count),
                "distinct_values_or_declared_counts": distinct,
                "empty_values": self.field_empty[field],
            }

        extremes: dict[str, object] = {}
        for name, heap in self.extremes.items():
            is_smallest = name.startswith("smallest_")
            rows = []
            for stored, line, product_id, detail in sorted(heap, reverse=True):
                value = -stored if is_smallest else stored
                rows.append({"value": value, "line": line, "product_id": product_id, "detail": detail})
            extremes[name] = rows

        anomalies = {
            kind: {
                "count": count,
                "examples": self.anomaly_examples.get(kind, []),
            }
            for kind, count in sorted(self.anomaly_counts.items())
        }

        return {
            "analysis_contract": {
                "all_counts_are_exact": True,
                "percentile_method": "nearest-rank; median is the conventional middle-value median",
                "outlier_method": "Tukey 1.5*IQR fences; descriptive flag, not proof of corruption",
                "streaming": True,
            },
            "source": {
                **source_stat,
                "bytes_read": self.size_bytes,
                "sha256": self.sha256.hexdigest(),
                "total_lines": self.total_lines,
                "blank_lines": self.blank_lines,
                "whitespace_only_nonblank_lines": self.whitespace_only_lines,
                "crlf_terminated_lines": self.crlf_lines,
                "lf_only_terminated_lines": self.lf_only_lines,
                "unterminated_lines": self.no_terminator_lines,
                "bare_cr_bytes": self.total_cr_bytes - self.crlf_lines,
                "bare_lf_bytes": self.total_lf_bytes - self.crlf_lines,
                "non_ascii_bytes": self.non_ascii_bytes,
                "utf8_decode_error_lines": self.utf8_decode_error_lines,
                "nul_bytes": self.nul_bytes,
                "unexpected_control_bytes": self.control_bytes,
                "encoding_assessment": (
                    "US-ASCII"
                    if self.non_ascii_bytes == 0
                    else "UTF-8" if self.utf8_decode_error_lines == 0 else "mixed or invalid UTF-8"
                ),
                "max_line_bytes_excluding_terminator": self.max_line_bytes,
                "max_line_number": self.max_line_number,
                "final_64_bytes_hex": self.final_bytes.hex(),
                "ends_with_crlf_crlf": self.final_bytes.endswith(b"\r\n\r\n"),
            },
            "header": {
                "lines": self.header_lines,
                "valid_expected_form": self.header_valid,
                "declared_total_items": self.header_total_items,
                "declared_matches_parsed_records": self.header_total_items == self.records,
            },
            "records": {
                "total": self.records,
                "normal": self.normal_records,
                "discontinued": self.discontinued_records,
                "valid": self.valid_records,
                "incomplete": self.incomplete_records,
                "malformed": self.malformed_records,
                "first_record_line": self.first_record_line,
                "last_record_line": self.last_record_line,
                "id_min": min(self.ids) if self.ids else None,
                "id_max": max(self.ids) if self.ids else None,
                "distinct_ids": len(self.ids),
                "duplicate_id_extra_occurrences": sum(self.duplicate_ids.values()),
                "missing_id_count_within_min_max": expected_missing_id_count,
                "missing_id_examples": missing_ids[:20],
                "distinct_asins": len(self.asins),
                "duplicate_asin_extra_occurrences": sum(self.duplicate_asins.values()),
                "distinct_record_blocks": len(self.record_hashes),
            },
            "fields": field_profiles,
            "field_order_signatures": [
                {"order": list(signature), "count": count}
                for signature, count in sorted(self.field_order.items(), key=lambda item: (-item[1], item[0]))
            ],
            "groups": {
                "distinct": len(self.groups),
                "distribution": top_counter(self.groups, 100),
            },
            "distributions": {
                name: summarize_values(values)
                for name, values in sorted(self.values.items())
            },
            "reviews": {
                "parsed_review_lines": self.total_review_lines,
                "distinct_customers": len(self.review_customers),
                "distinct_date_strings": len(self.review_dates),
                "earliest_date": self.earliest_review_date.isoformat() if self.earliest_review_date else None,
                "latest_date": self.latest_review_date.isoformat() if self.latest_review_date else None,
                "rating_distribution": top_counter(self.rating_values, 20),
                "rating_summary": summarize_counter(self.rating_values),
                "votes_summary": summarize_counter(self.vote_values),
                "helpful_summary": summarize_counter(self.helpful_values),
                "customer_length_summary": summarize_counter(self.customer_lengths),
                "year_distribution": [{"year": year, "count": count} for year, count in sorted(self.review_years.items())],
                "record_date_order": dict(sorted(self.review_order.items())),
                "total_vs_downloaded_relation": dict(sorted(self.review_total_downloaded_relation.items())),
                "stored_avg_rating_distribution": top_counter(self.product_avg_ratings, 30),
                "exact_mean_matches": self.exact_avg_matches,
                "nearest_half_step_matches": self.half_step_avg_matches,
                "average_mismatches": self.avg_mismatches,
                "average_recomputable_records": self.avg_recomputable_records,
                "average_not_recomputable_records": self.avg_not_recomputable_records,
                "records_with_exact_duplicate_review": self.anomaly_counts["exact_duplicate_review_in_record"],
                "exact_duplicate_review_extra_occurrences": self.exact_duplicate_review_extra_occurrences,
                "records_with_duplicate_reviewer_date": self.anomaly_counts["duplicate_reviewer_date_in_record"],
                "duplicate_reviewer_date_extra_occurrences": self.duplicate_reviewer_date_extra_occurrences,
            },
            "relationships": {
                "similar_reference_occurrences": sum(self.similar_ref_counts.values()),
                "distinct_similar_referenced_asins": len(self.similar_ref_counts),
                "orphan_similar_reference_occurrences": sum(orphan_refs.values()),
                "distinct_orphan_similar_asins": len(orphan_refs),
                "orphan_reference_examples": orphan_examples,
                "category_path_occurrences": sum(self.category_paths.values()),
                "distinct_category_paths": len(self.category_paths),
                "distinct_category_node_ids": len(self.category_node_ids),
                "distinct_category_labels": len(self.category_labels),
                "category_labels_containing_brackets": len(self.category_labels_with_brackets),
                "category_label_with_brackets_occurrences": sum(self.category_labels_with_brackets.values()),
                "top_category_labels_containing_brackets": top_counter(self.category_labels_with_brackets, 10),
                "category_depth_summary": summarize_counter(self.category_depths),
                "top_category_roots": top_counter(self.category_roots, 30),
                "top_category_paths": top_counter(self.category_paths, 20),
            },
            "cross_checks": {
                "header_count_matches": self.header_total_items == self.records,
                "all_records_classified": self.records == self.valid_records + self.incomplete_records + self.malformed_records,
                "id_cardinality_matches_records": len(self.ids) == self.records,
                "asin_cardinality_matches_records": len(self.asins) == self.records,
                "record_blocks_unique": len(self.record_hashes) == self.records,
                "bytes_read_match_stat": source_path is None or self.size_bytes == source_path.stat().st_size,
            },
            "anomalies": anomalies,
            "extremes": extremes,
        }


def analyze_path(source: Path) -> dict[str, object]:
    analyzer = Analyzer(str(source))
    with source.open("rb") as handle:
        analyzer.scan(handle)
    return analyzer.finalize(source)


def run_self_test() -> None:
    fixture = (
        b"# Full information about Amazon Share the Love products\r\n"
        b"Total items: 3\r\n\r\n"
        b"Id:   0\r\nASIN: 000000000X\r\n  discontinued product\r\n\r\n"
        b"Id:   1\r\nASIN: B000000001\r\n  title: Example: With Colon\r\n  group: Book\r\n"
        b"  salesrank: 0\r\n  similar: 0\r\n  categories: 1\r\n   |[1]|Books[2]\r\n"
        b"  reviews: total: 1  downloaded: 1  avg rating: 5\r\n"
        b"    2005-1-2  cutomer: A1  rating: 5  votes:   1  helpful:   1\r\n\r\n"
        b"Id:   2\r\nASIN: B000000002\r\n  title: Broken\r\n  group: Book\r\n"
        b"  salesrank: 1\r\n  similar: 0\r\n  categories: 0\r\n"
        b"  reviews: total: 0  downloaded: 0  avg rating: 0\r\n  unknown: x\r\n\r\n"
    )
    analyzer = Analyzer("self-test")
    analyzer.scan(io.BytesIO(fixture))
    result = analyzer.finalize()
    assert result["records"]["total"] == 3
    assert result["records"]["valid"] == 2
    assert result["records"]["malformed"] == 1
    assert result["records"]["discontinued"] == 1
    assert result["relationships"]["distinct_category_paths"] == 1
    assert result["reviews"]["parsed_review_lines"] == 1
    assert result["source"]["crlf_terminated_lines"] == result["source"]["total_lines"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", nargs="?", type=Path, help="Amazon metadata text file")
    parser.add_argument("--output", type=Path, help="deterministic JSON output path")
    parser.add_argument("--self-test", action="store_true", help="run the embedded parser fixture")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.self_test:
        run_self_test()
        print("self-test: ok")
        return 0
    if args.source is None or args.output is None:
        raise SystemExit("source and --output are required unless --self-test is used")
    result = analyze_path(args.source)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({
        "source": str(args.source),
        "output": str(args.output),
        "sha256": result["source"]["sha256"],
        "lines": result["source"]["total_lines"],
        "records": result["records"]["total"],
        "valid_records": result["records"]["valid"],
        "anomaly_types": len(result["anomalies"]),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
