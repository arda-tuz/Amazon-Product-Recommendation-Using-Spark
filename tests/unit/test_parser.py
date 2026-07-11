from __future__ import annotations

from decimal import Decimal

import pytest

from amazon_recommender.ingestion.parser import parse_block
from amazon_recommender.ingestion.schemas import BRONZE_PRODUCT_SCHEMA, QUARANTINE_SCHEMA


ACTIVE = (
    "Id:   12\r\n"
    "ASIN: B000000012\r\n"
    "  title: A title: with colon\r\n"
    "  group: Book\r\n"
    "  salesrank: 0\r\n"
    "  similar: 2  B000000013  B000000014\r\n"
    "  categories: 2\r\n"
    "   |[1]|Books[2]|[guitar] manuals[3]\r\n"
    "   |Music[4]|Styles[5]\r\n"
    "  reviews: total: 3  downloaded: 2  avg rating: 4.5\r\n"
    "    2004-1-2  cutomer: AUSER1  rating: 5  votes:   2  helpful:   2\r\n"
    "    2004-2-3  cutomer: AUSER2  rating: 4  votes:   0  helpful:   0"
)


@pytest.mark.unit
def test_parser_reads_active_record_without_splitting_title_or_labels() -> None:
    outcome = parse_block(
        ACTIVE.encode(), source_path="fixture", source_offset=10, record_ordinal=3
    )
    assert outcome.kind == "product"
    row = outcome.row
    assert row["asin"] == "B000000012"
    assert row["title"] == "A title: with colon"
    assert row["similars"][1] == {"target_asin": "B000000014", "position": 2}
    assert row["category_paths"][0]["segments"][0] == {
        "depth": 1,
        "label": "",
        "category_id": 1,
    }
    assert row["category_paths"][0]["segments"][2]["label"] == "[guitar] manuals"
    assert row["avg_rating_raw"] == Decimal("4.5")
    assert len(row["reviews"]) == 2
    assert row["quality_codes"] == [
        "nonpositive_salesrank",
        "reviews_total_greater_than_downloaded",
    ]


@pytest.mark.unit
def test_parser_reads_discontinued_record() -> None:
    outcome = parse_block(
        b"Id:   0\r\nASIN: 0771044445\r\n  discontinued product",
        source_path="fixture",
        source_offset=80,
    )
    assert outcome.kind == "product"
    assert outcome.row["status"] == "discontinued"
    assert outcome.row["reviews"] == []
    assert outcome.row["title"] is None


@pytest.mark.unit
def test_parser_preserves_observed_multiline_title_without_losing_text() -> None:
    wrapped = ACTIVE.replace(
        "  title: A title: with colon\r\n  group: Book",
        "  title: Good and Angry: Exchanging Frustration for Character\r\n"
        "in You and Your Kids!\r\n  group: Book",
    )
    outcome = parse_block(wrapped, source_path="fixture", source_offset=0)
    assert outcome.kind == "product"
    assert outcome.row["title"] == (
        "Good and Angry: Exchanging Frustration for Character\n"
        "in You and Your Kids!"
    )
    assert "multiline_title" in outcome.row["quality_codes"]


@pytest.mark.unit
def test_parser_reads_exact_header_only() -> None:
    outcome = parse_block(
        b"# Full information about Amazon Share the Love products\r\nTotal items: 548552",
        source_path="fixture",
        source_offset=0,
    )
    assert outcome.kind == "header"
    assert outcome.row["declared_items"] == 548_552


@pytest.mark.unit
@pytest.mark.parametrize(
    "replacement,error_code",
    [
        ("  similar: 1  B000000013  B000000014", "similar_count_mismatch"),
        ("  categories: 3", "invalid_category_path"),
        ("  reviews: total: 3  downloaded: 3  avg rating: 4.5", "downloaded_review_count_mismatch"),
        ("  unknown: value", "missing_or_reordered_salesrank"),
    ],
)
def test_parser_quarantines_count_and_order_errors(
    replacement: str, error_code: str
) -> None:
    if replacement.startswith("  similar"):
        broken = ACTIVE.replace("  similar: 2  B000000013  B000000014", replacement)
    elif replacement.startswith("  categories"):
        broken = ACTIVE.replace("  categories: 2", replacement)
    elif replacement.startswith("  reviews"):
        broken = ACTIVE.replace(
            "  reviews: total: 3  downloaded: 2  avg rating: 4.5", replacement
        )
    else:
        broken = ACTIVE.replace("  salesrank: 0", replacement)
    outcome = parse_block(
        broken.encode(), source_path="fixture", source_offset=20, record_ordinal=1
    )
    assert outcome.kind == "quarantine"
    assert outcome.row["error_code"] == error_code
    assert outcome.row["raw_block"] == broken


@pytest.mark.unit
def test_parser_keeps_structurally_valid_invalid_domain_values_as_quality_events() -> None:
    broken = ACTIVE.replace("2004-1-2", "2004-2-31").replace(
        "rating: 5  votes:   2  helpful:   2",
        "rating: 8  votes:  -1  helpful:   2",
    )
    outcome = parse_block(broken, source_path="fixture", source_offset=0)
    assert outcome.kind == "product"
    review = outcome.row["reviews"][0]
    assert review["review_date"] is None
    assert set(review["quality_codes"]) == {
        "invalid_review_date",
        "invalid_rating",
        "negative_votes",
        "helpful_exceeds_votes",
    }


@pytest.mark.unit
def test_customer_alias_is_quarantined_unless_explicitly_enabled() -> None:
    alias = ACTIVE.replace("cutomer:", "customer:", 1)
    strict = parse_block(alias, source_path="fixture", source_offset=0)
    compatible = parse_block(
        alias, source_path="fixture", source_offset=0, allow_customer_alias=True
    )
    assert strict.kind == "quarantine"
    assert strict.row["error_code"] == "invalid_review_line"
    assert compatible.kind == "product"
    assert "customer_alias_used" in compatible.row["reviews"][0]["quality_codes"]


@pytest.mark.unit
def test_parser_emits_rows_compatible_with_explicit_schemas(spark) -> None:
    product = parse_block(ACTIVE, source_path="fixture", source_offset=0).row
    spark.createDataFrame([product], BRONZE_PRODUCT_SCHEMA).collect()
    quarantine = parse_block(
        "not a product", source_path="fixture", source_offset=0
    ).row
    spark.createDataFrame([quarantine], QUARANTINE_SCHEMA).collect()
