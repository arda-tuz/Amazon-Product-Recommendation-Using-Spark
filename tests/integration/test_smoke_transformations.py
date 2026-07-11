from __future__ import annotations

from datetime import date

import pytest
from pyspark.sql import functions as F

from amazon_recommender.ingestion.parser import parse_block
from amazon_recommender.ingestion.schemas import (
    BRONZE_PRODUCT_SCHEMA,
    HEADER_SCHEMA,
    QUARANTINE_SCHEMA,
)
from amazon_recommender.pipelines.bronze import (
    BronzeFrames,
    SELECTION_SCHEMA,
    read_bronze_envelope,
    write_bronze_envelope,
)
from amazon_recommender.ingestion.delimiter import CRLF_DELIMITER
from amazon_recommender.pipelines.gold import build_gold
from amazon_recommender.pipelines.silver import build_silver
from amazon_recommender.quality.profile import (
    build_product_quality_profile,
    build_quality_events,
    build_quality_summary,
    build_review_duplicate_groups,
)
from amazon_recommender.features.split import (
    assign_temporal_split,
    build_active_catalog,
    build_cohorts,
    build_stage_seen_items,
    iterative_als_k_core,
    stable_evaluation_users,
)


def _product(product_id: int, day: int, duplicate: bool = False) -> dict:
    reviews = (
        f"    2004-1-{day}  cutomer: AUSER  rating: 5  votes: 1  helpful: 1"
    )
    lines = [
        f"Id:   {product_id}",
        f"ASIN: B{product_id:09d}",
        f"  title: Product {product_id}",
        "  group: Book",
        f"  salesrank: {product_id + 1}",
        "  similar: 0",
        "  categories: 1",
        "   |Books[1]|Testing[2]",
        f"  reviews: total: {2 if duplicate else 1}  downloaded: {2 if duplicate else 1}  avg rating: 5",
        reviews,
    ]
    if duplicate:
        lines.append(reviews)
    outcome = parse_block(
        "\r\n".join(lines),
        source_path="fixture",
        source_offset=product_id * 100,
        record_ordinal=product_id,
    )
    assert outcome.kind == "product"
    return outcome.row


def _bronze(spark) -> BronzeFrames:
    products = spark.createDataFrame(
        [_product(0, 1, duplicate=True)]
        + [_product(product_id, product_id + 1) for product_id in range(1, 5)],
        BRONZE_PRODUCT_SCHEMA,
    )
    return BronzeFrames(
        products=products,
        quarantine=spark.createDataFrame([], QUARANTINE_SCHEMA),
        header=spark.createDataFrame([], HEADER_SCHEMA),
        smoke_selection=spark.createDataFrame(
            [(product_id, "fixture") for product_id in range(5)], SELECTION_SCHEMA
        ),
    )


@pytest.mark.integration
def test_cleaning_order_deduplicates_before_user_item_aggregation(spark) -> None:
    silver = build_silver(_bronze(spark))
    assert silver.reviews_raw.count() == 6
    assert silver.reviews_deduplicated.count() == 5
    interaction = silver.user_item_interactions.filter("product_id = 0").first()
    assert interaction.review_count == 1
    assert interaction.rating == 5.0
    assert interaction.q_ui == 1.0
    customer = silver.customers.first()
    assert customer.customer_int_id == 0
    assert customer.distinct_items == 5
    edge = silver.category_edges.first()
    assert (edge.parent_category_id, edge.child_category_id) == (1, 2)
    assert edge.path_occurrences == 5


@pytest.mark.integration
def test_invalid_date_or_rating_never_enters_model_interactions(spark) -> None:
    valid = _product(10, 1)
    invalid_date = _product(11, 2)
    invalid_date["reviews"][0]["review_date"] = None
    invalid_date["reviews"][0]["quality_codes"] = ["invalid_date"]
    invalid_rating = _product(12, 3)
    invalid_rating["reviews"][0]["rating"] = 6
    invalid_rating["reviews"][0]["quality_codes"] = ["invalid_rating"]
    bronze = BronzeFrames(
        products=spark.createDataFrame(
            [valid, invalid_date, invalid_rating], BRONZE_PRODUCT_SCHEMA
        ),
        quarantine=spark.createDataFrame([], QUARANTINE_SCHEMA),
        header=spark.createDataFrame([], HEADER_SCHEMA),
        smoke_selection=spark.createDataFrame([], SELECTION_SCHEMA),
    )

    silver = build_silver(bronze)

    assert silver.reviews_raw.count() == 3
    assert silver.reviews_deduplicated.count() == 3
    assert [row.product_id for row in silver.user_item_interactions.collect()] == [10]


@pytest.mark.integration
def test_quality_views_reconcile_duplicate_occurrences_and_zero_events(spark) -> None:
    bronze = _bronze(spark)
    silver = build_silver(bronze)
    profile = build_product_quality_profile(silver.products, silver.reviews_raw)
    groups = build_review_duplicate_groups(silver.reviews_raw)
    events = build_quality_events(
        bronze.products,
        bronze.quarantine,
        profile,
        silver.reviews_raw,
        silver.similar_edges,
        silver.category_nodes,
    )
    summary = {
        row.event_type: row.event_count for row in build_quality_summary(events).collect()
    }

    assert profile.filter("avg_rating_mismatch").count() == 0
    assert groups.agg({"duplicate_extra_count": "sum"}).first()[0] == 1
    assert summary["DUPLICATE_REVIEW_OCCURRENCE"] == 1
    assert summary["PARSE_ERROR"] == 0
    assert summary["ORPHAN_GRAPH_TARGET"] == 0


@pytest.mark.integration
def test_g6_split_kcore_cohort_and_hash_contracts(spark) -> None:
    bronze = _bronze(spark)
    silver = build_silver(bronze)
    assigned = assign_temporal_split(silver.user_item_interactions)
    train = assigned.filter("split = 'train'").drop(
        "split",
        "evaluation_eligible",
        "dated_distinct_items",
        "temporal_position",
        "customer_interaction_count",
    )
    validation = assigned.filter("split = 'validation'").drop(
        "split",
        "evaluation_eligible",
        "dated_distinct_items",
        "temporal_position",
        "customer_interaction_count",
    )
    test = assigned.filter("split = 'test'").drop(
        "split",
        "evaluation_eligible",
        "dated_distinct_items",
        "temporal_position",
        "customer_interaction_count",
    )
    assert [row.product_id for row in train.orderBy("product_id").collect()] == [0, 1, 2]
    assert validation.first().product_id == 3
    assert test.first().product_id == 4

    train_with_ids = train.withColumn("customer_int_id", F.lit(0).cast("int"))
    kcore = iterative_als_k_core(
        train_with_ids, min_user_items=3, min_item_users=1
    )
    assert kcore.iterations[-1]["converged"] is True
    assert kcore.interactions.count() == 3
    kcore.interactions.unpersist()

    active = build_active_catalog(silver.products)
    seen = build_stage_seen_items(train, validation)
    als_users = spark.createDataFrame([("AUSER", 0)], "customer_id string, customer_int_id int")
    als_items = active.select("product_id")
    cohorts, flags = build_cohorts(
        train, validation, test, active, seen, als_users, als_items
    )
    assert flags.filter("target_already_seen").count() == 0
    assert cohorts.groupBy("cohort").count().count() == 2
    sampled = stable_evaluation_users(cohorts, seed=42, limit=20_000)
    assert sampled.count() == 4
    assert sampled.select("stable_hash").distinct().count() == 1


@pytest.mark.integration
def test_temporal_split_and_seen_contract_are_leakage_safe(spark) -> None:
    bronze = _bronze(spark)
    silver = build_silver(bronze)
    gold = build_gold(bronze, silver)
    assert [row.product_id for row in gold.train_interactions.orderBy("product_id").collect()] == [0, 1, 2]
    assert gold.validation_interactions.first().product_id == 3
    assert gold.test_interactions.first().product_id == 4
    validation_seen = {
        row.product_id
        for row in gold.stage_seen_items.filter("stage = 'validation'").collect()
    }
    test_seen = {
        row.product_id for row in gold.stage_seen_items.filter("stage = 'test'").collect()
    }
    assert validation_seen == {0, 1, 2}
    assert test_seen == {0, 1, 2, 3}
    assert 3 not in validation_seen
    assert 4 not in test_seen
    assert gold.validation_interactions.first().interaction_date == date(2004, 1, 4)


@pytest.mark.integration
def test_streamed_ingestion_envelope_round_trip(spark, tmp_path) -> None:
    product = (
        "Id:   9\r\nASIN: B000000009\r\n  title: Nine\r\n  group: Book\r\n"
        "  salesrank: 9\r\n  similar: 0\r\n  categories: 0\r\n"
        "  reviews: total: 0  downloaded: 0  avg rating: 0"
    )
    source = tmp_path / "source.txt"
    source.write_bytes(
        b"# Full information about Amazon Share the Love products\r\nTotal items: 1"
        + CRLF_DELIMITER
        + product.encode()
        + CRLF_DELIMITER
    )
    destination = tmp_path / "envelope"
    write_bronze_envelope(
        spark, source, CRLF_DELIMITER, destination, split_max_bytes=64
    )
    bronze = read_bronze_envelope(spark, destination)
    assert bronze.header.first().declared_items == 1
    assert bronze.products.first().product_id == 9
    assert bronze.quarantine.count() == 0
