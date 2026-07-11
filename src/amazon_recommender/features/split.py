"""Binding temporal split, ALS k-core, cohorts, and train-only features."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import shutil
import tempfile
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


HASH_SEPARATOR = "\x1f"


@dataclass(frozen=True)
class KCoreResult:
    interactions: DataFrame
    iterations: list[dict[str, int | bool]]


def assign_temporal_split(
    interactions: DataFrame, *, evaluation_min_distinct_items: int = 5
) -> DataFrame:
    """Assign exactly one train/validation/test label after aggregation."""

    eligible = (
        interactions.filter(F.col("interaction_date").isNotNull())
        .groupBy("customer_id")
        .agg(F.countDistinct("product_id").alias("dated_distinct_items"))
        .filter(F.col("dated_distinct_items") >= F.lit(evaluation_min_distinct_items))
        .select("customer_id", "dated_distinct_items")
        .withColumn("evaluation_eligible", F.lit(True))
    )
    ordering = Window.partitionBy("customer_id").orderBy(
        F.col("interaction_date").asc(), F.col("product_id").asc()
    )
    population = Window.partitionBy("customer_id")
    marked = (
        interactions.join(eligible, "customer_id", "left")
        .withColumn("temporal_position", F.row_number().over(ordering))
        .withColumn("customer_interaction_count", F.count(F.lit(1)).over(population))
        .withColumn(
            "evaluation_eligible",
            F.coalesce(F.col("evaluation_eligible"), F.lit(False)),
        )
    )
    return marked.withColumn(
        "split",
        F.when(
            F.col("evaluation_eligible")
            & (F.col("temporal_position") == F.col("customer_interaction_count")),
            F.lit("test"),
        )
        .when(
            F.col("evaluation_eligible")
            & (
                F.col("temporal_position")
                == F.col("customer_interaction_count") - F.lit(1)
            ),
            F.lit("validation"),
        )
        .otherwise(F.lit("train")),
    )


def _counts(frame: DataFrame) -> dict[str, int]:
    row = frame.agg(
        F.count(F.lit(1)).alias("interactions"),
        F.countDistinct("customer_id").alias("users"),
        F.countDistinct("product_id").alias("items"),
    ).first()
    return {name: int(row[name]) for name in ("interactions", "users", "items")}


def iterative_als_k_core(
    train: DataFrame,
    *,
    min_user_items: int = 3,
    min_item_users: int = 5,
    max_iterations: int = 100,
    checkpoint_root: Path | None = None,
) -> KCoreResult:
    """Repeatedly filter degrees, cutting lineage with durable Parquet each round."""

    current = train
    before = _counts(current)
    records: list[dict[str, int | bool]] = []
    owned_checkpoint = checkpoint_root is None
    root = (
        Path(tempfile.mkdtemp(prefix="amazon-kcore-"))
        if checkpoint_root is None
        else checkpoint_root
    )
    shutil.rmtree(root, ignore_errors=True)
    root.mkdir(parents=True)
    previous_path: Path | None = None
    for iteration in range(1, max_iterations + 1):
        eligible_users = (
            current.groupBy("customer_id")
            .agg(F.count(F.lit(1)).alias("item_count"))
            .filter(F.col("item_count") >= F.lit(min_user_items))
            .select("customer_id")
            .persist(StorageLevel.DISK_ONLY)
        )
        after_user_users = eligible_users.count()
        after_user = current.join(eligible_users, "customer_id", "inner").persist(
            StorageLevel.DISK_ONLY
        )
        item_degrees = (
            after_user.groupBy("product_id")
            .agg(F.count(F.lit(1)).alias("user_count"))
            .persist(StorageLevel.DISK_ONLY)
        )
        after_user_summary = item_degrees.agg(
            F.sum("user_count").alias("interactions"),
            F.count(F.lit(1)).alias("items"),
        ).first()
        after_user_counts = {
            "interactions": int(after_user_summary["interactions"] or 0),
            "users": int(after_user_users),
            "items": int(after_user_summary["items"]),
        }
        eligible_items = (
            item_degrees
            .filter(F.col("user_count") >= F.lit(min_item_users))
            .select("product_id")
            .persist(StorageLevel.DISK_ONLY)
        )
        after_item_items = eligible_items.count()
        next_plan = after_user.join(eligible_items, "product_id", "inner")
        iteration_path = root / f"iteration-{iteration:03d}"
        next_plan.write.mode("error").option("compression", "snappy").parquet(
            str(iteration_path)
        )
        next_frame = train.sparkSession.read.parquet(str(iteration_path))
        after_item_interactions = next_frame.count()
        after_item_users = next_frame.select("customer_id").distinct().count()
        after_item_counts = {
            "interactions": int(after_item_interactions),
            "users": int(after_item_users),
            "items": int(after_item_items),
        }
        eligible_users.unpersist(blocking=True)
        after_user.unpersist(blocking=True)
        item_degrees.unpersist(blocking=True)
        eligible_items.unpersist(blocking=True)
        train.sparkSession.catalog.clearCache()
        train.sparkSession.sparkContext._jvm.java.lang.System.gc()
        converged = after_item_counts["interactions"] == before["interactions"]
        records.append(
            {
                "iteration": iteration,
                "before_interactions": before["interactions"],
                "before_users": before["users"],
                "before_items": before["items"],
                "after_user_interactions": after_user_counts["interactions"],
                "after_user_users": after_user_counts["users"],
                "after_user_items": after_user_counts["items"],
                "after_item_interactions": after_item_counts["interactions"],
                "after_item_users": after_item_counts["users"],
                "after_item_items": after_item_counts["items"],
                "converged": converged,
            }
        )
        if previous_path is not None:
            shutil.rmtree(previous_path, ignore_errors=True)
        if converged:
            if owned_checkpoint:
                next_frame.persist(StorageLevel.DISK_ONLY)
                next_frame.count()
                shutil.rmtree(root, ignore_errors=True)
            return KCoreResult(next_frame, records)
        current = next_frame
        previous_path = iteration_path
        before = after_item_counts
    if owned_checkpoint:
        shutil.rmtree(root, ignore_errors=True)
    raise RuntimeError(f"ALS k-core did not converge in {max_iterations} iterations")


def build_active_catalog(products: DataFrame) -> DataFrame:
    return products.filter(
        F.col("is_active") & F.col("title").isNotNull() & F.col("group").isNotNull()
    ).select(
        "product_id",
        "asin",
        "title",
        "group",
        "salesrank_clean",
        "avg_rating_raw",
        "reviews_downloaded",
    )


def build_positive_baskets(train: DataFrame, max_basket_size: int = 50) -> DataFrame:
    recency = Window.partitionBy("customer_id").orderBy(
        F.col("interaction_date").desc(), F.col("product_id").asc()
    )
    return (
        train.filter(F.col("is_positive"))
        .withColumn("recent_rank", F.row_number().over(recency))
        .filter(F.col("recent_rank") <= F.lit(max_basket_size))
        .groupBy("customer_id")
        .agg(
            F.sort_array(F.collect_set("product_id")).alias("items"),
            F.countDistinct("product_id").cast("long").alias("basket_size"),
            F.max("interaction_date").alias("basket_as_of"),
        )
        .filter(F.col("basket_size") >= F.lit(2))
    )


def build_user_profiles(train: DataFrame) -> DataFrame:
    positive = train.filter(F.col("is_positive"))
    history = F.sort_array(
        F.collect_list(
            F.struct(
                (-F.datediff(F.col("interaction_date"), F.lit("1970-01-01"))).alias(
                    "sort_day"
                ),
                F.col("product_id"),
                F.col("q_ui"),
                F.col("interaction_date"),
                F.col("rating"),
            )
        )
    )
    return (
        positive.groupBy("customer_id")
        .agg(
            history.alias("_ordered_history"),
            F.countDistinct("product_id").cast("long").alias(
                "positive_item_count"
            ),
            F.max("interaction_date").alias("profile_as_of"),
        )
        .withColumn(
            "positive_history",
            F.transform(
                "_ordered_history",
                lambda item: F.struct(
                    item["product_id"].alias("product_id"),
                    item["q_ui"].alias("q_ui"),
                    item["interaction_date"].alias("interaction_date"),
                    item["rating"].alias("rating"),
                ),
            ),
        )
        .drop("_ordered_history")
    )


def build_stage_seen_items(
    train: DataFrame, validation: DataFrame
) -> DataFrame:
    validation_seen = train.select("customer_id", "product_id").withColumn(
        "stage", F.lit("validation")
    )
    test_seen = (
        train.select("customer_id", "product_id")
        .unionByName(validation.select("customer_id", "product_id"))
        .withColumn("stage", F.lit("test"))
    )
    return validation_seen.unionByName(test_seen).dropDuplicates(
        ["stage", "customer_id", "product_id"]
    )


def build_cohorts(
    train: DataFrame,
    validation: DataFrame,
    test: DataFrame,
    active_catalog: DataFrame,
    stage_seen_items: DataFrame,
    als_users: DataFrame,
    als_items: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    targets = validation.select(
        "customer_id", "product_id", "rating", "is_positive"
    ).withColumn("stage", F.lit("validation")).unionByName(
        test.select("customer_id", "product_id", "rating", "is_positive").withColumn(
            "stage", F.lit("test")
        )
    )
    positive_train_users = train.filter(F.col("is_positive")).select(
        "customer_id"
    ).distinct().withColumn("has_positive_train", F.lit(True))
    active = active_catalog.select("product_id").withColumn(
        "is_recommendable", F.lit(True)
    )
    seen_targets = stage_seen_items.select(
        "stage", "customer_id", F.col("product_id").alias("seen_target_product_id")
    ).withColumn("target_already_seen", F.lit(True))
    flags = (
        targets.join(active, "product_id", "left")
        .join(positive_train_users, "customer_id", "left")
        .join(
            als_users.select("customer_id").withColumn("in_als_user_universe", F.lit(True)),
            "customer_id",
            "left",
        )
        .join(
            als_items.select("product_id").withColumn("in_als_item_universe", F.lit(True)),
            "product_id",
            "left",
        )
        .join(
            seen_targets,
            (targets.stage == seen_targets.stage)
            & (targets.customer_id == seen_targets.customer_id)
            & (targets.product_id == seen_targets.seen_target_product_id),
            "left",
        )
        .drop(seen_targets.stage)
        .drop(seen_targets.customer_id)
        .withColumn(
            "exclusion_reason",
            F.when(~F.col("is_positive"), F.lit("NON_POSITIVE_TARGET"))
            .when(
                ~F.coalesce(F.col("is_recommendable"), F.lit(False)),
                F.lit("TARGET_NOT_RECOMMENDABLE"),
            )
            .when(
                F.coalesce(F.col("target_already_seen"), F.lit(False)),
                F.lit("TARGET_ALREADY_SEEN"),
            )
            .otherwise(F.lit("ELIGIBLE_OPERATIONAL")),
        )
    )
    operational = (
        flags.filter(F.col("exclusion_reason") == F.lit("ELIGIBLE_OPERATIONAL"))
        .select(
            "stage",
            "customer_id",
            F.col("product_id").alias("target_product_id"),
            "rating",
        )
        .withColumn("cohort", F.lit("operational"))
    )
    common_warm = (
        flags.filter(
            (F.col("exclusion_reason") == F.lit("ELIGIBLE_OPERATIONAL"))
            & F.coalesce(F.col("has_positive_train"), F.lit(False))
            & F.coalesce(F.col("in_als_user_universe"), F.lit(False))
            & F.coalesce(F.col("in_als_item_universe"), F.lit(False))
        )
        .select(
            "stage",
            "customer_id",
            F.col("product_id").alias("target_product_id"),
            "rating",
        )
        .withColumn("cohort", F.lit("common_warm"))
    )
    cohorts = operational.unionByName(common_warm).select(
        "stage", "cohort", "customer_id", "target_product_id", "rating"
    )
    return cohorts, flags


def stable_evaluation_users(
    cohort_candidates: DataFrame, *, seed: int = 42, limit: int = 20_000
) -> DataFrame:
    hashed = cohort_candidates.withColumn(
        "stable_hash",
        F.sha2(
            F.concat(
                F.col("customer_id"),
                F.lit(HASH_SEPARATOR),
                F.lit(str(seed)),
            ),
            256,
        ),
    )
    selection = Window.partitionBy("stage", "cohort").orderBy(
        F.col("stable_hash").asc(), F.col("customer_id").asc()
    )
    return (
        hashed.withColumn("sample_rank", F.row_number().over(selection))
        .filter(F.col("sample_rank") <= F.lit(limit))
        .orderBy("stage", "cohort", "sample_rank")
    )


def iteration_frame(spark: Any, iterations: list[dict[str, int | bool]]) -> DataFrame:
    return spark.createDataFrame(iterations)
