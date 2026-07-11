"""Bounded DuckDB access to completed run-scoped Parquet artifacts.

Every public detail query has a hard row cap.  User input is passed as DuckDB
parameters; only trusted table expressions and validated column names are interpolated.
The module does not import PySpark and cannot create a Spark session.
"""

from __future__ import annotations

import json
import math
import re
import statistics
import threading
from pathlib import Path
from typing import Final, Iterable, Mapping

import duckdb
import pandas as pd

from app.lib.catalog import RunContext


MAX_DETAIL_ROWS: Final[int] = 500
MAX_RECOMMENDATIONS: Final[int] = 100
MODEL_TABLES: Final[Mapping[str, str]] = {
    "popularity": "popularity_recommendations",
    "als": "als_recommendations",
    "fp": "fp_recommendations",
    "graph": "graph_recommendations",
    "category": "category_recommendations",
    "h_a": "h_a_recommendations",
    "h_b": "h_b_recommendations",
}
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
PERFORMANCE_COLUMNS: Final[tuple[str, ...]] = (
    "condition",
    "master",
    "worker_threads",
    "warmup_seconds",
    "measured_1_seconds",
    "measured_2_seconds",
    "measured_3_seconds",
    "median_seconds",
    "speedup_vs_single_core",
    "output_rows",
)
PERFORMANCE_CONDITIONS: Final[tuple[str, ...]] = (
    "single_core",
    "bounded_multi_core",
)


def _bounded(value: int, *, lower: int = 1, upper: int = MAX_DETAIL_ROWS) -> int:
    return max(lower, min(int(value), upper))


def _leaf_label(raw_path: object) -> str | None:
    if not isinstance(raw_path, str) or not raw_path:
        return None
    segment = raw_path.rsplit("|", 1)[-1]
    # Category labels may themselves contain brackets; strip only the terminal ID.
    return re.sub(r"\[\d+\]\s*$", "", segment).strip() or None


def _positive_number(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{label} must be numeric")
    result = float(value)
    if not math.isfinite(result) or result <= 0.0:
        raise ValueError(f"{label} must be finite and positive")
    return result


def _positive_integer(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{label} must be a positive integer")
    return value


def _same_number(left: float, right: float) -> bool:
    return math.isclose(left, right, rel_tol=1e-9, abs_tol=1e-9)


def _validated_performance_rows(
    summary: object, success: object
) -> list[dict[str, object]]:
    """Validate G11's immutable 2 x (1 warm-up + 3 measured) JSON contract."""

    if not isinstance(summary, dict) or not isinstance(success, dict):
        raise ValueError("G11 performance artifacts must be JSON objects")
    if success.get("gate") != "G11":
        raise ValueError("G11 success marker has the wrong gate")
    if success.get("trial_count") != 8:
        raise ValueError("G11 success marker must attest exactly eight trials")

    protocol = summary.get("protocol")
    conditions = summary.get("conditions")
    if not isinstance(protocol, dict) or not isinstance(conditions, dict):
        raise ValueError("G11 performance summary is incomplete")
    if protocol.get("warmups_per_condition") != 1:
        raise ValueError("G11 requires one warm-up per condition")
    if protocol.get("measured_runs_per_condition") != 3:
        raise ValueError("G11 requires three measured runs per condition")
    if set(conditions) != set(PERFORMANCE_CONDITIONS):
        raise ValueError("G11 must contain exactly the two binding conditions")

    output_rows = _positive_integer(summary.get("output_rows"), "output_rows")
    if success.get("output_rows") != output_rows:
        raise ValueError("G11 success marker output rows do not reconcile")
    summary_schema = summary.get("output_schema_sha256")
    if (
        not isinstance(summary_schema, str)
        or not re.fullmatch(r"[0-9a-f]{64}", summary_schema)
        or success.get("output_schema_sha256") != summary_schema
    ):
        raise ValueError("G11 output schema evidence does not reconcile")

    parsed: dict[str, dict[str, object]] = {}
    for name in PERFORMANCE_CONDITIONS:
        condition = conditions[name]
        if not isinstance(condition, dict):
            raise ValueError(f"{name} condition must be an object")
        master = condition.get("master")
        worker_threads = _positive_integer(
            condition.get("worker_threads"), f"{name}.worker_threads"
        )
        if name == "single_core":
            if master != "local[1]" or worker_threads != 1:
                raise ValueError("single_core must use local[1]")
        elif (
            not isinstance(master, str)
            or master != f"local[{worker_threads}]"
            or worker_threads > 4
        ):
            raise ValueError("bounded_multi_core master/thread contract changed")

        warmup = _positive_number(
            condition.get("warmup_wall_seconds"), f"{name}.warmup_wall_seconds"
        )
        measured_raw = condition.get("measured_wall_seconds")
        if not isinstance(measured_raw, list) or len(measured_raw) != 3:
            raise ValueError(f"{name} must contain exactly three measured times")
        measured = [
            _positive_number(value, f"{name}.measured_wall_seconds")
            for value in measured_raw
        ]
        median = _positive_number(
            condition.get("median_wall_seconds"), f"{name}.median_wall_seconds"
        )
        if not _same_number(median, statistics.median(measured)):
            raise ValueError(f"{name} median does not match measured times")

        trials = condition.get("trials")
        if not isinstance(trials, list) or len(trials) != 4:
            raise ValueError(f"{name} must preserve exactly four raw trials")
        warmup_trials: list[tuple[int, float]] = []
        measured_trials: list[tuple[int, float]] = []
        for trial in trials:
            if not isinstance(trial, dict):
                raise ValueError(f"{name} trial must be an object")
            spec = trial.get("spec")
            workload = trial.get("workload")
            if not isinstance(spec, dict) or not isinstance(workload, dict):
                raise ValueError(f"{name} trial evidence is incomplete")
            trial_condition = spec.get("condition")
            if not isinstance(trial_condition, dict) or trial_condition != {
                "name": name,
                "master": master,
                "worker_threads": worker_threads,
            }:
                raise ValueError(f"{name} trial condition does not reconcile")
            ordinal = spec.get("ordinal")
            is_warmup = spec.get("is_warmup")
            if isinstance(ordinal, bool) or not isinstance(ordinal, int):
                raise ValueError(f"{name} trial ordinal must be an integer")
            if type(is_warmup) is not bool:
                raise ValueError(f"{name} trial warm-up flag must be boolean")
            wall_seconds = _positive_number(
                workload.get("wall_seconds"), f"{name}.trial.wall_seconds"
            )
            if workload.get("output_rows") != output_rows:
                raise ValueError(f"{name} trial output rows do not reconcile")
            destination = warmup_trials if is_warmup else measured_trials
            destination.append((ordinal, wall_seconds))

        if warmup_trials != [(0, warmup)]:
            raise ValueError(f"{name} warm-up trial does not reconcile")
        measured_trials.sort()
        if [ordinal for ordinal, _ in measured_trials] != [1, 2, 3] or any(
            not _same_number(trial_seconds, summary_seconds)
            for (_, trial_seconds), summary_seconds in zip(measured_trials, measured)
        ):
            raise ValueError(f"{name} measured trials do not reconcile")

        parsed[name] = {
            "condition": name,
            "master": master,
            "worker_threads": worker_threads,
            "warmup_seconds": warmup,
            "measured_1_seconds": measured[0],
            "measured_2_seconds": measured[1],
            "measured_3_seconds": measured[2],
            "median_seconds": median,
            "output_rows": output_rows,
        }

    expected_speedup = float(parsed["single_core"]["median_seconds"]) / float(
        parsed["bounded_multi_core"]["median_seconds"]
    )
    speedup = _positive_number(
        summary.get("local_parallel_speedup"), "local_parallel_speedup"
    )
    if not _same_number(speedup, expected_speedup):
        raise ValueError("G11 speedup does not match condition medians")
    parsed["single_core"]["speedup_vs_single_core"] = 1.0
    parsed["bounded_multi_core"]["speedup_vs_single_core"] = speedup
    return [parsed[name] for name in PERFORMANCE_CONDITIONS]


class DashboardStore:
    """Read-only facade over one immutable artifact run."""

    def __init__(self, context: RunContext) -> None:
        self.context = context
        self._lock = threading.RLock()
        self._connection = duckdb.connect(database=":memory:")
        self._connection.execute("SET threads = 2")
        # Keep the serving layer polite on the same workstation that trained Spark.
        # DuckDB may spill aggregates instead of competing for multi-gigabyte RAM.
        self._connection.execute("SET memory_limit = '384MB'")
        self._connection.execute("SET preserve_insertion_order = false")
        self._connection.execute("PRAGMA enable_object_cache")

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def has_table(self, name: str) -> bool:
        return self.context.has_table(name)

    def _expr(self, name: str) -> str:
        path = self.context.table_path(name)
        if path is None:
            raise KeyError(f"Completed table is unavailable: {name}")
        glob = str(path / "*.parquet").replace("'", "''")
        return f"read_parquet('{glob}', union_by_name=true)"

    def query(self, sql: str, params: Iterable[object] = ()) -> pd.DataFrame:
        statement = sql.lstrip().upper()
        if not (
            statement.startswith("SELECT")
            or statement.startswith("WITH")
            or statement.startswith("DESCRIBE SELECT")
        ):
            raise ValueError("DashboardStore accepts read-only SELECT statements")
        with self._lock:
            return self._connection.execute(sql, list(params)).fetch_df()

    def columns(self, name: str) -> tuple[str, ...]:
        frame = self.query(f"DESCRIBE SELECT * FROM {self._expr(name)}")
        return tuple(str(value) for value in frame["column_name"].tolist())

    def _select_all(
        self,
        name: str,
        *,
        limit: int = 100,
        order_by: str | None = None,
    ) -> pd.DataFrame:
        limit = _bounded(limit)
        ordering = ""
        if order_by:
            if not IDENTIFIER.fullmatch(order_by) or order_by not in self.columns(name):
                raise ValueError(f"Unsafe or unknown order column: {order_by}")
            ordering = f' ORDER BY "{order_by}"'
        return self.query(
            f"SELECT * FROM {self._expr(name)}{ordering} LIMIT ?", [limit]
        )

    # ------------------------------------------------------------------ overview
    def overview_metrics(self) -> pd.DataFrame:
        if not self.has_table("overview_metrics"):
            return pd.DataFrame(columns=["metric", "value"])
        columns = set(self.columns("overview_metrics"))
        if {"metric", "value"}.issubset(columns):
            return self.query(
                f"SELECT metric, value FROM {self._expr('overview_metrics')} "
                "ORDER BY metric LIMIT 500"
            )
        return self._select_all("overview_metrics", limit=500)

    def quality_summary(self) -> pd.DataFrame:
        if not self.has_table("quality_summary"):
            return pd.DataFrame()
        columns = set(self.columns("quality_summary"))
        if "event_count" in columns:
            return self.query(
                f"SELECT * FROM {self._expr('quality_summary')} "
                "ORDER BY event_count DESC, event_type LIMIT 100"
            )
        return self._select_all("quality_summary", limit=100)

    def catalog_rollup(self) -> pd.DataFrame:
        source = "product_quality" if self.has_table("product_quality") else "products"
        if not self.has_table(source):
            return pd.DataFrame()
        columns = set(self.columns(source))
        pieces = ["COUNT(*)::BIGINT AS products"]
        pieces.append(
            "SUM(CASE WHEN is_active THEN 1 ELSE 0 END)::BIGINT AS active_products"
            if "is_active" in columns
            else "NULL::BIGINT AS active_products"
        )
        pieces.append(
            "SUM(CASE WHEN NOT is_active THEN 1 ELSE 0 END)::BIGINT AS discontinued_products"
            if "is_active" in columns
            else "NULL::BIGINT AS discontinued_products"
        )
        for source_column, alias in (
            ("reviews_total", "declared_reviews"),
            ("reviews_downloaded", "downloaded_reviews"),
            ("physical_review_count", "physical_reviews"),
        ):
            pieces.append(
                f"SUM({source_column})::BIGINT AS {alias}"
                if source_column in columns
                else f"NULL::BIGINT AS {alias}"
            )
        return self.query(f"SELECT {', '.join(pieces)} FROM {self._expr(source)}")

    def quality_samples(self, event_type: str | None = None) -> pd.DataFrame:
        if not self.has_table("quality_samples"):
            return pd.DataFrame()
        where, params = "", []
        if event_type:
            where, params = " WHERE event_type = ?", [event_type]
        return self.query(
            f"SELECT * FROM {self._expr('quality_samples')}{where} "
            "ORDER BY event_type, sample_rank LIMIT 50",
            params,
        )

    def product_group_distribution(self) -> pd.DataFrame:
        if self.has_table("group_distribution"):
            return self._select_all("group_distribution", limit=100)
        if not self.has_table("products"):
            return pd.DataFrame(columns=["product_group", "product_count"])
        return self.query(
            f"SELECT COALESCE(NULLIF(\"group\", ''), 'Bilinmiyor') AS product_group, "
            f"COUNT(*)::BIGINT AS product_count FROM {self._expr('products')} "
            "GROUP BY 1 ORDER BY product_count DESC, product_group LIMIT 100"
        )

    def rating_distribution(self) -> pd.DataFrame:
        if self.has_table("rating_distribution"):
            return self._select_all("rating_distribution", limit=10, order_by="rating")
        if not self.has_table("reviews"):
            return pd.DataFrame(columns=["rating", "review_count"])
        return self.query(
            f"SELECT rating, COUNT(*)::BIGINT AS review_count FROM {self._expr('reviews')} "
            "GROUP BY rating ORDER BY rating LIMIT 10"
        )

    def review_year_distribution(self) -> pd.DataFrame:
        if self.has_table("review_year_distribution"):
            return self._select_all(
                "review_year_distribution", limit=100, order_by="review_year"
            )
        if not self.has_table("reviews"):
            return pd.DataFrame(columns=["review_year", "review_count"])
        return self.query(
            f"SELECT YEAR(review_date)::INTEGER AS review_year, COUNT(*)::BIGINT AS review_count "
            f"FROM {self._expr('reviews')} WHERE review_date IS NOT NULL "
            "GROUP BY 1 ORDER BY 1 LIMIT 100"
        )

    def activity_quantiles(self) -> pd.DataFrame:
        if self.has_table("activity_quantiles"):
            return self._select_all("activity_quantiles", limit=10, order_by="entity_type")
        if not self.has_table("interactions"):
            return pd.DataFrame()
        interactions = self._expr("interactions")
        return self.query(
            "WITH degrees AS ("
            f" SELECT 'Kullanıcı' AS entity_type, COUNT(*)::BIGINT AS degree FROM {interactions} GROUP BY customer_id"
            " UNION ALL"
            f" SELECT 'Ürün' AS entity_type, COUNT(*)::BIGINT AS degree FROM {interactions} GROUP BY product_id"
            ") SELECT entity_type, COUNT(*)::BIGINT AS entities, "
            "approx_quantile(degree, 0.50)::DOUBLE AS p50, "
            "approx_quantile(degree, 0.90)::DOUBLE AS p90, "
            "approx_quantile(degree, 0.99)::DOUBLE AS p99, "
            "MAX(degree)::BIGINT AS maximum FROM degrees GROUP BY entity_type ORDER BY entity_type"
        )

    # --------------------------------------------------------------- product search
    def search_products(
        self,
        query: str,
        *,
        page: int = 1,
        page_size: int = 20,
    ) -> pd.DataFrame:
        if not self.has_table("products"):
            return pd.DataFrame()
        query = query.strip()[:120]
        page_size = _bounded(page_size, upper=50)
        offset = max(0, int(page) - 1) * page_size
        products = self._expr("products")
        product_columns = set(self.columns("products"))
        fields = [
            name
            for name in (
                "product_id",
                "asin",
                "title",
                "group",
                "status",
                "is_active",
                "reviews_total",
                "reviews_downloaded",
                "avg_rating_raw",
                "salesrank_clean",
            )
            if name in product_columns
        ]
        projection = ", ".join(f'p."{field}"' for field in fields)
        if not query:
            return self.query(
                f"SELECT {projection} FROM {products} p ORDER BY p.product_id LIMIT ? OFFSET ?",
                [page_size, offset],
            )
        pattern = f"%{query}%"
        predicates = [
            "CAST(p.asin AS VARCHAR) ILIKE ?",
            "COALESCE(p.title, '') ILIKE ?",
            "COALESCE(p.\"group\", '') ILIKE ?",
        ]
        params: list[object] = [pattern, pattern, pattern]
        if "category_search_text" in product_columns:
            predicates.append("COALESCE(p.category_search_text, '') ILIKE ?")
            params.append(pattern)
        elif self.has_table("category_paths"):
            predicates.append(
                f"EXISTS (SELECT 1 FROM {self._expr('category_paths')} cp "
                "WHERE cp.product_id = p.product_id AND cp.raw_path ILIKE ?)"
            )
            params.append(pattern)
        params.extend([query, f"{query}%", page_size, offset])
        return self.query(
            f"SELECT {projection} FROM {products} p WHERE {' OR '.join(predicates)} "
            "ORDER BY CASE WHEN p.asin = ? THEN 0 WHEN p.asin ILIKE ? THEN 1 "
            "WHEN p.title ILIKE ? THEN 2 ELSE 3 END, p.product_id LIMIT ? OFFSET ?",
            [*params[:-2], pattern, *params[-2:]],
        )

    def _product_metadata(self, product_ids: Iterable[int]) -> pd.DataFrame:
        ids = sorted({int(value) for value in product_ids})[:MAX_DETAIL_ROWS]
        if not ids or not self.has_table("products"):
            return pd.DataFrame(columns=["product_id"])
        source_name = "product_quality" if self.has_table("product_quality") else "products"
        placeholders = ",".join("?" for _ in ids)
        return self.query(
            f"SELECT * FROM {self._expr(source_name)} WHERE product_id IN ({placeholders}) "
            "LIMIT 500",
            ids,
        )

    def _leaf_categories(self, product_ids: Iterable[int]) -> pd.DataFrame:
        ids = sorted({int(value) for value in product_ids})[:MAX_DETAIL_ROWS]
        if not ids or not self.has_table("category_paths"):
            return pd.DataFrame(columns=["product_id", "leaf_category"])
        placeholders = ",".join("?" for _ in ids)
        path_columns = set(self.columns("category_paths"))
        if "category_paths" in path_columns:
            rows = self.query(
                f"SELECT product_id, category_paths FROM {self._expr('category_paths')} "
                f"WHERE product_id IN ({placeholders}) LIMIT 500",
                ids,
            )
            if rows.empty:
                return pd.DataFrame(columns=["product_id", "leaf_category"])
            rows["leaf_category"] = rows["category_paths"].map(
                lambda paths: _leaf_label(paths[0]) if paths is not None and len(paths) else None
            )
            return rows[["product_id", "leaf_category"]]
        paths = self.query(
            f"SELECT product_id, raw_path, path_ordinal FROM {self._expr('category_paths')} "
            f"WHERE product_id IN ({placeholders}) ORDER BY product_id, path_ordinal LIMIT 2000",
            ids,
        )
        if paths.empty:
            return pd.DataFrame(columns=["product_id", "leaf_category"])
        paths["leaf_category"] = paths["raw_path"].map(_leaf_label)
        return paths.drop_duplicates("product_id")[["product_id", "leaf_category"]]

    def product_detail(self, product_id: int) -> dict[str, object] | None:
        metadata = self._product_metadata([product_id])
        if metadata.empty:
            return None
        result = metadata.iloc[0].to_dict()
        categories = self.product_categories(product_id)
        result["category_paths"] = categories["raw_path"].tolist() if not categories.empty else []
        for logical, fields in (
            ("graph_pagerank", ("pagerank",)),
            ("graph_degrees", ("in_degree", "out_degree")),
            ("graph_components", ("component_id",)),
        ):
            if not self.has_table(logical):
                continue
            frame = self.query(
                f"SELECT * FROM {self._expr(logical)} WHERE product_id = ? LIMIT 1",
                [int(product_id)],
            )
            if not frame.empty:
                for field in fields:
                    if field in frame:
                        result[field] = frame.iloc[0][field]
        return result

    def product_categories(self, product_id: int) -> pd.DataFrame:
        if not self.has_table("category_paths"):
            return pd.DataFrame()
        if "category_paths" in self.columns("category_paths"):
            rows = self.query(
                f"SELECT category_paths FROM {self._expr('category_paths')} "
                "WHERE product_id = ? LIMIT 1",
                [int(product_id)],
            )
            if rows.empty:
                return pd.DataFrame()
            paths = rows.iloc[0]["category_paths"]
            if paths is None:
                return pd.DataFrame()
            return pd.DataFrame(
                {
                    "path_ordinal": range(1, len(paths) + 1),
                    "raw_path": list(paths),
                    "path_length": [str(path).count("|") for path in paths],
                }
            )
        return self.query(
            f"SELECT path_ordinal, raw_path, path_length FROM {self._expr('category_paths')} "
            "WHERE product_id = ? ORDER BY path_ordinal LIMIT 50",
            [int(product_id)],
        )

    def graph_neighbors(self, product_id: int, *, limit: int = 49) -> pd.DataFrame:
        if not self.has_table("graph_edges"):
            return pd.DataFrame()
        limit = _bounded(limit, upper=49)
        edges = self.query(
            f"SELECT source_product_id, target_product_id, similar_position FROM {self._expr('graph_edges')} "
            "WHERE source_product_id = ? OR target_product_id = ? "
            "ORDER BY CASE WHEN source_product_id = ? THEN 0 ELSE 1 END, similar_position "
            "LIMIT ?",
            [int(product_id), int(product_id), int(product_id), limit],
        )
        if edges.empty:
            return edges
        ids = set(edges["source_product_id"]).union(edges["target_product_id"])
        metadata = self._product_metadata(ids)
        label_map = (
            metadata.set_index("product_id")[[column for column in ("asin", "title", "group") if column in metadata]].to_dict("index")
            if not metadata.empty
            else {}
        )
        edges["source_title"] = edges["source_product_id"].map(
            lambda value: label_map.get(value, {}).get("title")
        )
        edges["target_title"] = edges["target_product_id"].map(
            lambda value: label_map.get(value, {}).get("title")
        )
        return edges

    # ------------------------------------------------------------ recommendation lab
    def search_customers(self, query: str, *, limit: int = 20) -> pd.DataFrame:
        source = "servable_customers" if self.has_table("servable_customers") else "evaluation_users"
        if not self.has_table(source):
            return pd.DataFrame(columns=["customer_id"])
        limit = _bounded(limit, upper=50)
        pattern = f"%{query.strip()[:80]}%"
        return self.query(
            f"SELECT DISTINCT customer_id FROM {self._expr(source)} "
            "WHERE customer_id ILIKE ? ORDER BY customer_id LIMIT ?",
            [pattern, limit],
        )

    def demo_users(self) -> pd.DataFrame:
        if not self.has_table("demo_users"):
            return pd.DataFrame(columns=["customer_id"])
        columns = set(self.columns("demo_users"))
        ordering = "demo_rank" if "demo_rank" in columns else "customer_id"
        return self._select_all("demo_users", limit=100, order_by=ordering)

    def customer_stages(self, customer_id: str) -> list[str]:
        source = "servable_customers" if self.has_table("servable_customers") else "evaluation_users"
        if not self.has_table(source) or "stage" not in self.columns(source):
            return []
        frame = self.query(
            f"SELECT DISTINCT stage FROM {self._expr(source)} WHERE customer_id = ? "
            "ORDER BY stage LIMIT 10",
            [customer_id],
        )
        return frame["stage"].astype(str).tolist()

    def selected_variant(self) -> str | None:
        if not self.has_table("selected_hybrid"):
            return None
        frame = self._select_all("selected_hybrid", limit=20)
        if frame.empty:
            return None
        for column in (
            "selected_hybrid",
            "selected_model",
            "selected_variant",
            "hybrid_variant",
            "model",
        ):
            if column in frame:
                values = frame[column].dropna().astype(str).str.lower()
                selected = next((value for value in values if value in {"h_a", "h_b"}), None)
                if selected:
                    return selected
        return None

    def available_models(self) -> list[str]:
        models = [model for model, table in MODEL_TABLES.items() if self.has_table(table)]
        selected = self.selected_variant()
        if selected and selected in models:
            models.append("selected")
        elif self.has_table("selected_hybrid_recommendations"):
            models.append("selected")
        return models

    def _table_for_model(self, model: str) -> str | None:
        if model == "selected":
            if self.has_table("selected_hybrid_recommendations"):
                return "selected_hybrid_recommendations"
            selected = self.selected_variant()
            return MODEL_TABLES.get(selected or "")
        return MODEL_TABLES.get(model)

    def _model_rows(self, model: str, stage: str, customer_id: str) -> pd.DataFrame:
        table = self._table_for_model(model)
        if not table or not self.has_table(table):
            return pd.DataFrame()
        columns = set(self.columns(table))
        if not {"customer_id", "product_id", "rank"}.issubset(columns):
            return pd.DataFrame()
        stage_clause, params = "", [customer_id]
        if "stage" in columns:
            stage_clause, params = " AND stage = ?", [customer_id, stage]
        return self.query(
            f"SELECT * FROM {self._expr(table)} WHERE customer_id = ?{stage_clause} "
            "ORDER BY rank LIMIT 100",
            params,
        )

    def recommendation_evidence(self, stage: str, customer_id: str) -> pd.DataFrame:
        merged: pd.DataFrame | None = None
        for model in ("als", "graph", "category", "fp", "popularity"):
            rows = self._model_rows(model, stage, customer_id)
            if rows.empty:
                continue
            part = rows[["product_id", "rank"]].drop_duplicates("product_id").rename(
                columns={"rank": f"{model}_rank"}
            )
            merged = part if merged is None else merged.merge(part, on="product_id", how="outer")
        if merged is None:
            return pd.DataFrame(columns=["product_id"])
        ids = merged["product_id"].tolist()
        if ids and self.has_table("popularity_scores"):
            placeholders = ",".join("?" for _ in ids)
            score_columns = set(self.columns("popularity_scores"))
            score = "global_bayesian_score" if "global_bayesian_score" in score_columns else "bayesian_score"
            popularity = self.query(
                f"SELECT product_id, {score} AS global_bayesian_score "
                f"FROM {self._expr('popularity_scores')} WHERE product_id IN ({placeholders}) LIMIT 500",
                ids,
            )
            merged = merged.merge(popularity, on="product_id", how="left")
        return merged

    def recommendations(
        self,
        *,
        model: str,
        stage: str,
        customer_id: str,
        top_k: int = 10,
        product_group: str | None = None,
        hide_seen: bool = True,
    ) -> pd.DataFrame:
        top_k = _bounded(top_k, upper=MAX_RECOMMENDATIONS)
        rows = self._model_rows(model, stage, customer_id)
        if rows.empty:
            return rows
        rows = rows.drop_duplicates("product_id", keep="first")
        ids = rows["product_id"].astype(int).tolist()
        metadata = self._product_metadata(ids)
        if not metadata.empty:
            rows = rows.merge(metadata, on="product_id", how="left", suffixes=("", "_product"))
        leaves = self._leaf_categories(ids)
        if not leaves.empty:
            rows = rows.merge(leaves, on="product_id", how="left")
        evidence = self.recommendation_evidence(stage, customer_id)
        if not evidence.empty:
            evidence_columns = [column for column in evidence if column != "product_id" and column not in rows]
            rows = rows.merge(evidence[["product_id", *evidence_columns]], on="product_id", how="left")
        if product_group and "group" in rows:
            rows = rows[rows["group"] == product_group]
        rows["was_seen"] = False
        if self.has_table("seen_items") and ids:
            placeholders = ",".join("?" for _ in ids)
            seen = self.query(
                f"SELECT DISTINCT product_id FROM {self._expr('seen_items')} "
                f"WHERE customer_id = ? AND stage = ? AND product_id IN ({placeholders}) LIMIT 100",
                [customer_id, stage, *ids],
            )
            seen_ids = set(seen["product_id"].astype(int)) if not seen.empty else set()
            rows["was_seen"] = rows["product_id"].astype(int).isin(seen_ids)
        if hide_seen:
            rows = rows[~rows["was_seen"]]
        rows = rows.sort_values("rank", kind="mergesort").head(top_k).copy()
        if "title" in rows:
            rows["title"] = rows["title"].fillna(
                "Bu ürünün meta verisi veri kümesinde bulunmuyor"
            )
        return rows

    def search_categories(self, query: str, *, limit: int = 30) -> pd.DataFrame:
        if not self.has_table("category_nodes"):
            return pd.DataFrame()
        return self.query(
            f"SELECT category_id, category_label, product_count FROM {self._expr('category_nodes')} "
            "WHERE category_label ILIKE ? ORDER BY product_count DESC, category_id LIMIT ?",
            [f"%{query.strip()[:100]}%", _bounded(limit, upper=50)],
        )

    def category_onboarding(self, category_id: int, *, top_k: int = 10) -> pd.DataFrame:
        if not self.has_table("category_top_products"):
            return pd.DataFrame()
        top_k = _bounded(top_k, upper=50)
        rows = self.query(
            f"SELECT * FROM {self._expr('category_top_products')} WHERE category_id = ? "
            "ORDER BY category_product_rank LIMIT 50",
            [int(category_id)],
        )
        if rows.empty:
            return rows
        metadata = self._product_metadata(rows["product_id"].tolist())
        if not metadata.empty:
            rows = rows.merge(metadata, on="product_id", how="left")
        return rows.head(top_k)

    def seed_graph_recommendations(
        self, seed_product_ids: Iterable[int], *, top_k: int = 10
    ) -> pd.DataFrame:
        seeds = sorted({int(value) for value in seed_product_ids})[:5]
        if not seeds or not self.has_table("graph_edges"):
            return pd.DataFrame()
        placeholders = ",".join("?" for _ in seeds)
        rows = self.query(
            "SELECT target_product_id AS product_id, COUNT(DISTINCT source_product_id)::BIGINT AS seed_coverage, "
            "SUM(1.0 / GREATEST(similar_position, 1))::DOUBLE AS seed_graph_score "
            f"FROM {self._expr('graph_edges')} WHERE source_product_id IN ({placeholders}) "
            f"AND target_product_id NOT IN ({placeholders}) GROUP BY target_product_id "
            "ORDER BY seed_coverage DESC, seed_graph_score DESC, product_id LIMIT 100",
            [*seeds, *seeds],
        )
        if rows.empty:
            return rows
        metadata = self._product_metadata(rows["product_id"].tolist())
        if not metadata.empty:
            rows = rows.merge(metadata, on="product_id", how="left")
        if "is_active" in rows:
            rows = rows[rows["is_active"].fillna(False)]
        rows = rows.head(_bounded(top_k, upper=50)).copy()
        rows["rank"] = range(1, len(rows) + 1)
        return rows

    # ---------------------------------------------------------- evaluation comparison
    def evaluation_summary(self) -> pd.DataFrame:
        if not self.has_table("evaluation_summary"):
            return pd.DataFrame()
        return self._select_all("evaluation_summary", limit=500)

    def als_prediction_summary(self) -> pd.DataFrame:
        if not self.has_table("als_prediction_summary"):
            return pd.DataFrame()
        return self._select_all("als_prediction_summary", limit=50)

    def model_runtime(self) -> pd.DataFrame:
        if not self.has_table("model_runtime"):
            return pd.DataFrame()
        return self._select_all("model_runtime", limit=50)

    def performance_summary(self) -> pd.DataFrame:
        empty = pd.DataFrame(columns=PERFORMANCE_COLUMNS)
        if self.context.last_passed_gate < 11:
            return empty
        root = self.context.run_dir.resolve()
        performance = (root / "performance").resolve()
        success_path = (performance / "_SUCCESS.json").resolve()
        summary_path = (performance / "summary.json").resolve()
        if not (
            performance.is_relative_to(root)
            and success_path.is_relative_to(root)
            and summary_path.is_relative_to(root)
            and success_path.is_file()
        ):
            return empty
        try:
            # The success marker is deliberately read and validated first.  A
            # partial G11 publication must never make summary.json servable.
            success = json.loads(success_path.read_text(encoding="utf-8"))
            if not isinstance(success, dict) or success.get("gate") != "G11":
                return empty
            if not summary_path.is_file():
                return empty
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            rows = _validated_performance_rows(summary, success)
        except (OSError, json.JSONDecodeError, TypeError, ValueError, KeyError):
            return empty
        return pd.DataFrame(rows, columns=PERFORMANCE_COLUMNS).head(2)

    def software_versions(self) -> dict[str, str]:
        path = self.context.manifests.get(0)
        if path is None:
            return {}
        try:
            payload = json.loads(Path(path).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        packages = payload.get("packages", {})
        selected = {
            key: str(packages[key])
            for key in ("duckdb", "streamlit", "plotly", "networkx", "pyspark")
            if key in packages
        }
        runtime = payload.get("runtime", {})
        if "spark_version" in runtime:
            selected["Spark"] = str(runtime["spark_version"])
        return selected
