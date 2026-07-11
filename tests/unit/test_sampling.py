from __future__ import annotations

import pytest

from amazon_recommender.pipelines.bronze import (
    smoke_selection_reason,
    stable_product_hash,
)


@pytest.mark.unit
def test_product_hash_is_stable_and_seeded() -> None:
    assert stable_product_hash(123, 42) == stable_product_hash(123, 42)
    assert stable_product_hash(123, 42) != stable_product_hash(123, 43)


@pytest.mark.unit
def test_smoke_selection_always_includes_binding_anchors() -> None:
    for product_id in (0, 1, 5):
        assert (
            smoke_selection_reason(f"Id:   {product_id}\r\nASIN: X")
            == "required_anchor"
        )


@pytest.mark.unit
def test_smoke_selection_keeps_header_and_rejects_unrelated_block() -> None:
    assert smoke_selection_reason("# Full information") == "header"
    assert smoke_selection_reason("not a record") is None
