from __future__ import annotations

import math

import pytest

from amazon_recommender.models.math import category_idf


@pytest.mark.unit
def test_category_idf_binding_value() -> None:
    assert category_idf(9, 4) == pytest.approx(1.0 + math.log(2.0))
