"""Gate handlers are registered here as implementation phases are completed."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable


GateHandler = Callable[[Any, Any, Path | None], dict[str, Any]]
HANDLERS: dict[str, GateHandler] = {}


def register(gate: str) -> Callable[[GateHandler], GateHandler]:
    def decorator(function: GateHandler) -> GateHandler:
        if gate in HANDLERS:
            raise RuntimeError(f"Duplicate gate handler: {gate}")
        HANDLERS[gate] = function
        return function

    return decorator


# Phase modules register their handlers by import. Keep imports at the bottom to
# avoid circular registration while this module is initialized.
from amazon_recommender.phases import g2 as _g2  # noqa: E402,F401
from amazon_recommender.phases import g3 as _g3  # noqa: E402,F401
from amazon_recommender.phases import g4 as _g4  # noqa: E402,F401
from amazon_recommender.phases import g5 as _g5  # noqa: E402,F401
from amazon_recommender.phases import g6 as _g6  # noqa: E402,F401
from amazon_recommender.phases import g7 as _g7  # noqa: E402,F401
from amazon_recommender.phases import g8 as _g8  # noqa: E402,F401
from amazon_recommender.phases import g9 as _g9  # noqa: E402,F401
from amazon_recommender.phases import g10 as _g10  # noqa: E402,F401
from amazon_recommender.phases import g11 as _g11  # noqa: E402,F401
from amazon_recommender.phases import g12 as _g12  # noqa: E402,F401
