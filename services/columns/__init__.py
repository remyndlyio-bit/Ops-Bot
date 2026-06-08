"""
Column registry — the single source of truth per column.

Why this exists
---------------
Bugs we shipped repeatedly looked like this:

  • Planner emits {"bill_sent": null}            → SQL builder ILIKE'd NULL
  • Planner emits {"bill_sent": "IS NOT NULL"}   → SQL builder ILIKE'd a string
  • Planner emits {"bill_sent": ["no","false"]}  → SQL builder used IN (...)
                                                    which EXCLUDES NULL rows
  • Planner emits {"poc_email": null}            → unsolicited filter excluded
                                                    the deliverable rows

Each time, the prompt knew one thing, the SQL builder knew another, the
classifier knew a third. Patches went to whichever file someone happened
to look at first. Drift was inevitable.

This module fixes that. Each column has ONE file:

  services/columns/bill_sent.py
  services/columns/paid.py
  services/columns/...

…that exports:

  * SEMANTIC      — a docstring explaining what the column means
  * PROMPT_FRAGMENT — the text that gets injected into BOTH the classifier
                      prompt and the planner prompt. Same string, no drift.
  * filter_handler(value)  → emits the SQL predicate for any well-typed
                              filter on this column. All shape variants
                              (single, list, null, "IS NOT NULL") live here.

Adding a new state or a new AI improvisation = update one file.

The registry is consulted by services/query_planner._build_filter_clause
(if a column has a handler registered, the handler wins). The prompt
composer in classifier.py / query_planner.py pulls PROMPT_FRAGMENT.

Adding tests for a column registered here is mandatory — see
tests/test_planner_boundary.py for the format. CI runs these on every push.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, Optional

# A column handler converts an AI-emitted value (which could be a string,
# list, dict, None, etc.) into a SQL predicate string. Returns None when
# the registry decides this handler doesn't apply — caller falls back.
ColumnHandler = Callable[[Any], Optional[str]]

# Internal registry; populated by per-column modules at import time.
_REGISTRY: Dict[str, "ColumnSpec"] = {}


class ColumnSpec:
    """Everything we know about a single column."""

    __slots__ = ("name", "semantic", "prompt_fragment", "filter_handler")

    def __init__(
        self,
        name: str,
        semantic: str,
        prompt_fragment: str,
        filter_handler: ColumnHandler,
    ):
        self.name = name
        self.semantic = semantic
        self.prompt_fragment = prompt_fragment
        self.filter_handler = filter_handler


def register(spec: ColumnSpec) -> None:
    """Called once per column at module import."""
    if spec.name in _REGISTRY:
        raise ValueError(f"column '{spec.name}' already registered")
    _REGISTRY[spec.name] = spec


def get(name: str) -> Optional[ColumnSpec]:
    """Look up a column spec. Returns None when not registered (caller
    should fall through to the generic SQL builder)."""
    return _REGISTRY.get(name)


def all_specs() -> Dict[str, ColumnSpec]:
    """All registered columns, keyed by name. Used by prompt composers."""
    return dict(_REGISTRY)


def composed_prompt_fragments() -> str:
    """Compose the planner / classifier prompt section that lists every
    registered column's semantic mapping. Use this in the prompt builders
    so they can never drift from the SQL handler."""
    parts = []
    for name in sorted(_REGISTRY):
        parts.append(_REGISTRY[name].prompt_fragment.rstrip())
    return "\n".join(parts)


# Eager import side-effect — registering all known columns. New columns
# go here as separate imports.
from services.columns import bill_sent as _bill_sent  # noqa: E402, F401
from services.columns import paid as _paid            # noqa: E402, F401
from services.columns import poc_email as _poc_email  # noqa: E402, F401
from services.columns import date_columns as _dates   # noqa: E402, F401
