"""
KnowledgeBook — runtime guidelines for the planner.

The KnowledgeBook is how we set the AI's SEMANTICS by example + rule, rather than
by hard-coding. For a user's question it assembles one grounding block:

    1. RULES + GLOSSARY  (knowledge/rules.py)  — the domain conventions, always on.
    2. EXAMPLES          (knowledge/examples.jsonl) — the nearest {question -> plan}
       exemplars, retrieved per query.

That block is injected into the planner prompt so the model applies our
conventions instead of guessing ("unpaid" = paid-is-null, "Pepsi" the brand maps
to its billing client, "how much" = SUM not a list). This is guidance, not a test.

Example retrieval is lexical (IDF-weighted token overlap) — zero infra, zero API
cost, offline. Swap _score() for embeddings later without touching call sites.
"""
import os
import re
import json
import math
from typing import Dict, List, Optional

from knowledge.rules import render as render_rules

_EXAMPLES_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "knowledge", "examples.jsonl")
_STOP = {
    "how", "many", "much", "the", "a", "an", "my", "me", "i", "do", "does", "did",
    "is", "are", "have", "has", "of", "in", "on", "for", "to", "and", "or", "what",
    "whats", "show", "list", "give", "get", "all", "total", "number", "jobs", "job",
}


def _tokens(text: str) -> List[str]:
    return [t for t in re.findall(r"[a-z0-9']+", (text or "").lower()) if t not in _STOP]


class ExampleIndex:
    """Lexical index over the KnowledgeBook's worked examples."""

    def __init__(self, path: Optional[str] = None):
        self.path = path or _EXAMPLES_PATH
        self.entries: List[Dict] = self._load()
        self._toks = [(_tokens(e["question"]), e) for e in self.entries]
        self._idf = self._compute_idf()

    def _load(self) -> List[Dict]:
        out = []
        try:
            with open(self.path) as f:
                for line in f:
                    line = line.strip()
                    if line:
                        out.append(json.loads(line))
        except FileNotFoundError:
            pass
        return out

    def _compute_idf(self) -> Dict[str, float]:
        n = len(self._toks) or 1
        df: Dict[str, int] = {}
        for toks, _ in self._toks:
            for t in set(toks):
                df[t] = df.get(t, 0) + 1
        return {t: math.log((n + 1) / (c + 0.5)) for t, c in df.items()}

    def _score(self, q_toks: List[str], cand_toks: List[str]) -> float:
        # IDF-weighted overlap: rare shared terms (client names, "unpaid", "average")
        # dominate, so retrieval keys on intent + entity, not filler.
        cand = set(cand_toks)
        return sum(self._idf.get(t, 0.0) for t in set(q_toks) if t in cand)

    def retrieve(self, query: str, k: int = 5) -> List[Dict]:
        qt = _tokens(query)
        scored = [(self._score(qt, ct), e) for ct, e in self._toks]
        scored = [(s, e) for s, e in scored if s > 0]
        scored.sort(key=lambda x: x[0], reverse=True)
        return [e for _, e in scored[:k]]

    def examples_block(self, query: str, k: int = 5) -> str:
        """Prompt-ready block of nearest worked examples, or '' if none match."""
        ex = self.retrieve(query, k)
        if not ex:
            return ""
        lines = ["# Similar questions answered correctly before (match the plan shape):"]
        for e in ex:
            lines.append(f"Q: {e['question']}")
            lines.append(f"PLAN: {json.dumps(e['plan'], ensure_ascii=False)}")
        return "\n".join(lines)


# Process-wide singleton (examples read once).
_index: Optional[ExampleIndex] = None


def get_index() -> ExampleIndex:
    global _index
    if _index is None:
        _index = ExampleIndex()
    return _index


def knowledge_context(query: str, k: int = 5) -> str:
    """The full KnowledgeBook grounding block for a query: always-on rules +
    glossary, then the nearest worked examples. Safe to call unconditionally —
    returns rules even if no example matches."""
    try:
        parts = [render_rules(), get_index().examples_block(query, k)]
        return "\n\n".join(p for p in parts if p)
    except Exception:
        return ""


def is_enabled() -> bool:
    """KnowledgeBook grounding is gated by an env flag so we can A/B it and keep
    prod on the current path until we flip it on."""
    return (os.getenv("KNOWLEDGE_BOOK", "") or "").strip().lower() in ("1", "true", "yes", "on")
