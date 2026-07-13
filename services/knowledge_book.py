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
    "is", "are", "have", "has", "had", "of", "in", "on", "for", "to", "and", "or",
    "what", "whats", "show", "list", "give", "get", "all", "jobs", "job", "please",
    "from", "by", "with", "still", "yet",
    # contractions: the tokenizer keeps the apostrophe, so these are single tokens
    # and must be stop-listed explicitly (plain "whats" above won't catch "what's").
    "what's", "i've", "who's", "haven't", "hasn't", "don't", "doesn't", "isn't",
    # Hinglish function words (low signal)
    "ke", "ka", "ki", "hai", "hain", "hua", "hue", "se", "ko", "mein",
    "mera", "meri", "mujhe", "kaun", "kaunse", "ko",
}

# Synonym canonicalisation — map the words users actually type to one token per
# concept, so "pending"/"due"/"baki" all retrieve the "unpaid" examples and
# "earnings"/"revenue"/"kamai" all retrieve the "total" ones. Mirrors the
# KnowledgeBook glossary; applied to BOTH the index and the query.
_SYN = {
    # unpaid family
    "unpaid": "unpaid", "outstanding": "unpaid", "pending": "unpaid", "owed": "unpaid",
    "owe": "unpaid", "owes": "unpaid", "owing": "unpaid", "baki": "unpaid",
    "baaki": "unpaid", "due": "unpaid",
    # paid family
    "paid": "paid", "cleared": "paid", "received": "paid", "settled": "paid",
    "collected": "paid", "aaya": "paid", "aayi": "paid",
    # money-value family — a distinct concept from the bare quantifier "total"
    # (which also appears in COUNT questions like "total number of jobs"). Keeping
    # them separate lets "total earning"/"revenue"/"kamai" retrieve SUM exemplars
    # instead of tying with counts on the shared word "total".
    "sum": "earnings", "earnings": "earnings", "earning": "earnings",
    "earned": "earnings", "earn": "earnings", "revenue": "earnings",
    "billing": "earnings", "billed": "earnings", "kamai": "earnings",
    "kamaya": "earnings", "income": "earnings", "made": "earnings", "worth": "earnings",
    "total": "total",
    # invoice / sent family
    "invoice": "invoice", "invoices": "invoice", "bill": "invoice", "bills": "invoice",
    "send": "sent", "sent": "sent", "bheja": "sent", "bheje": "sent",
    # count family
    "count": "count", "kitne": "count", "kitna": "count", "number": "count",
    # misc
    "avg": "average", "average": "average",
}


def _tokens(text: str) -> List[str]:
    out = []
    for t in re.findall(r"[a-z0-9']+", (text or "").lower()):
        if t in _STOP:
            continue
        out.append(_SYN.get(t, t))
    return out


def _summarize_plan(p: Dict) -> str:
    """Render a plan as a compact `key=value` hint — NOT raw JSON. Injecting raw
    JSON objects made the planner echo/garble them and emit invalid JSON; a prose
    summary teaches the same mapping without a blob to copy."""
    parts = []
    if p.get("metric"):
        parts.append(f"metric={p['metric']}")
    if p.get("column"):
        parts.append(f"column={p['column']}")
    for k, v in (p.get("filters") or {}).items():
        parts.append(f"{k}={v}")
    tr = p.get("time_range")
    if tr and tr.get("value"):
        parts.append(f"date={tr['value'].get('start')}..{tr['value'].get('end')}")
    if p.get("group_by"):
        parts.append(f"group_by={p['group_by']}")
    if p.get("order"):
        parts.append(f"order={p['order']}")
    if p.get("limit"):
        parts.append(f"limit={p['limit']}")
    return ", ".join(parts) if parts else "return matching rows (a list)"


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

    EXAMPLES_HEADER = "# Reference — how to read similar questions (guidance only, do NOT copy verbatim):"

    def examples_block(self, query: str, k: int = 5) -> str:
        """Prompt-ready block of nearest worked examples as compact hints, or ''.
        Hints are `"question" -> key=value` (never raw JSON), so the model can't
        echo an example blob in place of its own JSON answer."""
        ex = self.retrieve(query, k)
        if not ex:
            return ""
        lines = [self.EXAMPLES_HEADER]
        for e in ex:
            lines.append(f"- \"{e['question']}\" -> {_summarize_plan(e['plan'])}")
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
        block = "\n\n".join(p for p in parts if p)
        if block:
            # Re-assert the output contract so the guidance never derails the
            # planner's strict-JSON response.
            block += ("\n\n# The above is guidance only. Now output ONLY the single "
                      "JSON plan for the user's question, exactly per the schema below.")
        return block
    except Exception:
        return ""


def is_enabled() -> bool:
    """KnowledgeBook grounding. DEFAULT ON as of 2026-07-02: the held-out A/B
    (knowledge/ab_run.py over knowledge/eval_hard.py) showed a small, replicable
    lift — KB-OFF 43/50 (86%) -> KB-ON 46/50 (92%), +3, zero regressions, two
    identical runs. Set KNOWLEDGE_BOOK=0 (or false/no/off/empty) to revert to the
    pre-KB path."""
    return (os.getenv("KNOWLEDGE_BOOK", "1") or "").strip().lower() not in ("0", "false", "no", "off", "")


def value_fork_enabled() -> bool:
    """The billed-vs-received clarify fork (`intent_service._handle_value_fork`).
    DEFAULT ON as of 2026-07-02: measured via knowledge/value_fork_eval.py
    (deterministic detector, 34 labelled messages) — 100% precision / 100% recall
    AFTER adding the date guard that stops it hijacking dated value queries (it
    was 62% precision before). Kept on its OWN flag (separate from KNOWLEDGE_BOOK)
    so each can be reverted independently. Set KB_VALUE_FORK=0 to revert."""
    return (os.getenv("KB_VALUE_FORK", "1") or "").strip().lower() not in ("0", "false", "no", "off", "")
