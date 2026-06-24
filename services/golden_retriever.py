"""
Golden retriever — runtime grounding for the planner.

Given a user's question, retrieves the nearest golden {question -> plan} examples
and formats them as a few-shot block to inject into the planner prompt, so the AI
"knows what to answer" for questions like the ones we've curated. This is how the
golden source TUNES the model without fine-tuning.

Phase 1 uses lexical (IDF-weighted token overlap) similarity — zero infra, zero
API cost, fully offline. Swap _score() for embeddings later without touching the
call sites.
"""
import os
import re
import json
import math
from typing import Dict, List, Optional

_CORPUS_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "golden", "corpus.jsonl")
_STOP = {
    "how", "many", "much", "the", "a", "an", "my", "me", "i", "do", "does", "did",
    "is", "are", "have", "has", "of", "in", "on", "for", "to", "and", "or", "what",
    "whats", "show", "list", "give", "get", "all", "total", "number", "jobs", "job",
}


def _tokens(text: str) -> List[str]:
    return [t for t in re.findall(r"[a-z0-9']+", (text or "").lower()) if t not in _STOP]


class GoldenRetriever:
    def __init__(self, corpus_path: Optional[str] = None):
        self.path = corpus_path or _CORPUS_PATH
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

    def fewshot_block(self, query: str, k: int = 5) -> str:
        """A prompt-ready block of nearest golden exemplars, or '' if none match."""
        ex = self.retrieve(query, k)
        if not ex:
            return ""
        lines = ["# Similar questions you've answered correctly before "
                 "(match the plan shape):"]
        for e in ex:
            lines.append(f"Q: {e['question']}")
            lines.append(f"PLAN: {json.dumps(e['plan'], ensure_ascii=False)}")
        return "\n".join(lines)


# Process-wide singleton (corpus is read once).
_retriever: Optional[GoldenRetriever] = None


def get_retriever() -> GoldenRetriever:
    global _retriever
    if _retriever is None:
        _retriever = GoldenRetriever()
    return _retriever


def fewshot_for(query: str, k: int = 5) -> str:
    """Convenience: few-shot block for a query (empty string if grounding is off
    or nothing matches). Safe to call unconditionally."""
    try:
        return get_retriever().fewshot_block(query, k)
    except Exception:
        return ""
