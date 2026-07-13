"""
Tests for the KnowledgeBook (Phase 1): oracle correctness, examples integrity,
generator reproducibility, rules/glossary rendering, lexical retrieval relevance,
and the assembled grounding context. All offline (no LLM).
"""
import os
import sys
import json
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
from knowledge.dataset import build_dataset
from knowledge.oracle import compute_answer, _is_paid, _client_of
from knowledge.generate import build_corpus, CORPUS_PATH
from knowledge import rules as kb_rules
from services.knowledge_book import ExampleIndex, knowledge_context


ROWS = build_dataset()


def _plan(**kw):
    base = {"metric": None, "column": None, "filters": {}, "time_range": None,
            "group_by": None, "order": None, "limit": None}
    base.update(kw)
    return base


class TestOracle:
    """The oracle is the ground truth — cross-check it against an independent
    computation over the same dataset."""

    def test_count_all(self):
        assert compute_answer(_plan(metric="count"), ROWS)["value"] == len(ROWS)

    def test_total_all(self):
        assert compute_answer(_plan(metric="sum", column="fees"), ROWS)["value"] == sum(r["fees"] for r in ROWS)

    def test_avg_all(self):
        fees = [r["fees"] for r in ROWS]
        assert compute_answer(_plan(metric="avg", column="fees"), ROWS)["value"] == round(sum(fees) / len(fees))

    def test_count_unpaid_uses_messy_semantics(self):
        # unpaid = NOT in the paid-set (incl. null) — the whole point of the oracle.
        expected = len([r for r in ROWS if not _is_paid(r)])
        assert compute_answer(_plan(metric="count", filters={"paid": "no"}), ROWS)["value"] == expected
        assert expected > 0

    def test_paid_plus_unpaid_equals_all(self):
        paid = compute_answer(_plan(metric="count", filters={"paid": "yes"}), ROWS)["value"]
        unpaid = compute_answer(_plan(metric="count", filters={"paid": "no"}), ROWS)["value"]
        assert paid + unpaid == len(ROWS)

    def test_client_matches_brand(self):
        # "Pepsi" is a brand whose billing client is "Content Lab" — must still match.
        got = compute_answer(_plan(metric="count", filters={"client_name": "Pepsi"}), ROWS)["value"]
        expected = len([r for r in ROWS if "pepsi" in
                        " ".join(str(r.get(k) or "") for k in ("client_name", "brand_name", "production_house")).lower()])
        assert got == expected and got > 0

    def test_biggest_client(self):
        agg = {}
        for r in ROWS:
            agg[_client_of(r)] = agg.get(_client_of(r), 0) + r["fees"]
        top = max(agg.items(), key=lambda kv: kv[1])
        ans = compute_answer(_plan(metric="sum", column="fees", group_by="client_name", order="desc", limit=1), ROWS)
        assert ans["type"] == "client" and ans["value"] == top[0] and ans["amount"] == top[1]

    def test_month_range_filters(self):
        tr = {"type": "absolute", "value": {"start": "2026-03-01", "end": "2026-03-31"}}
        expected = len([r for r in ROWS if "2026-03-01" <= r["job_date"][:10] <= "2026-03-31"])
        assert compute_answer(_plan(metric="count", time_range=tr), ROWS)["value"] == expected


class TestCorpus:
    def test_corpus_file_present_and_valid(self):
        assert os.path.exists(CORPUS_PATH), "run `python -m knowledge.generate`"
        with open(CORPUS_PATH) as f:
            entries = [json.loads(l) for l in f if l.strip()]
        assert len(entries) >= 50
        ids = [e["id"] for e in entries]
        assert len(ids) == len(set(ids)), "duplicate ids"
        for e in entries:
            assert e["question"] and isinstance(e["plan"], dict) and e["answer"]

    def test_generator_is_reproducible(self):
        assert build_corpus() == build_corpus()

    def test_answers_match_oracle_on_current_dataset(self):
        # Every stored answer must equal a fresh oracle computation (corpus honest).
        for e in build_corpus():
            assert e["answer"] == compute_answer(e["plan"], ROWS), f"stale answer: {e['id']}"

    def test_no_eval_leakage(self):
        # The corpus must never contain a held-out eval_hard question, else KB-on
        # would win the A/B by memorisation instead of generalisation.
        import re
        from knowledge.eval_hard import cases

        def norm(s):
            return re.sub(r"\s+", " ", (s or "").strip().lower()).rstrip("?.! ")

        evalq = {norm(c["question"]) for c in cases()}
        corpusq = {norm(e["question"]) for e in build_corpus()}
        leaked = sorted(evalq & corpusq)
        assert not leaked, f"eval questions leaked into the corpus: {leaked}"

    def test_corpus_scaled_and_unique(self):
        corpus = build_corpus()
        assert len(corpus) >= 900, f"corpus shrank unexpectedly: {len(corpus)}"
        qs = [e["question"].lower() for e in corpus]
        assert len(qs) == len(set(qs)), "duplicate questions in corpus"


class TestABGradingNormaliser:
    """The A/B harness grades the planner's PLAN via the oracle, so it must model
    every filter the planner can legitimately emit. invoice_date IS [NOT] NULL
    ("invoiced"/"raised") selects the same rows as bill_sent in the seeded data;
    a missing mapping silently mis-graded correct plans (sent-04/09)."""

    def test_invoice_date_not_null_maps_to_bill_sent_yes(self):
        from knowledge.ab_run import _norm_filters
        for v in ("IS NOT NULL", "not_null", "isnotnull"):
            f, _ = _norm_filters({"invoice_date": v})
            assert f == {"bill_sent": "yes"}, v

    def test_invoice_date_null_maps_to_bill_sent_no(self):
        from knowledge.ab_run import _norm_filters
        assert _norm_filters({"invoice_date": None})[0] == {"bill_sent": "no"}
        assert _norm_filters({"invoice_date": "IS NULL"})[0] == {"bill_sent": "no"}

    def test_explicit_bill_sent_not_overridden(self):
        from knowledge.ab_run import _norm_filters
        f, _ = _norm_filters({"bill_sent": "yes", "invoice_date": "IS NULL"})
        assert f == {"bill_sent": "yes"}   # explicit wins, no clobber

    def test_invoice_date_and_bill_sent_agree_on_seeded_data(self):
        # The equivalence the mapping relies on: invoice_date set IFF bill_sent.
        from knowledge.ab_run import _plan_to_oracle
        p_inv, _ = _plan_to_oracle({"metric": "sum", "column": "fees",
                                    "filters": {"invoice_date": "IS NOT NULL"}})
        p_bs, _ = _plan_to_oracle({"metric": "sum", "column": "fees",
                                   "filters": {"bill_sent": "yes"}})
        assert compute_answer(p_inv, ROWS) == compute_answer(p_bs, ROWS)


class TestRetriever:
    def setup_method(self):
        self.r = ExampleIndex()

    def test_loads_examples(self):
        assert len(self.r.entries) >= 50

    def test_retrieves_relevant_status_query(self):
        hits = self.r.retrieve("how many unpaid jobs do I have", k=5)
        assert hits and any("status" in h["tags"] for h in hits[:3])

    def test_retrieves_client_query(self):
        hits = self.r.retrieve("show me all Garnier work", k=5)
        assert hits and any("garnier" in h["question"].lower() for h in hits[:3])

    def test_examples_block_is_compact_hints_not_json(self):
        block = self.r.examples_block("what's my total billing this period", k=3)
        assert "->" in block and '"' in block
        assert "PLAN:" not in block and "{" not in block  # no raw JSON to echo

    def test_no_match_returns_empty(self):
        assert self.r.examples_block("xyzzy plugh frobnicate", k=3) == ""


class TestRules:
    def test_render_has_rules_and_glossary(self):
        block = kb_rules.render()
        assert "KnowledgeBook" in block
        # load-bearing conventions are present
        assert "paid IS NULL" in block
        assert "client_name OR brand_name OR production_house" in block
        assert "glossary" in block.lower()
        assert "earnings" in block.lower()


class TestKnowledgeContext:
    def test_context_combines_rules_and_examples(self):
        ctx = knowledge_context("how much is still unpaid?", k=3)
        assert "KnowledgeBook" in ctx          # rules always on
        assert "Reference — how to read" in ctx  # examples retrieved
        assert "paid IS NULL" in ctx
        assert "{" not in ctx                  # never injects raw JSON
        assert "output ONLY the single" in ctx  # re-asserts the JSON contract

    def test_rules_present_even_with_no_example_match(self):
        ctx = knowledge_context("xyzzy plugh frobnicate", k=3)
        assert "KnowledgeBook" in ctx
        assert "Reference — how to read" not in ctx  # no example matched, rules still there


class TestPlannerWiring:
    """The KnowledgeBook grounds the planner only when the flag is on."""

    def _fake_gemini(self, cap):
        class G:
            def _call_api(self, prompt, generation_config=None):
                cap["prompt"] = prompt
                return '{"metric":"sum","column":"fees","filters":{"paid":"no"}}'
        return G()

    def test_injected_when_enabled(self, monkeypatch):
        from services.query_planner import build_operation_plan
        monkeypatch.setenv("KNOWLEDGE_BOOK", "1")
        cap = {}
        build_operation_plan("how much is unpaid", "query", "schema",
                             ["fees", "paid"], gemini_service=self._fake_gemini(cap))
        assert "KnowledgeBook" in cap["prompt"] and "output ONLY the single" in cap["prompt"]

    def test_not_injected_when_disabled(self, monkeypatch):
        from services.query_planner import build_operation_plan
        # KB is default-ON now, so the disabled path requires an explicit 0.
        monkeypatch.setenv("KNOWLEDGE_BOOK", "0")
        cap = {}
        build_operation_plan("how much is unpaid", "query", "schema",
                             ["fees", "paid"], gemini_service=self._fake_gemini(cap))
        assert "KnowledgeBook" not in cap["prompt"]

    def test_injected_by_default_when_flag_absent(self, monkeypatch):
        # Default flipped ON (2026-07-02) after the held-out A/B showed +3.
        from services.query_planner import build_operation_plan
        monkeypatch.delenv("KNOWLEDGE_BOOK", raising=False)
        cap = {}
        build_operation_plan("how much is unpaid", "query", "schema",
                             ["fees", "paid"], gemini_service=self._fake_gemini(cap))
        assert "KnowledgeBook" in cap["prompt"]


class TestFlagDefault:
    """is_enabled() default semantics after the 2026-07-02 flip to ON."""

    def test_on_by_default(self, monkeypatch):
        from services.knowledge_book import is_enabled
        monkeypatch.delenv("KNOWLEDGE_BOOK", raising=False)
        assert is_enabled() is True

    @pytest.mark.parametrize("val", ["0", "false", "no", "off", ""])
    def test_explicit_off_values(self, monkeypatch, val):
        from services.knowledge_book import is_enabled
        monkeypatch.setenv("KNOWLEDGE_BOOK", val)
        assert is_enabled() is False

    @pytest.mark.parametrize("val", ["1", "true", "yes", "on"])
    def test_explicit_on_values(self, monkeypatch, val):
        from services.knowledge_book import is_enabled
        monkeypatch.setenv("KNOWLEDGE_BOOK", val)
        assert is_enabled() is True

    def test_value_fork_on_by_default(self, monkeypatch):
        # Fork flipped ON (2026-07-02) after value_fork_eval hit 100% precision.
        # It is its OWN flag, independent of KNOWLEDGE_BOOK.
        from services.knowledge_book import value_fork_enabled
        monkeypatch.delenv("KB_VALUE_FORK", raising=False)
        assert value_fork_enabled() is True

    @pytest.mark.parametrize("val", ["0", "false", "no", "off", ""])
    def test_value_fork_explicit_off(self, monkeypatch, val):
        from services.knowledge_book import value_fork_enabled
        monkeypatch.setenv("KB_VALUE_FORK", val)
        assert value_fork_enabled() is False

    def test_fork_flag_independent_of_grounding(self, monkeypatch):
        # Turning grounding OFF must not turn the fork off (separate flags).
        from services.knowledge_book import value_fork_enabled
        monkeypatch.setenv("KNOWLEDGE_BOOK", "0")
        monkeypatch.delenv("KB_VALUE_FORK", raising=False)
        assert value_fork_enabled() is True


class TestSynonymRetrieval:
    """Synonym canonicalisation: words users actually type retrieve the right
    examples even when the corpus phrases them differently."""
    def setup_method(self):
        self.r = ExampleIndex()

    def _top_questions(self, q, k=3):
        return [h["question"].lower() for h in self.r.retrieve(q, k)]

    def test_pending_finds_unpaid_for_client(self):
        hits = self.r.retrieve("how much is pending from Samsung", k=3)
        assert hits and any("samsung" in h["question"].lower() and "status" in h["tags"] for h in hits)

    def test_due_finds_unpaid(self):
        hits = self.r.retrieve("what's due from Garnier", k=3)
        assert hits and any("garnier" in h["question"].lower() and "status" in h["tags"] for h in hits)

    def test_earnings_finds_total(self):
        hits = self.r.retrieve("earnings from Nike", k=3)
        assert hits and any("nike" in h["question"].lower() and "sum" in h["tags"] for h in hits)

    def test_revenue_synonym_of_total(self):
        # "revenue" and "billing" should land on the same bare-total cluster
        hits = self.r.retrieve("overall revenue", k=3)
        assert hits and any("total" in h["question"].lower() or "billing" in h["question"].lower() or "revenue" in h["question"].lower() for h in hits)

    def test_contraction_is_not_a_signal_token(self):
        # "what's" is filler — the tokenizer keeps the apostrophe, so it must be
        # stop-listed explicitly (plain "whats" won't catch it). If it leaks
        # through it dominates every "what's ..." exemplar and drowns the intent.
        from services.knowledge_book import _tokens
        assert "what's" not in _tokens("what's due from Garnier")
        assert _tokens("what's due from Garnier") == ["unpaid", "garnier"]


class TestNewCoverageAreas:
    """The four intents added from the 2026-07 WhatsApp transcripts. Each must be
    represented AND retrievable by the natural phrasing that exposed the bug."""

    def setup_method(self):
        self.r = ExampleIndex()

    def _plans(self, q, k=3):
        return [h["plan"] for h in self.r.retrieve(q, k)]

    # ── Area B — the headline bug: earnings != outstanding (IMG 2) ──────────
    def test_total_earnings_is_unfiltered_sum_not_unpaid(self):
        # "total earning from all jobs" must retrieve a SUM-with-NO-paid-filter
        # exemplar — not an outstanding/unpaid one. This is the exact confusion
        # in the transcript (₹75k "earnings" wrongly equated to the unpaid figure).
        plans = self._plans("what is my total earning from all jobs")
        assert any(p.get("metric") == "sum" and not (p.get("filters") or {}).get("paid")
                   for p in plans), "earnings must map to an unfiltered sum"

    def test_earnings_received_outstanding_reconcile(self):
        # The three buckets are distinct and add up: received + outstanding = all.
        allb = compute_answer(_plan(metric="sum", column="fees"), ROWS)["value"]
        recv = compute_answer(_plan(metric="sum", column="fees", filters={"paid": "yes"}), ROWS)["value"]
        outs = compute_answer(_plan(metric="sum", column="fees", filters={"paid": "no"}), ROWS)["value"]
        assert recv + outs == allb and recv != allb and outs != allb

    def test_received_maps_to_paid_sum(self):
        plans = self._plans("how much money have I actually received")
        assert any((p.get("filters") or {}).get("paid") == "yes" and p.get("metric") == "sum" for p in plans)

    # ── Area A — collections: who hasn't paid (IMG 1) ───────────────────────
    def test_who_hasnt_paid_retrieves_unpaid(self):
        plans = self._plans("who hasn't paid me yet")
        assert any((p.get("filters") or {}).get("paid") == "no" for p in plans)

    def test_who_owes_most_is_grouped_ranking(self):
        plans = self._plans("who owes me the most")
        assert any(p.get("group_by") == "client_name" and (p.get("filters") or {}).get("paid") == "no"
                   for p in plans)

    # ── Area C — contact / recipient lookup, scoped to ONE client (IMG 3) ───
    def test_email_for_client_scopes_to_that_client(self):
        # The bug dumped ALL jobs; the exemplar must filter to the named client.
        plans = self._plans("do you have the recipient email for Samsung India")
        assert any((p.get("filters") or {}).get("client_name") for p in plans), \
            "email-for-X must scope to that client, not list everything"

    def test_missing_email_maps_to_poc_null(self):
        plans = self._plans("which clients don't have an email on file")
        assert any((p.get("filters") or {}).get("poc_email") == "null" for p in plans)

    # ── Area D — invoice dispatch status (IMG 1: "payment reminder sent?") ───
    def test_pending_to_invoice_maps_to_bill_sent_no(self):
        plans = self._plans("which invoices haven't gone out yet")
        assert any((p.get("filters") or {}).get("bill_sent") == "no" for p in plans)
