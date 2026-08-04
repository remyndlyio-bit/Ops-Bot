"""
TODO.md Phase 4.2 — the scenario runner.

Two layers live here on purpose:

* `TestScenarioCorpusIsWellFormed` — OFFLINE, runs in normal CI. Validates
  the corpus itself: dependencies resolve, ids are unique, every assertion
  is a real Assertion, derived expectations agree with the fixture. A typo
  in a scenario definition should fail in CI in 2 seconds, not 40 minutes
  into a paid nightly run.

* `TestWhatsAppSuite` — `@pytest.mark.live`, skipped unless `--live` AND
  real credentials are present. Hits a REAL database and makes REAL paid AI
  calls against a seeded synthetic account.

Run the live suite with:
    python -m pytest tests/e2e -m live --live -v

State discipline (the thing Phase 4 exists for — per TODO.md, contamination
rather than bugs caused ~half the failures in past shared-account runs):
every `fresh` scenario RE-SEEDS the account and wipes conversation memory
first. Re-seeding is not belt-and-braces: scenario 1 writes a row and 20
updates one, so without it every later count assertion would be off and the
corpus would silently depend on execution order.
"""

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import json
import pytest

from tests.e2e.assertions import Assertion, ScenarioContext, run_assertions
from tests.e2e.scenarios import SCENARIOS, Scenario, by_id, prerequisite_of
from tests.e2e import seed as seed_mod


# ─────────────────────────────────────────────────────────────────────
# Offline — corpus validation (runs in CI, no credentials needed)
# ─────────────────────────────────────────────────────────────────────

class TestScenarioCorpusIsWellFormed:

    def test_all_29_present(self):
        assert len(SCENARIOS) == 29

    def test_ids_unique_and_contiguous(self):
        ids = [s.id for s in SCENARIOS]
        assert ids == sorted(ids), "scenarios should be in id order"
        assert len(set(ids)) == len(ids), "duplicate scenario id"
        assert ids == list(range(1, 30))

    def test_every_scenario_has_at_least_one_assertion(self):
        empty = [s.id for s in SCENARIOS if not s.assertions]
        assert not empty, f"scenarios with no assertions: {empty}"

    def test_every_assertion_is_a_real_assertion(self):
        bad = [
            (s.id, a) for s in SCENARIOS for a in s.assertions
            if not isinstance(a, Assertion)
        ]
        assert not bad, f"non-Assertion entries: {bad}"

    def test_every_dependency_resolves(self):
        for s in SCENARIOS:
            if s.requires == "fresh":
                continue
            assert s.requires.startswith("after:"), (
                f"scenario {s.id}: unknown requires {s.requires!r}"
            )
            prereq = prerequisite_of(s)
            assert prereq is not None
            assert prereq.id < s.id, (
                f"scenario {s.id} depends on {prereq.id}, which runs later"
            )

    def test_no_dependency_chains_deeper_than_one(self):
        """A prerequisite that itself has a prerequisite would need the
        runner to replay a chain; nothing needs that yet, so keep it out
        until something does."""
        for s in SCENARIOS:
            prereq = prerequisite_of(s)
            if prereq is not None:
                assert prereq.requires == "fresh", (
                    f"scenario {s.id} -> {prereq.id} -> {prereq.requires}: chain too deep"
                )

    def test_mutating_scenarios_are_declared(self):
        """Anything that writes must be flagged so the runner re-seeds after
        it. Approximated by intent keywords — a new write scenario that
        forgets the flag trips this."""
        for s in SCENARIOS:
            looks_write = any(w in s.message.lower()
                              for w in ("add a job", "mark this", "delete", "update my"))
            if looks_write:
                assert s.mutates, (
                    f"scenario {s.id} looks like a write but mutates=False: {s.message!r}"
                )

    def test_derived_expectations_match_the_fixture(self):
        """The numbers baked into assertions must come from the seed. If a
        fixture row changes and an expectation doesn't, that's a silently
        wrong test — this catches it."""
        from tests.e2e.scenarios import INVOICED_NO_EMAIL_COUNT
        from tests.e2e.seed import FIXTURE_ROWS, _is_yes
        recomputed = sum(1 for r in FIXTURE_ROWS
                         if _is_yes(r["bill_sent"]) and not r["poc_email"])
        assert INVOICED_NO_EMAIL_COUNT == recomputed

    def test_known_bug_regressions_are_labelled(self):
        """The CLAUDE.md bug list is the reason several of these exist;
        losing the note loses the 'why' for whoever sees it fail."""
        noted = [s.id for s in SCENARIOS if s.note]
        for expected in (1, 4, 12, 24, 28):   # Bugs 4, 1, 3, 1, 2
            assert expected in noted, f"scenario {expected} lost its regression note"


# ─────────────────────────────────────────────────────────────────────
# Live — the real suite
# ─────────────────────────────────────────────────────────────────────

def _credentials_present() -> bool:
    from tests.conftest import has_real_ai_key, has_real_db_url
    return has_real_ai_key() and has_real_db_url()


live_suite = pytest.mark.skipif(
    not _credentials_present(),
    reason="needs a real AI_KEY and SUPABASE_DB_URL",
)


@pytest.fixture(scope="module")
def account():
    """One synthetic account for the whole module, torn down at the end."""
    uid = seed_mod.seed()
    yield uid
    seed_mod.teardown(uid)


@pytest.fixture(scope="module")
def service():
    from services.intent_service import IntentService
    return IntentService()


def _reset(uid, svc):
    """Re-seed the rows and wipe conversation memory — a scenario must not
    be able to observe anything a previous one did."""
    seed_mod.seed(uid)
    try:
        from utils.memory_service import _DEFAULT
        svc.memory._write_raw(uid, dict(_DEFAULT))
    except Exception:
        # Fall back to explicitly clearing the keys that carry turn state.
        svc.memory.update_user_memory(uid, {
            "last_intent": None, "uscf_context": {}, "answer_ledger": [],
            "flow_v2": None, "last_generated_invoice": None,
            "pending_disambiguation": None, "suggested_next_action": None,
        })


def _db(uid):
    def _query(sql, params):
        conn = seed_mod._connect()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(sql, params)
                    cols = [c[0] for c in cur.description]
                    return [dict(zip(cols, row)) for row in cur.fetchall()]
        finally:
            conn.close()
    return _query


RESULTS = []


@live_suite
@pytest.mark.live
@pytest.mark.parametrize("scenario", SCENARIOS, ids=lambda s: f"{s.id:02d}-{s.category}")
class TestWhatsAppSuite:

    def test_scenario(self, scenario: Scenario, account, service):
        uid = account

        if scenario.requires == "fresh":
            _reset(uid, service)
        else:
            prereq = prerequisite_of(scenario)
            _reset(uid, service)
            service.process_request(uid, prereq.message)   # establish context

        result = service.process_request(uid, scenario.message)
        ctx = ScenarioContext(result=result, user_id=uid, db=_db(uid))
        checks = run_assertions(ctx, scenario.assertions)

        RESULTS.append({
            "id": scenario.id,
            "message": scenario.message,
            "category": scenario.category,
            "note": scenario.note,
            "passed": all(c["passed"] for c in checks),
            "checks": checks,
            "response": (result or {}).get("response", "")[:400],
            "operation": (result or {}).get("operation", ""),
        })

        if scenario.mutates:
            _reset(uid, service)   # don't let a write leak into the next scenario

        failed = [c for c in checks if not c["passed"]]
        assert not failed, (
            f"scenario {scenario.id} ({scenario.category}) failed:\n  "
            + "\n  ".join(f"{c['assertion']}: {c['detail']}" for c in failed)
            + f"\n  response: {(result or {}).get('response','')[:300]!r}"
        )


def pytest_sessionfinish(session, exitstatus):  # pragma: no cover - live only
    """Phase 4.3 hook: dump per-scenario results for score tracking."""
    if not RESULTS:
        return
    out = os.path.join(os.path.dirname(__file__), "last_run.json")
    passed = sum(1 for r in RESULTS if r["passed"])
    payload = {
        "total": len(RESULTS),
        "passed": passed,
        "pass_rate": round(passed / len(RESULTS), 4) if RESULTS else 0.0,
        "scenarios": RESULTS,
    }
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)
