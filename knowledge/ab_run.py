"""
KnowledgeBook A/B runner over the 200 hard held-out cases (knowledge/eval_hard.py).

For each case it calls the REAL planner (services.query_planner.build_operation_plan)
twice — KNOWLEDGE_BOOK off vs on — then grades each emitted plan by computing its
answer with the oracle over the seeded dataset and comparing to the gold answer.
Grading the PLAN (not live SQL) is deliberate: the KB influences the plan, and it
keeps the run fully offline apart from the one planner LLM call per arm.

  AI_KEY=sk-or-... python -m knowledge.ab_run                 # full 200x2 run
  AI_KEY=sk-or-... python -m knowledge.ab_run --limit 10      # first 10 cases
  AI_KEY=sk-or-... python -m knowledge.ab_run --arm off       # one arm only

Interleaves off/on per case and checkpoints to knowledge/ab_results.json, so a
dead/expired key mid-run still leaves a usable partial A/B (the handoff warns
cheap OpenRouter keys can exhaust after ~50 calls / expire in ~1hr).
"""
import os
import sys
import json
import time
from typing import Any, Dict, List, Optional, Tuple

from knowledge.dataset import build_dataset
from knowledge.oracle import compute_answer
from knowledge.eval_hard import cases
from services.supabase_service import JOB_ENTRIES_COLUMNS, SCHEMA_DESCRIPTION
from services import query_planner
from services.gemini_service import GeminiService

RESULTS_PATH = os.path.join(os.path.dirname(__file__), "ab_results.json")
_ROWS = build_dataset()

_PAID_TRUE = {"yes", "true", "t", "1", "paid"}
_PAID_FALSE = {"no", "false", "unpaid", "0", ""}
_SENT_TRUE = _PAID_TRUE | {"sent"}


# ── Normalise a raw planner plan into the oracle's plan vocabulary ────────────
def _norm_filters(raw: Dict) -> Tuple[Dict, bool]:
    """Map planner filters -> oracle filters. Returns (filters, poc_email_added).
    poc_email_added flags the over-reasoning trap: an unrequested
    poc_email=not_null predicate that the base prompt tells the model to add on
    'sent' queries (undercounts)."""
    f: Dict[str, Any] = {}
    poc_added = False
    for k, v in (raw or {}).items():
        key = str(k).lower()
        val = v[0] if isinstance(v, list) and v else v
        sval = str(val).strip().lower() if val is not None else None
        if key in ("client_name", "brand_name", "production_house"):
            if val and "client_name" not in f:
                f["client_name"] = str(val)
        elif key == "paid":
            if val is None or sval in _PAID_FALSE:
                f["paid"] = "no"
            elif sval in _PAID_TRUE:
                f["paid"] = "yes"
        elif key == "bill_sent":
            if val is None or sval in _PAID_FALSE:
                f["bill_sent"] = "no"
            elif sval in _SENT_TRUE:
                f["bill_sent"] = "yes"
        elif key == "poc_email":
            if val is None or sval in ("null", "is_null", "none", ""):
                f["poc_email"] = "null"
            else:
                f["poc_email"] = "not_null"
                poc_added = True
        elif key == "invoice_date":
            # The planner correctly maps "invoiced"/"raised" -> invoice_date IS
            # [NOT] NULL. In the seeded dataset invoice_date is set IFF bill_sent
            # is truthy (dataset.py), so this selects the SAME rows as bill_sent —
            # map it through so a correct invoice_date plan grades correctly
            # instead of being read as "no filter". setdefault: never override an
            # explicit bill_sent already present.
            s = (sval or "").replace(" ", "")
            if val is None or s in ("isnull", "null"):
                f.setdefault("bill_sent", "no")
            elif "notnull" in s or s in ("not_null", "any", "*"):
                f.setdefault("bill_sent", "yes")
    return f, poc_added


def _norm_metric(m: Optional[str]) -> Optional[str]:
    m = (m or "").strip().lower()
    return m if m in ("count", "sum", "avg") else None


def _plan_to_oracle(raw: Dict) -> Tuple[Dict, bool]:
    """Return (oracle_plan, poc_email_trap_flag) for grading."""
    filters, poc_added = _norm_filters(raw.get("filters") or {})
    tr = raw.get("time_range")
    if not (isinstance(tr, dict) and tr.get("value")):
        tr = None
    op = {
        "metric": _norm_metric(raw.get("metric")),
        "column": "fees",
        "filters": filters,
        "time_range": tr,
        "group_by": raw.get("group_by") if raw.get("group_by") == "client_name" else None,
        "order": raw.get("order") if raw.get("order") in ("asc", "desc") else None,
        "limit": raw.get("limit") if isinstance(raw.get("limit"), int) else None,
    }
    return op, poc_added


# ── Grade a computed answer against the gold answer ───────────────────────────
def _answers_match(got: Dict, gold: Dict) -> bool:
    if got.get("type") != gold.get("type"):
        return False
    t = gold["type"]
    if t in ("count", "money"):
        return got.get("value") == gold.get("value")
    if t == "client":
        return got.get("value") == gold.get("value") and got.get("amount") == gold.get("amount")
    if t == "ranking":
        return [r["client_name"] for r in got.get("value", [])] == \
               [r["client_name"] for r in gold.get("value", [])]
    if t == "list":
        return got.get("value") == gold.get("value") and \
               got.get("clients") == gold.get("clients")
    return got == gold


def _run_arm(gemini, message: str, kb_on: bool) -> Dict:
    os.environ["KNOWLEDGE_BOOK"] = "1" if kb_on else "0"
    raw = query_planner.build_operation_plan(
        message, "query", SCHEMA_DESCRIPTION.strip(), JOB_ENTRIES_COLUMNS,
        conversation_history=None, date_column="job_date", gemini_service=gemini,
    )
    if raw.get("_error"):
        return {"error": raw["_error"]}
    oplan, poc_trap = _plan_to_oracle(raw)
    ans = compute_answer(oplan, _ROWS)
    return {"raw_plan": raw, "oracle_plan": oplan, "answer": ans, "poc_trap": poc_trap}


def _fmt(a: Optional[Dict]) -> str:
    if not a or "type" not in a:
        return "ERR"
    t = a["type"]
    if t == "count":
        return f"count={a['value']}"
    if t == "money":
        return f"Rs{a['value']:,}"
    if t == "client":
        return f"top={a['value']}"
    if t == "ranking":
        return f"rank[{len(a['value'])}]"
    if t == "list":
        return f"list({a['value']}r/{len(a['clients'])}c)"
    return str(a)


def main(argv: List[str]) -> int:
    limit = None
    arms = ("off", "on")
    for i, a in enumerate(argv):
        if a == "--limit":
            limit = int(argv[i + 1])
        if a == "--arm":
            arms = (argv[i + 1],)

    if not (os.getenv("AI_KEY") or "").strip():
        print("AI_KEY not set. Run: AI_KEY=sk-or-... python -m knowledge.ab_run")
        return 2
    gemini = GeminiService()
    if not gemini._initialized:
        print("OpenRouter key failed to verify (expired/invalid?). Check the key.")
        return 2

    data = cases()
    if limit:
        data = data[:limit]

    results = []
    calls = 0
    off_ok = on_ok = 0
    off_trap = on_trap = 0
    print(f"Running {len(data)} cases x {len(arms)} arm(s) = {len(data)*len(arms)} planner calls\n")
    print(f"{'ID':<9} {'GOLD':<18} {'KB-OFF':<18} {'KB-ON':<18} VERDICT")
    print("-" * 92)

    for c in data:
        row: Dict[str, Any] = {"id": c["id"], "question": c["question"],
                               "gold": c["answer"], "tags": c["tags"]}
        for arm in arms:
            kb_on = arm == "on"
            # Retry on BOTH exceptions and planner _error (the LLM intermittently
            # returns truncated/malformed JSON, which build_operation_plan swallows
            # into {"error": ...}). Retrying de-noises the A/B so it measures KB
            # CONTENT, not JSON robustness — otherwise flaky truncations masquerade
            # as KB fixed/regressed.
            r = {"error": "no attempt"}
            for attempt in range(4):
                try:
                    r = _run_arm(gemini, c["question"], kb_on)
                    if not r.get("error"):
                        break
                except Exception as e:
                    r = {"error": str(e)[:120]}
                if attempt < 3:
                    time.sleep(1.5 * (attempt + 1))
            calls += 1
            ok = "answer" in r and _answers_match(r["answer"], c["answer"])
            row[arm] = {
                "ok": ok,
                "answer": r.get("answer"),
                "poc_trap": r.get("poc_trap", False),
                "error": r.get("error"),
                "raw_plan": r.get("raw_plan"),
            }
            if arm == "off":
                off_ok += ok
                off_trap += bool(r.get("poc_trap"))
            else:
                on_ok += ok
                on_trap += bool(r.get("poc_trap"))
            time.sleep(0.3)

        results.append(row)
        # Checkpoint after every case.
        with open(RESULTS_PATH, "w") as f:
            json.dump(results, f, indent=2)

        off_s = _fmt(row.get("off", {}).get("answer")) if "off" in row else "-"
        on_s = _fmt(row.get("on", {}).get("answer")) if "on" in row else "-"
        off_m = "✓" if row.get("off", {}).get("ok") else "✗"
        on_m = "✓" if row.get("on", {}).get("ok") else "✗"
        verdict = ""
        if "off" in row and "on" in row:
            if row["off"]["ok"] and not row["on"]["ok"]:
                verdict = "KB REGRESSED"
            elif not row["off"]["ok"] and row["on"]["ok"]:
                verdict = "KB FIXED"
        print(f"{c['id']:<9} {_fmt(c['answer']):<18} "
              f"{off_m} {off_s:<16} {on_m} {on_s:<16} {verdict}")

    n = len(results)
    print("-" * 92)
    print(f"\nRan {calls} planner calls over {n} cases.")
    if "off" in arms:
        print(f"  KB-OFF accuracy: {off_ok}/{n} ({off_ok/n*100:.0f}%)   "
              f"poc_email trap fired: {off_trap}")
    if "on" in arms:
        print(f"  KB-ON  accuracy: {on_ok}/{n} ({on_ok/n*100:.0f}%)   "
              f"poc_email trap fired: {on_trap}")
    if arms == ("off", "on"):
        delta = on_ok - off_ok
        fixed = sum(1 for r in results if r["off"]["ok"] is False and r["on"]["ok"] is True)
        regr = sum(1 for r in results if r["off"]["ok"] is True and r["on"]["ok"] is False)
        print(f"\n  NET: KB-ON {'+' if delta>=0 else ''}{delta} vs KB-OFF "
              f"(fixed {fixed}, regressed {regr})")
    print(f"\n  Full detail: {RESULTS_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
