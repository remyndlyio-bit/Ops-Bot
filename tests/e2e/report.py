"""
TODO.md Phase 4.3 — score tracking.

"The runner writes `tests/e2e/last_run.json`: per-scenario pass/fail +
overall %. Committing it on each run gives a pass-rate history in git log.
Fail the run (exit nonzero) if the pass rate drops below the previous
committed run — regressions become impossible to miss."

The logic lives here as plain functions rather than inside the pytest
hooks, so it can be unit-tested offline in normal CI. A regression gate
that has itself never been tested is worth very little: if `evaluate()`
silently returned "no regression" for every input, the whole mechanism
would read green forever while quality fell.

The most important safety property is in `should_write()`: a run in which
NO live scenarios executed must NOT overwrite the artifact. Every ordinary
CI run skips the live suite, so without that guard the first such run would
overwrite a real baseline with zero results — destroying the committed
history AND resetting the bar to nothing, which is the exact opposite of
what a regression gate is for.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional, Tuple

# Populated by the live runner as each scenario completes.
RESULTS: List[Dict[str, Any]] = []

ARTIFACT = os.path.join(os.path.dirname(__file__), "last_run.json")


def record(entry: Dict[str, Any]) -> None:
    RESULTS.append(entry)


def reset() -> None:
    RESULTS.clear()


def build_payload(results: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    """Shape written to last_run.json. `scenarios` is kept per-scenario (not
    just a total) so a diff of two committed runs shows WHICH scenario
    changed, which is the actually useful signal in a git history."""
    rows = RESULTS if results is None else results
    passed = sum(1 for r in rows if r.get("passed"))
    total = len(rows)
    return {
        "total": total,
        "passed": passed,
        "failed": total - passed,
        "pass_rate": round(passed / total, 4) if total else 0.0,
        "scenarios": rows,
    }


def should_write(payload: Dict[str, Any]) -> bool:
    """Only write when live scenarios actually ran.

    Guards the destructive case described in the module docstring: an
    ordinary `pytest tests/` run skips every live scenario, so payload
    would be an empty 0/0 result. Writing that would clobber the committed
    baseline and reset the regression bar to zero.
    """
    return bool(payload.get("total"))


def should_persist(regressed: bool) -> bool:
    """Whether this run may overwrite the committed baseline.

    Only a non-regressed run may. Writing on EVERY run ratchets the bar
    downward: a regressed run becomes the new baseline, the next run
    compares against the degraded number, sees no drop and passes — the
    gate catches a regression exactly once then accepts it forever. So
    last_run.json means "best known good".
    """
    return not regressed


def read_baseline(path: str = ARTIFACT) -> Optional[Dict[str, Any]]:
    """The previously committed run, or None on the first ever run / an
    unreadable file. A corrupt artifact must not crash the suite — it just
    means there is no baseline to compare against."""
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def write(payload: Dict[str, Any], path: str = ARTIFACT) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, sort_keys=False)
        fh.write("\n")


def newly_failing(baseline: Optional[Dict[str, Any]],
                  current: Dict[str, Any]) -> List[int]:
    """Scenario ids that passed in the baseline but fail now. This is what
    a human actually wants named when the gate trips — a pass-rate delta
    alone doesn't say what broke."""
    if not baseline:
        return []
    was_ok = {s.get("id") for s in baseline.get("scenarios", []) if s.get("passed")}
    now_bad = {s.get("id") for s in current.get("scenarios", []) if not s.get("passed")}
    return sorted(i for i in (was_ok & now_bad) if i is not None)


def evaluate(baseline: Optional[Dict[str, Any]],
             current: Dict[str, Any]) -> Tuple[bool, str]:
    """(regressed, human-readable summary).

    Regression is a DROP in pass rate against the previous committed run.
    Equal is fine — the gate is there to stop backsliding, not to demand
    monotonic improvement, and treating "no change" as failure would make
    every unrelated PR red.
    """
    rate = current.get("pass_rate", 0.0)
    total = current.get("total", 0)
    passed = current.get("passed", 0)
    head = f"{passed}/{total} scenarios passed ({rate:.1%})"

    if not baseline:
        return False, f"{head} — no previous run to compare against (baseline established)"

    prev = baseline.get("pass_rate", 0.0)
    if rate < prev:
        broke = newly_failing(baseline, current)
        detail = f" — newly failing: {broke}" if broke else ""
        return True, (
            f"REGRESSION: {head}, down from {prev:.1%}{detail}"
        )
    if rate > prev:
        return False, f"{head} — improved from {prev:.1%}"
    return False, f"{head} — unchanged from {prev:.1%}"
