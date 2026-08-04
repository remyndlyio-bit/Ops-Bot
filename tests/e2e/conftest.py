"""
Phase 4.3 hooks — score tracking for the live e2e suite.

These live in a conftest because that is the ONLY place pytest collects
session hooks from. An earlier draft defined `pytest_sessionfinish` inside
`test_scenarios.py`, where pytest never calls it — the artifact was silently
never written and the regression gate never ran. Verified with a scratch
project before moving it here.

Scoped to tests/e2e/ so nothing here affects the rest of the suite.
"""

from __future__ import annotations

from tests.e2e import report


def pytest_sessionstart(session):
    """Capture the previously committed run BEFORE the artifact is
    overwritten — that snapshot is the bar the current run must clear."""
    session.config._e2e_baseline = report.read_baseline()
    report.reset()


def pytest_sessionfinish(session, exitstatus):
    """Write last_run.json and fail the session on a pass-rate drop."""
    payload = report.build_payload()

    if not report.should_write(payload):
        # No live scenarios ran (the normal case — they need --live plus
        # real credentials). Leave the committed baseline untouched.
        return

    baseline = getattr(session.config, "_e2e_baseline", None)
    regressed, summary = report.evaluate(baseline, payload)

    # Only overwrite the artifact when the run did NOT regress.
    #
    # Writing on every run ratchets the bar DOWNWARD: a regressed run
    # becomes the new baseline, so the next run compares against the
    # degraded number, sees no drop, and passes. The gate would catch a
    # regression exactly once and then silently accept it forever. (This
    # bit the verification of this very feature — two consecutive local
    # runs and the second reported "unchanged".)
    #
    # So last_run.json means "best known good". A regression leaves it
    # alone; the failing detail is in the terminal summary and the test
    # output, and the non-zero exit stops CI from proceeding.
    if report.should_persist(regressed):
        report.write(payload)

    reporter = session.config.pluginmanager.get_plugin("terminalreporter")
    if reporter is not None:
        reporter.write_line("")
        reporter.write_line(f"[E2E SCORE] {summary}",
                            red=regressed, green=not regressed)
        reporter.write_line(
            f"[E2E SCORE] wrote {report.ARTIFACT}" if not regressed
            else "[E2E SCORE] baseline left unchanged (a regression must not lower the bar)"
        )

    if regressed:
        # Non-zero exit even if pytest itself would have passed, so a drop
        # can't be missed in CI. pytest's own failure status is preserved
        # when it already failed.
        session.exitstatus = 1
