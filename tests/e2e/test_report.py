"""
Offline tests for tests/e2e/report.py — NOT marked live, so they run in
normal CI.

The Phase 4.3 gate only has value if it actually trips. An `evaluate()`
that returned "no regression" for every input would read green forever
while quality fell — the failure mode is silent and indefinite, which is
exactly the kind this repo has been bitten by before (the live-LLM skip
guard, the unguarded synthesis sites).

The `should_write` cases matter most: every ordinary `pytest tests/` run
skips the live suite, so if an empty result set were allowed to overwrite
the artifact, the FIRST such run would wipe the committed history and reset
the bar to zero.
"""

import json
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import pytest

from tests.e2e import report


def _rows(*flags):
    """(id, passed) pairs -> scenario records."""
    return [{"id": i + 1, "passed": bool(f)} for i, f in enumerate(flags)]


def _payload(*flags):
    return report.build_payload(_rows(*flags))


class TestBuildPayload:
    def test_counts_and_rate(self):
        p = _payload(1, 1, 0, 1)
        assert (p["total"], p["passed"], p["failed"]) == (4, 3, 1)
        assert p["pass_rate"] == 0.75

    def test_empty_is_zero_not_a_crash(self):
        p = report.build_payload([])
        assert p["total"] == 0 and p["pass_rate"] == 0.0

    def test_keeps_per_scenario_detail(self):
        """A total alone can't tell you WHICH scenario changed between two
        committed runs — the per-scenario rows are the useful diff."""
        assert len(_payload(1, 0)["scenarios"]) == 2

    def test_is_json_serialisable(self):
        json.dumps(_payload(1, 0))


class TestShouldWrite:
    def test_writes_when_scenarios_ran(self):
        assert report.should_write(_payload(1, 0)) is True

    def test_does_not_write_an_empty_run(self):
        """THE critical guard: an ordinary CI run skips every live scenario.
        Writing that 0/0 result would clobber the committed baseline and
        reset the regression bar to nothing."""
        assert report.should_write(report.build_payload([])) is False


class TestEvaluate:
    def test_no_baseline_establishes_one_without_failing(self):
        regressed, msg = report.evaluate(None, _payload(1, 0))
        assert regressed is False
        assert "baseline established" in msg

    def test_drop_is_a_regression(self):
        regressed, msg = report.evaluate(_payload(1, 1), _payload(1, 0))
        assert regressed is True
        assert "REGRESSION" in msg

    def test_equal_is_not_a_regression(self):
        """The gate stops backsliding; it doesn't demand monotonic
        improvement. Treating 'unchanged' as failure would make every
        unrelated PR red."""
        regressed, msg = report.evaluate(_payload(1, 0), _payload(1, 0))
        assert regressed is False
        assert "unchanged" in msg

    def test_improvement_is_not_a_regression(self):
        regressed, msg = report.evaluate(_payload(1, 0), _payload(1, 1))
        assert regressed is False
        assert "improved" in msg

    def test_regression_message_names_the_newly_failing_scenarios(self):
        """A pass-rate delta alone doesn't say what broke."""
        base = report.build_payload([{"id": 1, "passed": True}, {"id": 2, "passed": True}])
        cur = report.build_payload([{"id": 1, "passed": True}, {"id": 2, "passed": False}])
        regressed, msg = report.evaluate(base, cur)
        assert regressed is True and "[2]" in msg

    def test_same_rate_different_scenarios_is_not_flagged(self):
        """One fixed, one broken nets to the same rate. Deliberately NOT a
        regression by pass-rate — documented here so the behaviour is a
        choice rather than an accident. newly_failing() still exposes it."""
        base = report.build_payload([{"id": 1, "passed": True}, {"id": 2, "passed": False}])
        cur = report.build_payload([{"id": 1, "passed": False}, {"id": 2, "passed": True}])
        regressed, _ = report.evaluate(base, cur)
        assert regressed is False
        assert report.newly_failing(base, cur) == [1]


class TestNewlyFailing:
    def test_identifies_regressed_ids(self):
        base = report.build_payload([{"id": 1, "passed": True}, {"id": 2, "passed": True}])
        cur = report.build_payload([{"id": 1, "passed": False}, {"id": 2, "passed": True}])
        assert report.newly_failing(base, cur) == [1]

    def test_ignores_already_failing(self):
        base = report.build_payload([{"id": 1, "passed": False}])
        cur = report.build_payload([{"id": 1, "passed": False}])
        assert report.newly_failing(base, cur) == []

    def test_empty_without_a_baseline(self):
        assert report.newly_failing(None, _payload(0)) == []


class TestBaselineIO:
    def test_round_trip(self, tmp_path):
        path = str(tmp_path / "last_run.json")
        payload = _payload(1, 1, 0)
        report.write(payload, path)
        assert report.read_baseline(path)["pass_rate"] == payload["pass_rate"]

    def test_missing_file_is_no_baseline(self, tmp_path):
        assert report.read_baseline(str(tmp_path / "nope.json")) is None

    def test_corrupt_file_is_no_baseline_not_a_crash(self, tmp_path):
        """A mangled artifact must degrade to 'no baseline', never take the
        whole suite down with it."""
        path = tmp_path / "last_run.json"
        path.write_text("{not json at all")
        assert report.read_baseline(str(path)) is None

    def test_non_dict_json_is_no_baseline(self, tmp_path):
        path = tmp_path / "last_run.json"
        path.write_text("[1, 2, 3]")
        assert report.read_baseline(str(path)) is None


class TestHooksAreInAConftest:
    """Regression guard for a real mistake: the session hooks were first
    written inside test_scenarios.py, where pytest never calls them — the
    artifact was silently never written and the gate never ran. Hooks must
    stay in a conftest."""

    def test_conftest_defines_the_session_hooks(self):
        path = os.path.join(os.path.dirname(__file__), "conftest.py")
        with open(path, encoding="utf-8") as fh:
            src = fh.read()
        assert "def pytest_sessionfinish" in src
        assert "def pytest_sessionstart" in src

    def test_runner_module_does_not_define_them(self):
        path = os.path.join(os.path.dirname(__file__), "test_scenarios.py")
        with open(path, encoding="utf-8") as fh:
            src = fh.read()
        assert "def pytest_sessionfinish" not in src, (
            "session hooks in a test module are never called by pytest"
        )


class TestShouldPersist:
    """The downward-ratchet guard. Without it the gate catches a regression
    exactly once and then silently accepts the lower bar forever — this bit
    the verification of the feature itself: two consecutive local runs of a
    regressed suite and the second reported 'unchanged'."""

    def test_persists_a_clean_run(self):
        assert report.should_persist(regressed=False) is True

    def test_does_not_persist_a_regressed_run(self):
        assert report.should_persist(regressed=True) is False
