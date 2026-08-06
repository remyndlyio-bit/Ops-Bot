"""
Offline guards for the Intent_Test_Matrix harness — NOT marked live, so they
run in normal CI.

Every test here corresponds to a defect that actually shipped and silently
corrupted a live run. That is the point: each of these bugs made the harness
report the BOT as broken when the harness was, and none of them announced
itself — the run completed, produced a plausible-looking spreadsheet, and
was wrong.

  * annotations sent as user input     → the bot answered a question nobody asked
  * annotation-only rows sent as ""    → 14 rows returned an identical default reply
  * truncated judge JSON               → verdicts the model reached, discarded as UNCLEAR
"""

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import pytest

from tests.e2e import grade
from tests.e2e.matrix import MatrixRow


def _row(message, num=1, category="Job Entry", scenario="s", expected="e"):
    return MatrixRow(num=num, category=category, scenario=scenario,
                     message=message, expected=expected, language="EN")


class TestAnnotationStripping:
    """The sheet's parentheticals are notes to a human tester. Sending them
    verbatim makes the bot answer something nobody asked."""

    def test_annotation_is_removed_from_the_sent_text(self):
        assert _row("Yes (after confirmation shown)").send_text == "Yes"

    def test_plain_message_is_untouched(self):
        assert _row("Show my last 5 jobs").send_text == "Show my last 5 jobs"

    def test_parenthetical_inside_the_message_is_kept(self):
        """Only a TRAILING annotation is metadata. A parenthetical in the
        middle is part of what the user typed."""
        r = _row("Add job for Nike (rush) 10 April 2500")
        assert r.send_text == "Add job for Nike (rush) 10 April 2500"

    def test_annotation_is_exposed_for_the_report(self):
        assert _row("April (after month prompt)").annotation == "after month prompt"


class TestStateHints:
    def test_after_means_continue_from_previous_row(self):
        assert _row("Yes (after confirmation shown)").continues_previous is True

    def test_plain_row_starts_fresh(self):
        assert _row("Show my jobs").continues_previous is False

    def test_data_precondition_is_flagged_not_silently_failed(self):
        """`(no bank saved)` asserts something about the FIXTURE. If the
        fixture disagrees, the failure is ours, not the bot's — the report
        has to say so rather than count it as a product defect."""
        assert _row("Generate invoice for Nike April (no bank saved)").has_data_precondition


class TestManualRows:
    """The defect that cost a whole run: rows that are ONLY an annotation
    strip to "", and sending "" falls through to a default query. Fourteen
    rows returned the same 'Found 15 results' and were scored as failures."""

    @pytest.mark.parametrize("message", [
        "(click Send button on reminder)",
        "(generate invoice on Telegram)",
        "(unpaid invoice 15+ days old)",
        "(check PDF)",
        "(check received email)",
    ])
    def test_non_chat_rows_are_manual(self, message):
        assert _row(message).is_manual is True

    def test_a_real_message_is_not_manual(self):
        assert _row("Show my jobs").is_manual is False

    def test_manual_rows_never_produce_an_empty_send(self):
        """The invariant that matters: nothing empty may ever reach
        process_request."""
        from tests.e2e.matrix import load
        for row in load():
            if not row.is_manual:
                assert row.send_text.strip(), (
                    f"row {row.num} would send an empty message: {row.message!r}"
                )


class TestJudgeParsing:
    """A grader that loses verdicts is worse than no grader: it reports a
    confident number that is quietly wrong."""

    def test_plain_json(self):
        d = grade._extract_json('{"verdict": "PASS", "reason": "ok"}')
        assert d["verdict"] == "PASS"

    def test_json_wrapped_in_prose_or_fences(self):
        d = grade._extract_json('Here you go:\n```json\n{"verdict": "FAIL", "reason": "no"}\n```')
        assert d["verdict"] == "FAIL"

    def test_truncated_object_still_yields_its_verdict(self):
        """THE regression. gemini-2.5-flash truncates mid-object at the
        token limit, leaving no closing brace. Raising the budget did not
        help (observed at 1500, 6000 AND 12000), so the parser has to cope:

            {"verdict": "PASS", "reason": "The bot correctly re-prompts...

        Before the fix this scored UNCLEAR and the real verdict was lost."""
        raw = '{\n  "verdict": "PASS",\n  "reason": "The bot correctly re-prompts for the name'
        d = grade._extract_json(raw)
        assert d is not None, "truncated judge output must still parse"
        assert d["verdict"] == "PASS"
        assert "re-prompts" in d["reason"]

    def test_truncated_before_any_verdict_is_unparseable(self):
        """Honest failure: if the verdict itself never arrived, there is
        nothing to recover and UNCLEAR is correct."""
        assert grade._extract_json("{\n  ") is None

    def test_garbage_is_none(self):
        assert grade._extract_json("no json here at all") is None

    def test_empty_is_none(self):
        assert grade._extract_json("") is None


class TestGradeNeverErrorsIntoPass:
    """A broken grader must never certify a broken bot."""

    class _Boom:
        def _call_api(self, *a, **kw):
            raise RuntimeError("network down")

    class _Garbage:
        def _call_api(self, *a, **kw):
            return "I could not decide."

    def test_judge_exception_is_unclear_not_pass(self):
        out = grade.grade(self._Boom(), "s", "m", "e", "some reply")
        assert out["verdict"] == grade.UNCLEAR
        assert "network down" in out["reason"]

    def test_unparseable_judge_is_unclear_not_pass(self):
        out = grade.grade(self._Garbage(), "s", "m", "e", "some reply")
        assert out["verdict"] == grade.UNCLEAR

    def test_empty_bot_reply_is_a_fail_without_calling_the_judge(self):
        """An empty reply is a product failure on its face; spending a paid
        call to confirm it would be waste."""
        out = grade.grade(self._Boom(), "s", "m", "e", "   ")
        assert out["verdict"] == grade.FAIL


class TestVoting:
    """The judge is not deterministic at temperature 0: re-grading a
    finished run on identical stored text flipped 9 of 128 rows in both
    directions. Voting is what makes a reported number mean something."""

    class _Scripted:
        """Returns a scripted verdict per call, so vote logic is testable
        without the network."""
        def __init__(self, *verdicts):
            self.verdicts = list(verdicts)
            self.calls = 0

        def _call_api(self, *a, **kw):
            v = self.verdicts[min(self.calls, len(self.verdicts) - 1)]
            self.calls += 1
            return '{"verdict": "%s", "reason": "r%d"}' % (v, self.calls)

    def test_two_agreeing_votes_settle_it_without_a_third_call(self):
        g = self._Scripted("PASS", "PASS", "FAIL")
        assert grade.grade(g, "s", "m", "e", "reply")["verdict"] == "PASS"
        assert g.calls == 2, "a third call is waste when the first two agree"

    def test_disagreement_is_broken_by_a_third_vote(self):
        g = self._Scripted("PASS", "FAIL", "FAIL")
        out = grade.grade(g, "s", "m", "e", "reply")
        assert out["verdict"] == "FAIL"
        assert g.calls == 3
        assert "2 of 3" in out["reason"]

    def test_three_way_disagreement_is_unclear_not_a_guess(self):
        """Three calls, three answers: the honest output is 'I don't know',
        not whichever one happened to come first."""
        g = self._Scripted("PASS", "FAIL", "UNCLEAR")
        out = grade.grade(g, "s", "m", "e", "reply")
        assert out["verdict"] == grade.UNCLEAR
        assert "disagreed with itself" in out["reason"]

    def test_votes_1_makes_a_single_call(self):
        g = self._Scripted("PASS", "FAIL")
        assert grade.grade(g, "s", "m", "e", "reply", votes=1)["verdict"] == "PASS"
        assert g.calls == 1
