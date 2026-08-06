"""
LLM-as-judge grading for the Intent_Test_Matrix run.

The sheet states expectations in prose ("Re-prompts for name, doesn't save
'Hello'"), so a mechanical assertion isn't available. A judge model reads
the expected behaviour and the actual reply and returns PASS / FAIL /
UNCLEAR.

Design constraints, learned from how this repo has been bitten before:

* **UNCLEAR is a first-class verdict, not a rounded-down FAIL.** A judge
  forced into a binary invents confidence. Rows it can't decide should land
  in front of a human rather than quietly inflating either number.

* **A judge error is never a PASS.** Any exception, unparseable reply, or
  missing verdict degrades to UNCLEAR with the reason attached. The failure
  mode to avoid at all costs is a broken grader reading green — the same
  shape as the skip-guard bug that kept 11 tests permanently red and the
  empty-payload bug that would have wiped the baseline.

* **The judge sees only the reply text.** Not the operation name, not the
  SQL. The sheet describes user-visible behaviour, and grading against
  internals would let a technically-correct-but-unhelpful reply pass.
"""

from __future__ import annotations

import json
import re
from typing import Dict, Optional

PASS = "PASS"
FAIL = "FAIL"
UNCLEAR = "UNCLEAR"

_PROMPT = """You are grading a single turn of a chatbot test suite.

The bot ("Remyndly") helps freelancers log jobs, generate invoices, and track
payments over WhatsApp/Telegram.

TEST SCENARIO: {scenario}
USER MESSAGE: {message}
EXPECTED BEHAVIOUR: {expected}

ACTUAL BOT REPLY:
\"\"\"
{actual}
\"\"\"

Decide whether the actual reply satisfies the expected behaviour.

Grade on SUBSTANCE, not wording. Different phrasing, extra helpful detail, a
different but valid number format, or a friendlier tone are all fine. What
matters is whether the reply does what the expectation describes.

Answer FAIL when the reply contradicts the expectation, ignores the request,
errors out, asks for something it was just given, or answers a different
question.

Answer UNCLEAR only when the expectation is too vague to judge, or when
deciding would require data you cannot see.

Reply with ONLY a JSON object:
{{"verdict": "PASS" | "FAIL" | "UNCLEAR", "reason": "<one short sentence>"}}"""


_VERDICT_RE = re.compile(r'"verdict"\s*:\s*"?(PASS|FAIL|UNCLEAR)"?', re.I)
_REASON_RE = re.compile(r'"reason"\s*:\s*"(.*?)(?:"|$)', re.I | re.DOTALL)


def _extract_json(text: str) -> Optional[dict]:
    """Parse the judge's reply into {"verdict", "reason"}.

    Three levels, because each stricter one was observed to fail on real
    output:

    1. plain json.loads;
    2. the first {...} block, for replies wrapped in prose or fences;
    3. field-level regex.

    Level 3 is not paranoia. gemini-2.5-flash truncates at the token limit
    mid-object, producing output with no closing brace:

        {"verdict": "PASS", "reason": "The bot correctly re-prompts...

    Levels 1 and 2 both reject that, so a verdict the model plainly reached
    was being discarded as UNCLEAR — the grader silently losing decisions,
    which is the failure mode this module exists to avoid. Raising the token
    budget did NOT fix it (the same input truncated at 1500, 6000 and
    12000); reading the fields directly does.
    """
    if not text:
        return None
    try:
        data = json.loads(text)
        if isinstance(data, dict):
            return data
    except Exception:
        pass

    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        try:
            data = json.loads(match.group(0))
            if isinstance(data, dict):
                return data
        except Exception:
            pass

    verdict = _VERDICT_RE.search(text)
    if not verdict:
        return None
    reason = _REASON_RE.search(text)
    return {
        "verdict": verdict.group(1).upper(),
        "reason": (reason.group(1).strip() if reason else "(reason truncated)"),
    }


def grade(gemini, scenario: str, message: str, expected: str,
          actual: str, votes: int = 2) -> Dict[str, str]:
    """-> {"verdict": PASS|FAIL|UNCLEAR, "reason": str}

    `gemini` is a GeminiService. Never raises: every failure path becomes
    UNCLEAR, because a grader that errors into PASS would silently certify
    a broken bot.

    `votes` exists because this judge is NOT deterministic even at
    temperature 0. Re-grading a completed run — identical stored replies,
    same prompt — flipped 9 of 128 rows, in both directions (5 FAIL→PASS,
    4 PASS→FAIL). Two rows with byte-identical bot replies were graded FAIL
    and PASS in the same run. That is roughly ±5 percentage points of noise
    on every category number, which is wider than most of the differences
    anyone would want to read from them.

    So: grade twice and require agreement, with a third call only as a
    tiebreak. Most rows agree on the first two, so the typical cost is
    ~1.1x rather than 2x, and a disagreeing tiebreak that lands on neither
    prior verdict degrades to UNCLEAR rather than guessing.
    """
    if not (actual or "").strip():
        return {"verdict": FAIL, "reason": "bot returned an empty reply"}

    prompt = _PROMPT.format(
        scenario=scenario or "(none given)",
        message=message,
        expected=expected or "(none given)",
        actual=actual[:2000],
    )

    if votes <= 1:
        return _grade_once(gemini, prompt)

    first = _grade_once(gemini, prompt)
    second = _grade_once(gemini, prompt)
    if first["verdict"] == second["verdict"]:
        return first

    # Disagreement — one tiebreak call decides, and only if it matches one
    # of the two. Otherwise three calls found three answers, which is
    # genuinely UNCLEAR and should reach a human rather than be rounded.
    third = _grade_once(gemini, prompt)
    for candidate in (first, second):
        if third["verdict"] == candidate["verdict"]:
            return {"verdict": third["verdict"],
                    "reason": f"{third['reason']} [2 of 3 votes]"}
    return {
        "verdict": UNCLEAR,
        "reason": (f"judge disagreed with itself across 3 votes "
                   f"({first['verdict']}/{second['verdict']}/{third['verdict']}): "
                   f"{first['reason']}"),
    }


def _grade_once(gemini, prompt: str) -> Dict[str, str]:
    """A single judge call, with the truncation retry."""
    # gemini-2.5-flash spends max_tokens on REASONING before it emits any
    # content, and how much it spends scales with the length of the reply
    # being judged. A 500-token cap returned the single character "{"; 1500
    # still truncated the longer onboarding replies. Both cases threw away a
    # verdict the judge had actually reached and scored it UNCLEAR — the
    # grader quietly losing decisions.
    #
    # So: retry once with a much larger budget. The retry only fires on a
    # parse failure, so the common path stays cheap.
    raw = ""
    for budget in (1500, 6000):
        try:
            raw = gemini._call_api(prompt, {"temperature": 0,
                                            "maxOutputTokens": budget,
                                            "responseMimeType": "application/json"})
        except Exception as e:
            return {"verdict": UNCLEAR, "reason": f"judge call failed: {e}"}

        data = _extract_json(raw or "")
        if isinstance(data, dict):
            break
    else:
        return {"verdict": UNCLEAR,
                "reason": f"judge returned unparseable output: {(raw or '')[:120]}"}

    if not isinstance(data, dict):
        return {"verdict": UNCLEAR,
                "reason": f"judge returned unparseable output: {(raw or '')[:120]}"}

    verdict = str(data.get("verdict", "")).strip().upper()
    if verdict not in (PASS, FAIL, UNCLEAR):
        return {"verdict": UNCLEAR, "reason": f"judge returned unknown verdict {verdict!r}"}

    return {"verdict": verdict, "reason": str(data.get("reason", "")).strip()[:300]}
