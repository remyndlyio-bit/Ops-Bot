"""
The Intent_Test_Matrix corpus — 148 rows across 13 categories.

This is the sheet the Phase 4 plan always referred to; the 29-row CLAUDE.md
suite is a hand-copied subset of it. Loading the xlsx directly gives the
corpus ONE source of truth: edit the sheet, the run changes, no transcription
step to drift.

Reading the sheet correctly
---------------------------
The sheet encodes conversational state in prose, and getting this wrong
silently invalidates a run — the first draft of this module did, scoring the
bot as broken when the harness was:

1. **Parenthetical annotations are notes to a human tester, not input.**
   Row 11's message is `Yes (after confirmation shown)`. The user says "Yes";
   the rest describes the precondition. Sending the whole string makes the
   bot answer a question nobody asked. `send_text` strips it.

2. **`(after …)` means "continues from the previous row".** Those rows have
   no meaning standalone — row 67 is the single word "April", which is only
   an answer to row 66's month prompt. Every other row starts from a clean
   account.

3. **`(check PDF)` / `(check received email)` are not chat probes at all.**
   Six rows ask a human to open an artifact. They can't be graded from a
   reply and are reported MANUAL rather than counted as failures.

4. **Onboarding needs an un-onboarded account.** Those 8 rows test the
   first-run experience; a seeded profile has `onboarded_at` set. The
   previous live run (2026-07-29) marked all 8 SKIPPED for exactly this
   reason — it ran against a real, already-onboarded profile. A synthetic id
   makes them testable for the first time, so each carries an explicit
   setup path (below) that drives the conversation to the step under test.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Dict, List, Tuple

SHEET_PATH = os.path.join(os.path.dirname(__file__), "Intent_Test_Matrix.xlsx")
SHEET_NAME = "Test Matrix"

ONBOARDING = "Onboarding"

# Trailing "(...)" annotation, e.g. "Yes (after confirmation shown)".
_ANNOTATION = re.compile(r"\s*\(([^)]*)\)\s*$")

# Rows whose annotation asks a human to inspect an artifact — ungradeable
# from a chat reply.
_MANUAL_HINTS = ("check pdf", "check received email", "check email")

# Rows whose annotation asserts a DATA precondition the fixture may not meet
# ("no bank saved", "no March data"). They still run, but a failure may be
# the fixture rather than the bot, so the report says so instead of
# silently counting it against the product.
_PRECONDITION_HINTS = ("no bank", "no billing", "no poc_email", "no march")

# Onboarding setup paths. Explicit for these 8 rows rather than inferred:
# the flow is short, the inference rules that would cover it are fragile,
# and being wrong here invalidates the whole category. Each entry is the
# messages to send BEFORE the row's own message, on a fresh account.
_ONBOARDING_SETUP: Dict[int, Tuple[str, ...]] = {
    1: (),                                  # the very first message
    2: ("Hi",),                             # at the name step
    3: ("Hi",),
    4: ("Hi",),
    5: ("Hi",),
    # The implemented flow is name → EMAIL → company, not the name → company
    # the sheet assumes. Answering the email step with "skip" completes
    # onboarding outright (skip reuses the name as the company), so these
    # two must supply a real address to reach the company step at all.
    6: ("Hi", "Darshit", "darshit@example.com"),
    7: ("Hi", "Darshit", "darshit@example.com"),
    8: ("Hi",),                             # Hindi name, at the name step
}


@dataclass(frozen=True)
class MatrixRow:
    num: int
    category: str
    scenario: str
    message: str
    expected: str
    language: str
    notes: str = ""

    @property
    def annotation(self) -> str:
        m = _ANNOTATION.search(self.message)
        return (m.group(1) if m else "").strip().lower()

    @property
    def send_text(self) -> str:
        """What the user actually types — annotation removed."""
        return _ANNOTATION.sub("", self.message).strip()

    @property
    def is_manual(self) -> bool:
        """Not gradeable by sending a chat message.

        Two shapes, and the second one cost a whole run to notice:

        * an artifact check — `(check PDF)`, `(check received email)`;
        * a row that is ONLY an annotation — `(click Send button)`,
          `(generate invoice on Telegram)`, `(unpaid invoice 15+ days old)`.
          These describe a cron-fired reminder, a button tap, or a
          platform-specific delivery: events that never enter through
          process_request at all. Stripping the annotation leaves an empty
          string, and sending "" falls through to a default query — which is
          why 14 such rows all returned the same "Found 15 results" and were
          scored as product failures. 20 rows are this shape.
        """
        if any(h in self.annotation for h in _MANUAL_HINTS):
            return True
        return not self.send_text.strip()

    @property
    def continues_previous(self) -> bool:
        """`(after …)` — needs the previous row's state, so no reset."""
        return self.annotation.startswith("after")

    @property
    def has_data_precondition(self) -> bool:
        return any(h in self.annotation for h in _PRECONDITION_HINTS)

    @property
    def needs_fresh_account(self) -> bool:
        return self.category == ONBOARDING

    @property
    def setup_messages(self) -> Tuple[str, ...]:
        """Messages to replay before this row, on a fresh account."""
        if self.category == ONBOARDING:
            return _ONBOARDING_SETUP.get(self.num, ("Hi",))
        return ()


def load(path: str = SHEET_PATH) -> List[MatrixRow]:
    """Every populated row of the Test Matrix sheet, in sheet order."""
    import openpyxl

    wb = openpyxl.load_workbook(path, data_only=True)
    ws = wb[SHEET_NAME]

    rows: List[MatrixRow] = []
    for raw in ws.iter_rows(min_row=2, values_only=True):
        num, category, scenario, message, expected, language = raw[:6]
        notes = raw[7] if len(raw) > 7 else None
        if num is None or not message:
            continue
        rows.append(MatrixRow(
            num=int(num),
            category=str(category or "").strip(),
            scenario=str(scenario or "").strip(),
            message=str(message).strip(),
            expected=str(expected or "").strip(),
            language=str(language or "EN").strip(),
            notes=str(notes or "").strip(),
        ))
    return rows


def by_category(rows: List[MatrixRow]) -> Dict[str, List[MatrixRow]]:
    out: Dict[str, List[MatrixRow]] = {}
    for r in rows:
        out.setdefault(r.category, []).append(r)
    return out
