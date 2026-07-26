"""
Per-turn telemetry — WP-0 of ASSISTANT_PLAN.md.

One structured log line per `process_request` call, so we can establish the
baseline (p50/p95 turn_ms, llm_calls/turn, fallback rate) that every later
work package (Understand v2, latency) has to beat.

Uses a contextvar rather than a thread-local so it works correctly whether the
webhook handler runs sync, in a thread pool, or under asyncio — a contextvar is
isolated per logical "turn" however it's scheduled, where a thread-local would
leak across requests reusing the same worker thread.

Deliberately fails soft everywhere: telemetry must never break a user-facing
turn. If logging itself raises, we swallow it.
"""
import time
import contextvars
from typing import Optional

from utils.logger import logger

# Number of LLM calls made so far in the current turn. None outside a turn
# (e.g. offline scripts, tests that call GeminiService directly) — callers
# check for None rather than assuming 0, so we never fabricate a call count
# for code that isn't actually inside a tracked turn.
_llm_call_count: contextvars.ContextVar[Optional[int]] = contextvars.ContextVar(
    "llm_call_count", default=None
)

# Substrings that mark a reply as a generic fallback rather than a real
# answer. Kept here (not imported from intent_service) so telemetry has no
# dependency on the module it's instrumenting — avoids import cycles and lets
# this module be reused for other entry points later (e.g. worker replies).
_FALLBACK_MARKERS = (
    "couldn't format the reply",
    "couldn't quite work out",
    "two ways i could read",
    "i didn't quite get that",
    "not sure what you mean",
    "could you rephrase",
    "could you specify",
)


def start_turn() -> None:
    """Call once at the top of a tracked turn (process_request entry).

    Idempotent w.r.t. nesting: several call sites re-enter process_request
    synchronously to resume a gated flow after the user supplies a missing
    field (_resume_invoice_flow and friends). That's one logical turn from
    the caller's perspective — resetting the counter partway through would
    undercount the outer turn's real llm_calls. Only initialise if no turn
    is currently active."""
    if _llm_call_count.get() is None:
        _llm_call_count.set(0)


def note_llm_call() -> None:
    """Call from GeminiService._call_api (or any other LLM entry point) each
    time a real API call is made. No-op outside a tracked turn."""
    n = _llm_call_count.get()
    if n is not None:
        _llm_call_count.set(n + 1)


def current_llm_calls() -> Optional[int]:
    return _llm_call_count.get()


def is_fallback_response(response_text: str) -> bool:
    if not response_text:
        return False
    low = response_text.lower()
    return any(marker in low for marker in _FALLBACK_MARKERS)


def log_turn(*, user_id: str, turn_ms: float, operation: Optional[str],
             fallback: bool, llm_calls: Optional[int] = None,
             error: Optional[str] = None, **extra) -> None:
    """Emit the one structured line WP-0 dashboards are built from. Extra
    fields (verdict_intent, verdict_confidence, route, ...) are added by later
    work packages without changing this function's contract.

    llm_calls is passed in explicitly by the caller (Turn reads it from
    current_llm_calls() before resetting the counter) rather than this
    function reaching into contextvar state itself — keeps log_turn a pure
    function of its arguments, which is what makes it straightforward to
    assert against in tests instead of needing to fake ambient state."""
    try:
        parts = [
            f"turn_ms={turn_ms:.0f}",
            f"llm_calls={llm_calls if llm_calls is not None else '?'}",
            f"operation={operation or 'none'}",
            f"fallback={fallback}",
        ]
        for k, v in extra.items():
            parts.append(f"{k}={v}")
        if error:
            parts.append(f"error={error!r}")
        # user_id last — keeps the fixed-shape fields grep-able at a stable
        # prefix regardless of how long the id is.
        parts.append(f"user={user_id}")
        logger.info("[TELEMETRY] " + " ".join(parts))
    except Exception as e:  # telemetry must never break a turn
        try:
            logger.warning(f"[TELEMETRY] logging failed (non-fatal): {e}")
        except Exception:
            pass


class Turn:
    """Context manager wrapping one process_request call.

        with Turn(user_id) as t:
            result = ...
            t.operation = result.get("operation")
            t.response_text = result.get("response")

    Logs on both normal exit and exception (re-raises after logging — this
    is observability, not error handling).

    Reentrant: process_request calls itself synchronously to resume a gated
    flow (e.g. after the user supplies a missing invoice field). Only the
    OUTERMOST Turn logs — a nested one leaves the shared llm_calls counter
    running and lets the outer instance's own operation/response_text
    (already the final, propagated result) do the reporting. Without this,
    one user-visible turn would emit two telemetry lines and the outer line
    would undercount llm_calls (reset to 0 by the inner start_turn()).
    """

    def __init__(self, user_id: str):
        self.user_id = user_id
        self.operation: Optional[str] = None
        self.response_text: str = ""
        self._t0 = 0.0
        self._is_nested = False

    def __enter__(self) -> "Turn":
        self._t0 = time.monotonic()
        self._is_nested = current_llm_calls() is not None
        start_turn()
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if not self._is_nested:
            turn_ms = (time.monotonic() - self._t0) * 1000
            llm_calls = current_llm_calls()
            # Belt-and-suspenders: log_turn already guards its own body, but
            # if a caller replaces log_turn wholesale (a monkeypatch, a bad
            # future edit that drops the internal try/except), the ORIGINAL
            # exception from the wrapped work must still win — telemetry may
            # never be why a user-facing turn fails.
            try:
                log_turn(
                    user_id=self.user_id,
                    turn_ms=turn_ms,
                    operation=self.operation,
                    fallback=is_fallback_response(self.response_text),
                    llm_calls=llm_calls,
                    error=(f"{exc_type.__name__}: {exc}" if exc else None),
                )
            except Exception:
                pass
            # Reset so a later, genuinely unrelated turn on a reused
            # thread/greenlet doesn't inherit this turn's leftover count.
            _llm_call_count.set(None)
        return False  # never swallow the original exception
