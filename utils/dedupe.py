"""
Process-local TTL dedupe for inbound webhook deliveries.

PLAN_OF_ACTION.md P0-4: once webhooks ack immediately instead of awaiting the
full turn, a network blip or a slow turn can still cause Twilio/Telegram to
redeliver the same message. This catches that redelivery by MessageSid /
update_id / callback_query id before it re-enters the pipeline (a second LLM
call, a second reply, or — for reminder-send buttons — a second email).

In-memory and single-instance is a deliberate, stated scope limit (see the
plan): this bot runs as one Railway process. Move to a DB-backed uniqueness
check only if it's ever scaled to multiple instances.
"""

import threading
import time
from typing import Dict, Optional


class TTLDedupe:
    def __init__(self, ttl_seconds: float = 300.0):
        self._ttl = ttl_seconds
        self._seen: Dict[str, float] = {}
        self._lock = threading.Lock()

    def seen_recently(self, key: Optional[str]) -> bool:
        """True if `key` was already recorded within the TTL window. Always
        records `key` as seen now, whatever the result — a caller that skips
        because of the return still starts the same TTL clock for the next
        redelivery."""
        if not key:
            return False
        now = time.monotonic()
        with self._lock:
            if len(self._seen) > 2000:
                cutoff = now - self._ttl
                self._seen = {k: v for k, v in self._seen.items() if v > cutoff}
            last = self._seen.get(key)
            self._seen[key] = now
            return last is not None and (now - last) < self._ttl
