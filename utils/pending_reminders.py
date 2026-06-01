"""
Cross-service pending reminder state.

The reminder worker (cron) writes pending reminders for WhatsApp users.
The main app reads them when a WhatsApp user replies with "1", "all", "skip",
"paid 1", etc.

IMPORTANT: cron and webhook run as SEPARATE Railway services with separate
filesystems — so the original file-based approach silently lost state across
services. This module now persists to Supabase (public.pending_reminders)
which both services share, with a JSON-on-disk fallback for local dev when
SUPABASE_DB_URL isn't set.

The API (save_pending / get_pending / clear_pending / remove_single) is
unchanged — callers see the same shapes.
"""

import json
import os
from typing import Dict, List, Optional

from utils.logger import logger

_PENDING_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "pending_reminders.json")


# ── DB backend ────────────────────────────────────────────────────────

def _db_url() -> Optional[str]:
    url = (os.getenv("SUPABASE_DB_URL") or "").strip()
    return url or None


def _db_conn():
    """Lazy psycopg2 connection. Returns None on any failure → caller falls back to file."""
    url = _db_url()
    if not url:
        return None
    try:
        import psycopg2
        conn = psycopg2.connect(url)
        conn.autocommit = True
        return conn
    except Exception as e:
        logger.warning(f"[PENDING_REMINDERS] DB connect failed, falling back to file: {e}")
        return None


def _db_save(user_id: str, reminders: List[Dict]) -> bool:
    conn = _db_conn()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.pending_reminders (user_id, payload, updated_at)
                VALUES (%s, %s::jsonb, NOW())
                ON CONFLICT (user_id) DO UPDATE
                  SET payload    = EXCLUDED.payload,
                      updated_at = NOW()
                """,
                (user_id, json.dumps(reminders)),
            )
        conn.close()
        return True
    except Exception as e:
        logger.warning(f"[PENDING_REMINDERS] DB save failed: {e}")
        try:
            conn.close()
        except Exception:
            pass
        return False


def _db_load(user_id: str) -> Optional[List[Dict]]:
    """Return the pending list for a user, or None if no row / DB unavailable."""
    conn = _db_conn()
    if not conn:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT payload FROM public.pending_reminders WHERE user_id = %s",
                (user_id,),
            )
            row = cur.fetchone()
        conn.close()
        if not row:
            return None
        payload = row[0]
        # psycopg2 may return JSONB as already-parsed Python or as a string —
        # handle both defensively.
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                return None
        if isinstance(payload, list):
            return payload
        return None
    except Exception as e:
        logger.warning(f"[PENDING_REMINDERS] DB load failed: {e}")
        try:
            conn.close()
        except Exception:
            pass
        return None


def _db_clear(user_id: str) -> bool:
    conn = _db_conn()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM public.pending_reminders WHERE user_id = %s", (user_id,))
        conn.close()
        return True
    except Exception as e:
        logger.warning(f"[PENDING_REMINDERS] DB clear failed: {e}")
        try:
            conn.close()
        except Exception:
            pass
        return False


# ── File backend (local dev fallback) ──────────────────────────────────

def _ensure_dir():
    os.makedirs(os.path.dirname(_PENDING_FILE), exist_ok=True)


def _file_read_all() -> Dict:
    if not os.path.exists(_PENDING_FILE):
        return {}
    try:
        with open(_PENDING_FILE, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {}


def _file_write_all(data: Dict):
    _ensure_dir()
    with open(_PENDING_FILE, "w") as f:
        json.dump(data, f, indent=2)


# ── Public API (unchanged signatures) ──────────────────────────────────

def save_pending(user_id: str, reminders: List[Dict]):
    """Store pending reminders for a user. Tries DB first; falls back to file."""
    if _db_save(user_id, reminders):
        logger.info(f"[PENDING_REMINDERS] Saved {len(reminders)} reminder(s) for user {user_id} (db)")
        return
    # File fallback (local dev)
    data = _file_read_all()
    data[user_id] = reminders
    _file_write_all(data)
    logger.info(f"[PENDING_REMINDERS] Saved {len(reminders)} reminder(s) for user {user_id} (file)")


def get_pending(user_id: str) -> Optional[List[Dict]]:
    """Get pending reminders for a user, or None if none exist."""
    # Try DB first
    if _db_url():
        rem = _db_load(user_id)
        if rem:
            return rem
        # No row found OR DB had a transient error — try file as fallback
        # only when DB isn't configured at all. If DB is configured we trust
        # its result so we don't accidentally read stale local data.
        return None
    # File fallback
    data = _file_read_all()
    reminders = data.get(user_id)
    return reminders or None


def clear_pending(user_id: str):
    """Remove pending reminders for a user after they've been handled."""
    if _db_clear(user_id):
        logger.info(f"[PENDING_REMINDERS] Cleared reminders for user {user_id} (db)")
        return
    # File fallback
    data = _file_read_all()
    if user_id in data:
        del data[user_id]
        _file_write_all(data)
        logger.info(f"[PENDING_REMINDERS] Cleared reminders for user {user_id} (file)")


def remove_single(user_id: str, job_id) -> Optional[Dict]:
    """Remove a single reminder by job_id and return it.
    If no reminders left, clears the user entirely."""
    reminders = get_pending(user_id) or []
    found = None
    remaining = []
    for r in reminders:
        if str(r.get("id")) == str(job_id):
            found = r
        else:
            remaining.append(r)
    if found is None:
        return None
    if remaining:
        save_pending(user_id, remaining)
    else:
        clear_pending(user_id)
    return found
