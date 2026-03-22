"""
Cross-process pending reminder state.

The reminder worker (cron) writes pending reminders for WhatsApp users.
The main app reads them when a WhatsApp user replies with "1", "2", "skip", etc.

File-based (JSON) so it works across separate processes without shared memory.
"""

import json
import os
from typing import Dict, List, Optional
from utils.logger import logger

_PENDING_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "pending_reminders.json")


def _ensure_dir():
    os.makedirs(os.path.dirname(_PENDING_FILE), exist_ok=True)


def _read_all() -> Dict:
    if not os.path.exists(_PENDING_FILE):
        return {}
    try:
        with open(_PENDING_FILE, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {}


def _write_all(data: Dict):
    _ensure_dir()
    with open(_PENDING_FILE, "w") as f:
        json.dump(data, f, indent=2)


def save_pending(user_id: str, reminders: List[Dict]):
    """
    Store pending reminders for a user.
    Each reminder dict should contain: id, client_name, bill_no, fees, poc_email, poc_name, _reminder_level.
    """
    data = _read_all()
    data[user_id] = reminders
    _write_all(data)
    logger.info(f"[PENDING_REMINDERS] Saved {len(reminders)} reminder(s) for user {user_id}")


def get_pending(user_id: str) -> Optional[List[Dict]]:
    """Get pending reminders for a user, or None if none exist."""
    data = _read_all()
    reminders = data.get(user_id)
    if reminders:
        return reminders
    return None


def clear_pending(user_id: str):
    """Remove pending reminders for a user after they've been handled."""
    data = _read_all()
    if user_id in data:
        del data[user_id]
        _write_all(data)
        logger.info(f"[PENDING_REMINDERS] Cleared reminders for user {user_id}")


def remove_single(user_id: str, job_id) -> Optional[Dict]:
    """
    Remove a single reminder by job_id and return it.
    If no reminders left, clears the user entirely.
    """
    data = _read_all()
    reminders = data.get(user_id, [])
    found = None
    remaining = []
    for r in reminders:
        if str(r.get("id")) == str(job_id):
            found = r
        else:
            remaining.append(r)
    if found:
        if remaining:
            data[user_id] = remaining
        else:
            data.pop(user_id, None)
        _write_all(data)
    return found
