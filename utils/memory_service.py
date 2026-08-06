import json
import os
import threading
from typing import Dict, Optional, List
from datetime import datetime, date
from decimal import Decimal
import uuid

from utils.logger import logger

# Default shape for a brand-new user.
_DEFAULT = {"name": "User", "role": "Client", "last_sheet": "Leads"}


class _SafeEncoder(json.JSONEncoder):
    """JSON encoder that handles types psycopg2 returns but stdlib json can't encode."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, uuid.UUID):
            return str(obj)
        return super().default(obj)


class MemoryService:
    """Per-user conversation + flow state.

    Backed by Supabase (public.user_memory) when SUPABASE_DB_URL is set, so the
    state survives redeploys and is shared across multiple app instances. Falls
    back to a local JSON file for dev when no DB is configured.

    Why DB-backed: the webhook can be redeployed (fresh container = fresh disk) or
    scaled to several instances, each with its own filesystem. With the old
    file-only store, an in-flight 'awaiting_*' flag set while prompting (e.g. for
    the invoice address) could vanish before the user's reply arrived, orphaning
    the reply. The shared DB keeps state consistent across both.
    """

    def __init__(self, file_path: str = "user_memory.json"):
        self.file_path = file_path
        self._lock = threading.Lock()                     # global lock (file I/O + DB access)
        self._user_locks: Dict[str, threading.Lock] = {}  # per-user read-modify-write locks
        self.memory_level = int(os.getenv("CHAT_MEMORYLEVEL", "5"))
        # P1-4 (PLAN_OF_ACTION.md): one DB read per (user, turn) instead of
        # one per call site — a single turn calls get_user_memory /
        # update_user_memory a dozen-plus times across intent_service.py
        # (uscf_context lookups, awaiting-state checks, _save_last_intent,
        # ...), each previously a fresh round trip for the identical row.
        # threading.local(), not an instance dict: MemoryService is a
        # process-wide singleton and FastAPI runs each sync request on its
        # own worker thread (one turn per thread at a time), so this is the
        # same pattern IntentService._turn_cache already uses for the
        # user profile. reset_turn_cache() is called at the top of every
        # turn (process_request) so a leftover entry from an EARLIER turn
        # on a reused worker thread can never look valid for a new one —
        # matching is checked on user_id too, as a second, cheap guard.
        self._turn_cache = threading.local()

        self._db_url = (os.getenv("SUPABASE_DB_URL") or "").strip() or None
        self._conn = None          # single reused connection (lazy)
        self._db_ok = False
        self.memory: Dict = {}     # file-fallback store / dev cache

        if self._db_url:
            self._ensure_table()
        if not self._db_ok:
            self._load_file()

    # ── per-user lock ──────────────────────────────────────────────────────
    def _get_user_lock(self, user_id: str) -> threading.Lock:
        if user_id not in self._user_locks:
            with self._lock:
                if user_id not in self._user_locks:
                    self._user_locks[user_id] = threading.Lock()
        return self._user_locks[user_id]

    # ── DB backend (single reused connection, lock-guarded) ────────────────
    def _connection(self):
        if self._conn is not None:
            try:
                if self._conn.closed == 0:
                    return self._conn
            except Exception:
                pass
        import psycopg2
        self._conn = psycopg2.connect(self._db_url)
        self._conn.autocommit = True
        return self._conn

    def _ensure_table(self):
        try:
            with self._lock:
                conn = self._connection()
                with conn.cursor() as cur:
                    cur.execute(
                        "CREATE TABLE IF NOT EXISTS public.user_memory ("
                        "  user_id text PRIMARY KEY,"
                        "  payload jsonb NOT NULL DEFAULT '{}'::jsonb,"
                        "  updated_at timestamptz NOT NULL DEFAULT now()"
                        ")"
                    )
            self._db_ok = True
            logger.info("[MEMORY] Using Supabase-backed user memory (public.user_memory).")
        except Exception as e:
            self._db_ok = False
            self._conn = None
            logger.warning(f"[MEMORY] DB init failed, falling back to file store: {e}")

    def _db_get(self, user_id: str) -> Optional[Dict]:
        try:
            with self._lock:
                conn = self._connection()
                with conn.cursor() as cur:
                    cur.execute("SELECT payload FROM public.user_memory WHERE user_id = %s", (user_id,))
                    row = cur.fetchone()
            return row[0] if row and row[0] is not None else None
        except Exception as e:
            self._conn = None
            logger.warning(f"[MEMORY] db_get failed for {user_id}: {e}")
            return None

    def _db_set(self, user_id: str, payload: Dict) -> bool:
        try:
            with self._lock:
                conn = self._connection()
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO public.user_memory (user_id, payload, updated_at) "
                        "VALUES (%s, %s::jsonb, now()) "
                        "ON CONFLICT (user_id) DO UPDATE SET payload = EXCLUDED.payload, updated_at = now()",
                        (user_id, json.dumps(payload, cls=_SafeEncoder)),
                    )
            return True
        except Exception as e:
            self._conn = None
            logger.warning(f"[MEMORY] db_set failed for {user_id}: {e}")
            return False

    # ── file fallback ──────────────────────────────────────────────────────
    def _load_file(self):
        if os.path.exists(self.file_path):
            try:
                with open(self.file_path, 'r') as f:
                    self.memory = json.load(f)
            except Exception:
                self.memory = {}
        else:
            self.memory = {}

    def _save_file(self):
        with self._lock:
            try:
                with open(self.file_path, 'w') as f:
                    json.dump(self.memory, f, indent=2, cls=_SafeEncoder)
            except Exception as e:
                logger.warning(f"[MEMORY] file save failed: {e}")

    # ── per-turn cache (P1-4, PLAN_OF_ACTION.md) ────────────────────────────
    def reset_turn_cache(self) -> None:
        """Call once at the very top of a turn, before any get/update_user_memory
        call. Bounds the cache's lifetime explicitly to "this turn" instead of
        relying on it happening to still be correct if the same worker thread
        picks up a later turn (for the same or a different user)."""
        self._turn_cache.entry = None

    def _cached_entry(self, user_id: str) -> Optional[Dict]:
        entry = getattr(self._turn_cache, "entry", None)
        if entry is not None and entry[0] == user_id:
            return entry[1]
        return None

    def _set_cached_entry(self, user_id: str, payload: Dict) -> None:
        self._turn_cache.entry = (user_id, dict(payload))

    # ── unified per-user read/write (DB first, file fallback) ──────────────
    # _write_raw is the ONE place every write path ends up — update_user_memory,
    # start_form, set_form_value, advance_form_step, complete_form, cancel_form,
    # add_message all funnel through it — so populating the cache here (not
    # separately in each of those) is what keeps ALL of them correct instead
    # of just the ones some future edit remembers to also update.
    def _read_raw(self, user_id: str, use_cache: bool = True) -> Optional[Dict]:
        """Return the stored dict for a user, or None if no record yet.

        `use_cache=False` (update_user_memory only): two call sites write
        memory from OUTSIDE process_request's turn boundary (main.py's
        background invoice-send tasks), so they never call
        reset_turn_cache() — trusting a possibly-stale cached value as
        their read-modify-write base could silently clobber a concurrent
        update made through a real turn in between. Bypassing the cache
        for that one read keeps update_user_memory's own DB cost exactly
        what it was before this cache existed: one read, one write,
        always.
        """
        if use_cache:
            cached = self._cached_entry(user_id)
            if cached is not None:
                return dict(cached)
        data = self._db_get(user_id) if self._db_ok else self.memory.get(user_id)
        if data is not None:
            self._set_cached_entry(user_id, data)
        return data

    def _write_raw(self, user_id: str, payload: Dict):
        if self._db_ok and self._db_set(user_id, payload):
            self._set_cached_entry(user_id, payload)
            return
        # DB unavailable → file fallback (also mirror to the in-RAM dict)
        self.memory[user_id] = payload
        self._save_file()
        self._set_cached_entry(user_id, payload)

    # ── public API (signatures unchanged) ──────────────────────────────────
    def get_user_memory(self, user_id: str) -> Dict:
        data = self._read_raw(user_id)
        return dict(data) if data else dict(_DEFAULT)

    def update_user_memory(self, user_id: str, data: Dict):
        lock = self._get_user_lock(user_id)
        with lock:
            current = self._read_raw(user_id, use_cache=False) or dict(_DEFAULT)
            current.update(data)
            self._write_raw(user_id, current)

    def get_memory_context(self, user_id: str) -> str:
        mem = self.get_user_memory(user_id)
        return f"User: {mem.get('name')}, Role: {mem.get('role')}, Last Sheet: {mem.get('last_sheet')}"

    def get_conversation_history(self, user_id: str) -> List[Dict[str, str]]:
        """Return the last N message pairs (user + assistant) for a user."""
        data = self._read_raw(user_id)
        if not data:
            return []
        conversation = data.get("conversation", [])
        return conversation[-self.memory_level * 2:] if conversation else []

    def add_message(self, user_id: str, role: str, content: str):
        """Append a message to conversation history (role: 'user' or 'assistant')."""
        lock = self._get_user_lock(user_id)
        with lock:
            current = self._read_raw(user_id) or dict(_DEFAULT)
            conversation = current.get("conversation") or []
            conversation.append({
                "role": role,
                "content": content,
                "timestamp": datetime.now().isoformat(),
            })
            if len(conversation) > self.memory_level * 2:
                conversation = conversation[-self.memory_level * 2:]
            current["conversation"] = conversation
            self._write_raw(user_id, current)

    # --- Form state for multi-step data entry (e.g. "add new job") ---

    def start_form(self, user_id: str, fields: List[str], form_override: Dict = None) -> None:
        lock = self._get_user_lock(user_id)
        with lock:
            current = self._read_raw(user_id) or dict(_DEFAULT)
            if form_override:
                form_override["active"] = True
                form_override.setdefault("created_at", datetime.now().isoformat())
                form_override["retry_count"] = 0
                current["form"] = form_override
            else:
                current["form"] = {
                    "active": True,
                    "fields": fields,
                    "step": 0,
                    "values": {},
                    "created_at": datetime.now().isoformat(),
                    "retry_count": 0,
                }
            # P0-2 (PLAN_OF_ACTION.md): a freshly-started form supersedes any
            # other pending single-question state — a stale disambiguation
            # list or invoice-send confirmation the user was never shown
            # again must not outrank the prompt they're now being shown.
            # Mirrors IntentService._arm_disambiguation's own clearing of
            # form/pending_send_invoice in the other direction.
            current["pending_disambiguation"] = None
            current["pending_send_invoice"] = None
            self._write_raw(user_id, current)

    def get_form_state(self, user_id: str) -> Optional[Dict]:
        data = self._read_raw(user_id)
        if not data:
            return None
        form = data.get("form")
        if form and form.get("active"):
            return form
        return None

    def set_form_value(self, user_id: str, field: str, value: str) -> None:
        lock = self._get_user_lock(user_id)
        with lock:
            current = self._read_raw(user_id)
            if not current:
                return
            form = current.get("form")
            if form and form.get("active"):
                form.setdefault("values", {})[field] = value
                current["form"] = form
                self._write_raw(user_id, current)

    def advance_form_step(self, user_id: str) -> None:
        lock = self._get_user_lock(user_id)
        with lock:
            current = self._read_raw(user_id)
            if not current:
                return
            form = current.get("form")
            if form and form.get("active"):
                form["step"] = form.get("step", 0) + 1
                current["form"] = form
                self._write_raw(user_id, current)

    def complete_form(self, user_id: str) -> Optional[Dict[str, str]]:
        lock = self._get_user_lock(user_id)
        with lock:
            current = self._read_raw(user_id)
            if not current:
                return None
            form = current.get("form")
            if not (form and form.get("active")):
                return None
            values = dict(form.get("values", {}))
            current["form"] = {"active": False}
            self._write_raw(user_id, current)
            return values

    def cancel_form(self, user_id: str) -> None:
        lock = self._get_user_lock(user_id)
        with lock:
            current = self._read_raw(user_id)
            if not current:
                return
            current["form"] = {"active": False}
            self._write_raw(user_id, current)
