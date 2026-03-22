import json
import os
import threading
from typing import Dict, Optional, List
from datetime import datetime

class MemoryService:
    def __init__(self, file_path: str = "user_memory.json"):
        self.file_path = file_path
        self._lock = threading.Lock()          # Global lock for file I/O
        self._user_locks: Dict[str, threading.Lock] = {}  # Per-user locks
        self._load_memory()
        # Get memory level from environment variable, default to 5 if not set
        self.memory_level = int(os.getenv("CHAT_MEMORYLEVEL", "5"))

    def _get_user_lock(self, user_id: str) -> threading.Lock:
        """Get or create a per-user lock to prevent concurrent modifications."""
        if user_id not in self._user_locks:
            with self._lock:
                if user_id not in self._user_locks:
                    self._user_locks[user_id] = threading.Lock()
        return self._user_locks[user_id]

    def _load_memory(self):
        if os.path.exists(self.file_path):
            with open(self.file_path, 'r') as f:
                self.memory = json.load(f)
        else:
            self.memory = {}

    def _save_memory(self):
        with self._lock:
            with open(self.file_path, 'w') as f:
                json.dump(self.memory, f, indent=2)

    def get_user_memory(self, user_id: str) -> Dict:
        lock = self._get_user_lock(user_id)
        with lock:
            return dict(self.memory.get(user_id, {"name": "User", "role": "Client", "last_sheet": "Leads"}))

    def update_user_memory(self, user_id: str, data: Dict):
        lock = self._get_user_lock(user_id)
        with lock:
            if user_id not in self.memory:
                self.memory[user_id] = {"name": "User", "role": "Client", "last_sheet": "Leads"}

            self.memory[user_id].update(data)
            self._save_memory()

    def get_memory_context(self, user_id: str) -> str:
        mem = self.get_user_memory(user_id)
        return f"User: {mem.get('name')}, Role: {mem.get('role')}, Last Sheet: {mem.get('last_sheet')}"

    def get_conversation_history(self, user_id: str) -> List[Dict[str, str]]:
        """
        Get the last N messages from conversation history for a user.
        Returns a list of message dictionaries with 'role' (user/assistant) and 'content'.
        """
        if user_id not in self.memory:
            return []
        
        conversation = self.memory[user_id].get("conversation", [])
        # Return only the last N messages (memory_level * 2 because we count both user and assistant messages)
        # But we want N message pairs, so we take last N*2 messages
        return conversation[-self.memory_level * 2:] if conversation else []

    def add_message(self, user_id: str, role: str, content: str):
        """
        Add a message to the conversation history.
        role: 'user' or 'assistant'
        content: the message content
        """
        lock = self._get_user_lock(user_id)
        with lock:
            if user_id not in self.memory:
                self.memory[user_id] = {"name": "User", "role": "Client", "last_sheet": "Leads", "conversation": []}

            if "conversation" not in self.memory[user_id]:
                self.memory[user_id]["conversation"] = []

            # Add the new message
            self.memory[user_id]["conversation"].append({
                "role": role,
                "content": content,
                "timestamp": datetime.now().isoformat()
            })

            # Keep only the last N*2 messages (N user messages + N assistant messages)
            conversation = self.memory[user_id]["conversation"]
            if len(conversation) > self.memory_level * 2:
                self.memory[user_id]["conversation"] = conversation[-self.memory_level * 2:]

            self._save_memory()

    # --- Form state for multi-step data entry (e.g. "add new job") ---

    def start_form(self, user_id: str, fields: List[str], form_override: Dict = None) -> None:
        """Start a new form flow for the user. fields = list of column names to collect.
        If form_override is provided, use it directly (for smart capture states)."""
        lock = self._get_user_lock(user_id)
        with lock:
            if user_id not in self.memory:
                self.memory[user_id] = {"name": "User", "role": "Client", "last_sheet": "Leads"}
            if form_override:
                form_override["active"] = True
                self.memory[user_id]["form"] = form_override
            else:
                self.memory[user_id]["form"] = {
                    "active": True,
                    "fields": fields,
                    "step": 0,
                    "values": {},
                }
            self._save_memory()

    def get_form_state(self, user_id: str) -> Optional[Dict]:
        """Return form state dict or None if no active form."""
        if user_id not in self.memory:
            return None
        form = self.memory[user_id].get("form")
        if form and form.get("active"):
            return form
        return None

    def set_form_value(self, user_id: str, field: str, value: str) -> None:
        """Store a value for the current form field."""
        lock = self._get_user_lock(user_id)
        with lock:
            form = self.get_form_state(user_id)
            if form:
                form["values"][field] = value
                self._save_memory()

    def advance_form_step(self, user_id: str) -> None:
        """Move to the next step in the form."""
        lock = self._get_user_lock(user_id)
        with lock:
            form = self.get_form_state(user_id)
            if form:
                form["step"] += 1
                self._save_memory()

    def complete_form(self, user_id: str) -> Optional[Dict[str, str]]:
        """Mark form complete and return collected values. Clears form state."""
        lock = self._get_user_lock(user_id)
        with lock:
            form = self.get_form_state(user_id)
            if not form:
                return None
            values = dict(form.get("values", {}))
            self.memory[user_id]["form"] = {"active": False}
            self._save_memory()
            return values

    def cancel_form(self, user_id: str) -> None:
        """Cancel any active form."""
        lock = self._get_user_lock(user_id)
        with lock:
            if user_id in self.memory:
                self.memory[user_id]["form"] = {"active": False}
                self._save_memory()
