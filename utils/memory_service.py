import json
import os
from typing import Dict, Optional

class MemoryService:
    def __init__(self, file_path: str = "user_memory.json"):
        self.file_path = file_path
        self._load_memory()

    def _load_memory(self):
        if os.path.exists(self.file_path):
            with open(self.file_path, 'r') as f:
                self.memory = json.load(f)
        else:
            self.memory = {}

    def _save_memory(self):
        with open(self.file_path, 'w') as f:
            json.dump(self.memory, f, indent=2)

    def get_user_memory(self, user_id: str) -> Dict:
        return self.memory.get(user_id, {"name": "User", "role": "Client", "last_sheet": "Leads"})

    def update_user_memory(self, user_id: str, data: Dict):
        if user_id not in self.memory:
            self.memory[user_id] = {"name": "User", "role": "Client", "last_sheet": "Leads"}
        
        self.memory[user_id].update(data)
        self._save_memory()

    def get_memory_context(self, user_id: str) -> str:
        mem = self.get_user_memory(user_id)
        return f"User: {mem.get('name')}, Role: {mem.get('role')}, Last Sheet: {mem.get('last_sheet')}"
