import json
import os
from typing import Dict, Optional, List
from datetime import datetime

class MemoryService:
    def __init__(self, file_path: str = "user_memory.json"):
        self.file_path = file_path
        self._load_memory()
        # Get memory level from environment variable, default to 5 if not set
        self.memory_level = int(os.getenv("CHAT_MEMORYLEVEL", "5"))

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
