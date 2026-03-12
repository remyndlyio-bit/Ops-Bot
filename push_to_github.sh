#!/bin/bash
# Run this from the root of your ops-bot project

set -e

echo "Applying small talk patch to services/intent_service.py ..."

# ── 1. Insert class-level constants after _FORM_JOB_FIELDS definition ─────────
python3 - <<'PYEOF'
import re

with open("services/intent_service.py", "r") as f:
    src = f.read()

CONSTANTS = '''
    # Small-talk trigger words / phrases (case-insensitive, matched as whole tokens)
    _SMALL_TALK_TRIGGERS = {
        "hi", "hey", "hello", "hiya", "howdy", "yo", "sup", "heya",
        "how are you", "how r u", "how are u", "how are you doing",
        "how\\'s it going", "hows it going", "how do you do",
        "what\\'s up", "whats up", "wassup",
        "thanks", "thank you", "thx", "ty", "cheers",
        "bye", "goodbye", "good bye", "see you", "see ya", "cya", "ttyl",
        "ok", "okay", "cool", "got it", "great", "nice", "awesome",
        "good morning", "good afternoon", "good evening", "good night",
        "morning", "afternoon", "evening",
    }

    _SMALL_TALK_RESPONSES = {
        "greeting": [
            "Hey! What can I help you with today?",
            "Hi there! Need an invoice, a query, or something else?",
            "Hello! Ready when you are — just tell me what you need.",
        ],
        "how_are_you": [
            "Doing great, thanks for asking! What can I pull up for you?",
            "All good on my end! What do you need today?",
            "Running smoothly! What can I help with?",
        ],
        "thanks": [
            "Happy to help! Anything else?",
            "Anytime! Let me know if you need more.",
            "Of course! Just ask if there\\'s anything else.",
        ],
        "bye": [
            "Take care! Come back anytime.",
            "Goodbye! Have a great day.",
            "See you! I\\'ll be here whenever you need me.",
        ],
        "affirmation": [
            "Got it! Let me know if there\\'s anything else.",
            "Sure thing! Anything else I can help with?",
        ],
        "time_of_day": [
            "Good to hear from you! What do you need?",
            "Hope your day\\'s going well! What can I help with?",
        ],
    }
'''

HELPER = '''
    def _detect_small_talk(self, message: str) -> Optional[str]:
        """
        Returns a canned response if the message is pure small talk, else None.
        Short messages with no data keywords are matched against _SMALL_TALK_TRIGGERS.
        """
        import hashlib
        msg = message.strip().lower().rstrip("!?.,:;")

        data_keywords = {
            "invoice", "bill", "payment", "fees", "client", "job",
            "remind", "overdue", "due", "total", "billing", "record",
            "add", "show", "get", "send", "fetch", "how much", "how many",
            "query", "list", "find", "search", "last", "latest",
        }

        is_exact = msg in self._SMALL_TALK_TRIGGERS
        is_short = len(msg.split()) <= 6
        has_data = any(kw in msg for kw in data_keywords)

        if has_data:
            return None
        if not is_exact:
            if not is_short:
                return None
            multi_match = any(trigger in msg for trigger in self._SMALL_TALK_TRIGGERS if " " in trigger)
            if not multi_match:
                return None

        def _pick(options):
            idx = int(hashlib.md5(message.encode()).hexdigest(), 16) % len(options)
            return options[idx]

        bye_words = {"bye", "goodbye", "good bye", "see you", "see ya", "cya", "ttyl"}
        thanks_words = {"thanks", "thank you", "thx", "ty", "cheers"}
        how_words = {"how are you", "how r u", "how are u", "how are you doing",
                     "how\\'s it going", "hows it going", "what\\'s up", "whats up", "wassup"}
        time_words = {"good morning", "good afternoon", "good evening", "good night",
                      "morning", "afternoon", "evening"}
        affirmation_words = {"ok", "okay", "cool", "got it", "great", "nice", "awesome"}

        if msg in bye_words:
            return _pick(self._SMALL_TALK_RESPONSES["bye"])
        if msg in thanks_words:
            return _pick(self._SMALL_TALK_RESPONSES["thanks"])
        if any(hw in msg for hw in how_words):
            return _pick(self._SMALL_TALK_RESPONSES["how_are_you"])
        if msg in time_words:
            return _pick(self._SMALL_TALK_RESPONSES["time_of_day"])
        if msg in affirmation_words:
            return _pick(self._SMALL_TALK_RESPONSES["affirmation"])
        return _pick(self._SMALL_TALK_RESPONSES["greeting"])

'''

SMALL_TALK_CALL = '''
            # 0c. Small talk — respond directly, skip all data paths
            small_talk_response = self._detect_small_talk(message)
            if small_talk_response:
                self._store_conversation(user_id, message, small_talk_response)
                return {
                    "operation": "small_talk",
                    "response": small_talk_response,
                    "trigger_invoice": False,
                    "invoice_data": {},
                }

'''

# Insert constants after _FORM_JOB_FIELDS block
anchor_constants = '    ]\n\n    def __init__'
if anchor_constants in src and '_SMALL_TALK_TRIGGERS' not in src:
    src = src.replace(anchor_constants, '    ]\n' + CONSTANTS + '\n    def __init__', 1)
    print("✓ Inserted _SMALL_TALK_TRIGGERS and _SMALL_TALK_RESPONSES constants")
elif '_SMALL_TALK_TRIGGERS' in src:
    print("⚠  Constants already present, skipping")
else:
    print("✗ Could not find anchor for constants — check intent_service.py manually")

# Insert helper method before process_request
anchor_helper = '    def process_request'
if anchor_helper in src and '_detect_small_talk' not in src:
    src = src.replace(anchor_helper, HELPER + '    def process_request', 1)
    print("✓ Inserted _detect_small_talk helper method")
elif '_detect_small_talk' in src:
    print("⚠  Helper already present, skipping")
else:
    print("✗ Could not find process_request — check intent_service.py manually")

# Insert small talk call inside process_request after add_job_triggers block
anchor_call = '            # 1. Payment reminder (keyword-based)'
if anchor_call in src and 'small_talk_response' not in src:
    src = src.replace(anchor_call, SMALL_TALK_CALL + '            # 1. Payment reminder (keyword-based)', 1)
    print("✓ Inserted small talk call in process_request")
elif 'small_talk_response' in src:
    print("⚠  Small talk call already present, skipping")
else:
    print("✗ Could not find payment reminder anchor — check intent_service.py manually")

with open("services/intent_service.py", "w") as f:
    f.write(src)

print("Done patching intent_service.py")
PYEOF

echo ""
echo "Committing and pushing to GitHub ..."
git add services/intent_service.py
git commit -m "feat: add small talk handler (hi/bye/thanks etc.) to intent service"
git push origin main

echo ""
echo "✅ Done! Small talk feature is live on GitHub."
