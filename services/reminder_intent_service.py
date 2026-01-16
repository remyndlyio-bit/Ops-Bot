import json
import re
from datetime import datetime, timedelta
from typing import Dict, Optional
from utils.logger import logger


class ReminderIntentService:
    """
    Silent reminder-intent extraction module.
    Returns structured JSON only when reminder-related actions are detected.
    Returns {"intent": "none"} for all other messages.
    """

    @staticmethod
    def extract_intent(message: str) -> Dict:
        """
        Extracts reminder intent from message.
        Returns structured JSON or {"intent": "none"}.
        """
        if not message:
            return {"intent": "none"}

        msg_lower = message.lower().strip()

        # Check for create_reminder
        create_patterns = [
            r"remind\s+(?:me\s+)?(?:to\s+)?(?:about\s+)?",
            r"follow\s+up\s+(?:later|in|on)",
            r"nudge\s+(?:me\s+)?(?:later|in|on)",
            r"notify\s+(?:me\s+)?(?:later|in|on)",
            r"check\s+back\s+(?:later|in|on)",
        ]
        if any(re.search(p, msg_lower) for p in create_patterns):
            return ReminderIntentService._extract_create_reminder(message)

        # Check for list_reminders
        list_patterns = [
            r"list\s+reminders",
            r"show\s+reminders",
            r"what\s+reminders",
            r"reminders\s+(?:for|in|this)",
            r"upcoming\s+reminders",
        ]
        if any(re.search(p, msg_lower) for p in list_patterns):
            return ReminderIntentService._extract_list_reminders(message)

        # Check for update_reminder
        update_patterns = [
            r"reschedule\s+reminder",
            r"update\s+reminder",
            r"change\s+reminder",
            r"move\s+reminder",
            r"postpone\s+reminder",
        ]
        if any(re.search(p, msg_lower) for p in update_patterns):
            return ReminderIntentService._extract_update_reminder(message)

        # Check for cancel_reminder
        cancel_patterns = [
            r"cancel\s+reminder",
            r"delete\s+reminder",
            r"remove\s+reminder",
            r"stop\s+reminder",
        ]
        if any(re.search(p, msg_lower) for p in cancel_patterns):
            return ReminderIntentService._extract_cancel_reminder(message)

        return {"intent": "none"}

    @staticmethod
    def _extract_create_reminder(message: str) -> Dict:
        """Extract create_reminder intent."""
        msg_lower = message.lower()

        # Extract subject (client name, invoice, task)
        subject = None
        # Look for common patterns
        subject_patterns = [
            r"(?:about|for|to)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)",  # Capitalized names
            r"client\s+([A-Z][a-z]+)",
            r"invoice\s+#?(\w+)",
        ]
        for pattern in subject_patterns:
            match = re.search(pattern, message)
            if match:
                subject = match.group(1).strip()
                break

        # Extract action
        action = None
        action_patterns = [
            r"to\s+(\w+(?:\s+\w+)?)",  # "to call", "to email", "to follow up"
            r"about\s+(\w+(?:\s+\w+)?)",  # "about payment", "about invoice"
        ]
        for pattern in action_patterns:
            match = re.search(pattern, msg_lower)
            if match:
                action = match.group(1).strip()
                break
        if not action:
            # Default actions
            if "follow up" in msg_lower:
                action = "follow up"
            elif "call" in msg_lower:
                action = "call"
            elif "email" in msg_lower:
                action = "email"
            elif "pay" in msg_lower:
                action = "pay"
            else:
                action = "follow up"

        # Extract time
        delay_days = None
        absolute_date = None

        # Relative time patterns
        relative_patterns = [
            (r"tomorrow", 1),
            (r"in\s+(\d+)\s+days?", lambda m: int(m.group(1))),
            (r"in\s+one\s+week", 7),
            (r"in\s+a\s+week", 7),
            (r"next\s+week", 7),
        ]
        for pattern, days_or_func in relative_patterns:
            match = re.search(pattern, msg_lower)
            if match:
                if callable(days_or_func):
                    delay_days = days_or_func(match)
                else:
                    delay_days = days_or_func
                break

        # Absolute date patterns
        if not delay_days:
            date_patterns = [
                r"(?:on\s+)?(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+(\d{1,2})(?:st|nd|rd|th)?",
                r"(\d{1,2})(?:st|nd|rd|th)?\s+(?:of\s+)?(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*",
                r"(\d{4}-\d{2}-\d{2})",  # YYYY-MM-DD
            ]
            for pattern in date_patterns:
                match = re.search(pattern, msg_lower)
                if match:
                    try:
                        # Try to parse and format as YYYY-MM-DD
                        date_str = match.group(0)
                        # Simple parsing - would need more robust date parsing in production
                        parsed = datetime.strptime(date_str, "%Y-%m-%d")
                        absolute_date = parsed.strftime("%Y-%m-%d")
                        break
                    except:
                        pass

        # If no time provided, return none
        if delay_days is None and absolute_date is None:
            return {"intent": "none"}

        return {
            "intent": "create_reminder",
            "subject": subject,
            "action": action,
            "delay_days": delay_days,
            "absolute_date": absolute_date,
        }

    @staticmethod
    def _extract_list_reminders(message: str) -> Dict:
        """Extract list_reminders intent."""
        msg_lower = message.lower()

        timeframe = "all"
        if "today" in msg_lower:
            timeframe = "today"
        elif "week" in msg_lower or "this week" in msg_lower:
            timeframe = "week"
        elif "month" in msg_lower or "this month" in msg_lower:
            timeframe = "month"

        return {
            "intent": "list_reminders",
            "timeframe": timeframe,
        }

    @staticmethod
    def _extract_update_reminder(message: str) -> Dict:
        """Extract update_reminder intent."""
        msg_lower = message.lower()

        # Extract reference
        reference = None
        if "last" in msg_lower:
            reference = "last"
        elif "today" in msg_lower:
            reference = "today"
        else:
            # Try to extract client/subject name
            subject_match = re.search(r"(?:for|about)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)", message)
            if subject_match:
                reference = subject_match.group(1).strip()

        # Extract time (same logic as create_reminder)
        delay_days = None
        absolute_date = None

        relative_patterns = [
            (r"tomorrow", 1),
            (r"in\s+(\d+)\s+days?", lambda m: int(m.group(1))),
            (r"in\s+one\s+week", 7),
            (r"next\s+week", 7),
        ]
        for pattern, days_or_func in relative_patterns:
            match = re.search(pattern, msg_lower)
            if match:
                if callable(days_or_func):
                    delay_days = days_or_func(match)
                else:
                    delay_days = days_or_func
                break

        if not delay_days:
            date_match = re.search(r"(\d{4}-\d{2}-\d{2})", msg_lower)
            if date_match:
                absolute_date = date_match.group(1)

        if delay_days is None and absolute_date is None:
            return {"intent": "none"}

        return {
            "intent": "update_reminder",
            "reference": reference,
            "delay_days": delay_days,
            "absolute_date": absolute_date,
        }

    @staticmethod
    def _extract_cancel_reminder(message: str) -> Dict:
        """Extract cancel_reminder intent."""
        msg_lower = message.lower()

        reference = None
        if "last" in msg_lower:
            reference = "last"
        elif "today" in msg_lower:
            reference = "today"
        else:
            subject_match = re.search(r"(?:for|about)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)", message)
            if subject_match:
                reference = subject_match.group(1).strip()

        return {
            "intent": "cancel_reminder",
            "reference": reference,
        }
