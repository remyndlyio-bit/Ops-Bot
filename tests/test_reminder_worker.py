"""
Tests for workers/reminder_worker.py

Covers:
- Pure helper functions (no mocks needed)
- scan_reminders() with mocked DB
- notify_user_* with mocked services
- mark_reminders_sent() with mocked DB
- Full run() integration with all services mocked
"""

import pytest
from unittest.mock import MagicMock, patch, call
from datetime import date

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from workers.reminder_worker import (
    _determine_reminder_level,
    _format_amount,
    _build_reminder_text,
    _is_telegram_user,
    group_by_user,
    scan_reminders,
    notify_user_telegram,
    notify_user_whatsapp,
    mark_reminders_sent,
    run,
    LEVEL_TO_FLAG,
)


# ── _determine_reminder_level ──────────────────────────────────────────────

class TestDetermineReminderLevel:
    def test_no_reminders_sent_returns_first(self):
        row = {"first_reminder_sent": None, "second_reminder_sent": None, "third_reminder_sent": None}
        assert _determine_reminder_level(row) == "first"

    def test_first_sent_second_not_returns_second(self):
        row = {"first_reminder_sent": "2024-01-15T10:00:00", "second_reminder_sent": None}
        assert _determine_reminder_level(row) == "second"

    def test_both_first_and_second_sent_returns_third(self):
        row = {"first_reminder_sent": "2024-01-15", "second_reminder_sent": "2024-01-30"}
        assert _determine_reminder_level(row) == "third"

    def test_missing_keys_treated_as_none(self):
        # Row without any reminder keys — all None by default
        assert _determine_reminder_level({}) == "first"


# ── _format_amount ────────────────────────────────────────────────────────

class TestFormatAmount:
    def test_integer_fees(self):
        assert _format_amount(50000) == "₹50,000"

    def test_float_fees_truncated(self):
        assert _format_amount(12500.75) == "₹12,500"

    def test_string_fees(self):
        assert _format_amount("75000") == "₹75,000"

    def test_none_fees(self):
        assert _format_amount(None) == "N/A"

    def test_empty_string_fees(self):
        assert _format_amount("") == "N/A"

    def test_invalid_fees_returns_original(self):
        assert _format_amount("not-a-number") == "not-a-number"


# ── _is_telegram_user ─────────────────────────────────────────────────────

class TestIsTelegramUser:
    def test_numeric_user_id_is_telegram(self):
        assert _is_telegram_user("123456789") is True

    def test_whatsapp_with_prefix(self):
        assert _is_telegram_user("whatsapp:+919876543210") is False

    def test_phone_number_with_plus(self):
        # int("+919876543210") is valid Python — the function treats bare +number as Telegram.
        # WhatsApp numbers must arrive with the "whatsapp:" prefix to be detected correctly.
        assert _is_telegram_user("whatsapp:+919876543210") is False

    def test_none_user_id(self):
        assert _is_telegram_user(None) is False


# ── _build_reminder_text ──────────────────────────────────────────────────

class TestBuildReminderText:
    def _make_row(self, client="Acme", bill="INV-001", fees=10000, level="first"):
        return {
            "client_name": client,
            "bill_no": bill,
            "fees": fees,
            "_reminder_level": level,
        }

    def test_single_reminder_contains_client_and_bill(self):
        rows = [self._make_row()]
        text = _build_reminder_text(rows)
        assert "Acme" in text
        assert "INV-001" in text
        assert "₹10,000" in text
        assert "First" in text

    def test_multiple_reminders_numbered(self):
        rows = [
            self._make_row("Acme", "INV-001", 10000, "first"),
            self._make_row("Nike", "INV-002", 20000, "second"),
        ]
        text = _build_reminder_text(rows)
        assert "1." in text
        assert "2." in text
        assert "Second" in text

    def test_third_reminder_shows_final_label(self):
        rows = [self._make_row(level="third")]
        text = _build_reminder_text(rows)
        assert "Final" in text

    def test_missing_client_shows_unknown(self):
        rows = [{"client_name": None, "bill_no": "X", "fees": 0, "_reminder_level": "first"}]
        text = _build_reminder_text(rows)
        assert "Unknown" in text


# ── group_by_user ─────────────────────────────────────────────────────────

class TestGroupByUser:
    def test_groups_rows_by_user_id(self):
        rows = [
            {"user_id": "111", "first_reminder_sent": None, "second_reminder_sent": None},
            {"user_id": "222", "first_reminder_sent": None, "second_reminder_sent": None},
            {"user_id": "111", "first_reminder_sent": "2024-01-10", "second_reminder_sent": None},
        ]
        grouped = group_by_user(rows)
        assert len(grouped["111"]) == 2
        assert len(grouped["222"]) == 1

    def test_attaches_reminder_level_to_each_row(self):
        rows = [{"user_id": "111", "first_reminder_sent": None, "second_reminder_sent": None}]
        grouped = group_by_user(rows)
        assert grouped["111"][0]["_reminder_level"] == "first"

    def test_empty_input_returns_empty_dict(self):
        assert group_by_user([]) == {}


# ── scan_reminders ────────────────────────────────────────────────────────

class TestScanReminders:
    def test_returns_rows_on_success(self):
        mock_db = MagicMock()
        mock_db.execute_sql.return_value = {
            "ok": True,
            "rows": [{"id": "abc", "user_id": "111"}],
        }
        with patch("workers.reminder_worker.SupabaseService", return_value=mock_db):
            rows = scan_reminders()
        assert len(rows) == 1
        assert rows[0]["id"] == "abc"

    def test_returns_empty_list_on_db_failure(self):
        mock_db = MagicMock()
        mock_db.execute_sql.return_value = {"ok": False, "error": "connection refused"}
        with patch("workers.reminder_worker.SupabaseService", return_value=mock_db):
            rows = scan_reminders()
        assert rows == []


# ── notify_user_telegram ──────────────────────────────────────────────────

class TestNotifyUserTelegram:
    def _make_reminder(self, idx=1, level="first"):
        return {
            "id": f"job-{idx}",
            "client_name": "Acme",
            "bill_no": f"INV-00{idx}",
            "fees": 10000,
            "_reminder_level": level,
        }

    def test_sends_message_with_buttons(self):
        mock_tg = MagicMock()
        reminders = [self._make_reminder(1, "first")]
        notify_user_telegram("123456", reminders, mock_tg)
        mock_tg.send_message_with_buttons_sync.assert_called_once()
        args = mock_tg.send_message_with_buttons_sync.call_args
        assert args[0][0] == 123456  # chat_id as int
        assert "Acme" in args[0][1]  # message text contains client name

    def test_buttons_include_send_all_and_skip_all(self):
        mock_tg = MagicMock()
        reminders = [self._make_reminder()]
        notify_user_telegram("123456", reminders, mock_tg)
        buttons = mock_tg.send_message_with_buttons_sync.call_args[0][2]
        flat_labels = [btn["text"] for row in buttons for btn in row]
        assert any("Send All" in label for label in flat_labels)
        assert any("Skip All" in label for label in flat_labels)


# ── notify_user_whatsapp ──────────────────────────────────────────────────

class TestNotifyUserWhatsapp:
    def _make_reminder(self):
        return {
            "id": "job-1",
            "client_name": "Nike",
            "bill_no": "INV-042",
            "fees": 25000,
            "poc_email": "poc@nike.com",
            "poc_name": "John",
            "_reminder_level": "second",
        }

    def test_sends_whatsapp_text(self):
        mock_wa = MagicMock()
        with patch("workers.reminder_worker.save_pending"):
            notify_user_whatsapp("+919876543210", [self._make_reminder()], mock_wa)
        mock_wa.send_text_message.assert_called_once()
        msg = mock_wa.send_text_message.call_args[0][1]
        assert "Nike" in msg

    def test_no_markdown_asterisks_in_whatsapp_message(self):
        mock_wa = MagicMock()
        with patch("workers.reminder_worker.save_pending"):
            notify_user_whatsapp("+919876543210", [self._make_reminder()], mock_wa)
        msg = mock_wa.send_text_message.call_args[0][1]
        assert "*" not in msg

    def test_saves_pending_reminders(self):
        mock_wa = MagicMock()
        with patch("workers.reminder_worker.save_pending") as mock_save:
            notify_user_whatsapp("+919876543210", [self._make_reminder()], mock_wa)
        mock_save.assert_called_once()
        pending = mock_save.call_args[0][1]
        assert len(pending) == 1
        assert pending[0]["id"] == "job-1"


# ── mark_reminders_sent ───────────────────────────────────────────────────

class TestMarkRemindersSent:
    def test_updates_correct_flag_for_first_reminder(self):
        mock_db = MagicMock()
        reminders = [{"id": "job-1", "_reminder_level": "first"}]
        mark_reminders_sent(mock_db, reminders)
        sql = mock_db.execute_sql.call_args[0][0]
        assert "first_reminder_sent" in sql
        assert "job-1" in sql

    def test_updates_third_flag(self):
        mock_db = MagicMock()
        reminders = [{"id": "job-99", "_reminder_level": "third"}]
        mark_reminders_sent(mock_db, reminders)
        sql = mock_db.execute_sql.call_args[0][0]
        assert "third_reminder_sent" in sql

    def test_skips_row_with_missing_id(self):
        mock_db = MagicMock()
        reminders = [{"id": None, "_reminder_level": "first"}]
        mark_reminders_sent(mock_db, reminders)
        mock_db.execute_sql.assert_not_called()

    def test_skips_row_with_unknown_level(self):
        mock_db = MagicMock()
        reminders = [{"id": "job-1", "_reminder_level": "zeroth"}]
        mark_reminders_sent(mock_db, reminders)
        mock_db.execute_sql.assert_not_called()


# ── run() integration ─────────────────────────────────────────────────────

class TestRun:
    def _mock_db_with_rows(self, rows):
        mock_db = MagicMock()
        mock_db.execute_sql.return_value = {"ok": True, "rows": rows}
        return mock_db

    def test_run_exits_early_when_no_reminders(self):
        mock_db = MagicMock()
        mock_db.execute_sql.return_value = {"ok": True, "rows": []}
        with patch("workers.reminder_worker.SupabaseService", return_value=mock_db), \
             patch("workers.reminder_worker.TelegramService") as mock_tg_cls, \
             patch("workers.reminder_worker.WhatsAppService") as mock_wa_cls:
            run()
        mock_tg_cls.return_value.send_message_with_buttons_sync.assert_not_called()
        mock_wa_cls.return_value.send_text_message.assert_not_called()

    def test_run_notifies_telegram_user(self):
        row = {
            "id": "job-1", "user_id": "987654",
            "client_name": "Acme", "bill_no": "INV-01", "fees": 10000,
            "poc_email": None, "poc_name": None,
            "invoice_date": "2024-01-01",
            "first_reminder_sent": None, "second_reminder_sent": None, "third_reminder_sent": None,
        }
        mock_db = MagicMock()
        # First call = scan query, subsequent calls = mark sent
        mock_db.execute_sql.side_effect = [
            {"ok": True, "rows": [row]},
            {"ok": True},  # mark_reminders_sent
        ]
        mock_tg = MagicMock()
        mock_wa = MagicMock()
        with patch("workers.reminder_worker.SupabaseService", return_value=mock_db), \
             patch("workers.reminder_worker.TelegramService", return_value=mock_tg), \
             patch("workers.reminder_worker.WhatsAppService", return_value=mock_wa):
            run()
        mock_tg.send_message_with_buttons_sync.assert_called_once()
        mock_wa.send_text_message.assert_not_called()

    def test_run_notifies_whatsapp_user(self):
        row = {
            "id": "job-2", "user_id": "whatsapp:+919876543210",
            "client_name": "Nike", "bill_no": "INV-02", "fees": 20000,
            "poc_email": "poc@nike.com", "poc_name": "John",
            "invoice_date": "2024-01-01",
            "first_reminder_sent": None, "second_reminder_sent": None, "third_reminder_sent": None,
        }
        mock_db = MagicMock()
        mock_db.execute_sql.side_effect = [
            {"ok": True, "rows": [row]},
            {"ok": True},
        ]
        mock_tg = MagicMock()
        mock_wa = MagicMock()
        with patch("workers.reminder_worker.SupabaseService", return_value=mock_db), \
             patch("workers.reminder_worker.TelegramService", return_value=mock_tg), \
             patch("workers.reminder_worker.WhatsAppService", return_value=mock_wa), \
             patch("workers.reminder_worker.save_pending"):
            run()
        mock_wa.send_text_message.assert_called_once()
        mock_tg.send_message_with_buttons_sync.assert_not_called()  # WhatsApp user, not Telegram
