"""
Regression tests for P0-4 (PLAN_OF_ACTION.md): webhooks must ack before the
turn finishes, and a redelivered webhook must not re-process the same
message.

Needs a REAL `fastapi` install (checked via importlib.util.find_spec, not a
bare `import` — tests/conftest.py stubs `fastapi` with a MagicMock when it
isn't installed, and a bare `import fastapi` would "succeed" against that
stub and then fail confusingly deep inside these tests instead of skipping
cleanly). Run under an environment with requirements.txt installed, e.g.:

    .venv-e2e/bin/python -m pytest tests/test_webhook_dispatch.py -v

CI installs requirements.txt before `pytest tests/`, so these run for real
there. No pytest-asyncio dependency — each test drives its own asyncio
event loop via asyncio.run() around a small async body.

Every test drives main.py's webhook coroutines directly (not through
Starlette's TestClient/ASGI transport), so nothing here ever triggers
`@app.on_event("startup")` — that handler runs real schema migrations
against SUPABASE_DB_URL if it's set, which must never happen from a test.
All credential env vars are cleared before `main` is imported as a second,
independent guard against a real network/DB call from any service
constructor, regardless of what's in the ambient shell.
"""

import asyncio
import importlib.util
import sys
from unittest.mock import Mock

import pytest


def _fastapi_really_installed() -> bool:
    # tests/conftest.py pre-populates sys.modules["fastapi"] with a
    # MagicMock when the real package isn't installed — a bare `import
    # fastapi` or `"fastapi" in sys.modules` would both see that stub and
    # think it's present. Check for the stub explicitly first.
    cached = sys.modules.get("fastapi")
    if isinstance(cached, Mock):
        return False
    try:
        # find_spec raises ValueError (not ImportError) if a module is
        # already cached in sys.modules with no real __spec__ — exactly
        # what happens if some OTHER stub left a Mock behind under a
        # different check than the one above; belt and suspenders.
        return importlib.util.find_spec("fastapi") is not None
    except (ImportError, ValueError):
        return False


if not _fastapi_really_installed():
    pytest.skip(
        "fastapi not installed in this interpreter — run under "
        ".venv-e2e (pip install -r requirements.txt) to exercise these",
        allow_module_level=True,
    )

_CREDENTIAL_ENV_VARS = [
    "AI_KEY", "SUPABASE_URL", "SUPABASE_SERVICE_ROLE_KEY", "SUPABASE_DB_URL",
    "TWILIO_ACCOUNT_SID", "TWILIO_AUTH_TOKEN", "TWILIO_WHATSAPP_NUMBER",
    "TELEGRAM_BOT_TOKEN", "RESEND_API", "RESEND_FROM_EMAIL", "BASE_URL",
]


class _FakeTelegramRequest:
    """Stands in for starlette.Request — telegram_webhook only calls
    `await request.json()`, so that's all this needs to provide."""

    def __init__(self, payload):
        self._payload = payload

    async def json(self):
        return self._payload


async def _async_noop(*args, **kwargs):
    return None


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture
def main_module(monkeypatch):
    for var in _CREDENTIAL_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    sys.modules.pop("main", None)
    import main as _main
    yield _main
    sys.modules.pop("main", None)


async def _drain_background_tasks(passes: int = 5, delay: float = 0.05):
    """asyncio.create_task schedules but doesn't run immediately — give the
    loop enough turns for a fire-and-forget task (and anything IT awaits) to
    actually finish before asserting on its side effects."""
    for _ in range(passes):
        await asyncio.sleep(delay)


class TestWhatsAppWebhookAcksImmediately:
    def test_returns_before_the_slow_turn_finishes(self, main_module, monkeypatch):
        TURN_SECONDS = 0.15

        def _sync_slow(user_id, message):
            import time
            time.sleep(TURN_SECONDS)
            return {"operation": "query", "response": "ok",
                    "trigger_invoice": False, "invoice_data": {}}

        monkeypatch.setattr(main_module.intent_service, "process_request", _sync_slow)
        sent = []
        monkeypatch.setattr(main_module.whatsapp_service, "send_text_message",
                             lambda to, body: sent.append((to, body)))
        monkeypatch.setattr(main_module.whatsapp_service, "send_typing_indicator",
                             lambda sid: None)

        async def _body():
            loop = asyncio.get_event_loop()
            t0 = loop.time()
            resp = await main_module.whatsapp_webhook(
                Body="show my jobs", From="whatsapp:+15550001111", MessageSid="SMabc123",
            )
            elapsed = loop.time() - t0

            assert resp.status_code == 204
            # The endpoint must return well before the turn completes.
            assert elapsed < TURN_SECONDS / 2, (
                f"webhook blocked for {elapsed:.3f}s waiting on the turn"
            )

            # Comfortably longer than TURN_SECONDS — the turn runs in a
            # real threadpool executor thread, not just a scheduled
            # coroutine, so it needs actual wall-clock time to finish.
            await _drain_background_tasks(passes=10, delay=0.05)
            assert sent == [("whatsapp:+15550001111", "ok")], (
                "the turn should still complete and send its reply — just "
                "not before the webhook acked"
            )

        _run(_body())


class TestWhatsAppWebhookDedupe:
    def test_same_message_sid_is_processed_only_once(self, main_module, monkeypatch):
        calls = []

        def _counting_process_request(user_id, message):
            calls.append((user_id, message))
            return {"operation": "query", "response": "ok",
                    "trigger_invoice": False, "invoice_data": {}}

        monkeypatch.setattr(main_module.intent_service, "process_request",
                             _counting_process_request)
        monkeypatch.setattr(main_module.whatsapp_service, "send_text_message",
                             lambda to, body: None)
        monkeypatch.setattr(main_module.whatsapp_service, "send_typing_indicator",
                             lambda sid: None)

        async def _body():
            for _ in range(2):
                resp = await main_module.whatsapp_webhook(
                    Body="show my jobs", From="whatsapp:+15550001111", MessageSid="SMdupe1",
                )
                assert resp.status_code == 204
            await _drain_background_tasks()

        _run(_body())
        assert len(calls) == 1, f"expected 1 processed turn, got {len(calls)}"

    def test_different_message_sids_both_process(self, main_module, monkeypatch):
        calls = []
        monkeypatch.setattr(
            main_module.intent_service, "process_request",
            lambda user_id, message: calls.append(message) or
            {"operation": "query", "response": "ok",
             "trigger_invoice": False, "invoice_data": {}},
        )
        monkeypatch.setattr(main_module.whatsapp_service, "send_text_message",
                             lambda to, body: None)
        monkeypatch.setattr(main_module.whatsapp_service, "send_typing_indicator",
                             lambda sid: None)

        async def _body():
            await main_module.whatsapp_webhook(Body="msg one", From="whatsapp:+1555", MessageSid="SM-A")
            await main_module.whatsapp_webhook(Body="msg two", From="whatsapp:+1555", MessageSid="SM-B")
            await _drain_background_tasks()

        _run(_body())
        assert calls == ["msg one", "msg two"]


class TestInvoiceGenerationStillFiresInBackground:
    """Correctness guard for the mechanical change this refactor required:
    process_and_send_invoice used to be scheduled via FastAPI's
    BackgroundTasks (which only runs tasks queued before the endpoint's OWN
    response is sent). Once the webhook stopped awaiting the turn, anything
    _handle_bot_message queued onto that BackgroundTasks instance would run
    AFTER FastAPI had already collected an empty task list — silently
    dropped. _handle_bot_message now awaits process_and_send_invoice
    directly instead; this proves it still actually runs."""

    def test_trigger_invoice_calls_process_and_send_invoice(self, main_module, monkeypatch):
        invoice_calls = []

        async def _fake_process_and_send_invoice(to_number, client_name, month, **kw):
            invoice_calls.append((to_number, client_name, month, kw))

        monkeypatch.setattr(main_module, "process_and_send_invoice", _fake_process_and_send_invoice)
        monkeypatch.setattr(
            main_module.intent_service, "process_request",
            lambda user_id, message: {
                "operation": "invoice", "response": "On it…",
                "trigger_invoice": True,
                "invoice_data": {"client_name": "Nike", "month": "April"},
            },
        )
        monkeypatch.setattr(main_module.whatsapp_service, "send_text_message",
                             lambda to, body: None)
        monkeypatch.setattr(main_module.whatsapp_service, "send_typing_indicator",
                             lambda sid: None)

        async def _body():
            await main_module.whatsapp_webhook(
                Body="generate invoice for nike april",
                From="whatsapp:+15550001111", MessageSid="SMinv1",
            )
            await _drain_background_tasks()

        _run(_body())

        assert len(invoice_calls) == 1
        to_number, client_name, month, kw = invoice_calls[0]
        assert to_number == "whatsapp:+15550001111"
        assert client_name == "Nike"
        assert month == "April"


class TestTelegramWebhookDedupe:
    def test_same_update_id_is_processed_only_once(self, main_module, monkeypatch):
        calls = []
        monkeypatch.setattr(
            main_module.intent_service, "process_request",
            lambda user_id, message: calls.append(message) or
            {"operation": "query", "response": "ok",
             "trigger_invoice": False, "invoice_data": {}},
        )
        monkeypatch.setattr(main_module.telegram_service, "send_chat_action", _async_noop)
        monkeypatch.setattr(main_module.telegram_service, "send_text_message", _async_noop)

        payload = {
            "update_id": 555111,
            "message": {"chat": {"id": 42}, "text": "show my jobs"},
        }

        async def _body():
            r1 = await main_module.telegram_webhook(_FakeTelegramRequest(payload))
            r2 = await main_module.telegram_webhook(_FakeTelegramRequest(payload))
            await _drain_background_tasks()
            assert r1 == {"status": "ok"}
            assert r2 == {"status": "ok"}

        _run(_body())
        assert len(calls) == 1, f"expected 1 processed turn, got {len(calls)}"

    def test_callback_query_with_duplicate_id_runs_once(self, main_module, monkeypatch):
        handled = []

        async def _fake_handle_reminder_callback(callback_query):
            handled.append(callback_query["id"])

        monkeypatch.setattr(main_module, "_handle_reminder_callback", _fake_handle_reminder_callback)

        payload = {"callback_query": {"id": "cbq-999", "data": "remind:skip:all",
                                       "message": {"chat": {"id": 1}, "message_id": 2}}}

        async def _body():
            await main_module.telegram_webhook(_FakeTelegramRequest(payload))
            await main_module.telegram_webhook(_FakeTelegramRequest(payload))

        _run(_body())
        assert handled == ["cbq-999"], (
            "a redelivered button tap must not re-run the callback handler "
            "(e.g. re-sending a batch of reminder emails)"
        )
