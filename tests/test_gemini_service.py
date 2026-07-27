"""
WP-5 of ASSISTANT_PLAN.md — latency. GeminiService is a process-lifetime
singleton (constructed once inside IntentService's own singleton
construction in main.py), but _verify()/_call_api() used to open
`with httpx.Client(...) as client:` on EVERY call — a fresh TCP connection +
TLS handshake per OpenRouter round-trip despite the service living for the
whole process. A chat turn makes 1-4+ of these calls. This pins the fix: one
persistent, lazily-created httpx.Client reused across every call.

No test file existed for services/gemini_service.py before this one.
"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from unittest.mock import patch, MagicMock

from services.gemini_service import GeminiService


def _svc():
    """A GeminiService with a fake key, bypassing the real _verify() network
    call at construction time."""
    with patch.object(GeminiService, "_verify", return_value=True):
        with patch.dict(os.environ, {"AI_KEY": "fake-key-for-tests"}):
            return GeminiService()


class TestHttpClientReuse:
    def test_client_created_lazily(self):
        """No httpx.Client exists until the first call needs one."""
        with patch("httpx.Client") as mock_cls:
            with patch.object(GeminiService, "_verify", return_value=True):
                with patch.dict(os.environ, {"AI_KEY": "fake"}):
                    GeminiService()
            mock_cls.assert_not_called()

    def test_same_client_reused_across_call_api_calls(self):
        svc = _svc()
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = {"choices": [{"message": {"content": "hi"}}]}

        with patch("httpx.Client") as mock_cls:
            mock_client = MagicMock()
            mock_client.post.return_value = mock_resp
            mock_cls.return_value = mock_client

            svc._call_api("prompt one")
            svc._call_api("prompt two")
            svc._call_api("prompt three")

        # httpx.Client() the CONSTRUCTOR must only be invoked once — the
        # anti-pattern this fixes is a fresh Client (and TCP+TLS handshake)
        # on every single call.
        assert mock_cls.call_count == 1
        assert mock_client.post.call_count == 3

    def test_same_client_reused_between_verify_and_call_api(self):
        """_verify() (startup) and _call_api() (every turn) must share the
        SAME pooled connection, not each open their own."""
        with patch("httpx.Client") as mock_cls:
            mock_client = MagicMock()
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.raise_for_status.return_value = None
            mock_resp.json.return_value = {"choices": [{"message": {"content": "ok"}}]}
            mock_client.post.return_value = mock_resp
            mock_cls.return_value = mock_client

            with patch.dict(os.environ, {"AI_KEY": "fake-key"}):
                svc = GeminiService()  # runs the real _verify() this time
            svc._call_api("prompt")

        assert mock_cls.call_count == 1

    def test_call_api_no_longer_uses_context_manager_teardown(self):
        """The old pattern (`with httpx.Client(...) as client`) calls
        __exit__ (closing the connection) after every single request — the
        exact behaviour being removed. The persistent client's __exit__/close
        must NOT be invoked between calls."""
        svc = _svc()
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = {"choices": [{"message": {"content": "hi"}}]}

        with patch("httpx.Client") as mock_cls:
            mock_client = MagicMock()
            mock_client.post.return_value = mock_resp
            mock_cls.return_value = mock_client
            svc._call_api("prompt one")
            svc._call_api("prompt two")

        mock_client.__exit__.assert_not_called()
        mock_client.close.assert_not_called()

    def test_close_releases_the_client(self):
        svc = _svc()
        with patch("httpx.Client") as mock_cls:
            mock_client = MagicMock()
            mock_resp = MagicMock()
            mock_resp.raise_for_status.return_value = None
            mock_resp.json.return_value = {"choices": [{"message": {"content": "hi"}}]}
            mock_client.post.return_value = mock_resp
            mock_cls.return_value = mock_client
            svc._call_api("prompt")
            svc.close()
        mock_client.close.assert_called_once()
        assert svc._http_client is None

    def test_close_is_safe_when_never_used(self):
        svc = _svc()
        svc.close()  # must not raise even though no client was ever created

    def test_new_client_created_after_close(self):
        svc = _svc()
        with patch("httpx.Client") as mock_cls:
            mock_client1, mock_client2 = MagicMock(), MagicMock()
            mock_resp = MagicMock()
            mock_resp.raise_for_status.return_value = None
            mock_resp.json.return_value = {"choices": [{"message": {"content": "hi"}}]}
            mock_client1.post.return_value = mock_resp
            mock_client2.post.return_value = mock_resp
            mock_cls.side_effect = [mock_client1, mock_client2]

            svc._call_api("prompt one")
            svc.close()
            svc._call_api("prompt two")

        assert mock_cls.call_count == 2

    def test_call_api_still_counts_toward_turn_telemetry(self):
        """The WP-0 telemetry hook (note_llm_call) must survive this
        refactor unchanged."""
        from utils.telemetry import Turn, current_llm_calls
        svc = _svc()
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = {"choices": [{"message": {"content": "hi"}}]}
        with patch("httpx.Client") as mock_cls:
            mock_client = MagicMock()
            mock_client.post.return_value = mock_resp
            mock_cls.return_value = mock_client
            with Turn("u1"):
                svc._call_api("p1")
                svc._call_api("p2")
                assert current_llm_calls() == 2

    def test_verify_uses_a_shorter_timeout_than_call_api(self):
        """_verify() (a startup health check) must not block as long as a
        real generation call — timeout is passed per-request now that both
        share one Client with a 30s default."""
        with patch("httpx.Client") as mock_cls:
            mock_client = MagicMock()
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_client.post.return_value = mock_resp
            mock_cls.return_value = mock_client
            with patch.dict(os.environ, {"AI_KEY": "fake-key"}):
                GeminiService()
        call_kwargs = mock_client.post.call_args.kwargs
        assert call_kwargs.get("timeout") == 10.0
