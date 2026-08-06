"""Regression tests for utils/dedupe.py (P0-4, PLAN_OF_ACTION.md)."""

import utils.dedupe as dedupe_mod
from utils.dedupe import TTLDedupe


class TestTTLDedupe:
    def test_first_sighting_is_not_a_duplicate(self):
        d = TTLDedupe(ttl_seconds=60)
        assert d.seen_recently("abc") is False

    def test_second_sighting_within_ttl_is_a_duplicate(self):
        d = TTLDedupe(ttl_seconds=60)
        d.seen_recently("abc")
        assert d.seen_recently("abc") is True

    def test_sighting_after_ttl_expires_is_not_a_duplicate(self, monkeypatch):
        t = [1000.0]
        monkeypatch.setattr(dedupe_mod.time, "monotonic", lambda: t[0])
        d = TTLDedupe(ttl_seconds=10)
        d.seen_recently("abc")
        t[0] += 11
        assert d.seen_recently("abc") is False

    def test_none_key_is_never_a_duplicate(self):
        d = TTLDedupe(ttl_seconds=60)
        assert d.seen_recently(None) is False
        assert d.seen_recently(None) is False

    def test_empty_string_key_is_never_a_duplicate(self):
        d = TTLDedupe(ttl_seconds=60)
        assert d.seen_recently("") is False
        assert d.seen_recently("") is False

    def test_different_keys_are_independent(self):
        d = TTLDedupe(ttl_seconds=60)
        assert d.seen_recently("a") is False
        assert d.seen_recently("b") is False
        assert d.seen_recently("a") is True

    def test_cleanup_sweep_does_not_evict_a_fresh_key(self):
        # The >2000-entry sweep only evicts entries OLDER than the TTL
        # window — everything inserted here is fresh, so a key checked a
        # second time right after the sweep must still read as a duplicate.
        d = TTLDedupe(ttl_seconds=60)
        for i in range(2005):
            d.seen_recently(f"noise-{i}")
        assert d.seen_recently("noise-2004") is True
