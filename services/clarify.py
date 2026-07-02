"""
Clarify — answer genuine intent forks with an assumption + an offer, instead of
silently guessing.

Some questions have two right answers. The classic one: "how much have I MADE
from X?" — billed (SUM fees) vs received (SUM fees where paid). Rather than pick
silently (and be wrong half the time) or block with a question on every query,
we answer the most-likely reading, STATE the assumption, and offer the other in
one line: the user gets a number now and can correct in a tap.

This module is the pure detector. The execution (running both figures, storing
the pending choice, resolving the reply) lives in intent_service, gated by the
KnowledgeBook flag.

Scope is deliberately NARROW — a fork only fires on neutral value words
("made/earned/worth/kamaya") naming a client, and NOT when the user already
specified billed/received/owed. Over-asking is the failure mode we avoid.
"""
import re

from services.query_guard import _client_in_message

# Neutral value words that don't say billed-vs-received.
_NEUTRAL = re.compile(r"\b(made|make|earn|earned|earning|earnings|worth|kamaya|kamaai|kamai)\b")
# If any of these appear the user HAS specified the reading — no fork.
_SPECIFIED = re.compile(
    r"\b(billed|invoiced|invoice|raised|gross|received|recieved|collected|cleared|"
    r"in the bank|aaya|aayi|owe|owed|owes|owing|unpaid|outstanding|pending|baki|baaki|due|paid)\b")

# A date / period reference. The fork's SQL is all-time + client-only, so it
# CANNOT honour a window — firing on a dated value query ("earnings from Nike
# last quarter") would silently return an all-time figure. When a date is
# present we bow out and let the planner (which does honour time_range) answer.
_TIME = re.compile(
    r"\b(?:last|this|next|past|current|coming)\s+(?:week|fortnight|month|quarter|qtr|year)\b"
    r"|\b(?:pichhle|pichle|is|iss|agle|agla)\s+(?:hafte|hafte|mahine|maheene|saal|varsh)\b"
    r"|\bq[1-4]\b|\b20\d{2}\b"
    r"|\b(?:jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec)\b"
    r"|\b(?:january|february|march|april|june|july|august|september|october|november|december)\b"
    r"|\b(?:today|yesterday|tomorrow|aaj|kal|parso)\b")

# Reply intent when resolving a pending fork.
_RECV = re.compile(r"\b(received|recieved|paid|collected|cleared|bank|aaya|aayi|cash)\b")
_BILL = re.compile(r"\b(billed|invoiced|total|raised|gross|all)\b")


def detect_value_fork(message: str, known_clients=()):
    """Return {"fork":"billed_vs_received","client":X} when the message asks a
    neutral 'how much from X' that forks billed vs received, else None. Bows out
    when a reading is already specified OR a date window is present (the fork is
    all-time; the planner honours dates)."""
    m = " " + (message or "").lower().strip() + " "
    if not _NEUTRAL.search(m) or _SPECIFIED.search(m) or _TIME.search(m):
        return None
    client = _client_in_message(m, known_clients, use_heuristic=True)
    if not client:
        return None
    return {"fork": "billed_vs_received", "client": client}


def resolve_reply(message: str):
    """Map a reply to a pending billed-vs-received offer → 'received' | 'billed' | None."""
    m = " " + (message or "").lower().strip() + " "
    if _RECV.search(m):
        return "received"
    if _BILL.search(m):
        return "billed"
    return None
