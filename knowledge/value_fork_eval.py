"""
Value-fork (billed-vs-received clarify) precision/recall eval.

The fork is a DETERMINISTIC detector (services.clarify.detect_value_fork) — no
LLM, so this measures at zero API cost. The decision it gates: when a user asks
a neutral "how much have I made from X", answer billed + offer received instead
of guessing. The safety-critical property is PRECISION — it must never hijack a
query that already has a clear reading, because the fork's SQL is all-time and
client-only (it drops any date window and any status the user specified).

Labels:
  * fire=True  — a genuine neutral value-from-client ambiguity the fork SHOULD own.
  * fire=False — a clear/blocking reading it must LEAVE ALONE (specified status,
    no client, no value word, OR a DATED value query whose date the fork can't
    honour).

    KB_VALUE_FORK=1 makes it live; measure here first.

    python -m knowledge.value_fork_eval
"""
from services.clarify import detect_value_fork, resolve_reply
from knowledge.dataset import entities

# Known clients as the live path sees them (client + brand names, lowercased).
_ents = entities()
KNOWN = sorted({*(c.lower() for c in _ents["clients"]), *(b.lower() for b in _ents["brands"])})

# (message, should_fire, note)
CORPUS = [
    # ── SHOULD FIRE: neutral value word + real client, no reading specified ──
    ("how much have I made from Star Studios", True, "made+client"),
    ("how much did I earn from Nike", True, "earn+brand"),
    ("Samsung se kitna kamaya", True, "hinglish kamaya"),
    ("what's Garnier worth to me", True, "worth+client"),
    ("total I've made off Maruti", True, "made off"),
    ("earnings from Pepsi", True, "earnings+brand"),
    ("how much money have I made from Swiggy", True, "made money"),
    ("Cadbury se kitni kamai hui", True, "hinglish kamai"),
    ("what have I earned from Adidas", True, "earned+brand"),
    ("how much have I made from Lays", True, "made+brand"),

    # ── SHOULD NOT FIRE: reading already specified (clear question) ──────────
    ("how much has Star Studios paid", False, "paid specified"),
    ("total billed for Nike", False, "billed specified"),
    ("how much does Samsung owe me", False, "owed specified"),
    ("unpaid amount from Garnier", False, "unpaid specified"),
    ("how much have I received from Maruti", False, "received specified"),
    ("what's outstanding from Pepsi", False, "outstanding specified"),
    ("Nike se paisa aaya kya", False, "hinglish paisa aaya"),
    ("invoiced total for Swiggy", False, "invoiced specified"),
    ("what's pending from Cadbury", False, "pending specified"),
    ("Garnier ka due kitna hai", False, "hinglish due"),

    # ── SHOULD NOT FIRE: no value word (a different intent entirely) ─────────
    ("how many jobs for Nike", False, "count, no value word"),
    ("show Samsung jobs", False, "list"),
    ("list unpaid invoices", False, "status list"),
    ("when was my last Star Studios job", False, "date lookup"),
    ("average fee for Nike", False, "avg, no neutral word"),

    # ── SHOULD NOT FIRE: neutral value word but NO client ────────────────────
    ("how much have I made", False, "bare, no client"),
    ("what are my total earnings", False, "earnings, no client"),
    ("how much did I earn overall", False, "earn, no client"),

    # ── SHOULD NOT FIRE: DATED value query — fork drops the date → wrong ──────
    ("how much did I earn from Nike last quarter", False, "earn+client+DATE"),
    ("earnings from Samsung this month", False, "earnings+client+DATE"),
    ("what have I made from Garnier in 2026", False, "made+client+DATE"),
    ("Pepsi se pichle mahine kitna kamaya", False, "hinglish made+client+DATE"),
    ("how much have I earned from Maruti this year", False, "earned+client+DATE"),
    ("total made from Swiggy last month", False, "made+client+DATE"),
]

RESOLVE = [
    ("received please", "received"), ("in the bank", "received"),
    ("paisa aaya", "received"), ("the billed total", "billed"),
    ("gross", "billed"), ("all of it", "billed"), ("hmm not sure", None),
]


def run():
    tp = fp = tn = fn = 0
    misses = []
    for msg, should, note in CORPUS:
        fired = detect_value_fork(msg, KNOWN) is not None
        if should and fired:
            tp += 1
        elif should and not fired:
            fn += 1
            misses.append(("FN (missed a real fork)", msg, note))
        elif not should and fired:
            fp += 1
            det = detect_value_fork(msg, KNOWN)
            misses.append((f"FP (hijacked! client={det['client']!r})", msg, note))
        else:
            tn += 1

    prec = tp / (tp + fp) if (tp + fp) else 1.0
    rec = tp / (tp + fn) if (tp + fn) else 1.0

    print(f"Value-fork detector over {len(CORPUS)} labelled messages "
          f"({len(KNOWN)} known clients)\n")
    print(f"  TP={tp}  FP={fp}  TN={tn}  FN={fn}")
    print(f"  Precision = {prec*100:.0f}%   Recall = {rec*100:.0f}%\n")

    if misses:
        print("  Misclassifications:")
        for kind, msg, note in misses:
            print(f"    [{kind}]  {msg!r}  ({note})")
    else:
        print("  Clean sweep — no misclassifications.")

    # resolve_reply sanity
    rbad = [(m, resolve_reply(m), exp) for m, exp in RESOLVE if resolve_reply(m) != exp]
    print(f"\n  resolve_reply: {len(RESOLVE)-len(rbad)}/{len(RESOLVE)} correct", end="")
    print("" if not rbad else f"  MISMATCH: {rbad}")

    # Ship gate: precision must be perfect (a hijack gives a WRONG answer);
    # a couple of missed forks (FN) only cost a fallback to the planner.
    ok = fp == 0 and not rbad
    print(f"\n  SHIP GATE (precision==100%, resolve clean): {'PASS' if ok else 'FAIL'}")
    return ok


if __name__ == "__main__":
    import sys
    sys.exit(0 if run() else 1)
