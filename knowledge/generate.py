"""
KnowledgeBook — examples generator.

Expands templates × entities × paraphrases into {question, plan} pairs, then the
oracle fills the correct `answer` from the seeded dataset. Run it to (re)build
knowledge/examples.jsonl — deterministic, so the examples are reproducible.

    python -m knowledge.generate         # writes knowledge/examples.jsonl

Scaling = add templates / paraphrases / entities below (and grow the dataset).
Answers never need hand-labelling: the oracle computes them.

INTEGRITY: every generated question is checked against the held-out eval set
(knowledge/eval_hard.py) and skipped on a match, so the KnowledgeBook can never
win the A/B by memorising an eval question. Keep it that way.
"""
import os
import re
import json
import calendar

from knowledge.dataset import build_dataset, entities
from knowledge.oracle import compute_answer
from knowledge.eval_hard import cases as _eval_cases

CORPUS_PATH = os.path.join(os.path.dirname(__file__), "examples.jsonl")


def _norm(s: str) -> str:
    """Normalise a question for dedup + eval-leak checks: lowercase, collapse
    whitespace, drop trailing punctuation."""
    return re.sub(r"\s+", " ", (s or "").strip().lower()).rstrip("?.! ")


# Held-out eval questions — NEVER emit these into the corpus.
_EVAL_Q = {_norm(c["question"]) for c in _eval_cases()}


def _abs(start: str, end: str):
    return {"type": "absolute", "value": {"start": start, "end": end}}


def _month_range(month_name: str, year: int):
    mnum = list(calendar.month_name).index(month_name)
    last = calendar.monthrange(year, mnum)[1]
    return _abs(f"{year}-{mnum:02d}-01", f"{year}-{mnum:02d}-{last:02d}")


# Relative windows resolved against today = 2026-07-02 (matches eval_hard).
_THIS_YEAR = _abs("2026-01-01", "2026-12-31")
_LAST_YEAR = _abs("2025-01-01", "2025-12-31")
_LAST_QUARTER = _abs("2026-04-01", "2026-06-30")   # Q2 2026
_LAST_MONTH = _abs("2026-06-01", "2026-06-30")      # June 2026
_Q1_2026 = _abs("2026-01-01", "2026-03-31")
_Q2_2026 = _abs("2026-04-01", "2026-06-30")
_Q3_2025 = _abs("2025-07-01", "2025-09-30")
_Q4_2025 = _abs("2025-10-01", "2025-12-31")


# ── Slot pools ───────────────────────────────────────────────────────────────
_CLIENTS = entities()["clients"]                 # all 10 billing clients
_BRANDS = ["Nike", "Adidas", "Puma", "Samsung", "Galaxy", "Maruti", "Swift",
           "Pepsi", "Lays", "Cadbury", "Bournvita", "Swiggy", "Zomato", "Surf Excel"]
_ENTS = _CLIENTS + _BRANDS                        # 22 entities we slot into client cats
_MONTHS = [("January", 2026), ("February", 2026), ("March", 2026), ("April", 2026),
           ("May", 2026), ("December", 2025), ("November", 2025), ("June", 2025)]
_QUARTERS = [("Q1 2026", _Q1_2026), ("Q2 2026", _Q2_2026),
             ("Q3 2025", _Q3_2025), ("Q4 2025", _Q4_2025)]
_YEARS = [("2026", _THIS_YEAR), ("2025", _LAST_YEAR)]
_REL = [("this year", _THIS_YEAR), ("last quarter", _LAST_QUARTER)]

# ── Paraphrase templates. Grow these to scale. (template, lang) ──────────────
_T_COUNT_CLIENT = [("how many jobs for {c}?", "en"), ("how many {c} jobs", "en"),
                   ("number of {c} projects", "en"), ("count of {c} gigs", "en"),
                   ("{c} ke kitne jobs", "hi"), ("{c} ka kitna kaam hua", "hi")]
_T_TOTAL_CLIENT = [("total fees for {c}", "en"), ("how much have I billed {c}", "en"),
                   ("total billing for {c}", "en"), ("{c} ka total billing", "hi"),
                   ("{c} se total kitna aaya", "hi"), ("{c} se ab tak kitna kamaya", "hi")]
_T_LIST_CLIENT = [("show me {c} jobs", "en"), ("list all {c} work", "en"),
                  ("show all {c} projects", "en"), ("{c} ka kaam dikhao", "hi"),
                  ("{c} ke saare jobs", "hi")]
_T_AVG_CLIENT = [("average fee for {c}", "en"), ("what's my average on {c} jobs", "en"),
                 ("{c} ka average kitna hai", "hi")]
_T_OWES = [("how much does {c} owe me", "en"), ("total unpaid for {c}", "en"),
           ("how much is outstanding from {c}", "en"), ("{c} ka kitna baki hai", "hi"),
           ("{c} se kitna aana baki hai", "hi"), ("{c} ka kitna paisa fasa hai", "hi")]
_T_UNPAID_COUNT_CLIENT = [("how many unpaid jobs for {c}", "en"),
                          ("{c} ke kitne unpaid hain", "hi"),
                          ("{c} ke kitne payment baki hain", "hi")]
_T_PAID_COUNT_CLIENT = [("how many paid jobs for {c}", "en"), ("{c} ke kitne paid hain", "hi")]
_T_PAID_TOTAL_CLIENT = [("how much has {c} paid me", "en"), ("total received from {c}", "en"),
                        ("{c} se kitna paisa mila", "hi")]
_T_SENT_CLIENT = [("how many invoices sent to {c}", "en"), ("{c} ko kitne invoice bheje", "hi")]

# ══════════════════════════════════════════════════════════════════════════════
# NEW COVERAGE AREAS (from live WhatsApp transcripts, 2026-07). Four intents the
# planner kept fumbling. Every template below maps to an oracle-computable plan.
# ══════════════════════════════════════════════════════════════════════════════

# ── Area A — COLLECTIONS: who hasn't paid / who owes / chase ──────────────────
# The people-centric framing of "unpaid" (IMG 1: "who hasnt paid me yet"). Global
# phrasings default to the unpaid LIST (the set of jobs/clients still owing).
_A_BARE_LIST = [
    ("who hasn't paid me yet", "en"), ("who still owes me money", "en"),
    ("which clients haven't paid me", "en"), ("show me everyone who owes me", "en"),
    ("who do I need to chase for payment", "en"), ("who should I follow up with for payment", "en"),
    ("list everyone with a pending payment", "en"), ("which payments are still outstanding", "en"),
    ("show me all my pending collections", "en"), ("who hasn't cleared their dues yet", "en"),
    ("which clients still need to pay me", "en"), ("show me who is yet to pay", "en"),
    ("who owes me at the moment", "en"), ("list the clients that still owe me", "en"),
    ("kisne abhi tak payment nahi kiya", "hi"), ("kaun kaun paisa dena baki hai", "hi"),
    ("kis kis ka payment pending hai", "hi"), ("kis se paisa lena baki hai", "hi"),
    ("kaun log payment nahi kiye", "hi"), ("abhi tak kaun nahi paya", "hi"),
    ("kiska kiska paisa fasa hua hai", "hi"),
]
_A_BARE_COUNT = [
    ("how many clients still owe me", "en"), ("how many payments are pending", "en"),
    ("how many are yet to pay me", "en"), ("how many jobs are still unpaid", "en"),
    ("kitne log payment nahi kiye", "hi"), ("kitne payment pending hain", "hi"),
]
_A_BARE_SUM = [
    ("how much money is stuck in unpaid invoices", "en"),
    ("how much are people yet to pay me in total", "en"),
    ("what's my total pending collection", "en"),
    ("how much am I owed altogether", "en"),
    ("kitna paisa abhi aana baki hai", "hi"),
]
# "who owes the MOST" → group_by client over unpaid rows, top 1.
_A_OWES_TOP = [
    ("who owes me the most", "en"), ("which client owes me the most money", "en"),
    ("my biggest outstanding client", "en"), ("sabse zyada kiska paisa baki hai", "hi"),
]
_A_OWES_RANK = [
    ("rank my clients by how much they owe", "en"),
    ("which clients owe me the most, in order", "en"),
]
# Per-client "still owes" phrasings (distinct strings from _T_OWES) → unpaid sum.
_A_CLIENT = [
    ("is there anything pending from {c}", "en"), ("does {c} still owe me", "en"),
    ("what does {c} still need to pay", "en"), ("how much is stuck with {c}", "en"),
    ("{c} se kitna lena baki hai", "hi"),
]

# ── Area B — MONEY BUCKETS: earnings vs received vs outstanding ───────────────
# IMG 2 bug: "total earning from all jobs" must be SUM of ALL fees (no paid
# filter) — NOT the unpaid figure. Three buckets, kept strictly separate.
_B_TOTAL_ALL = [        # sum, NO filter — everything earned/billed
    ("what is my total earning from all jobs", "en"),
    ("total earnings from all jobs so far", "en"),
    ("how much have I earned in total across all jobs", "en"),
    ("my total earnings to date", "en"), ("what have I earned overall", "en"),
    ("grand total of all my job fees", "en"), ("sum of every job's fee", "en"),
    ("total value of all my work", "en"),
    ("how much have I made from all jobs combined", "en"),
    ("everything I've earned so far", "en"), ("total income from all my projects", "en"),
    ("total lifetime earnings", "en"), ("add up all my job fees", "en"),
    ("what's the total across every job", "en"),
    ("saare jobs se kul kitna kamaya", "hi"), ("ab tak sabhi kaam se kitna kamaya", "hi"),
    ("meri total kamai kitni hai", "hi"), ("sab milakar kitna banaya", "hi"),
]
_B_RECEIVED = [         # sum, paid=yes — money actually in
    ("how much money have I actually received", "en"),
    ("how much has landed in my account", "en"),
    ("total payment received so far", "en"),
    ("how much have clients actually paid me", "en"),
    ("how much have I collected so far", "en"), ("total cleared payments", "en"),
    ("how much money has come in", "en"), ("what's my received total", "en"),
    ("how much is actually in the bank from my jobs", "en"),
    ("kitna paisa actually aaya", "hi"), ("kitna payment mil chuka hai", "hi"),
    ("ab tak kitna received hua", "hi"), ("kitna paisa account me aaya", "hi"),
]
_B_OUTSTANDING = [      # sum, paid=no — still owed (IMG 2: "total outstanding payment")
    ("what is my total outstanding payment", "en"),
    ("total outstanding amount owed to me", "en"),
    ("how much is still to come in", "en"),
    ("how much are clients yet to pay me in total", "en"),
    ("my total dues outstanding", "en"),
    ("how much payment is still pending overall", "en"),
    ("of everything I've billed how much is still unpaid", "en"),
    ("how much of my earnings is still uncollected", "en"),
    ("total billed but not yet paid", "en"),
    ("kitna paisa abhi aana hai", "hi"), ("total bakaya kitna hai", "hi"),
]
# Per-client three-way split (novel strings): earned(all) / received(paid) / owed(unpaid)
_B_CLIENT_EARNED = [("what's my grand total from {c}", "en"), ("{c} se total kitni kamai hui", "hi")]
_B_CLIENT_RECEIVED = [("how much has {c} cleared so far", "en"), ("{c} ne kitna clear kiya", "hi")]
_B_CLIENT_OWED = [("how much is {c} yet to pay", "en"), ("{c} ka kitna bakaya hai", "hi")]

# ── Area C — CONTACT / RECIPIENT LOOKUP (poc_email) ──────────────────────────
# IMG 3 bug: "email for the Wilson job" dumped ALL jobs. Teach: scope to the one
# client (client-filtered list) — the row carries poc_email.
_C_CLIENT_EMAIL = [
    ("what's the email for {c}", "en"), ("do you have the recipient email for {c}", "en"),
    ("who do I send {c}'s invoice to", "en"), ("what's the contact email for {c}", "en"),
    ("what email should the {c} invoice go to", "en"), ("{c} ka email address kya hai", "hi"),
]
_C_NO_EMAIL = [         # list, poc_email null
    ("which clients don't have an email on file", "en"),
    ("which clients are missing an email", "en"),
    ("who doesn't have an email on file", "en"),
    ("show me jobs with no contact email", "en"),
    ("list clients without an email address", "en"),
    ("which contacts are missing their email", "en"),
    ("kis client ka email nahi hai", "hi"), ("kaunse jobs me email nahi hai", "hi"),
    ("email nahi hai aise clients dikhao", "hi"),
]
_C_HAS_EMAIL = [        # list, poc_email not_null
    ("which clients have an email on file", "en"),
    ("show me jobs that have a contact email", "en"),
    ("list clients with an email address", "en"),
    ("who do I have emails for", "en"),
    ("kis client ka email hai", "hi"), ("email wale jobs dikhao", "hi"),
]

# ── Area D — INVOICE DISPATCH: which bills sent / pending to raise ────────────
# IMG 1: "payment reminder sent?" → list of what's gone out vs still pending.
_D_SENT_LIST = [        # list, bill_sent yes
    ("which invoices have I already sent", "en"), ("show me the invoices I've sent", "en"),
    ("list the bills that have gone out", "en"), ("show me everything I've invoiced", "en"),
    ("which clients have I billed already", "en"), ("kaunse invoice bhej diye", "hi"),
]
_D_NOTSENT_LIST = [     # list, bill_sent no
    ("which invoices haven't gone out yet", "en"), ("show me invoices still to send", "en"),
    ("which jobs still need invoicing", "en"), ("who am I yet to invoice", "en"),
    ("list the bills pending to be sent", "en"), ("show me the invoices I still have to raise", "en"),
    ("show me who still needs an invoice", "en"), ("which clients haven't been invoiced yet", "en"),
    ("kaunse invoice bhejne baki hain", "hi"), ("kis kis ka invoice bhejna hai", "hi"),
    ("abhi kiska bill nahi bheja", "hi"), ("bhejne wale invoice dikhao", "hi"),
]
_D_SENT_COUNT = [       # count, bill_sent yes
    ("how many bills have gone out so far", "en"), ("how many invoices did I already raise", "en"),
    ("what's the count of invoices sent out", "en"),
]
_D_NOTSENT_COUNT = [    # count, bill_sent no
    ("how many invoices are still to go out", "en"), ("how many bills haven't I sent yet", "en"),
    ("how many clients still need invoicing", "en"), ("kitne invoice bhejne reh gaye", "hi"),
]
_D_CLIENT_SENT = [      # per client, list, bill_sent yes
    ("did I invoice {c}", "en"), ("have I billed {c} yet", "en"),
    ("show me the invoices sent to {c}", "en"), ("has {c}'s invoice gone out", "en"),
    ("{c} ko invoice bheja kya", "hi"),
]


def _plan(metric=None, column=None, filters=None, time_range=None,
          group_by=None, order=None, limit=None):
    return {"metric": metric, "column": column, "filters": filters or {},
            "time_range": time_range, "group_by": group_by, "order": order, "limit": limit}


def _entries():
    """Yield (category, question, plan, tags, lang) tuples."""
    P = _plan

    # ── bare aggregates ──────────────────────────────────────────────────
    for q, l in [("how many jobs do I have?", "en"), ("total number of jobs", "en"),
                 ("how many projects in total", "en"), ("count my jobs", "en"),
                 ("how many gigs in total", "en"), ("kitne jobs hue", "hi"),
                 ("total kitne kaam kiye", "hi")]:
        yield "count_all", q, P(metric="count"), ["count", "bare"], l
    for q, l in [("what's my total billing?", "en"), ("overall revenue so far", "en"),
                 ("what have I billed in total", "en"), ("sum of all my fees", "en"),
                 ("total revenue to date", "en"), ("total kitna kamaya", "hi"),
                 ("ab tak ka total kitna hai", "hi")]:
        yield "total_all", q, P(metric="sum", column="fees"), ["sum", "bare"], l
    for q, l in [("average fee per job", "en"), ("what's my average billing", "en"),
                 ("avg job value", "en"), ("mean fee per project", "en"),
                 ("ek job ka average kitna", "hi")]:
        yield "avg_all", q, P(metric="avg", column="fees"), ["avg", "bare"], l

    # ── payment status (bare) ────────────────────────────────────────────
    for q, l in [("how many paid jobs do I have", "en"), ("how many jobs are paid", "en"),
                 ("kitne paid hain", "hi")]:
        yield "count_paid", q, P(metric="count", filters={"paid": "yes"}), ["count", "status"], l
    for q, l in [("how many unpaid jobs do I have", "en"), ("how many jobs are unpaid", "en"),
                 ("how many are still unpaid", "en"), ("kitne unpaid hain", "hi")]:
        yield "count_unpaid", q, P(metric="count", filters={"paid": "no"}), ["count", "status"], l
    for q, l in [("how much have I collected in total", "en"), ("total paid amount", "en"),
                 ("total received so far", "en")]:
        yield "total_paid", q, P(metric="sum", column="fees", filters={"paid": "yes"}), ["sum", "status"], l
    for q, l in [("how much is unpaid in total", "en"), ("total outstanding amount", "en"),
                 ("how much am I still owed", "en"), ("total baki kitna hai", "hi")]:
        yield "total_unpaid", q, P(metric="sum", column="fees", filters={"paid": "no"}), ["sum", "status"], l
    for q, l in [("list my unpaid invoices", "en"), ("show unpaid jobs", "en"),
                 ("which jobs are unpaid", "en"), ("kaunse unpaid hain", "hi")]:
        yield "list_unpaid", q, P(filters={"paid": "no"}), ["list", "status"], l
    for q, l in [("list my paid jobs", "en"), ("show paid work", "en"), ("kaunse paid hain", "hi")]:
        yield "list_paid", q, P(filters={"paid": "yes"}), ["list", "status"], l

    # ── invoices sent (bill_sent) ────────────────────────────────────────
    for q, l in [("how many invoices have I sent", "en"), ("how many bills have I sent", "en"),
                 ("how many invoices went out", "en"), ("number of invoices sent", "en"),
                 ("kitne invoice bheje", "hi")]:
        yield "count_sent", q, P(metric="count", filters={"bill_sent": "yes"}), ["count", "bill_sent"], l
    for q, l in [("how many invoices are pending to send", "en"),
                 ("how many jobs still need invoicing", "en"),
                 ("how many invoices haven't gone out", "en"),
                 ("kitne invoice bhejne baki hain", "hi"),
                 ("kaunse invoice nahi bheje", "hi")]:
        yield "count_not_sent", q, P(metric="count", filters={"bill_sent": "no"}), ["count", "bill_sent"], l
    for q, l in [("total value of invoices sent", "en"), ("how much have I invoiced and sent", "en"),
                 ("total billed and sent out", "en")]:
        yield "total_sent", q, P(metric="sum", column="fees", filters={"bill_sent": "yes"}), ["sum", "bill_sent"], l

    # ── grouped: biggest / smallest / ranking ────────────────────────────
    for q, l in [("who is my biggest client?", "en"), ("top client by revenue", "en"),
                 ("which client pays the most", "en"), ("my largest client", "en"),
                 ("sabse bada client kaun hai", "hi"), ("sabse zyada paisa kaun deta hai", "hi")]:
        yield "biggest_client", q, P(metric="sum", column="fees", group_by="client_name", order="desc", limit=1), ["group", "client"], l
    for q, l in [("who is my smallest client", "en"), ("which client brings the least revenue", "en"),
                 ("my lowest revenue client", "en"), ("sabse chhota client kaun hai", "hi")]:
        yield "smallest_client", q, P(metric="sum", column="fees", group_by="client_name", order="asc", limit=1), ["group", "client"], l
    for q, l in [("rank my clients by revenue", "en"), ("clients by total billing", "en"),
                 ("list clients from highest to lowest earning", "en")]:
        yield "rank_clients", q, P(metric="sum", column="fees", group_by="client_name", order="desc"), ["group", "ranking"], l

    # ── client-scoped (clients + brands) × paraphrases ───────────────────
    for c in _ENTS:
        f = {"client_name": c}
        fu = {"client_name": c, "paid": "no"}
        fp = {"client_name": c, "paid": "yes"}
        fs = {"client_name": c, "bill_sent": "yes"}
        for t, l in _T_COUNT_CLIENT:
            yield "count_client", t.format(c=c), P(metric="count", filters=f), ["count", "client"], l
        for t, l in _T_TOTAL_CLIENT:
            yield "total_client", t.format(c=c), P(metric="sum", column="fees", filters=f), ["sum", "client"], l
        for t, l in _T_LIST_CLIENT:
            yield "list_client", t.format(c=c), P(filters=f), ["list", "client"], l
        for t, l in _T_AVG_CLIENT:
            yield "avg_client", t.format(c=c), P(metric="avg", column="fees", filters=f), ["avg", "client"], l
        for t, l in _T_OWES:
            yield "client_owes", t.format(c=c), P(metric="sum", column="fees", filters=fu), ["sum", "client", "status"], l
        for t, l in _T_UNPAID_COUNT_CLIENT:
            yield "count_client_unpaid", t.format(c=c), P(metric="count", filters=fu), ["count", "client", "status"], l
        for t, l in _T_PAID_COUNT_CLIENT:
            yield "count_client_paid", t.format(c=c), P(metric="count", filters=fp), ["count", "client", "status"], l
        for t, l in _T_PAID_TOTAL_CLIENT:
            yield "total_client_paid", t.format(c=c), P(metric="sum", column="fees", filters=fp), ["sum", "client", "status"], l
        for t, l in _T_SENT_CLIENT:
            yield "count_client_sent", t.format(c=c), P(metric="count", filters=fs), ["count", "client", "bill_sent"], l

    # ── date ranges: months, quarters, years ─────────────────────────────
    for mname, yr in _MONTHS:
        tr = _month_range(mname, yr)
        yield "count_month", f"how many jobs in {mname} {yr}", P(metric="count", time_range=tr), ["count", "date"], "en"
        yield "total_month", f"total billing in {mname} {yr}", P(metric="sum", column="fees", time_range=tr), ["sum", "date"], "en"
    for qname, tr in _QUARTERS:
        yield "count_quarter", f"how many jobs in {qname}", P(metric="count", time_range=tr), ["count", "date"], "en"
        yield "total_quarter", f"total billing in {qname}", P(metric="sum", column="fees", time_range=tr), ["sum", "date"], "en"
    for yname, tr in _YEARS:
        yield "count_year", f"how many jobs in {yname}", P(metric="count", time_range=tr), ["count", "date"], "en"
        yield "total_year", f"total billing in {yname}", P(metric="sum", column="fees", time_range=tr), ["sum", "date"], "en"
        yield "avg_year", f"average fee in {yname}", P(metric="avg", column="fees", time_range=tr), ["avg", "date"], "en"

    # ── relative-date bare (resolved to absolute) ────────────────────────
    for phrase, tr in _REL:
        yield "count_rel", f"how many jobs {phrase}", P(metric="count", time_range=tr), ["count", "date"], "en"
        yield "total_rel", f"total billing {phrase}", P(metric="sum", column="fees", time_range=tr), ["sum", "date"], "en"
    yield "total_rel", "total earnings last month", P(metric="sum", column="fees", time_range=_LAST_MONTH), ["sum", "date"], "en"
    yield "count_rel", "how many jobs last month", P(metric="count", time_range=_LAST_MONTH), ["count", "date"], "en"

    # ── compound: status + date ──────────────────────────────────────────
    yield "unpaid_date", "how many unpaid jobs this year", P(metric="count", filters={"paid": "no"}, time_range=_THIS_YEAR), ["count", "status", "date"], "en"
    yield "unpaid_date", "total unpaid last quarter", P(metric="sum", column="fees", filters={"paid": "no"}, time_range=_LAST_QUARTER), ["sum", "status", "date"], "en"
    yield "paid_date", "how many paid jobs this year", P(metric="count", filters={"paid": "yes"}, time_range=_THIS_YEAR), ["count", "status", "date"], "en"
    yield "paid_date", "total received this year", P(metric="sum", column="fees", filters={"paid": "yes"}, time_range=_THIS_YEAR), ["sum", "status", "date"], "en"
    yield "sent_date", "how many invoices sent this year", P(metric="count", filters={"bill_sent": "yes"}, time_range=_THIS_YEAR), ["count", "bill_sent", "date"], "en"

    # ── compound: client + date (a slice of entities × windows) ──────────
    for c in _CLIENTS[:5] + _BRANDS[:4]:
        for phrase, tr in _REL:
            yield "client_date_total", f"total billing for {c} {phrase}", P(metric="sum", column="fees", filters={"client_name": c}, time_range=tr), ["sum", "client", "date"], "en"
            yield "client_date_count", f"how many {c} jobs {phrase}", P(metric="count", filters={"client_name": c}, time_range=tr), ["count", "client", "date"], "en"

    # ══ Area A — collections: who hasn't paid / owes / chase ═════════════
    UNPAID = {"paid": "no"}
    for q, l in _A_BARE_LIST:
        yield "who_unpaid", q, P(filters=UNPAID), ["list", "status", "collections"], l
    for q, l in _A_BARE_COUNT:
        yield "how_many_owe", q, P(metric="count", filters=UNPAID), ["count", "status", "collections"], l
    for q, l in _A_BARE_SUM:
        yield "total_pending", q, P(metric="sum", column="fees", filters=UNPAID), ["sum", "status", "collections"], l
    for q, l in _A_OWES_TOP:
        yield "owes_most", q, P(metric="sum", column="fees", filters=UNPAID, group_by="client_name", order="desc", limit=1), ["group", "status", "collections"], l
    for q, l in _A_OWES_RANK:
        yield "owes_rank", q, P(metric="sum", column="fees", filters=UNPAID, group_by="client_name", order="desc"), ["group", "ranking", "collections"], l
    for c in _CLIENTS:                       # billing clients only (you chase a client, not a brand)
        for t, l in _A_CLIENT:
            yield "client_chase", t.format(c=c), P(metric="sum", column="fees", filters={"client_name": c, "paid": "no"}), ["sum", "client", "status", "collections"], l

    # ══ Area B — money buckets: earnings vs received vs outstanding ══════
    for q, l in _B_TOTAL_ALL:
        yield "earnings_all", q, P(metric="sum", column="fees"), ["sum", "bare", "earnings"], l
    for q, l in _B_RECEIVED:
        yield "received_all", q, P(metric="sum", column="fees", filters={"paid": "yes"}), ["sum", "status", "received"], l
    for q, l in _B_OUTSTANDING:
        yield "outstanding_all", q, P(metric="sum", column="fees", filters={"paid": "no"}), ["sum", "status", "outstanding"], l
    for c in _CLIENTS:
        for t, l in _B_CLIENT_EARNED:
            yield "client_earned", t.format(c=c), P(metric="sum", column="fees", filters={"client_name": c}), ["sum", "client", "earnings"], l
        for t, l in _B_CLIENT_RECEIVED:
            yield "client_received", t.format(c=c), P(metric="sum", column="fees", filters={"client_name": c, "paid": "yes"}), ["sum", "client", "status", "received"], l
        for t, l in _B_CLIENT_OWED:
            yield "client_owed", t.format(c=c), P(metric="sum", column="fees", filters={"client_name": c, "paid": "no"}), ["sum", "client", "status", "outstanding"], l

    # ══ Area C — contact / recipient lookup (poc_email) ═════════════════
    for c in _CLIENTS:                       # you email a billing client, not a brand
        for t, l in _C_CLIENT_EMAIL:
            yield "email_for_client", t.format(c=c), P(filters={"client_name": c}), ["list", "client", "poc_email"], l
    for q, l in _C_NO_EMAIL:
        yield "no_email", q, P(filters={"poc_email": "null"}), ["list", "poc_email"], l
    for q, l in _C_HAS_EMAIL:
        yield "has_email", q, P(filters={"poc_email": "not_null"}), ["list", "poc_email"], l

    # ══ Area D — invoice dispatch: sent / pending to raise ══════════════
    for q, l in _D_SENT_LIST:
        yield "sent_list", q, P(filters={"bill_sent": "yes"}), ["list", "bill_sent", "dispatch"], l
    for q, l in _D_NOTSENT_LIST:
        yield "notsent_list", q, P(filters={"bill_sent": "no"}), ["list", "bill_sent", "dispatch"], l
    for q, l in _D_SENT_COUNT:
        yield "sent_count", q, P(metric="count", filters={"bill_sent": "yes"}), ["count", "bill_sent", "dispatch"], l
    for q, l in _D_NOTSENT_COUNT:
        yield "notsent_count", q, P(metric="count", filters={"bill_sent": "no"}), ["count", "bill_sent", "dispatch"], l
    for c in _CLIENTS:
        for t, l in _D_CLIENT_SENT:
            yield "client_sent_list", t.format(c=c), P(filters={"client_name": c, "bill_sent": "yes"}), ["list", "client", "bill_sent", "dispatch"], l


def build_corpus(seed: int = 42):
    rows = build_dataset(seed=seed)
    corpus = []
    seen = set()
    skipped_eval = 0
    idx = 0
    for cat, q, plan, tags, lang in _entries():
        key = _norm(q)
        if key in _EVAL_Q:          # never leak a held-out eval question
            skipped_eval += 1
            continue
        if key in seen:
            continue
        seen.add(key)
        idx += 1
        corpus.append({
            "id": f"{cat}-{idx:04d}",
            "question": q,
            "plan": plan,
            "answer": compute_answer(plan, rows),
            "tags": tags,
            "source": "synthetic",
            "lang": lang,
        })
    build_corpus._skipped_eval = skipped_eval
    return corpus


def main():
    corpus = build_corpus()
    with open(CORPUS_PATH, "w") as f:
        for e in corpus:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")
    print(f"wrote {len(corpus)} golden entries to {CORPUS_PATH} "
          f"(skipped {getattr(build_corpus, '_skipped_eval', 0)} eval-leak questions)")


if __name__ == "__main__":
    main()
