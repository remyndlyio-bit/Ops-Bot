"""
The KnowledgeBook's RULES + GLOSSARY — the guidelines that set the AI's semantics
for THIS domain. These are injected into the planner so the model applies our
conventions instead of guessing (e.g. "unpaid" means paid-is-null, a brand maps
to its billing client). This is the "guidelines", not a test: it shapes behaviour
before the answer, it doesn't grade after.

Each rule carries a short, imperative statement the model can follow. The
glossary maps the words users actually type to the canonical meaning. Both render
to a compact prompt block via render().
"""

# Ordered most-load-bearing first; these are the conventions the model gets wrong.
RULES = [
    ("unpaid_means_null",
     "\"unpaid\" / \"outstanding\" / \"pending\" / \"owed\" / \"baki\" mean paid is NOT yes "
     "(paid IS NULL or not in yes/true/paid). There is no literal 'No' in the data."),
    ("paid_means_yes",
     "\"paid\" / \"cleared\" / \"received\" / \"settled\" mean paid = yes."),
    ("client_matches_brand",
     "A client term matches client_name OR brand_name OR production_house. Users name a "
     "BRAND (e.g. \"Pepsi\") even though the billing client may differ (e.g. \"Content Lab\"). "
     "Label the result by what the user asked for."),
    ("value_is_sum",
     "Value words — \"how much\" / \"total\" / \"earnings\" / \"billing\" / \"revenue\" / "
     "\"kamai\" / \"made\" — mean SUM(fees), a single number, NOT a row list."),
    ("count_is_count",
     "Count words — \"how many\" / \"number of\" / \"kitne\" — mean COUNT, a single number, "
     "NOT a row list."),
    ("owes_is_client_unpaid_sum",
     "\"how much does X owe me\" / \"X ka paisa baki\" / \"X se kitna aana hai\" → SUM(fees) "
     "for client X AND paid = no."),
    ("biggest_client_is_grouped",
     "\"biggest\" / \"top\" / \"largest\" client → group by client, SUM(fees), order desc, "
     "limit 1. \"by revenue\" = billed; if they say \"paid the most\", filter paid = yes."),
    ("invoice_sent_is_bill_sent",
     "\"invoice sent\" / \"invoice raised\" / \"invoice bheja\" → bill_sent = yes. "
     "\"invoice generated\" is a different action, not a query."),
    ("billed_is_ambiguous_read_the_shape",
     "\"bill / billed / billing\" has TWO meanings — decide from the sentence shape, "
     "never from the word alone. (a) MONEY when the question asks for an amount: "
     "\"how much have I billed X\", \"total billing\", \"what did I bill in March\" → "
     "SUM(fees), and do NOT add a bill_sent filter. (b) INVOICE DISPATCH when the "
     "question asks whether an invoice went out: \"have I billed X yet\", \"which "
     "clients have I billed\", \"who am I yet to bill\" → bill_sent, and do NOT sum "
     "fees. A how-much question is always (a); a yes/no or which-ones question paired "
     "with \"yet\" / \"already\" / \"sent\" / \"gone out\" is always (b)."),
    ("payment_is_not_dispatch",
     "paid and bill_sent are INDEPENDENT. \"Has X paid?\" is paid; \"have I invoiced "
     "X?\" is bill_sent. Never answer one with the other, and never require both "
     "unless the question states both (e.g. \"invoiced but not yet paid\")."),
    ("list_only_when_asked",
     "Only return a row list when the user says \"show\" / \"list\" / \"which\". Otherwise a "
     "value/count question gets a number."),
    ("made_from_x_ambiguous",
     "\"made / earned from X\" is ambiguous: billed = SUM(fees); received = SUM(fees) where "
     "paid = yes. Default to billed, but if it's genuinely unclear, ask."),
    ("no_spurious_filters",
     "Apply ONLY the filters the question states. Do NOT invent extra conditions: "
     "\"invoices sent\" / \"invoice bheje\" is bill_sent = yes ALONE — never also require "
     "poc_email or invoice_date. \"paid\" is paid = yes ALONE."),
    ("how_many_clients_is_distinct",
     "\"how many clients\" / \"kitne logon ko\" / \"how many people\" → COUNT(DISTINCT client), "
     "not COUNT(*) of jobs."),
    ("currency_is_inr",
     "Amounts are Indian Rupees; format with Indian grouping (e.g. Rs 1,75,000)."),
]

# What users type  →  the canonical meaning.
GLOSSARY = {
    "earnings": "SUM(fees)", "billing": "SUM(fees)", "revenue": "SUM(fees)",
    "kamai": "SUM(fees)", "kamaya": "SUM(fees)", "income": "SUM(fees)",
    "unpaid": "paid = no", "outstanding": "paid = no", "pending": "paid = no",
    "owed": "paid = no", "baki": "paid = no", "baaki": "paid = no", "due": "paid = no",
    "milna baki": "paid = no", "aana baki": "paid = no",
    "paid": "paid = yes", "cleared": "paid = yes", "received": "paid = yes",
    "aaya": "paid = yes", "aayi": "paid = yes", "mil gaya": "paid = yes",
    "invoice sent": "bill_sent = yes", "invoice raised": "bill_sent = yes",
    "invoice bheja": "bill_sent = yes", "invoice bheje": "bill_sent = yes",
    # "billed" is deliberately NOT a single mapping — it is ambiguous. Give both
    # readings with the sentence shape that selects each (see the
    # billed_is_ambiguous_read_the_shape rule above).
    "how much have I billed X": "SUM(fees) for X — an amount, no bill_sent filter",
    "have I billed X yet": "bill_sent for X — a dispatch check, do not sum fees",
    "kaam": "job", "kitne logon ko": "COUNT(DISTINCT client)",
    "biggest client": "group by client, SUM(fees), top 1",
    "average fee": "AVG(fees)",
}


def render(max_rules: int = None) -> str:
    """A compact prompt block of the rules + glossary."""
    rules = RULES if max_rules is None else RULES[:max_rules]
    lines = ["# KnowledgeBook — how to interpret questions about this data:"]
    for _id, text in rules:
        lines.append(f"- {text}")
    if GLOSSARY:
        lines.append("# Term glossary:")
        for term, meaning in GLOSSARY.items():
            lines.append(f"- \"{term}\" -> {meaning}")
    return "\n".join(lines)
