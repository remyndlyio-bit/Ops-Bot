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
     "\"invoice sent\" / \"billed\" / \"invoice bheja\" → bill_sent = yes. "
     "\"invoice generated\" is a different action, not a query."),
    ("list_only_when_asked",
     "Only return a row list when the user says \"show\" / \"list\" / \"which\". Otherwise a "
     "value/count question gets a number."),
    ("made_from_x_ambiguous",
     "\"made / earned from X\" is ambiguous: billed = SUM(fees); received = SUM(fees) where "
     "paid = yes. Default to billed, but if it's genuinely unclear, ask."),
    ("currency_is_inr",
     "Amounts are Indian Rupees; format with Indian grouping (e.g. Rs 1,75,000)."),
]

# What users type  →  the canonical meaning.
GLOSSARY = {
    "earnings": "SUM(fees)", "billing": "SUM(fees)", "revenue": "SUM(fees)",
    "kamai": "SUM(fees)", "income": "SUM(fees)",
    "unpaid": "paid = no", "outstanding": "paid = no", "pending": "paid = no",
    "owed": "paid = no", "baki": "paid = no", "due": "paid = no",
    "paid": "paid = yes", "cleared": "paid = yes", "received": "paid = yes",
    "invoice sent": "bill_sent = yes", "billed": "bill_sent = yes",
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
