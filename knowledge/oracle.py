"""
The reference oracle — the GROUND TRUTH for the golden source.

Given a planner-native plan and the dataset, it computes the correct answer in
plain Python, encoding the domain semantics the LLM keeps getting wrong:
  • "unpaid" = paid IS NULL or not in {yes,true,paid,...}  (NOT the literal 'No')
  • a client term matches client_name OR brand_name OR production_house
  • value questions → a number; list questions → a set of rows

Because answers are computed (never hand-labelled), the corpus scales for free
and stays correct when the dataset changes.

Plan shape (subset of the planner's own JSON, so corpus entries double as
few-shot exemplars in the planner's native format):

    {"metric": "count"|"sum"|"avg"|None,
     "column": "fees"|None,
     "filters": {"client_name":str, "paid":"yes"|"no",
                 "bill_sent":"yes"|"no", "poc_email":"not_null"|"null"},
     "time_range": {"type":"absolute","value":{"start":"YYYY-MM-DD","end":"YYYY-MM-DD"}}|None,
     "group_by": "client_name"|None, "order": "desc"|"asc"|None, "limit": int|None}
"""
from typing import Any, Dict, List

_PAID_TRUE = {"yes", "true", "t", "1", "paid"}


def _client_of(r):
    return (r.get("client_name") or r.get("brand_name") or r.get("production_house") or "")


def _is_paid(r):
    return (r.get("paid") or "").strip().lower() in _PAID_TRUE


def _matches(r: Dict, plan: Dict) -> bool:
    f = plan.get("filters") or {}
    if f.get("client_name"):
        term = f["client_name"].strip().lower()
        hay = " ".join(str(r.get(k) or "") for k in ("client_name", "brand_name", "production_house")).lower()
        if term not in hay:
            return False
    if f.get("paid") == "yes" and not _is_paid(r):
        return False
    if f.get("paid") == "no" and _is_paid(r):
        return False
    if f.get("bill_sent") == "yes" and (r.get("bill_sent") or "").strip().lower() not in _PAID_TRUE | {"sent"}:
        return False
    if f.get("bill_sent") == "no" and (r.get("bill_sent") or "").strip().lower() in _PAID_TRUE | {"sent"}:
        return False
    if f.get("poc_email") == "not_null" and not r.get("poc_email"):
        return False
    if f.get("poc_email") == "null" and r.get("poc_email"):
        return False
    tr = plan.get("time_range")
    if tr and tr.get("value"):
        jd = (r.get("job_date") or "")[:10]
        if not jd:
            return False
        if jd < tr["value"]["start"] or jd > tr["value"]["end"]:
            return False
    return True


def compute_answer(plan: Dict, rows: List[Dict]) -> Dict[str, Any]:
    """Return the canonical answer for ``plan`` over ``rows``:
       {"type": "count"|"money"|"client"|"list", "value": ...}."""
    sel = [r for r in rows if _matches(r, plan)]

    if plan.get("group_by") == "client_name":
        agg: Dict[str, int] = {}
        for r in sel:
            agg[_client_of(r)] = agg.get(_client_of(r), 0) + (r.get("fees") or 0)
        ranked = sorted(agg.items(), key=lambda kv: -kv[1])
        if plan.get("order") == "asc":
            ranked = ranked[::-1]
        if plan.get("limit") == 1 and ranked:
            return {"type": "client", "value": ranked[0][0], "amount": ranked[0][1]}
        return {"type": "ranking", "value": [{"client_name": k, "total": v} for k, v in ranked]}

    metric = plan.get("metric")
    if metric == "count":
        return {"type": "count", "value": len(sel)}
    if metric == "sum":
        return {"type": "money", "value": sum((r.get("fees") or 0) for r in sel)}
    if metric == "avg":
        fees = [r["fees"] for r in sel if r.get("fees")]
        return {"type": "money", "value": round(sum(fees) / len(fees)) if fees else 0}

    # No metric → a list. We grade on the row count + the client set.
    clients = sorted({_client_of(r) for r in sel if _client_of(r)})
    return {"type": "list", "value": len(sel), "clients": clients}
