"""
Golden dataset + a deterministic, LLM-free SQL executor for the query-router CI net.

This is the in-memory stand-in for Supabase used by tests/test_golden_queries.py.
It executes the exact SQL shapes the router emits (COUNT / SUM / AVG / SUM+GROUP BY
/ DISTINCT client / SELECT * with client·paid·date filters) so we can assert the
ACTUAL answer a query produces — no AI key, runs in CI.

Kept separate from tests/test_e2e_live.py on purpose: that module hard-exits at
import time when AI_KEY is unset, which would break CI collection.
"""
import re
from typing import Any, Dict, List

# 8-row dataset with known, hand-computable answers.
#   total billed     = 1,175,000          paid (Yes: 1,3,8) = 405,000  → 3 jobs
#   unpaid (others)  = 770,000 / 5 jobs   bill_sent (1,2,3,5,8) = 5
#   clients (5): Star Studios, Garnier India, Pedigree Films, Samsung India, Maruti Suzuki
#   by client total: Star 350k, Samsung 300k, Garnier 230k, Maruti 175k, Pedigree 120k
GOLDEN_ROWS: List[Dict[str, Any]] = [
    {"id":1,"client_name":"Star Studios","brand_name":"Nike","job_date":"2026-03-15","fees":150000,"paid":"Yes","bill_sent":"Yes","poc_email":"a@star.com","invoice_date":"2026-03-20","bill_no":"INV-001","isDeleted":None},
    {"id":2,"client_name":"Star Studios","brand_name":"Adidas","job_date":"2026-01-10","fees":200000,"paid":None,"bill_sent":"Yes","poc_email":"a@star.com","invoice_date":"2026-01-15","bill_no":"INV-002","isDeleted":None},
    {"id":3,"client_name":"Garnier India","brand_name":"Garnier","job_date":"2026-02-20","fees":80000,"paid":"Yes","bill_sent":"Yes","poc_email":"p@garnier.com","invoice_date":"2026-02-25","bill_no":"INV-003","isDeleted":None},
    {"id":4,"client_name":"Pedigree Films","brand_name":"Pedigree","job_date":"2026-04-05","fees":120000,"paid":None,"bill_sent":None,"poc_email":None,"invoice_date":None,"bill_no":"INV-004","isDeleted":None},
    {"id":5,"client_name":"Garnier India","brand_name":"L'Oreal","job_date":"2026-05-08","fees":95000,"paid":None,"bill_sent":"Yes","poc_email":"p@garnier.com","invoice_date":"2026-05-10","bill_no":"INV-005","isDeleted":None},
    {"id":6,"client_name":"Samsung India","brand_name":"Samsung","job_date":"2026-06-10","fees":300000,"paid":None,"bill_sent":None,"poc_email":None,"invoice_date":None,"bill_no":"INV-006","isDeleted":None},
    {"id":7,"client_name":"Garnier India","brand_name":"Garnier Men","job_date":"2026-03-01","fees":55000,"paid":None,"bill_sent":None,"poc_email":None,"invoice_date":None,"bill_no":"INV-007","isDeleted":None},
    {"id":8,"client_name":"Maruti Suzuki","brand_name":"Maruti","job_date":"2026-02-05","fees":175000,"paid":"Yes","bill_sent":"Yes","poc_email":"ads@maruti.in","invoice_date":"2026-02-10","bill_no":"INV-008","isDeleted":None},
]


def _client_of(r):
    return r.get("client_name") or r.get("brand_name") or r.get("production_house") or ""


def _is_paid(r):
    return (r.get("paid") or "").strip().lower() in ("yes", "true", "t", "1", "paid")


class GoldenDB:
    """Executes the router's SQL shapes against GOLDEN_ROWS."""

    def execute_sql(self, sql: str) -> Dict[str, Any]:
        s = sql
        su = sql.upper()
        rows = list(GOLDEN_ROWS)

        # client filter: ... ILIKE '%term%'
        mc = re.search(r"ILIKE\s+'%([^']+)%'", s, re.I)
        if mc:
            term = mc.group(1).lower()
            rows = [r for r in rows if term in _client_of(r).lower()]

        # date range on job_date
        d1 = re.search(r"job_date\s*>=\s*'(\d{4}-\d{2}-\d{2})'", s, re.I)
        d2 = re.search(r"job_date\s*<=\s*'(\d{4}-\d{2}-\d{2})'", s, re.I)
        if d1:
            rows = [r for r in rows if (r.get("job_date") or "") >= d1.group(1)]
        if d2:
            rows = [r for r in rows if (r.get("job_date") or "") <= d2.group(1)]

        # paid status filter
        if re.search(r"IN\s*\('true','t','yes','1','paid'\)", s, re.I) and "NOT IN" not in su:
            rows = [r for r in rows if _is_paid(r)]
        elif "PAID IS NULL" in su or "NOT IN ('TRUE','T','YES','1','PAID')" in su:
            rows = [r for r in rows if not _is_paid(r)]

        # bill_sent filter
        if re.search(r"bill_sent[^)]*\)\s*IN\s*\('true'", s, re.I) or "BILL_SENT IS NOT NULL" in su:
            rows = [r for r in rows if (r.get("bill_sent") or "").lower() in ("yes","true","t","1","sent")]

        # poc_email
        if "POC_EMAIL IS NOT NULL" in su:
            rows = [r for r in rows if r.get("poc_email")]
        if "POC_EMAIL IS NULL" in su:
            rows = [r for r in rows if not r.get("poc_email")]

        # aggregates
        if "COUNT(" in su:
            return {"ok": True, "rows": [{"result": len(rows)}]}
        if "AVG(" in su:
            fees = [r["fees"] for r in rows if r.get("fees")]
            return {"ok": True, "rows": [{"result": round(sum(fees)/len(fees)) if fees else 0}]}
        if "SUM(" in su and "GROUP BY" in su:
            g: Dict[str, int] = {}
            for r in rows:
                g[_client_of(r) or "Unknown"] = g.get(_client_of(r) or "Unknown", 0) + (r.get("fees") or 0)
            out = [{"client_name": k, "result": v} for k, v in sorted(g.items(), key=lambda kv: -kv[1])]
            lm = re.search(r"LIMIT\s+(\d+)", s, re.I)
            return {"ok": True, "rows": out[:int(lm.group(1))] if lm else out}
        if "SUM(" in su:
            return {"ok": True, "rows": [{"result": sum((r.get("fees") or 0) for r in rows)}]}

        # DISTINCT client list
        if "DISTINCT" in su and "CLIENT_NAME" in su:
            seen, out = set(), []
            for r in rows:
                c = _client_of(r)
                if c and c not in seen:
                    seen.add(c); out.append({"client_name": c})
            return {"ok": True, "rows": sorted(out, key=lambda x: x["client_name"])}

        # SELECT * (rows)
        lm = re.search(r"LIMIT\s+(\d+)", s, re.I)
        if lm:
            rows = rows[:int(lm.group(1))]
        return {"ok": True, "rows": rows}
