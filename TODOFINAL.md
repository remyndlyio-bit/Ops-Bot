# Production-Grade Fixes — Remyndly Intent Test Matrix Findings

## Executive Summary

The report identified 62.5% pass rate on the 148-scenario live run. Four confirmed defects block production readiness, all localised to specific, fixable code paths. Industry-standard mitigations are listed below with exact file locations and concrete solutions.

## Critical Path — Production Blockers

### 1. **SCOPE DISCLOSURE LIES** (Revenue-facing)

**Status**: CONFIRMED, HIGH PRIORITY
**Root cause**: `answer_ledger.py:214–224` builds scope description from a filters dict that doesn't represent the actual SQL filters applied.
**Impact**: Users see "every client" for a Nike-only query, or "no date filter" for a date-filtered result. Correct data, untrue sentence = untrustworthy product.

**Fix** (30 mins):
- **Option A (Recommended)**: Pass the actual SQL filters through to the disclosure layer instead of guessing from a dict. Change `scope_from_sql()` to extract date-range constraints from the WHERE clause using regex, matching the same patterns the planner emits.
  ```python
  # services/answer_ledger.py:89–120
  def scope_from_sql(sql: str) -> Dict[str, Any]:
      filters = {}
      # Existing: PAID_TRUE/FALSE, client ILIKE
      # Add: date range extraction
      # WHERE job_date >= '2026-04-01' AND job_date <= '2026-04-30'
      m = re.search(r"job_date\s*>=\s*'([^']+)'.*job_date\s*<=\s*'([^']+)'", sql, re.DOTALL)
      if m:
          filters['date_range'] = {'start': m.group(1), 'end': m.group(2)}
      return {"filters": filters, "time_range": filters.get('date_range')}
  ```
- **Option B (Faster, Interim)**: Tag all router queries with a comment containing the disclosure string, then parse it back out. Low-craft but it works for a release.

**Acceptance**: User sees "Nike, 2026-04-01 to 2026-04-30" and that's what the WHERE clause says.

---

### 2. **INSTRUCTION SWALLOWED AS CLIENT NAME** (UX-facing, user-visible failure)

**Status**: CONFIRMED, HIGH PRIORITY
**Root cause**: NLU pipeline doesn't distinguish "Generate invoice for bill BB2" (command + lookup) from "Generate invoice for Nike" (command + client).
**Impact**: Every unrecognised phrasing falls back to client-name extraction, destroying the prompt.

**Fix** (45 mins):
- **Before** extracting a client name from "Generate invoice [for X]", check if X is a known client:
  ```python
  # services/intent_service.py, in the invoice-request path (around line 2900)
  def _extract_client_for_invoice(self, message: str, user_id: str) -> Optional[str]:
      # Try to parse "for SOMETHING" without assuming SOMETHING is a client
      m = re.search(r'\bfor\s+([A-Za-z0-9\s]+?)(?:\s+(?:for|in|on)|$)', message, re.I)
      if not m:
          return None
      candidate = m.group(1).strip()
      
      # Check if candidate is a real client (query the DB)
      conn = self.supabase.get_client_list(user_id)
      known_clients = {c['client_name'].lower(): c['client_name'] for c in conn}
      
      if candidate.lower() in known_clients:
          return known_clients[candidate.lower()]
      
      # Not a client. Is it something else the user said?
      # Bill numbers, dates, etc. should be handled BEFORE client extraction,
      # not silently treated as "client named X".
      return None
  ```
- **Acceptance**: "Generate invoice for bill BB2" returns "I don't recognize 'bill BB2'. Did you mean a client name, or are you using a bill number? (Bill number lookup isn't supported yet.)"

---

### 3. **BANK DETAILS MESSAGE CREATES JOB ROW** (Data integrity)

**Status**: CONFIRMED, HIGH PRIORITY
**Root cause**: Bank-details input is being routed into smart_capture (job entry), not the bank_config update flow.
**Impact**: Sending account numbers creates a job row. Correcting this by "editing" the job invites the account number into `job_entries` permanently.

**Fix** (20 mins):
- **Classify before routing**: In the routing stage (before intent dispatch), detect if the message is structured bank-details input:
  ```python
  # services/intent_service.py, early in process_request() at the router/dispatcher stage
  
  BANK_FIELDS = {'account', 'bank', 'ifsc', 'account name', 'upi', 'holder', 'pan', 'gst'}
  
  def _looks_like_bank_details(message: str) -> bool:
      """Does the message look like 'Account: 123 / Bank: HDFC / ...' input?"""
      lines = [line.strip().lower() for line in message.split('\n') if line.strip()]
      found_fields = sum(1 for line in lines for field in BANK_FIELDS if line.startswith(field + ':'))
      return found_fields >= 2  # At least 2 field labels
  
  # Then in the routing logic:
  if self._looks_like_bank_details(message):
      return self._handle_bank_details_update(user_id, message)
  ```
- **Acceptance**: Sending structured bank details routes to the bank config flow, not job entry.

---

### 4. **HINDI INVOICE REQUESTS ROUTE TO READ QUERY** (Localization)

**Status**: CONFIRMED, MEDIUM PRIORITY
**Root cause**: Hindi invoice trigger phrases aren't in the intent classifier's pattern list. Hinglish works because English keywords are caught first.
**Impact**: "Nike ka invoice bhejo April ka" (Hindi) → list rows instead of generating. "invoice banao Nike ke liye April" (Hinglish) → works.

**Fix** (15 mins):
- **Add Hindi phrase variants** to the invoice-request classifier:
  ```python
  # services/intent_service.py, in the invoke request detection (around line 900)
  
  INVOICE_TRIGGERS = {
      'en': ['generate invoice', 'send invoice', 'create invoice', 'make invoice'],
      'hi': ['invoice bhejo', 'invoice bana', 'invoice banana', 'bill bhejo'],
      'hinglish': ['invoice banao', 'invoice send karo']
  }
  
  def _is_invoice_request(self, message: str) -> bool:
      msg_lower = message.lower()
      for lang_phrases in INVOICE_TRIGGERS.values():
          if any(phrase in msg_lower for phrase in lang_phrases):
              return True
      return False
  ```
- **Acceptance**: "Nike ka invoice bhejo April ka" generates the invoice.

---

## Stability Improvements — Industry-Standard Guardrails

### 5. **EMAIL-ENTRY FLOW REACHES DELETE PROMPT**

**Status**: UNDER INVESTIGATION, MEDIUM PRIORITY
**Workaround (immediate)**: No email-entry turn should ever arrive at a DELETE option. Add an explicit guard:
```python
# services/intent_service.py, in _handle_invoice_email_send()

if message.lower() in ('all', 'delete', 'cancel all'):
    return {
        "response": "I won't delete records based on email input. Are you trying to cancel sending emails instead?",
        "operation": "clarification"
    }
```

**Longer term**: Investigate whether the "Which one did you mean?" → delete path is supposed to exist for emails at all. It's legitimate for job disambiguation, not for email dispatch.

---

### 6. **UNRECOGNIZED INPUT ANSWERS WITH THE ENTIRE TABLE**

**Status**: UNDER INVESTIGATION, MEDIUM PRIORITY
**Workaround (immediate)**: Catch empty or clearly-junk input before it reaches the query pipeline:
```python
# services/intent_service.py, early in the query path

INJECTION_SIGNATURES = [
    r"'; (DROP|DELETE|INSERT)",
    r"(UNION|OR|AND)\s+1\s*=\s*1",
]

def _looks_like_injection(message: str) -> bool:
    return any(re.search(sig, message, re.I) for sig in INJECTION_SIGNATURES)

if _looks_like_injection(message):
    return {
        "response": "I can't process that request. Try something like 'Show my jobs' or 'Total fees this month'.",
        "operation": "rejected"
    }
```

**Longer term**: The fallback should be "I didn't understand. Did you mean…?" not "Here's everything."

---

## Documentation & Onboarding Alignment

### 7. **UPDATE ONBOARDING FLOW SPEC**

**Status**: DECISION NEEDED
**Finding**: The sheet expects name→company; the code does name→email→company. The bot extracts correctly in both cases, so this is a documentation/spec alignment, not a bug.

**Options**:
- **A**: Update the sheet to match the code (name→email→company)
- **B**: Update the code to match the sheet (name→company, optional email)

**Recommendation**: A. Email before company lets the bot validate the address immediately, providing better UX feedback.

---

## Test Coverage Additions

All fixes above should land with regression tests:

```python
# tests/test_scope_disclosure.py
def test_scope_discloses_applied_filters():
    """Regression: scope line must accurately describe the WHERE clause."""
    # Query: Nike jobs in April
    # Scope: should say "Nike" and "April", not "every client" or "no filter"
    result = service.process_request(uid, "Nike jobs in April")
    assert "Nike" in result['response']
    assert "April" in result['response']
    assert "every client" not in result['response']

# tests/test_intent_routing.py
def test_bank_details_route_to_config_not_job():
    """Bank details input should not create a job row."""
    msg = "Account Name: Test\nBank: HDFC\nAccount: 123456\nIFSC: HDFC0001"
    result = service.process_request(uid, msg)
    assert result['operation'] != 'smart_capture'
    assert "Save this job" not in result['response']
    # Verify no row was created
    rows_after = db.query(f"SELECT * FROM job_entries WHERE user_id = '{uid}'")
    assert len(rows_after) == len(rows_before)

# tests/test_hindi_localization.py
def test_hindi_invoice_request():
    """Hindi invoice triggers must route to generation, not query."""
    result = service.process_request(uid, "Nike ka invoice bhejo April ka")
    assert result['operation'] in ('ACTION_TRIGGER', 'invoice_request')
    assert "Save this job" not in result['response']
```

---

## Implementation Roadmap

| Priority | Fix | Effort | Risk | Acceptance |
|---|---|---|---|---|
| P0 | Scope disclosure | 30m | Low | Accurate filter language |
| P0 | Instruction parsing | 45m | Low | "bill BB2" not treated as client |
| P0 | Bank details routing | 20m | Low | Account numbers don't become jobs |
| P1 | Hindi phrases | 15m | None | Hindi invoice requests work |
| P2 | Email-entry guard | 10m | None | No delete option from email flow |
| P2 | Input validation | 20m | Low | Bad input asks for clarification |
| P3 | Spec alignment | 5m | None | Sheet matches code flow |

**Total**: ~2.5 hours to production-ready state. Verify via re-running the 148-scenario suite afterward.

---

## Production Checklist Before Launch

- [ ] All P0 fixes merged and tested
- [ ] Regression tests passing (352 existing + 6 new)
- [ ] Live suite re-run: target ≥75% (was 62.5%)
- [ ] Scope disclosure verified on 5+ real queries
- [ ] Bank details flow tested end-to-end
- [ ] Hindi and Hinglish invoice requests tested
- [ ] Credentials rotated (Supabase JWT, API key, DB password)
- [ ] Deployment to staging and canary before prod
