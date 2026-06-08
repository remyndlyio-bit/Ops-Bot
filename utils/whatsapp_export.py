"""
WhatsApp export delivery — the single source of truth for which export
file we send to a WhatsApp recipient.

Background: Twilio's WhatsApp channel is restrictive about media types.
The same data export, produced as three sibling files at the same base
path, has very different delivery odds:

  xlsx → Twilio code 63019 (Meta-side internal failure, silent reject)
  csv  → Twilio code 63005 (channel does not support text/csv)
  pdf  → reliably accepted

This module owns the picker. Unit-tested by
tests/test_scenarios_from_matrix.py::TestWhatsAppExportPicker.
"""

from __future__ import annotations

import os


def pick_whatsapp_export_path(xlsx_path: str) -> str:
    """Pick the right exported-data file to send over WhatsApp.

    Order: PDF first (preferred), CSV as a fallback if PDF generation
    failed, then xlsx as a last resort. The fallbacks exist only to avoid
    a hard failure — in practice PDF should always be present when
    services.intent_service._generate_jobs_excel ran successfully.

    Returns the original `xlsx_path` if NOTHING exists, so the downstream
    send fails visibly rather than silently swallowing.
    """
    if not xlsx_path:
        return xlsx_path
    base = xlsx_path[:-5] if xlsx_path.endswith(".xlsx") else xlsx_path
    for candidate in (base + ".pdf", base + ".csv", xlsx_path):
        if os.path.exists(candidate):
            return candidate
    return xlsx_path
