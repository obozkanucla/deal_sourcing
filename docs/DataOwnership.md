📘 Deal Data Ownership & Enrichment Rules

This document defines who owns which fields, who is allowed to write them, and under what conditions.
Its purpose is to prevent silent data corruption, overwrite bugs, and enrichment drift.

If code violates this document, the code is wrong.

⸻

1. Core Principle
Every field has exactly one authoritative owner.
Other processes may read the field, but may not overwrite it unless explicitly allowed.
There are no shared owners.

⸻

2. Ownership Types
Owner	Meaning
SYSTEM	Lifecycle, metadata, orchestration
SCRAPE	Structured HTML extraction
TEXT	Unstructured text inference
ANALYST	Manual / human edits

⸻

3. deals Table – Field Ownership Matrix
3.1 Identity & Lifecycle
Field	Owner	Rules
id	SYSTEM	immutable
source	SYSTEM	immutable
source_listing_id	SCRAPE	set once
source_url	SYSTEM	immutable
status	ANALYST	never auto-overwrite
needs_detail_refresh	SYSTEM	system-only
detail_fetch_reason	SYSTEM	system-only
detail_fetched_at	SYSTEM	set on successful scrape
last_updated	SYSTEM	always set
last_updated_source	SYSTEM	AUTO / MANUAL

⸻

3.2 Descriptive Content
Field	Owner	Rules
title	SCRAPE	overwrite allowed
description	SCRAPE	overwrite allowed
location	SCRAPE	overwrite allowed
content_hash	SYSTEM	derived

⸻

3.3 Financial Fields (Critical)
Field	Owner	Rules
asking_price_k	SCRAPE	ONLY set if NULL
revenue_k	TEXT	ONLY set if NULL
ebitda_k	TEXT	ONLY set if NULL
profit_margin_pct	TEXT	ONLY set if NULL
revenue_growth_pct	TEXT	ONLY set if NULL
leverage_pct	TEXT	ONLY set if NULL

Important:
	•	HTML enrichment must never write revenue or EBITDA
	•	Text extraction must never write asking price

⸻

3.4 Sector & Classification
Field	Owner	Rules
industry	SCRAPE / INFER	overwrite allowed
sector	SCRAPE / INFER	overwrite allowed
sector_source	SYSTEM	derived
sector_inference_confidence	SYSTEM	derived
sector_inference_reason	SYSTEM	derived

⸻

3.5 Drive & Storage

Field	Owner	Rules
drive_folder_id	SYSTEM	set once
drive_folder_url	SYSTEM	derived
pdf_drive_url	SYSTEM	optional, derived

⸻

4. deal_artifacts Table Rules

Artifacts are append-only.

Field	Rule
All fields	Immutable
Updates	❌ Not allowed
Changes	Create new artifact row

If a PDF changes → new artifact, never update.

⸻

5. Mandatory Update Patterns

✅ Allowed

asking_price_k = CASE
    WHEN asking_price_k IS NULL THEN ?
    ELSE asking_price_k
END

revenue_k = COALESCE(revenue_k, ?)


⸻

❌ Forbidden

These must never appear in code:

asking_price_k = ?
revenue_k = NULL
ebitda_k = NULL

Unless explicitly gated by ownership logic.

⸻

6. Enrichment Responsibility Map

Process	Allowed Fields
enrich_businessbuyers.py	asking_price_k, title, description
enrich_businesses4sale.py	asking_price_k (and structured financials if present)
financial_extractor.py	revenue_k, ebitda_k, margins
Manual edits	Any field + last_updated_source = 'MANUAL'


⸻

7. Refresh Logic

A deal may be re-enriched when:
	•	needs_detail_refresh = 1
	•	OR detail_fetched_at IS NULL
	•	OR detail_fetched_at < now - N days

BUT:
	•	status = 'Lost' is analyst-owned and must never be auto-reset

⸻

8. Design Intent (Why This Exists)

This system optimizes for:
	•	Data integrity over completeness
	•	Explicit provenance
	•	Repeatable enrichment
	•	Safe re-runs

If data looks wrong, the fix is ownership, not heuristics.

⸻

9. Enforcement

Any code that violates this document:
	•	Must be fixed
	•	Must not be worked around
	•	Must not be “just this once”

This document is the source of truth.

⸻

If you want next steps, we can:
	•	Add runtime guards that assert violations
	•	Add CI checks for forbidden SQL patterns
	•	Generate broker-specific enrichment templates