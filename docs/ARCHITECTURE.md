Deal Sourcing System – Architecture & Design Decisions

1. Core Principles
This system is designed around broker-native truth, auditability, and incremental enrichment.

Key principles:
	1.	Never lose broker data
	2.	Separate discovery from interpretation
	3.	Make every write idempotent
	4.	Never override manual decisions
	5.	Inference is optional, reversible, and explainable

2. High-Level Pipeline
Each broker follows the same 4-stage lifecycle:
Index  →  Persist (raw)  →  Detail Enrichment  →  Inference / Manual

These stages are intentionally decoupled.

3. File & Module Structure (Per Broker)

Each broker uses the same conceptual layout:

src/
├── brokers/
│   ├── businessbuyers_client.py
│   ├── dealopportunities_client.py
│
├── scripts/
│   ├── import_businessbuyers.py
│   ├── import_dealopportunities.py
│   ├── enrich_businessbuyers.py
│   ├── enrich_dealopportunities.py
│   ├── infer_sectors.py
│
├── persistence/
│   └── repository.py
│
├── utils/
│   └── pdf_playwright.py

Responsibility split

Layer	Responsibility
*_client.py	Browser control, scraping logic, NO DB writes
import_*.py	Index orchestration + DB writes
enrich_*.py	Detail enrichment + DB writes
infer_sectors.py	Sector/industry inference only
repository.py	All SQLite access

4. Index Stage (Discovery)
What index scraping does
	•	Navigates broker search results
	•	Extracts all fast, list-page data
	•	Does not attempt interpretation
	•	Is safe to re-run infinitely

Index-owned fields (written during import)

These fields must be written during index import:

Column	Source
source	Broker
source_listing_id	Broker
source_url	Broker
first_seen	System
last_seen	System
sector_raw	Broker
sectors_multi	Broker
industry_raw	Broker (if present)
location	Broker
revenue / revenue_raw	Broker
asking_price	Broker
deadline	Broker

Index data is considered high confidence broker truth.

5. Detail Enrichment Stage
What enrichment does
	•	Visits individual listing pages
	•	Generates PDFs
	•	Captures full HTML
	•	Extracts long-form descriptions and structured facts

Enrichment-owned fields

Written only during enrichment:

Column	Purpose
description	Long-form text
extracted_json	Structured facts
content_hash	Change detection
pdf_path	Local artifact
detail_fetched_at	Timestamp
needs_detail_refresh	Control flag

Important constraints
	•	Enrichment never decides sector
	•	Enrichment never overwrites manual fields
	•	PDFs are generated before sector inference
	•	Temporary PDFs live in /tmp/* until Drive upload

6. SQLite as the System of Record
There is one canonical table: deals

Deduplication rule

UNIQUE (source, source_listing_id)

All brokers conform to this.

Update semantics
	•	first_seen is immutable
	•	last_seen updates on re-index
	•	Detail fields update only when enrichment runs
	•	Manual fields are never overwritten programmatically

7. Sector & Industry Model (Critical)
Column meanings

Column	Meaning
sector_raw	Broker taxonomy
industry_raw	Broker taxonomy
sector	Final normalized sector
industry	Final normalized industry
sector_source	manual | inferred
sector_inference_reason	Explanation
sector_inference_confidence	0–1

Hard rules
	1.	Manual beats everything
	2.	Inference only runs if sector_source IS NULL OR inferred
	3.	Inference must be explainable
	4.	Low-confidence inference is discarded
	5.	Broker raw values are never deleted

8. Sector Inference (infer_sectors.py)
Inference is a pure function:
	•	Input: sector_raw, industry_raw, description
	•	Output: (industry, sector, reason, confidence)

Safeguards:
	•	Confidence threshold enforced
	•	No DB writes if confidence < threshold
	•	No override of sector_source = manual

Inference can be re-run safely at any time.

9. PDFs & Drive Uploads
Philosophy
	•	PDFs are evidence, not truth
	•	Always generated from HTML
	•	Always reproducible

Workflow
	1.	Generate PDF locally
	2.	Store path in SQLite
	3.	Upload to Drive later
	4.	Store Drive folder + URL separately

Sector is not required to generate PDFs.

10. Why Index and Detail Are Separate
This is intentional and non-negotiable.

Index	Detail
Fast	Slow
Cheap	Rate-limited
High coverage	Selective
Safe to spam	Carefully budgeted

This separation enables:
	•	Resume-safe crawling
	•	Partial system failures
	•	Incremental enrichment
	•	Broker click budgeting

11. What We Explicitly Avoid
	•	❌ Writing interpretation during scraping
	•	❌ One-shot “do everything” crawlers
	•	❌ Hidden side effects
	•	❌ Overwriting human input
	•	❌ Silent inference

12. Current Status (Truth Snapshot)
As of now:
	•	✅ Indexing complete for BB & DealOpportunities
	•	✅ PDFs generated and stored
	•	✅ Detail enrichment working
	•	⏳ Index data fields need to be persisted fully
	•	⏳ Sector inference ready once raw fields are populated

13. Future Extensions (Planned)
	•	Drive folder hierarchy by industry/sector
	•	Resume-safe enrichment queues
	•	ML-assisted sector inference
	•	Confidence-weighted scoring
	•	Broker reliability metrics

14. One Sentence Summary

We store broker truth first, enrich later, infer cautiously, and never lose information.

15. Google Sheets Integration (Decisions & Rationale)
Google Sheets is used as a human decision interface, not a data source of truth.

It exists to:
	•	Enable fast human review
	•	Capture qualitative decisions
	•	Support collaboration without engineers
	•	Preserve auditability of human intent

It does not exist to:
	•	Replace SQLite
	•	Drive scraping logic
	•	Act as a canonical dataset

16. Role of Google Sheets in the System
SQLite vs Google Sheets

Aspect	SQLite	Google Sheets
System of record	✅ Yes	❌ No
Broker truth	✅ Yes	❌ No
Human decisions	❌ No	✅ Yes
Automation-safe	✅ Yes	❌ No
Auditability	✅ Yes	⚠️ Partial
Multi-user editing	❌ No	✅ Yes

Rule:

If there is a conflict, SQLite always wins.

17. What Flows Into Google Sheets
Sheets is populated from SQLite using explicit export jobs.

Typical columns exported:

Column	Purpose
deal_id	Stable reference
source	Broker
source_listing_id	Broker ID
company_name	Human-readable
description	Review context
sector_raw	Broker taxonomy
industry_raw	Broker taxonomy
sector	Current inferred / manual
industry	Current inferred / manual
revenue	Broker-reported
location	Geography
pdf_drive_url	Evidence
first_seen	Timeline
last_seen	Freshness

Exports are append-or-update, never destructive.

18. What Flows Back From Google Sheets
Only explicitly allowed columns are imported back.

Allowed inbound fields

Column	SQLite Field	Notes
manual_decision	manual_decision	Yes / No / Watch
decision	decision	Final stance
decision_confidence	decision_confidence	Human judgment
decision_rationale	decision_rationale	Free text
owner	owner	Coverage
priority	priority	Workflow
notes	notes	Commentary
industry	industry	Manual override
sector	sector	Manual override

Mandatory behavior on import
	•	sector_source is set to manual
	•	Inference is permanently disabled for that row
	•	last_updated is updated
	•	A reason is stored (manual_sheet_import)

19. What Sheets Can Never Modify
Sheets must never write to:
	•	source
	•	source_listing_id
	•	source_url
	•	content_hash
	•	pdf_path
	•	first_seen
	•	last_seen
	•	sector_raw
	•	industry_raw
	•	extracted_json

These are immutable or machine-owned fields.

20. Decision Semantics
Decision lifecycle

inferred → reviewed → manual → outcome

	•	Inference suggests
	•	Human confirms or overrides
	•	Outcome is recorded explicitly

Outcomes (example)

Outcome	Meaning
advance	Actively pursue
hold	Watchlist
reject	Do not pursue
duplicate	Same opportunity
invalid	Data issue

Outcomes are human-only.

21. Why Google Sheets (and Not a UI)
This is deliberate.

Advantages
	•	Zero friction for users
	•	Familiar interface
	•	Real-time collaboration
	•	Easy annotation
	•	No engineering dependency for decisions

Trade-offs (accepted)
	•	No strong typing
	•	No referential integrity
	•	Potential human error

These risks are mitigated by:
	•	SQLite remaining authoritative
	•	Narrow import scope
	•	Explicit override rules

22. Synchronization Strategy
Directionality

SQLite  →  Google Sheets  →  SQLite
   ↑            ↓              ↑
 truth       decisions       overrides

Sync cadence
	•	Export: on-demand or scheduled
	•	Import: manual trigger only
	•	No automatic bi-directional sync

23. Design Guardrails (Non-Negotiable)
	1.	Sheets cannot create deals
	2.	Sheets cannot delete deals
	3.	Sheets cannot modify broker truth
	4.	Sheets cannot trigger scraping
	5.	Sheets overrides are permanent unless explicitly reverted

24. One-Line Summary
Google Sheets is where humans think; SQLite is where the system remembers.

If you want next, we can:
	•	Define the exact sheet schema (column order, data validation)
	•	Add a “human error recovery” protocol
	•	Add a sheet_revision_id for traceability
	•	Lock down Drive permissions per role

This is already a very clean, institutional-grade setup.