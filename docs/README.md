1️⃣ README.md — Project Architecture & Design Record

# Deal Sourcing & Intelligence Platform

## Purpose

This repository implements a **deal ingestion, enrichment, normalisation, and analyst workflow system** for private market opportunities sourced from multiple brokers and data feeds.

The system is designed to:
- Ingest heterogeneous deal data (Google Sheets, broker sites, PDFs)
- Canonicalise financial and workflow fields
- Preserve analyst edits
- Support explainable enrichment (scraped, inferred, calculated)
- Maintain deterministic, auditable data flows

---

## Core Design Principles

### 1. Canonical Database Truth
- SQLite (`deals` table) is the **single source of truth**
- Google Sheets is a **projection / interface**, never the authority
- All calculations are recomputed from stored primitives

### 2. Separation of Concerns
| Layer | Responsibility |
|-----|----------------|
| Ingestion | Raw broker data → DB |
| Enrichment | Scraped / inferred fields |
| Calculation | Derived financial metrics |
| Sync | DB ↔ Google Sheets |
| Analyst Layer | Human edits only |

### 3. Idempotency
All scripts can be re-run safely:
- No duplication
- No drift
- No irreversible transformations

### 4. Provenance First
Each field is classified as:
- **System**
- **Broker Truth**
- **Derived**
- **Analyst Controlled**

This determines whether it can be:
- pushed
- pulled
- overwritten
- recalculated

---

## High-Level Data Flow

Broker Sources
↓
[ Import Scripts ]
↓
SQLite (deals)
↓
[ Enrichment Scripts ]
↓
[ Calculation Scripts ]
↓
Google Sheets (Analyst UI)
↓
[ Reverse Sync ]
↓
SQLite (human fields only)

---

## Key Concepts

### Deal Identity
A deal is uniquely identified by:

(source, source_listing_id)

A derived human-friendly key exists:

deal_uid = source:source_listing_id

---

## Column Governance (`ColumnSpec`)

All deal columns are centrally declared in:

src/domain/deal_columns.py

Each column has:

```python
ColumnSpec(
    name="revenue_k",
    push=True,
    pull=True,
    system=False
)

Attribute	Meaning
push	DB → Sheets
pull	Sheets → DB
system	Never editable by humans
allow_blank_pull	Whether blank overwrites DB

No column may exist outside DEAL_COLUMNS.

⸻

Financial Normalisation

Canonical Units

All financials are stored as:

Field	Unit
revenue_k	£000
ebitda_k	£000
asking_price_k	£000

Derived Metrics

Calculated only in DB:
	•	ebitda_margin
	•	revenue_multiple
	•	ebitda_multiple

Script:

src/scripts/recalculate_financial_metrics.py


⸻

Google Sheets Philosophy
	•	Sheets are stateless projections
	•	Entire sheet may be wiped & rebuilt
	•	Formatting is applied programmatically
	•	Humans only edit allowed columns

⸻

Reset & Rebuild Strategy

To fully reset a source:
	1.	Delete source rows from deals
	2.	Delete related artifacts
	3.	Re-run import
	4.	Re-run enrichment
	5.	Recalculate metrics
	6.	Re-sync sheets

This ensures determinism.

⸻

What This Repo Is NOT
	•	Not a CRM
	•	Not a document management system
	•	Not an analytics dashboard

It is a deal intelligence pipeline.

---

# 2️⃣ `docs/DEVELOPMENT_GUIDE.md` — How to Extend the System

```md
# Development Guide

This document describes **how to safely extend** the system without breaking invariants.

---

## A. Adding a New Column (Inferred / Scraped / Calculated)

### Step 1 — Decide Column Type

| Type | Examples |
|----|----|
| Broker Truth | revenue_k, ebitda_k |
| Inferred | sector, industry |
| Calculated | ebitda_margin |
| Analyst | status, notes |

---

### Step 2 — Declare in `deal_columns.py`

```python
ColumnSpec(
    "new_field",
    push=True,
    pull=False,
    system=True
)

Rules:
	•	Calculated → system=True, pull=False
	•	Analyst → push=True, pull=True
	•	Broker → push=True, usually pull=True

⸻

Step 3 — Add DB Column

Edit:

src/persistence/schema.sql

Then:

sqlite3 db/deals.sqlite < schema.sql

(or delete DB in dev)

⸻

Step 4 — Populate the Column

Option A: Enrichment Script

src/scripts/enrich_<thing>.py

Uses:

repo.update_deal_fields(...)

Option B: Calculation Script

src/scripts/recalculate_*.py

Never calculated in Sheets.

⸻

Step 5 — Sync to Sheets

No extra work if push=True.

⸻

Step 6 — Guardrails
	•	Never overwrite analyst fields
	•	Never calculate inside Sheets
	•	Always log provenance in notes if inferred

⸻

B. Adding a New Broker

Required Files

File	Purpose
import_<broker>_deals.py	Initial ingestion
client_<broker>.py	Fetch HTML / PDFs
enrich_<broker>.py	Optional enrichment


⸻

Step-by-Step

1. Define Identity Strategy
Choose stable identifiers:
	•	listing ID
	•	fingerprint (hash)
	•	URL

Never include description text in identity.

⸻

2. Import Script
Responsibilities:
	•	Map raw fields → canonical columns
	•	Normalize money
	•	Populate source, source_listing_id
	•	Insert via insert_raw_deal

Never:
	•	calculate derived metrics
	•	write analyst fields

⸻

3. Enrichment (Optional)
Use:

fetch_deals_with_descriptions()

Rules:
	•	Only fill NULL values
	•	Never overwrite broker truth
	•	Log inference confidence

⸻

4. Google Sheets
No broker-specific logic allowed here.

⸻

Broker Deletion / Reset

Always delete:
	•	deals
	•	artifacts

Never delete:
	•	intermediaries
	•	clicks
	•	config

⸻

C. Financial Extraction from Text

Location:

src/enrichment/financial_extractor.py

Rules:
	•	Conservative regex
	•	Plausibility checks
	•	Never overwrite populated values
	•	Units always normalized to £k

⸻

D. Debugging Checklist

If data “doesn’t update”:
	1.	Check DEALS_DB_COLUMNS
	2.	Check update method signature
	3.	Verify WHERE source + source_listing_id
	4.	Confirm column exists in schema
	5.	Print SQL + params once

99% of issues are here.

---

# 3️⃣ `docs/ANALYST_GUIDE.md` — How to Use the System

```md
# Analyst Guide — Deal Sheet Usage

This document explains **what each column means**, and **what you may safely edit**.

---

## Column Categories

### 🔒 System Columns (Do Not Edit)

| Column | Meaning |
|-----|-----|
| deal_uid | Unique deal identifier |
| source | Broker name |
| source_listing_id | Broker’s internal ID |
| source_url | Link to original deal |
| first_seen | First appearance |
| last_seen | Last appearance |
| last_updated | System timestamp |

---

### 💷 Financials (Broker / Extracted)

| Column | Meaning |
|----|----|
| revenue_k | Revenue (£000) |
| ebitda_k | EBITDA (£000) |
| asking_price_k | Asking price (£000) |

**Editable only if broker data is wrong.**

---

### 📊 Calculated Metrics (Read Only)

| Column | Meaning |
|----|----|
| ebitda_margin | EBITDA % |
| revenue_multiple | Price / Revenue |
| ebitda_multiple | Price / EBITDA |

These are recalculated automatically.

---

### 🧭 Workflow Columns (You Own These)

| Column | Editable | Meaning |
|----|----|----|
| status | ✅ | Current workflow state |
| owner | ✅ | Responsible analyst |
| priority | ✅ | Review urgency |
| notes | ✅ | Analyst notes |

---

### 🧠 Decisioning

| Column | Meaning |
|----|----|
| decision | Investment intent |
| decision_reason | Rationale |

These may be pre-populated from brokers but are analyst-controlled.

---

### 📂 Assets

| Column | Meaning |
|----|----|
| drive_folder_url | Deal folder |

Displayed as a hyperlink.

---

## Editing Rules

✅ You may:
- Change status, owner, priority
- Add notes
- Correct obvious broker errors

❌ Do not:
- Delete rows
- Change deal_uid
- Edit calculated fields
- Reformat columns

---

## Sync Behaviour

- Your edits are pulled back into the database
- Blank cells **do not overwrite** values
- System rebuilds will preserve your edits

---

## If Something Looks Wrong

Do **not** fix manually if:
- multiple rows affected
- calculation seems off
- values revert

Instead:
- flag it
- enrichment will be corrected centrally

---

## Philosophy

The sheet is a **workspace**, not the database.

Trust the pipeline.


⸻

Final Note

This is enterprise-grade documentation, not just dev notes.
You can hand this to:
	•	future you
	•	an engineer
	•	an analyst
	•	a compliance reviewer

If you want next:
	•	an ER diagram
	•	a data lineage diagram
	•	or tests/specs

Say the word — but for now, you’ve earned a break 😄