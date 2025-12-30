from dataclasses import dataclass

@dataclass(frozen=True)
class ColumnSpec:
    name: str
    push: bool
    pull: bool
    system: bool = False
    allow_blank_pull: bool = False


DEAL_COLUMNS = [
    # --- Identity / system ---
    ColumnSpec("deal_uid", push=True, pull=False, system=True),
    ColumnSpec("source", push=True, pull=False, system=True),
    ColumnSpec("source_listing_id", push=True, pull=False, system=True),
    ColumnSpec("source_url", push=True, pull=False, system=True),

    # --- Core descriptors ---
    ColumnSpec("title", push=True, pull=True),
    ColumnSpec("industry", push=True, pull=True),
    ColumnSpec("sector", push=True, pull=True),
    ColumnSpec("location", push=True, pull=True),

    # --- Financials (truth from legacy / brokers) ---
    ColumnSpec("revenue_k", push=True, pull=True),
    ColumnSpec("ebitda_k", push=True, pull=True),
    ColumnSpec("asking_price_k", push=True, pull=True),

    # --- Calculated financial metrics (DB truth only) ---
    ColumnSpec("ebitda_margin", push=True, pull=False, system=True),
    ColumnSpec("revenue_multiple", push=True, pull=False, system=True),
    ColumnSpec("ebitda_multiple", push=True, pull=False, system=True),

    # --- Analyst workflow ---
    ColumnSpec("status", push=True, pull=True),
    ColumnSpec("owner", push=True, pull=True),
    ColumnSpec("priority", push=True, pull=True),
    ColumnSpec("notes", push=True, pull=True),  # ← Legacy “Update”

    # --- Decisioning ---
    ColumnSpec("decision", push=True, pull=True),          # ← Legacy “Outcome”
    ColumnSpec("decision_reason", push=True, pull=True),   # ← Legacy “Reason”

    # --- Dates (system) ---
    ColumnSpec("first_seen", push=True, pull=False, system=True),
    ColumnSpec("last_seen", push=True, pull=False, system=True),
    ColumnSpec("last_updated", push=True, pull=False, system=True),

    # --- Assets ---
    ColumnSpec("drive_folder_url", push=True, pull=False, system=True),

    # --- Parked for later ---
    ColumnSpec("decision_confidence", push=False, pull=False),
]