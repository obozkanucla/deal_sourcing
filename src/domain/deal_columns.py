from dataclasses import dataclass

@dataclass(frozen=True)
class ColumnSpec:
    name: str
    push: bool
    pull: bool
    system: bool
    allow_blank_pull: bool = False

DEAL_COLUMNS = [
    # --- Identity / system ---
    ColumnSpec("deal_uid", push=True, pull=False, system=True),
    ColumnSpec("source", push=True, pull=False, system=True),
    ColumnSpec("source_listing_id", push=True, pull=False, system=True),
    ColumnSpec("source_url", push=True, pull=False, system=True),

    # --- Core descriptors (hybrid) ---
    ColumnSpec("title", push=True, pull=True, system=False),
    ColumnSpec("industry", push=True, pull=True, system=False),
    ColumnSpec("sector", push=True, pull=True, system=False),
    ColumnSpec("location", push=True, pull=True, system=False),

    # --- Analyst workflow ---
    ColumnSpec("status", push=True, pull=True, system=False),
    ColumnSpec("owner", push=True, pull=True, system=False),
    ColumnSpec("priority", push=True, pull=True, system=False),
    ColumnSpec("notes", push=True, pull=True, system=False),

    # --- Dates ---
    ColumnSpec("first_seen", push=True, pull=False, system=True),
    ColumnSpec("last_seen", push=True, pull=False, system=True),
    ColumnSpec("last_updated", push=True, pull=False, system=True),

    # --- Decisions ---
    ColumnSpec("decision", push=True, pull=True, system=False),
    ColumnSpec("decision_confidence", push=True, pull=True, system=False),

    # --- Assets ---
    ColumnSpec("drive_folder_url", push=True, pull=False, system=True),
]