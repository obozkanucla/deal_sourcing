from dataclasses import dataclass

@dataclass(frozen=True)
class ColumnSpec:
    name: str
    push: bool
    pull: bool
    system: bool = False
    allow_blank_pull: bool = False
    virtual: bool = False

DEAL_COLUMNS = [
    # --- Identity / system ---
    ColumnSpec("deal_uid", push=True, pull=False, system=True, virtual=True),
    ColumnSpec("source", push=True, pull=False, system=True),
    ColumnSpec("source_listing_id", push=True, pull=False, system=True),
    ColumnSpec("source_url", push=True, pull=False, system=True),

    # --- Core descriptors ---
    ColumnSpec("title", push=True, pull=False),
    ColumnSpec("industry", push=True, pull=False),
    ColumnSpec("sector", push=True, pull=False),
    ColumnSpec("location", push=True, pull=False),
    ColumnSpec("incorporation_year", push=True, pull=False, system=True),
    ColumnSpec("sector_raw", push=True, pull=False),

    # --- Assets ---
    ColumnSpec("drive_folder_url", push=True, pull=False, system=True),

    # --- Derived financial metrics (calculated) ---
    ColumnSpec("revenue_k_effective", push=True, pull=False, system=True),
    ColumnSpec("ebitda_k_effective", push=True, pull=False, system=True),
    ColumnSpec("asking_price_k_effective", push=True, pull=False, system=True),

    # --- Analyst editable financials fields ---
    ColumnSpec("revenue_k_manual", push=True, pull=True),
    ColumnSpec("ebitda_k_manual", push=True, pull=True),
    ColumnSpec("asking_price_k_manual", push=True, pull=True),

    # --- Financials (truth from legacy / brokers) ---
    ColumnSpec("revenue_k", push=True, pull=False),
    ColumnSpec("ebitda_k", push=True, pull=False),
    ColumnSpec("asking_price_k", push=True, pull=False),

    # --- Calculated financial metrics (DB truth only) ---
    ColumnSpec("ebitda_margin", push=True, pull=False, system=True),
    ColumnSpec("revenue_multiple", push=True, pull=False, system=True),
    ColumnSpec("ebitda_multiple", push=True, pull=False, system=True),

    # --- Financials (truth from legacy / brokers) ---
    ColumnSpec("profit_margin_pct", push=True, pull=False),
    ColumnSpec("revenue_growth_pct", push=True, pull=False),
    ColumnSpec("leverage_pct", push=True, pull=False),

    ColumnSpec("last_updated_source", push=True, pull=False),

    # --- Dates (system) ---
    ColumnSpec("added_at", push=True, pull=False, system=True),
    ColumnSpec("first_seen", push=True, pull=False, system=True),
    ColumnSpec("last_seen", push=True, pull=False, system=True),
    ColumnSpec("last_updated", push=True, pull=False, system=True),

    # --- Decisioning ---
    ColumnSpec("decision", push=True, pull=True),

    # --- Analyst workflow ---
    ColumnSpec("status", push=True, pull=True),
    ColumnSpec("owner", push=True, pull=True),
    ColumnSpec("priority", push=True, pull=True),
    ColumnSpec("notes", push=True, pull=True),

    ColumnSpec("pass_reason", push=True, pull=True),

    # --- Deal columns to cater for Aggregator Websites ---
    ColumnSpec("canonical_external_id", push=True, pull=False, system=True,),
    ColumnSpec("broker_name",push=True, pull=False, system=True,),
    ColumnSpec("broker_listing_url", push=True, pull=False, system=True,),
    ColumnSpec("source_role", push=True, pull=False, system=True,),

    # --- Flexible broker / enrichment metadata ---
    ColumnSpec("attributes", push=True, pull=False, system=True),

]

def deal_column_names():
    return [c.name for c in DEAL_COLUMNS]


VIRTUAL_COLUMNS = {"deal_uid"}

def sqlite_select_columns() -> list[str]:
    """
    Columns that physically exist in SQLite and are safe to SELECT
    for enrichment. Virtual columns are excluded.
    Always includes the primary key `id`.
    """
    cols = ["id"]  # primary key is required by all enrichers

    for c in DEAL_COLUMNS:
        if c.virtual:
            continue
        cols.append(c.name)

    return cols