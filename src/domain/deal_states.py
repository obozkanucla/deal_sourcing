# src/domain/deal_states.py

ACTIVE_STAGES = {
    "Initial Contact",
    "CIM",
    "CIM DD",
    "Meeting",
    "LOI",
}

TERMINAL_STAGES = {
    "Pass",
    "Lost",
}

SPECIAL_STAGES = {
    "Under Offer",
}

STATUS_ORDER = [
    "Initial Contact",
    "CIM",
    "CIM DD",
    "Meeting",
    "LOI",
    "Under Offer",
    "Pass",
    "Lost",
]


# ------------------------------------------------------------------
# GUARDRAILS
# ------------------------------------------------------------------

_ALL_DEFINED = ACTIVE_STAGES | SPECIAL_STAGES | TERMINAL_STAGES

assert set(STATUS_ORDER) == _ALL_DEFINED, (
    "STATUS_ORDER is out of sync with stage definitions"
)

def derive_state(stage: str | None) -> str:
    if stage in ACTIVE_STAGES:
        return "Active"
    if stage in TERMINAL_STAGES:
        return stage
    if stage in SPECIAL_STAGES:
        return stage
    return "Unknown"