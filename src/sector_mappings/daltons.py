"""
Daltons → Canonical Industry / Sector Mapping

Source:
- Daltons breadcrumb taxonomy (detail pages)
- Broker-declared, hierarchical
"""

BROKER_NAME = "Daltons"

DALTONS_SECTOR_MAP = {

    # -----------------------------
    # RETAIL
    # -----------------------------
    "Off Licences": {
        "industry": "Consumer_Retail",
        "sector": "Alcohol Retail",
        "confidence": 0.95,
        "reason": "Daltons breadcrumb: Off Licences",
    },

    # -----------------------------
    # SERVICES
    # -----------------------------
    "Garden Repair Businesses": {
        "industry": "Consumer_Services",
        "sector": "Gardening & Landscaping Services",
        "confidence": 0.95,
        "reason": "Daltons breadcrumb: Garden Repair Businesses",
    },

    # add incrementally
}

def map_daltons_sector(*, sector_raw: str | None):
    if not sector_raw:
        return {
            "industry": "Other",
            "sector": "Miscellaneous",
            "confidence": 0.2,
            "reason": "Missing Daltons sector",
            "sector_source": "unclassified",
        }

    parts = [
        p.strip()
        for p in sector_raw.split(">")
        if p.strip() and p.strip() != "Business"
    ]

    # try leaf → root
    for key in reversed(parts):
        mapping = DALTONS_SECTOR_MAP.get(key)
        if mapping:
            return {
                **mapping,
                "sector_source": "broker_declared",
            }

    return {
        "industry": "Other",
        "sector": "Miscellaneous",
        "confidence": 0.3,
        "reason": f"Unmapped Daltons sector '{sector_raw}'",
        "sector_source": "unclassified",
    }