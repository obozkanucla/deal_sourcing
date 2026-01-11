"""
Hilton Smythe → Canonical Industry / Sector Mapping

Rules:
- This file is the ONLY place Hilton Smythe semantics live
- No inference, no scraping logic, no keyword guessing
- Mapping is broker-declared, but buckets are broad
- Confidence reflects semantic precision, not data quality
- Used during index + enrichment + BAU backfill
"""

BROKER_NAME = "HiltonSmythe"

# -------------------------------------------------------------------
# RAW HILTON SMYTHE SECTOR → CANONICAL MAPPING
# -------------------------------------------------------------------
# Format:
#   raw_label: {
#       "industry": <canonical industry>,
#       "sector": <canonical sector>,
#       "confidence": float,
#       "reason": str
#   }
# -------------------------------------------------------------------

HILTON_SMYTHE_SECTOR_MAP = {

    # ---------------------------------------------------------------
    # AUTOMOTIVE
    # ---------------------------------------------------------------
    "Automotive": {
        "industry": "Consumer_Retail",
        "sector": "Automotive Services",
        "confidence": 1.0,
        "reason": "Broker-declared automotive businesses (repairs, servicing, workshops)",
    },

    # ---------------------------------------------------------------
    # BUSINESS SERVICES
    # ---------------------------------------------------------------
    "Business Services": {
        "industry": "Business_Services",
        "sector": "Consulting / Professional Services",
        "confidence": 0.9,
        "reason": "Broad business services bucket including professional and advisory firms",
    },

    "Service industries": {
        "industry": "Business_Services",
        "sector": "Outsourced Services",
        "confidence": 0.7,
        "reason": "Mixed service-led businesses (cleaning, care, ops); not always FM",
    },

    # ---------------------------------------------------------------
    # CONSUMER / RETAIL / LEISURE
    # ---------------------------------------------------------------
    "Consumer, Retail & Leisure": {
        "industry": "Consumer_Retail",
        "sector": "Leisure / Hospitality",
        "confidence": 0.8,
        "reason": "Mixed consumer-facing businesses spanning retail, leisure and hospitality",
    },

    # ---------------------------------------------------------------
    # LOGISTICS & DISTRIBUTION
    # ---------------------------------------------------------------
    "Distribution, freight & logistics": {
        "industry": "Logistics_Distribution",
        "sector": "Supply Chain Management",
        "confidence": 1.0,
        "reason": "Broker-declared logistics, haulage, freight and distribution businesses",
    },

    # ---------------------------------------------------------------
    # INDUSTRIALS
    # ---------------------------------------------------------------
    "Industrial Manufacturing": {
        "industry": "Industrials",
        "sector": "Manufacturing",
        "confidence": 1.0,
        "reason": "Explicit manufacturing and industrial production businesses",
    },

    "Electricity, gas & water supply": {
        "industry": "Industrials",
        "sector": "Energy / Utilities",
        "confidence": 1.0,
        "reason": "Energy, utilities and infrastructure-related service providers",
    },

    # ---------------------------------------------------------------
    # OTHER / EDGE
    # ---------------------------------------------------------------
    "All": {
        "industry": "Other",
        "sector": "Miscellaneous",
        "confidence": 0.5,
        "reason": "Hilton Smythe catch-all sector used when no specific filter is applied",
    },
}