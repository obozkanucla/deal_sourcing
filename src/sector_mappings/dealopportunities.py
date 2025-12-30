"""
DealOpportunities → Canonical Industry / Sector Mapping

Rules:
- Broker sector dropdown is authoritative
- Deterministic resolution for multi-sector listings
- High confidence (broker-declared)
- No keyword inference
"""

BROKER_NAME = "DealOpportunities"


# -------------------------------------------------------------------
# NORMALIZATION
# -------------------------------------------------------------------

def _normalize_sector_key(s: str) -> str:
    return s.strip().lower()


# -------------------------------------------------------------------
# INDUSTRY PRIORITY (deterministic conflict resolution)
# -------------------------------------------------------------------

INDUSTRY_PRIORITY = [
    "Healthcare",
    "Education",
    "Food_Beverage",
    "Consumer_Retail",
    "Business_Services",
    "Technology",
    "Media_Communications",
    "Industrials",
    "Construction_Built_Environment",
    "Agriculture",
    "Other",
]


# -------------------------------------------------------------------
# RAW DEALOPPORTUNITIES SECTOR → CANONICAL MAPPING
# -------------------------------------------------------------------

DEALOPPORTUNITIES_SECTOR_MAP = {

    # -----------------------------
    # HEALTHCARE & CARE (TOP PRIORITY)
    # -----------------------------
    "Health & care": {
        "industry": "Healthcare",
        "sector": "Healthcare Services",
        "confidence": 0.95,
        "reason": "DealOpportunities broker-declared Health & care sector",
    },
    "Medical": {
        "industry": "Healthcare",
        "sector": "Medical Services",
        "confidence": 0.95,
        "reason": "DealOpportunities broker-declared Medical sector",
    },

    # -----------------------------
    # EDUCATION
    # -----------------------------
    "Education": {
        "industry": "Education",
        "sector": "Education Services",
        "confidence": 0.95,
        "reason": "DealOpportunities broker-declared Education sector",
    },

    # -----------------------------
    # FOOD, HOSPITALITY & LEISURE
    # -----------------------------
    "Food & drink": {
        "industry": "Food_Beverage",
        "sector": "Food & Beverage",
        "confidence": 0.95,
        "reason": "DealOpportunities broker-declared Food & drink sector",
    },
    "Hotels & restaurants": {
        "industry": "Consumer_Retail",
        "sector": "Hospitality",
        "confidence": 0.95,
        "reason": "DealOpportunities broker-declared Hotels & restaurants sector",
    },
    "Leisure": {
        "industry": "Consumer_Retail",
        "sector": "Leisure / Entertainment",
        "confidence": 0.95,
        "reason": "DealOpportunities broker-declared Leisure sector",
    },
    "Tourism": {
        "industry": "Consumer_Retail",
        "sector": "Tourism & Travel",
        "confidence": 0.95,
        "reason": "DealOpportunities broker-declared Tourism sector",
    },

    # -----------------------------
    # INDUSTRIALS & ENGINEERING
    # -----------------------------
    "Manufacturing": {
        "industry": "Industrials",
        "sector": "Manufacturing",
        "confidence": 0.95,
        "reason": "DealOpportunities broker-declared Manufacturing sector",
    },
    "Engineering": {
        "industry": "Industrials",
        "sector": "Engineering Services",
        "confidence": 0.95,
        "reason": "DealOpportunities broker-declared Engineering sector",
    },
    "Automotive": {
        "industry": "Industrials",
        "sector": "Automotive",
        "confidence": 0.95,
        "reason": "DealOpportunities broker-declared Automotive sector",
    },
    "Aviation": {
        "industry": "Industrials",
        "sector": "Aviation & Aerospace",
        "confidence": 0.95,
        "reason": "DealOpportunities broker-declared Aviation sector",
    },
    "Mining & quarrying": {
        "industry": "Industrials",
        "sector": "Mining & Quarrying",
        "confidence": 0.95,
        "reason": "DealOpportunities broker-declared Mining & quarrying sector",
    },

    # -----------------------------
    # CONSTRUCTION & PROPERTY
    # -----------------------------
    "Construction": {
        "industry": "Construction_Built_Environment",
        "sector": "Construction",
        "confidence": 0.95,
        "reason": "DealOpportunities broker-declared Construction sector",
    },
    "Real estate activities": {
        "industry": "Construction_Built_Environment",
        "sector": "Real Estate",
        "confidence": 0.95,
        "reason": "DealOpportunities broker-declared Real estate activities sector",
    },
    "Electricity, gas & water supply": {
        "industry": "Construction_Built_Environment",
        "sector": "Utilities",
        "confidence": 0.95,
        "reason": "DealOpportunities broker-declared Utilities sector",
    },

    # -----------------------------
    # BUSINESS & PROFESSIONAL SERVICES
    # -----------------------------
    "Business services": {
        "industry": "Business_Services",
        "sector": "Business Services",
        "confidence": 0.95,
        "reason": "DealOpportunities broker-declared Business services sector",
    },
    "Professional & financial": {
        "industry": "Business_Services",
        "sector": "Professional & Financial Services",
        "confidence": 0.95,
        "reason": "DealOpportunities broker-declared Professional & financial sector",
    },
    "Recruitment": {
        "industry": "Business_Services",
        "sector": "Recruitment Services",
        "confidence": 0.95,
        "reason": "DealOpportunities broker-declared Recruitment sector",
    },
    "Service industries": {
        "industry": "Business_Services",
        "sector": "Service Industries",
        "confidence": 0.9,
        "reason": "DealOpportunities broker-declared Service industries sector",
    },

    # -----------------------------
    # RETAIL, WHOLESALE & DISTRIBUTION
    # -----------------------------
    "Retail & wholesale": {
        "industry": "Consumer_Retail",
        "sector": "Retail & Wholesale",
        "confidence": 0.95,
        "reason": "DealOpportunities broker-declared Retail & wholesale sector",
    },
    "Distribution, freight & logistics": {
        "industry": "Industrials",
        "sector": "Logistics & Distribution",
        "confidence": 0.95,
        "reason": "DealOpportunities broker-declared Distribution & logistics sector",
    },
    "Motor dealerships": {
        "industry": "Consumer_Retail",
        "sector": "Motor Dealerships",
        "confidence": 0.95,
        "reason": "DealOpportunities broker-declared Motor dealerships sector",
    },

    # -----------------------------
    # MEDIA, CREATIVE & TECHNOLOGY
    # -----------------------------
    "Advertising & media": {
        "industry": "Media_Communications",
        "sector": "Advertising & Media",
        "confidence": 0.95,
        "reason": "DealOpportunities broker-declared Advertising & media sector",
    },
    "Creative industries": {
        "industry": "Media_Communications",
        "sector": "Creative Industries",
        "confidence": 0.95,
        "reason": "DealOpportunities broker-declared Creative industries sector",
    },
    "Printing & publishing": {
        "industry": "Media_Communications",
        "sector": "Printing & Publishing",
        "confidence": 0.95,
        "reason": "DealOpportunities broker-declared Printing & publishing sector",
    },
    "Technology": {
        "industry": "Technology",
        "sector": "Technology Services",
        "confidence": 0.95,
        "reason": "DealOpportunities broker-declared Technology sector",
    },

    # -----------------------------
    # AGRICULTURE
    # -----------------------------
    "Agriculture, forestry & fishing": {
        "industry": "Agriculture",
        "sector": "Agriculture, Forestry & Fishing",
        "confidence": 0.95,
        "reason": "DealOpportunities broker-declared Agriculture sector",
    },

    # -----------------------------
    # MISC
    # -----------------------------
    "Miscellaneous": {
        "industry": "Other",
        "sector": "Miscellaneous",
        "confidence": 0.8,
        "reason": "DealOpportunities broker-declared Miscellaneous sector",
    },
}
# -------------------------------------------------------------------
# PUBLIC API
# -------------------------------------------------------------------
def _canonicalize_sector(s: str) -> str:
    """
    Normalizes broker sector strings so dictionary lookup is stable.
    """
    return (
        s.replace("\xa0", " ")   # non-breaking spaces
         .replace("&amp;", "&") # HTML entities
         .strip()
    )

def map_dealopportunities_sector(*, raw_sector: str | None):
    """
    Maps DealOpportunities broker-declared sector(s) to canonical industry/sector.
    Supports comma-separated sectors by choosing the first known one.
    """

    if not raw_sector:
        return {
            "industry": "Other",
            "sector": "Miscellaneous",
            "confidence": 0.2,
            "reason": "Missing DealOpportunities sector",
        }

    # Split + canonicalize
    candidates = [
        _canonicalize_sector(s)
        for s in raw_sector.split(",")
        if s.strip()
    ]

    for sector in candidates:
        mapping = DEALOPPORTUNITIES_SECTOR_MAP.get(sector)
        if mapping:
            return mapping

    # Nothing matched
    return {
        "industry": "Other",
        "sector": "Miscellaneous",
        "confidence": 0.3,
        "reason": f"Unknown DealOpportunities sector(s): '{raw_sector}'",
    }