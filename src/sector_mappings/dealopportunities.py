"""
DealOpportunities → Canonical Industry / Sector Mapping

Principles:
- Broker sector is authoritative
- Industry reflects economic model (how money is made)
- Deterministic resolution (no NLP / keyword inference)
- Defaults bias toward Business Services, not Industrials
"""

BROKER_NAME = "DealOpportunities"


# -------------------------------------------------------------------
# NORMALIZATION
# -------------------------------------------------------------------

def _canonicalize_sector(s: str) -> str:
    return (
        s.replace("\xa0", " ")
         .replace("&amp;", "&")
         .strip()
         .title()
    )


def normalize_do_sector(raw: str | None) -> str | None:
    if not raw:
        return None

    s = _canonicalize_sector(raw)

    COLLAPSE = {
        "Engineering": "Engineering Services",
        "Service Industries": "Business Services",
        "Professional & Financial": "Professional Services",
        "Distribution, Freight & Logistics": "Logistics",
        "Food & Drink": "Food & Beverage",
    }

    return COLLAPSE.get(s, s)


# -------------------------------------------------------------------
# RAW DEALOPPORTUNITIES SECTOR → CANONICAL MAPPING
# -------------------------------------------------------------------

DEALOPPORTUNITIES_SECTOR_MAP = {

    # -----------------------------
    # HEALTHCARE
    # -----------------------------
    "Health & Care": {
        "industry": "Healthcare",
        "sector": "Healthcare Services",
        "confidence": 0.95,
        "reason": "Broker-declared healthcare services",
    },
    "Medical": {
        "industry": "Healthcare",
        "sector": "Medical Services",
        "confidence": 0.95,
        "reason": "Broker-declared medical sector",
    },

    # -----------------------------
    # EDUCATION
    # -----------------------------
    "Education": {
        "industry": "Education",
        "sector": "Education Services",
        "confidence": 0.95,
        "reason": "Broker-declared education sector",
    },

    # -----------------------------
    # FOOD / HOSPITALITY
    # -----------------------------
    "Food & Beverage": {
        "industry": "Food_Beverage",
        "sector": "Food & Beverage",
        "confidence": 0.95,
        "reason": "Broker-declared food & beverage",
    },
    "Hotels & Restaurants": {
        "industry": "Consumer_Retail",
        "sector": "Hospitality",
        "confidence": 0.95,
        "reason": "Broker-declared hospitality",
    },
    "Leisure": {
        "industry": "Consumer_Retail",
        "sector": "Leisure & Entertainment",
        "confidence": 0.95,
        "reason": "Broker-declared leisure sector",
    },
    "Tourism": {
        "industry": "Consumer_Retail",
        "sector": "Travel & Tourism",
        "confidence": 0.95,
        "reason": "Broker-declared tourism sector",
    },

    # -----------------------------
    # INDUSTRIALS (ASSET-HEAVY ONLY)
    # -----------------------------
    "Manufacturing": {
        "industry": "Industrials",
        "sector": "Manufacturing",
        "confidence": 0.95,
        "reason": "Asset-heavy manufacturing business",
    },
    "Automotive": {
        "industry": "Industrials",
        "sector": "Automotive",
        "confidence": 0.95,
        "reason": "Broker-declared automotive sector",
    },
    "Aviation": {
        "industry": "Industrials",
        "sector": "Aviation & Aerospace",
        "confidence": 0.95,
        "reason": "Broker-declared aviation sector",
    },
    "Mining & Quarrying": {
        "industry": "Industrials",
        "sector": "Mining & Quarrying",
        "confidence": 0.95,
        "reason": "Broker-declared extractive industry",
    },

    # -----------------------------
    # CONSTRUCTION
    # -----------------------------
    "Construction": {
        "industry": "Construction_Built_Environment",
        "sector": "Construction",
        "confidence": 0.95,
        "reason": "Broker-declared construction sector",
    },
    "Real Estate Activities": {
        "industry": "Construction_Built_Environment",
        "sector": "Real Estate",
        "confidence": 0.95,
        "reason": "Broker-declared real estate activities",
    },
    "Electricity, Gas & Water Supply": {
        "industry": "Construction_Built_Environment",
        "sector": "Utilities",
        "confidence": 0.95,
        "reason": "Broker-declared utilities sector",
    },

    # -----------------------------
    # BUSINESS SERVICES (DEFAULT)
    # -----------------------------
    "Business Services": {
        "industry": "Business_Services",
        "sector": "Business Services",
        "confidence": 0.95,
        "reason": "Fee-based business services",
    },
    "Professional Services": {
        "industry": "Business_Services",
        "sector": "Professional Services",
        "confidence": 0.95,
        "reason": "Professional / advisory services",
    },
    "Recruitment": {
        "industry": "Business_Services",
        "sector": "Recruitment Services",
        "confidence": 0.95,
        "reason": "Broker-declared recruitment services",
    },
    "Engineering Services": {
        "industry": "Business_Services",
        "sector": "Engineering & Technical Services",
        "confidence": 0.95,
        "reason": "Engineering services are fee-based",
    },

    # -----------------------------
    # LOGISTICS (SERVICE BIAS)
    # -----------------------------
    "Logistics": {
        "industry": "Business_Services",
        "sector": "Logistics & Supply Chain Services",
        "confidence": 0.9,
        "reason": "Managed logistics services",
    },

    # -----------------------------
    # RETAIL
    # -----------------------------
    "Retail & Wholesale": {
        "industry": "Consumer_Retail",
        "sector": "Retail & Wholesale",
        "confidence": 0.95,
        "reason": "Broker-declared retail & wholesale",
    },
    "Motor Dealerships": {
        "industry": "Consumer_Retail",
        "sector": "Motor Dealerships",
        "confidence": 0.95,
        "reason": "Broker-declared motor dealerships",
    },

    # -----------------------------
    # MEDIA → OTHER (INFRA-SAFE)
    # -----------------------------
    "Advertising & Media": {
        "industry": "Other",
        "sector": "Advertising & Media",
        "confidence": 0.8,
        "reason": "Media collapsed to Other (no dedicated infra)",
    },
    "Creative Industries": {
        "industry": "Other",
        "sector": "Creative Industries",
        "confidence": 0.8,
        "reason": "Creative industries collapsed to Other",
    },
    "Printing & Publishing": {
        "industry": "Other",
        "sector": "Printing & Publishing",
        "confidence": 0.8,
        "reason": "Publishing collapsed to Other",
    },

    # -----------------------------
    # TECHNOLOGY
    # -----------------------------
    "Technology": {
        "industry": "Technology",
        "sector": "Technology Services",
        "confidence": 0.95,
        "reason": "Broker-declared technology services",
    },

    # -----------------------------
    # AGRICULTURE → OTHER
    # -----------------------------
    "Agriculture, Forestry & Fishing": {
        "industry": "Other",
        "sector": "Agriculture / Primary",
        "confidence": 0.8,
        "reason": "Agriculture collapsed to Other",
    },
}


# -------------------------------------------------------------------
# PUBLIC API
# -------------------------------------------------------------------

def map_dealopportunities_sector(*, raw_sector: str | None) -> dict:
    if not raw_sector:
        return {
            "industry": "Business_Services",
            "sector": "General Services",
            "confidence": 0.3,
            "reason": "Missing DO sector, defaulted to services",
        }

    candidates = [
        normalize_do_sector(s)
        for s in raw_sector.split(",")
        if s.strip()
    ]

    for sector in candidates:
        mapping = DEALOPPORTUNITIES_SECTOR_MAP.get(sector)
        if mapping:
            return mapping

    return {
        "industry": "Business_Services",
        "sector": "General Services",
        "confidence": 0.3,
        "reason": f"Unmapped DO sector(s): '{raw_sector}'",
    }