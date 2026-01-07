"""
BusinessBuyers → Canonical Industry / Sector Mapping

Rules:
- Broker sector dropdown is authoritative
- No guessing if broker sector exists
- Confidence is high (broker-declared)
- Keyword inference ONLY as fallback
"""

BROKER_NAME = "BusinessBuyers"

# -------------------------------------------------------------------
# RAW BUSINESSBUYERS SECTOR → CANONICAL MAPPING
# -------------------------------------------------------------------

BUSINESSBUYERS_SECTOR_MAP = {

    # -----------------------------
    # HEALTHCARE & CARE
    # -----------------------------
    "Healthcare": {
        "industry": "Healthcare",
        "sector": "Social Care / Healthcare Services",
        "confidence": 0.95,
        "reason": "BusinessBuyers broker-declared Healthcare sector",
    },
    "Medical": {
        "industry": "Healthcare",
        "sector": "Medical Services",
        "confidence": 0.95,
        "reason": "BusinessBuyers broker-declared Medical sector",
    },

    # -----------------------------
    # EDUCATION / CHILDCARE
    # -----------------------------
    "Childcare & Education": {
        "industry": "Education",
        "sector": "Childcare / Education Services",
        "confidence": 0.95,
        "reason": "BusinessBuyers broker-declared Childcare & Education sector",
    },

    # -----------------------------
    # CONSUMER / RETAIL / LEISURE
    # -----------------------------
    "Fast Food": {
        "industry": "Food_Beverage",
        "sector": "Quick Service Restaurants",
        "confidence": 0.95,
        "reason": "BusinessBuyers broker-declared Fast Food sector",
    },
    "Hotels": {
        "industry": "Consumer_Retail",
        "sector": "Hospitality",
        "confidence": 0.95,
        "reason": "BusinessBuyers broker-declared Hotels sector",
    },
    "Leisure": {
        "industry": "Consumer_Retail",
        "sector": "Leisure / Entertainment",
        "confidence": 0.95,
        "reason": "BusinessBuyers broker-declared Leisure sector",
    },
    "Licensed": {
        "industry": "Consumer_Retail",
        "sector": "Pubs / Bars / Licensed Premises",
        "confidence": 0.95,
        "reason": "BusinessBuyers broker-declared Licensed sector",
    },
    "Health & Beauty": {
        "industry": "Consumer_Retail",
        "sector": "Health & Beauty Services",
        "confidence": 0.95,
        "reason": "BusinessBuyers broker-declared Health & Beauty sector",
    },
    "Kennels Catteries": {
        "industry": "Consumer_Retail",
        "sector": "Pet Services",
        "confidence": 0.9,
        "reason": "BusinessBuyers broker-declared Kennels & Catteries sector",
    },
    "Retail": {
        "industry": "Consumer_Retail",
        "sector": "Retail Stores",
        "confidence": 0.95,
        "reason": "BusinessBuyers broker-declared Retail sector",
    },
    "E-Commerce": {
        "industry": "Consumer_Retail",
        "sector": "E-Commerce",
        "confidence": 0.95,
        "reason": "BusinessBuyers broker-declared E-Commerce sector",
    },

    # -----------------------------
    # BUSINESS SERVICES
    # -----------------------------
    "Professional": {
        "industry": "Business_Services",
        "sector": "Professional Services",
        "confidence": 0.9,
        "reason": "BusinessBuyers broker-declared Professional sector",
    },
    "Commercial": {
        "industry": "Business_Services",
        "sector": "Commercial Services",
        "confidence": 0.9,
        "reason": "BusinessBuyers broker-declared Commercial sector",
    },
    "Corporate": {
        "industry": "Business_Services",
        "sector": "Corporate Services",
        "confidence": 0.85,
        "reason": "BusinessBuyers broker-declared Corporate sector",
    },

    # -----------------------------
    # PROPERTY / INDUSTRIALS
    # -----------------------------
    "Property": {
        "industry": "Construction_Built_Environment",
        "sector": "Property Services",
        "confidence": 0.9,
        "reason": "BusinessBuyers broker-declared Property sector",
    },
    "Garage": {
        "industry": "Industrials",
        "sector": "Automotive Services",
        "confidence": 0.9,
        "reason": "BusinessBuyers broker-declared Garage sector",
    },
}

# -------------------------------------------------------------------
# PUBLIC API
# -------------------------------------------------------------------

def map_businessbuyers_sector(*, raw_sector: str | None):
    """
    Maps BusinessBuyers broker-declared sector to canonical industry/sector.
    Falls back to 'Other' if missing or unknown.
    """
    if not raw_sector:
        return {
            "industry": "Other",
            "sector": "Miscellaneous",
            "confidence": 0.2,
            "reason": "Missing BusinessBuyers sector",
            "sector_source": "unclassified",
        }

    mapping = BUSINESSBUYERS_SECTOR_MAP.get(raw_sector)
    if mapping:
        return mapping

    return {
        "industry": "Other",
        "sector": "Miscellaneous",
        "confidence": 0.3,
        "reason": f"Unknown BusinessBuyers sector '{raw_sector}'",
        "sector_source": "unclassified",
    }