"""
Hilton Smythe → Canonical Industry / Sector Mapping

Rules:
- This file is the ONLY place Hilton Smythe semantics live
- No inference, no keyword guessing
- Confidence is broker-declared
"""

BROKER_NAME = "HiltonSmythe"

HILTON_SMYTHE_SECTOR_MAP = {

    "Automotive": {
        "industry": "Consumer_Retail",
        "sector": "Automotive Services",
        "confidence": 1.0,
        "reason": "Hilton Smythe sector: Automotive",
    },

    "Business Services": {
        "industry": "Business_Services",
        "sector": "Consulting / Professional Services",
        "confidence": 0.9,
        "reason": "Hilton Smythe sector: Business Services",
    },

    "Consumer, Retail & Leisure": {
        "industry": "Consumer_Retail",
        "sector": "Leisure / Hospitality",
        "confidence": 0.85,
        "reason": "Hilton Smythe consumer-facing businesses",
    },

    "Distribution, freight & logistics": {
        "industry": "Logistics_Distribution",
        "sector": "Supply Chain Management",
        "confidence": 1.0,
        "reason": "Hilton Smythe logistics and freight businesses",
    },

    "Electricity, gas & water supply": {
        "industry": "Industrials",
        "sector": "Energy / Utilities",
        "confidence": 1.0,
        "reason": "Utilities and infrastructure services",
    },

    "Industrial Manufacturing": {
        "industry": "Industrials",
        "sector": "Manufacturing",
        "confidence": 1.0,
        "reason": "Hilton Smythe manufacturing businesses",
    },

    "Service industries": {
        "industry": "Business_Services",
        "sector": "Outsourced Services",
        "confidence": 0.75,
        "reason": "Generic services category used by Hilton Smythe",
    },

    "Engineering": {
        "industry": "Industrials",
        "sector": "Engineering",
        "confidence": 0.95,
        "reason": "Engineering and technical service businesses",
    },

    "Recruitment": {
        "industry": "Business_Services",
        "sector": "Recruitment / HR Services",
        "confidence": 0.95,
        "reason": "Recruitment agencies and staffing firms",
    },

    "Technology & Media": {
        "industry": "Technology",
        "sector": "IT Services / Support",
        "confidence": 0.9,
        "reason": "Technology, digital, and media businesses",
    },
}