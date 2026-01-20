# src/sector_mappings/b4s.py

"""
Canonical sector mapping for BusinessesForSale (B4S).

- sector_raw: B4S main category key
- industry / sector: internal canonical taxonomy
- confidence: intentionally < 1.0 (category-level inference)
"""

B4S_SECTOR_MAP = {
    "agriculture": {
        "industry": "Agriculture",
        "sector": "Agribusiness",
        "confidence": 0.7,
        "reason": "BusinessesForSale main category: agriculture",
    },
    "energy": {
        "industry": "Energy",
        "sector": "Energy Services",
        "confidence": 0.7,
        "reason": "BusinessesForSale main category: energy",
    },
    "engineering": {
        "industry": "Industrials",
        "sector": "Engineering Services",
        "confidence": 0.7,
        "reason": "BusinessesForSale main category: engineering",
    },
    "commercial_property": {
        "industry": "Real Estate",
        "sector": "Commercial Property",
        "confidence": 0.7,
        "reason": "BusinessesForSale main category: commercial property",
    },
    "food": {
        "industry": "Consumer",
        "sector": "Food & Beverage",
        "confidence": 0.7,
        "reason": "BusinessesForSale main category: food",
    },
    "manufacturing": {
        "industry": "Industrials",
        "sector": "Manufacturing",
        "confidence": 0.7,
        "reason": "BusinessesForSale main category: manufacturing",
    },
    "services": {
        "industry": "Business Services",
        "sector": "Professional & Operational Services",
        "confidence": 0.65,
        "reason": "BusinessesForSale main category: services",
    },
    "wholesale_distribution": {
        "industry": "Industrials",
        "sector": "Wholesale & Distribution",
        "confidence": 0.7,
        "reason": "BusinessesForSale main category: wholesale and distribution",
    },
}