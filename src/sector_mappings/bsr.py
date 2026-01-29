"""
Canonical sector mapping for BusinessSaleReport (BSR).

- sector_raw: BSR broker-declared sector label (normalized)
- industry / sector: internal canonical taxonomy
- confidence: < 1.0 (category-level inference)
"""

BSR_SECTOR_MAP = {
    "advertising and media": {
        "industry": "Business_Services",
        "sector": "Advertising & Media Services",
        "confidence": 0.65,
        "reason": "BusinessSaleReport sector: advertising and media",
    },
    "asset sales & fast sales": {
        "industry": "Other",
        "sector": "Asset Sales & Distressed Situations",
        "confidence": 0.6,
        "reason": "BusinessSaleReport sector: asset sales and fast sales",
    },
    "building and construction": {
        "industry": "Construction_Built_Environment",
        "sector": "Building & Construction Services",
        "confidence": 0.7,
        "reason": "BusinessSaleReport sector: building and construction",
    },
    "cafes, restaurants & pubs": {
        "industry": "Consumer_Retail",
        "sector": "Hospitality & Food Service",
        "confidence": 0.75,
        "reason": "BusinessSaleReport sector: cafes, restaurants and pubs",
    },
    "education": {
        "industry": "Education",
        "sector": "Education & Training Services",
        "confidence": 0.7,
        "reason": "BusinessSaleReport sector: education",
    },
    "engineering": {
        "industry": "Industrials",
        "sector": "Engineering Services",
        "confidence": 0.7,
        "reason": "BusinessSaleReport sector: engineering",
    },
    "leisure and lifestyle": {
        "industry": "Consumer_Retail",
        "sector": "Leisure & Lifestyle",
        "confidence": 0.65,
        "reason": "BusinessSaleReport sector: leisure and lifestyle",
    },
    "manufacturing": {
        "industry": "Industrials",
        "sector": "Manufacturing",
        "confidence": 0.75,
        "reason": "BusinessSaleReport sector: manufacturing",
    },
    "medical": {
        "industry": "Healthcare",
        "sector": "Medical & Healthcare Services",
        "confidence": 0.7,
        "reason": "BusinessSaleReport sector: medical",
    },
    "online businesses": {
        "industry": "Technology",
        "sector": "Digital & Online Businesses",
        "confidence": 0.65,
        "reason": "BusinessSaleReport sector: online businesses",
    },
    "other": {
        "industry": "Other",
        "sector": "Miscellaneous",
        "confidence": 0.4,
        "reason": "BusinessSaleReport sector: other (explicit catch-all)",
    },
    "professional and financial": {
        "industry": "Business_Services",
        "sector": "Professional & Financial Services",
        "confidence": 0.7,
        "reason": "BusinessSaleReport sector: professional and financial",
    },
    "retail": {
        "industry": "Consumer_Retail",
        "sector": "Retail",
        "confidence": 0.75,
        "reason": "BusinessSaleReport sector: retail",
    },
    "services": {
        "industry": "Business_Services",
        "sector": "General Services",
        "confidence": 0.6,
        "reason": "BusinessSaleReport sector: services",
    },
    "technology": {
        "industry": "Technology",
        "sector": "Technology Services & Products",
        "confidence": 0.75,
        "reason": "BusinessSaleReport sector: technology",
    },
    "transportation & distribution": {
        "industry": "Industrials",
        "sector": "Transportation & Distribution",
        "confidence": 0.7,
        "reason": "BusinessSaleReport sector: transportation and distribution",
    },
    "wholesale": {
        "industry": "Industrials",
        "sector": "Wholesale & Distribution",
        "confidence": 0.7,
        "reason": "BusinessSaleReport sector: wholesale",
    },
}