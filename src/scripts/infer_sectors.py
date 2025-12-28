from pathlib import Path
from src.persistence.repository import SQLiteRepository

# ============================================================
# CANONICAL INDUSTRY → SECTOR TAXONOMY (SOURCE OF TRUTH)
# ============================================================

CANONICAL_TAXONOMY = {
    "Business_Services": [
        "Consulting / Professional Services",
        "Facilities Management",
        "IT Services / Support",
        "Marketing / Advertising",
        "Recruitment / HR Services",
    ],
    "Construction_Built_Environment": [
        "Architecture / Design",
        "Building Materials",
        "Construction Contractors",
        "Facilities Management",
        "Property Development",
    ],
    "Consumer_Retail": [
        "Consumer Goods",
        "E-commerce",
        "Food & Beverage Retail",
        "Leisure / Hospitality",
        "Retail Stores",
    ],
    "Education": [
        "Educational Products",
        "Higher Education",
        "Schools / Colleges",
        "Training / E-learning",
    ],
    "Financial_Services": [
        "Accounting / Auditing",
        "Asset Management",
        "Banking",
        "FinTech",
        "Insurance",
    ],
    "Food_Beverage": [
        "Beverage Production (including Beer Distribution)",
        "Catering / Hospitality",
        "Food Production / Processing",
        "Food Retail",
    ],
    "Healthcare": [
        "Domiciliary Care / Home Healthcare",
        "Hospitals / Clinics",
        "Medical Devices / Equipment",
        "Pharmaceuticals",
        "Social Care",
    ],
    "Industrials": [
        "Chemicals",
        "Energy / Utilities",
        "Engineering",
        "Equipment Rental / Leasing",
        "Manufacturing",
    ],
    "Logistics_Distribution": [
        "Courier / Delivery Services",
        "Freight / Shipping",
        "Supply Chain Management",
        "Warehousing",
    ],
    "Other": [
        "Agriculture / Farming",
        "Government / Public Sector",
        "Media / Publishing",
        "Miscellaneous",
        "Non-Profit / Charities",
    ],
    "Technology": [
        "Cybersecurity",
        "Hardware",
        "IT Consulting",
        "Software / SaaS",
        "Telecommunications",
    ],
}

# ============================================================
# KEYWORD → CANONICAL SECTOR RULES
# ============================================================

SECTOR_KEYWORDS = [
    # Healthcare
    ("Healthcare", "Domiciliary Care / Home Healthcare",
     ["home care", "domiciliary", "care agency", "care business"]),
    ("Healthcare", "Social Care",
     ["supported living", "learning disabilities", "autism"]),
    ("Healthcare", "Hospitals / Clinics",
     ["clinic", "hospital", "medical centre"]),
    ("Healthcare", "Medical Devices / Equipment",
     ["medical device", "diagnostic equipment"]),
    ("Healthcare", "Pharmaceuticals",
     ["pharma", "pharmaceutical", "drug manufacturing"]),

    # Technology
    ("Technology", "Software / SaaS",
     ["software", "saas", "platform", "subscription", "cloud"]),
    ("Technology", "IT Consulting",
     ["it consulting", "technology consulting", "systems integration"]),
    ("Technology", "Cybersecurity",
     ["cybersecurity", "security software", "infosec"]),
    ("Technology", "Telecommunications",
     ["telecom", "network operator", "broadband"]),
    ("Technology", "Hardware",
     ["hardware", "devices", "electronics"]),

    # Business Services
    ("Business_Services", "Marketing / Advertising",
     ["marketing", "branding", "advertising", "digital agency"]),
    ("Business_Services", "Recruitment / HR Services",
     ["recruitment", "staffing", "headhunting", "hr services"]),
    ("Business_Services", "Consulting / Professional Services",
     ["consulting", "advisory", "professional services"]),

    # Industrials
    ("Industrials", "Manufacturing",
     ["manufacturing", "factory", "production facility"]),
    ("Industrials", "Engineering",
     ["engineering", "fabrication", "machining"]),
    ("Industrials", "Energy / Utilities",
     ["energy", "utilities", "power generation"]),

    # Consumer / Retail
    ("Consumer_Retail", "E-commerce",
     ["e-commerce", "ecommerce", "shopify", "amazon"]),
    ("Consumer_Retail", "Retail Stores",
     ["retail store", "high street shop"]),
    ("Consumer_Retail", "Leisure / Hospitality",
     ["hospitality", "leisure", "hotel", "restaurant"]),
]

# ============================================================
# HELPERS
# ============================================================

def normalize(text: str) -> str:
    return (text or "").lower()


def infer_sector(text: str):
    """
    Returns canonical (industry, sector, reason, confidence) or None.
    """
    text_l = normalize(text)

    for industry, sector, keywords in SECTOR_KEYWORDS:
        hits = [kw for kw in keywords if kw in text_l]
        if hits:
            confidence = min(0.9, 0.4 + 0.1 * len(hits))
            return {
                "industry": industry,
                "sector": sector,
                "reason": f"Matched keywords: {', '.join(hits[:3])}",
                "confidence": confidence,
            }

    return None


# ============================================================
# MAIN
# ============================================================

def main():
    repo = SQLiteRepository(Path("db/deals.sqlite"))

    deals = repo.fetch_all(
        """
        SELECT deal_id,
               sector_source,
               sector_raw,
               industry_raw,
               description
        FROM deals
        WHERE sector_source IS NULL
           OR sector_source = 'inferred'
        """
    )

    updated = 0
    skipped = 0

    for d in deals:
        # 🔒 Never override manual classification
        if d["sector_source"] == "manual":
            skipped += 1
            continue

        text_blob = " ".join(
            filter(
                None,
                [
                    d.get("sector_raw"),
                    d.get("industry_raw"),
                    d.get("description"),
                ],
            )
        )

        inference = infer_sector(text_blob)
        if not inference:
            continue

        # 🔒 Safety threshold
        if inference["confidence"] < 0.5:
            continue

        # 🔒 Final validation against canonical taxonomy
        if inference["sector"] not in CANONICAL_TAXONOMY[inference["industry"]]:
            continue  # should never happen, but guards future mistakes

        repo.update_sector_inference(
            deal_id=d["deal_id"],
            industry=inference["industry"],
            sector=inference["sector"],
            source="inferred",
            reason=inference["reason"],
            confidence=inference["confidence"],
        )

        updated += 1

    print(f"✅ Sector inference complete — updated={updated}, skipped={skipped}")


if __name__ == "__main__":
    main()