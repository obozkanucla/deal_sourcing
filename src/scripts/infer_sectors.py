from pathlib import Path
from src.persistence.repository import SQLiteRepository
from src.integrations.drive_folders import get_drive_parent_folder_id
from src.integrations.google_drive import move_folder_to_parent

DRY_RUN = False  # flip to False when confident

# ============================================================
# TAXONOMY
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
        "Beverage Production",
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

SECTOR_KEYWORDS = [
    ("Healthcare", "Domiciliary Care / Home Healthcare",
     ["home care", "domiciliary", "care agency"]),
    ("Technology", "Software / SaaS",
     ["software", "saas", "platform"]),
    ("Industrials", "Manufacturing",
     ["manufacturing", "factory"]),
    ("Consumer_Retail", "E-commerce",
     ["e-commerce", "shopify", "amazon"]),
]

# ============================================================
# HELPERS
# ============================================================

def normalize(text: str) -> str:
    return (text or "").lower()

def infer_sector(text: str):
    text_l = normalize(text)
    for industry, sector, keywords in SECTOR_KEYWORDS:
        hits = [kw for kw in keywords if kw in text_l]
        if hits:
            return {
                "industry": industry,
                "sector": sector,
                "confidence": min(0.9, 0.4 + 0.1 * len(hits)),
                "reason": f"Matched keywords: {', '.join(hits)}",
            }
    return None

def maybe_move_drive_folder(deal, new_industry):
    old_industry = deal.get("industry")
    folder_id = deal.get("drive_folder_id")

    if not folder_id:
        print("   ⏭️ No Drive folder")
        return

    if not old_industry or old_industry == new_industry:
        print("   ⏭️ Industry unchanged")
        return

    new_parent = get_drive_parent_folder_id(
        industry=new_industry,
        broker=deal["source"],
    )

    print(f"   📁 Drive move {old_industry} → {new_industry}")

    if DRY_RUN:
        print("   🧪 DRY RUN — not moving folder")
        return

    move_folder_to_parent(folder_id, new_parent)

# ============================================================
# MAIN
# ============================================================

def main():
    repo = SQLiteRepository(Path("db/deals.sqlite"))

    deals = repo.fetch_all("""
            SELECT
                id,
                source,
                industry,
                sector,
                sector_source,
                sector_raw,
                description,
                drive_folder_id
            FROM deals
            WHERE sector_source IS NULL
               OR sector_source = 'unclassified'
    """)

    updated = 0

    for d in deals:
        old_industry = d.get("industry")

        text = " ".join(
            filter(None, [
                d.get("sector_raw"),
                d.get("industry_raw"),
                d.get("description"),
            ])
        )

        inference = infer_sector(text)
        if not inference:
            continue

        if inference["confidence"] < 0.5:
            continue

        if inference["sector"] not in CANONICAL_TAXONOMY[inference["industry"]]:
            continue

        print(
            f"\n➡️ {d['source']}:{d['id']} | "
            f"{old_industry} → {inference['industry']}"
        )

        if DRY_RUN:
            print(
                f"   🧪 DRY RUN — would update "
                f"{inference['industry']} / {inference['sector']}"
            )
        else:
            print(inference["industry"], inference["sector"], inference["reason"])
            repo.update_sector_inference(
                deal_id=d["id"],
                industry=inference["industry"],
                sector=inference["sector"],
                source="inferred",
                reason=inference["reason"],
                confidence=inference["confidence"],
            )

            # Now Drive follows DB
            maybe_move_drive_folder(d, inference["industry"])
        updated += 1

    print(f"\n✅ Sector inference complete — updated={updated}")

if __name__ == "__main__":
    main()