import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"

RULES = [
    # SaaS / platforms
    {
        "keywords": ["website development"],
        "industry": "Technology",
        "sector": "Software / SaaS",
    },

    # Hardware / equipment suppliers
    {
        "keywords": ["Healthcare and hospitality supplies"],
        "industry": "Healthcare",
        "sector": "Medical Devices / Equipment",
    },

    # Domiciliary / care hybrids
    {
        "keywords": ["washroom products"],
        "industry": "Industrials",
        "sector": "Manufacturing",
    },

    # Domiciliary / care hybrids
    {
        "keywords": ["pest control"],
        "industry": "Business_Services",
        "sector": "Facilities Management",
    },

    # Domiciliary / care hybrids
    {
        "keywords": ["furniture removal"],
        "industry": "Logistics_Distribution",
        "sector": "Transportation & Moving Services",
    },
]


def classify(title: str):
    t = (title or "").lower()
    for rule in RULES:
        if any(k in t for k in rule["keywords"]):
            return rule["industry"], rule["sector"]
    return None, None


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        """
        SELECT id, title
        FROM deals
        WHERE source = 'Knightsbridge'
          AND detail_fetch_reason = 'MISSING_SECTOR_CANONICAL'
          AND (industry IS NULL OR sector IS NULL)
        """
    ).fetchall()

    updated = 0
    skipped = 0

    for r in rows:
        industry, sector = classify(r["title"])
        if not industry:
            skipped += 1
            continue

        conn.execute("""
            UPDATE deals
            SET
                industry                    = ?,
                sector                      = ?,
                sector_source               = 'manual',
                sector_inference_confidence = 0.7,
                sector_inference_reason     = 'legacy_title_reconciliation_pass2',
            
                detail_fetched_at           = CURRENT_TIMESTAMP,
                needs_detail_refresh        = 0,
                detail_fetch_reason         = NULL,
            
                last_updated                = CURRENT_TIMESTAMP,
                last_updated_source         = 'AUTO'
            WHERE id = ?
        """, (industry, sector, r["id"] ))
        updated += 1

    conn.commit()
    conn.close()

    print(f"Updated: {updated}")
    print(f"Skipped (still unresolved): {skipped}")


if __name__ == "__main__":
    main()