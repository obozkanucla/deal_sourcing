import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"

RULES = [
    # SaaS / platforms
    {
        "keywords": ["software", "platform", "code-free", "scalable", "digital"],
        "industry": "Technology",
        "sector": "Software / SaaS",
    },

    # Hardware / equipment suppliers
    {
        "keywords": ["equipment", "supplier", "distributor", "pressure washer", "machinery"],
        "industry": "Industrials",
        "sector": "Engineering",
    },

    # Domiciliary / care hybrids
    {
        "keywords": ["domiciliary", "home support", "care services"],
        "industry": "Healthcare",
        "sector": "Social Care",
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

        conn.execute(
            """
            UPDATE deals
            SET industry                    = ?,
                sector                      = ?,
                sector_source               = 'broker',
                sector_inference_confidence = 0.7,
                sector_inference_reason     = 'legacy_title_reconciliation_pass2',
                last_updated                = CURRENT_TIMESTAMP,
                last_updated_source         = 'AUTO'
            WHERE id = ?
            """,
            (industry, sector, r["id"]),
        )
        updated += 1

    conn.commit()
    conn.close()

    print(f"Updated: {updated}")
    print(f"Skipped (still unresolved): {skipped}")


if __name__ == "__main__":
    main()