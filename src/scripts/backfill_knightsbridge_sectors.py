import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"

FACILITIES_KEYWORDS = [
    "cleaning", "pest", "drain", "waste", "grounds",
    "ventilation", "extraction", "mould", "damp",
    "site cleaning", "facilities", "window cleaning"
]

BUILDING_MATERIALS_KEYWORDS = [
    "window", "door", "glazing", "fenestration", "joinery",
    "shutter", "blind", "upvc", "aluminium", "curtain",
    "canopy"
]

CONSTRUCTION_KEYWORDS = [
    "construction", "contractor", "restoration", "renovation",
    "maintenance", "roof", "builder", "installation", "repair"
]

def classify(title: str):
    t = title.lower()

    if any(k in t for k in FACILITIES_KEYWORDS):
        return "Business_Services", "Facilities Management"

    if any(k in t for k in BUILDING_MATERIALS_KEYWORDS):
        return "Construction_Built_Environment", "Building Materials"

    if any(k in t for k in CONSTRUCTION_KEYWORDS):
        return "Construction_Built_Environment", "Construction Contractors"

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
        """
    ).fetchall()

    updated = 0
    skipped = 0

    for r in rows:
        industry, sector = classify(r["title"] or "")
        if not industry:
            skipped += 1
            continue

        conn.execute(
            """
            UPDATE deals
            SET industry                    = ?,
                sector                      = ?,
                sector_source               = 'broker',
                sector_inference_confidence = 0.8,
                sector_inference_reason     = 'legacy_title_reconciliation',
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
    print(f"Skipped (no match): {skipped}")


if __name__ == "__main__":
    main()