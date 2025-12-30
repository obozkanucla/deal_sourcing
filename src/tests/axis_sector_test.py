import sqlite3
from pathlib import Path
from src.sector_mappings.axis import infer_axis_industry

DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row

rows = conn.execute("""
    SELECT
        id,
        source_listing_id,
        description
    FROM deals
    WHERE source = 'AxisPartnership'
    ORDER BY id
""").fetchall()

print(f"Axis rows: {len(rows)}\n")

for r in rows[:20]:
    result = infer_axis_industry(r["description"])
    print(
        f"Axis {r['source_listing_id']}: "
        f"{result['industry']} | "
        f"confidence={result['confidence']} | "
        f"{result['reason']}"
    )