import sqlite3
import re
from pathlib import Path
from typing import Optional

DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"

SOURCES = ("AxisPartnership", "Knightsbridge")
DRY_RUN = False  # flip to False after review

FIELDS = [
    "asking_price_k",
    "revenue_k",
    "ebitda_k",
]

GBP_RE = re.compile(r"^£\s*([\d,]+)$")


def normalize(value: str) -> Optional[int]:
    if not value:
        return None
    if not isinstance(value, str):
        return None
    m = GBP_RE.match(value.strip())
    if not m:
        return None
    return int(m.group(1).replace(",", ""))


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    total_updated = 0
    total_skipped = 0

    for col in FIELDS:
        print(f"\n--- {col} ---")

        rows = conn.execute(
            f"""
            SELECT id, source, {col}
            FROM deals
            WHERE source IN (?, ?)
              AND {col} IS NOT NULL
              AND typeof({col}) = 'text'
            """,
            SOURCES,
        ).fetchall()

        updated = 0
        skipped = 0

        for r in rows:
            new_val = normalize(r[col])

            if new_val is None:
                skipped += 1
                print(
                    f"SKIP  id={r['id']} "
                    f"source={r['source']} "
                    f"{col}='{r[col]}'"
                )
                continue

            print(
                f"{'DRY' if DRY_RUN else 'APPLY'} "
                f"id={r['id']} "
                f"source={r['source']} "
                f"{col}: '{r[col]}' → {new_val}"
            )

            if not DRY_RUN:
                conn.execute(
                    f"""
                    UPDATE deals
                    SET {col} = ?,
                        last_updated = CURRENT_TIMESTAMP,
                        last_updated_source = 'AUTO'
                    WHERE id = ?
                      AND typeof({col}) = 'text'
                    """,
                    (new_val, r["id"]),
                )

            updated += 1

        if not DRY_RUN:
            conn.commit()

        total_updated += updated
        total_skipped += skipped

        print(f"SUMMARY {col}: updated={updated}, skipped={skipped}")

    conn.close()

    print("\nDONE")
    print(f"Total updated: {total_updated}")
    print(f"Total skipped: {total_skipped}")
    print(f"DRY_RUN={DRY_RUN}")


if __name__ == "__main__":
    main()