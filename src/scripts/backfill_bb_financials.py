import sqlite3
from pathlib import Path
from src.enrichment.financial_extractor import extract_financial_metrics

DB_PATH = Path(__file__).resolve().parents[2] / "db" / "deals.sqlite"

DRY_RUN = True   # flip explicitly when ready

MAX_REASONABLE_K = 50_000  # £50m hard cap for BB

def value_of(m):
    if not isinstance(m, dict):
        return None
    return m.get("value")


conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row

rows = conn.execute(
    """
    SELECT id, description,
           asking_price_k, revenue_k, ebitda_k
    FROM deals
    WHERE source = 'BusinessBuyers'
      AND description IS NOT NULL
      AND (status IS NULL OR status != 'Lost')
    """
).fetchall()

updated = 0
skipped = 0
flagged = 0

for r in rows:
    facts = extract_financial_metrics(r["description"])
    if not facts:
        continue

    asking_price_k = value_of(facts.get("asking_price_k"))
    revenue_k      = value_of(facts.get("revenue_k"))
    ebitda_k       = value_of(facts.get("ebitda_k"))

    # ---------- sanity guards ----------
    sane = True
    for name, val in {
        "asking_price_k": asking_price_k,
        "revenue_k": revenue_k,
        "ebitda_k": ebitda_k,
    }.items():
        if val is None:
            continue
        if val < 0 or val > MAX_REASONABLE_K:
            sane = False
            flagged += 1
            print(f"⚠️  Outlier {name}={val}k for deal {r['id']}")
            break

    if not sane:
        continue

    # nothing new → skip
    if (
        asking_price_k is None and
        revenue_k is None and
        ebitda_k is None
    ):
        skipped += 1
        continue

    if DRY_RUN:
        print(
            f"[DRY] id={r['id']} → "
            f"asking={asking_price_k}, "
            f"revenue={revenue_k}, "
            f"ebitda={ebitda_k}"
        )
        updated += 1
        continue

    conn.execute(
        """
        UPDATE deals
        SET
            asking_price_k = COALESCE(?, asking_price_k),
            revenue_k      = COALESCE(?, revenue_k),
            ebitda_k       = COALESCE(?, ebitda_k),
            last_updated   = CURRENT_TIMESTAMP,
            last_updated_source = 'AUTO'
        WHERE id = ?
        """,
        (
            asking_price_k,
            revenue_k,
            ebitda_k,
            r["id"],
        ),
    )

    updated += 1

if not DRY_RUN:
    conn.commit()

conn.close()

print(
    f"✅ Backfill summary — "
    f"updated: {updated}, "
    f"skipped: {skipped}, "
    f"flagged: {flagged}"
)