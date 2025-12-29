# src/scripts/apply_broker_mappings.py
"""
Apply broker-declared sector mappings to deals.

Rules:
- Broker mappings have HIGHER authority than inference
- Never override manual classifications
- Safe to run repeatedly (idempotent)
- Intended for BAU backfill + post-import normalization
"""

from pathlib import Path
import importlib
import pkgutil

from src.persistence.repository import SQLiteRepository


# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------

DB_PATH = Path("db/deals.sqlite")
MAPPINGS_PACKAGE = "src.sector_mappings"


# ---------------------------------------------------------------------
# DISCOVERY
# ---------------------------------------------------------------------

def load_broker_mappings():
    """
    Dynamically load all broker mapping modules.
    """
    mappings = []

    package = importlib.import_module(MAPPINGS_PACKAGE)

    for _, module_name, _ in pkgutil.iter_modules(package.__path__):
        mod = importlib.import_module(f"{MAPPINGS_PACKAGE}.{module_name}")

        broker_name = getattr(mod, "BROKER_NAME", None)
        sector_map = None

        # convention: <BROKER>_SECTOR_MAP
        for attr in dir(mod):
            if attr.endswith("_SECTOR_MAP"):
                sector_map = getattr(mod, attr)

        if not broker_name or not sector_map:
            continue

        mappings.append(
            {
                "broker": broker_name,
                "map": sector_map,
                "module": module_name,
            }
        )

    return mappings


# ---------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------

def main():
    repo = SQLiteRepository(DB_PATH)

    broker_mappings = load_broker_mappings()

    if not broker_mappings:
        print("⚠️ No broker mappings found")
        return

    total_updated = 0
    total_skipped = 0

    for bm in broker_mappings:
        broker = bm["broker"]
        sector_map = bm["map"]

        print(f"\n🔎 Applying mappings for broker: {broker}")

        deals = repo.fetch_all(
            """
            SELECT deal_id,
                   sector_raw,
                   sector_source
            FROM deals
            WHERE source = ?
            """,
            (broker,),
        )

        updated = 0
        skipped = 0
        unmapped = set()

        for d in deals:
            deal_id = d["deal_id"]
            sector_raw = d["sector_raw"]
            sector_source = d["sector_source"]

            # 🔒 Never override manual classification
            if sector_source == "manual":
                skipped += 1
                continue

            if not sector_raw:
                skipped += 1
                continue

            mapping = sector_map.get(sector_raw)

            if not mapping:
                unmapped.add(sector_raw)
                skipped += 1
                continue

            repo.update_sector_inference(
                deal_id=deal_id,
                industry=mapping["industry"],
                sector=mapping["sector"],
                source="broker",
                reason=mapping["reason"],
                confidence=mapping["confidence"],
            )

            updated += 1

        print(f"✅ {broker}: updated={updated}, skipped={skipped}")

        if unmapped:
            print(f"⚠️ Unmapped {broker} sectors:")
            for s in sorted(unmapped):
                print(f"   - {s}")

        total_updated += updated
        total_skipped += skipped

    print(
        f"\n🏁 Broker mapping complete — "
        f"total updated={total_updated}, total skipped={total_skipped}"
    )


# ---------------------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------------------

if __name__ == "__main__":
    main()