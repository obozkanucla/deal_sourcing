from pathlib import Path
from src.persistence.repository import SQLiteRepository

repo = SQLiteRepository(Path("db/deals.sqlite"))

with repo.get_conn() as conn:
    # conn.execute("ALTER TABLE deals ADD COLUMN content_hash TEXT")
    # conn.execute("ALTER TABLE deals ADD COLUMN drive_folder_id TEXT")
    # conn.execute("ALTER TABLE deals ADD COLUMN detail_fetched_at DATETIME")
    conn.execute("ALTER TABLE deals ADD COLUMN pdf_drive_url TEXT")
    # count = conn.execute(
    #     "SELECT COUNT(*) FROM deals WHERE source = 'DealOpportunities'"
    # ).fetchone()[0]
    # print(f"Deleting {count} DealOpportunities deals")
    #
    # conn.execute("""
    #     DELETE FROM deal_artifacts
    #     WHERE deal_id IN (
    #         SELECT id FROM deals WHERE source = 'DealOpportunities'
    #     )
    # """)
    #
    # conn.execute("""
    #     DELETE FROM deals WHERE source = 'DealOpportunities'
    # """)

    conn.commit()