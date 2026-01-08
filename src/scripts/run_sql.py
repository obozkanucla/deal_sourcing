from pathlib import Path
from src.persistence.repository import SQLiteRepository

repo = SQLiteRepository(Path("db/deals.sqlite"))
def column_exists(conn, table, column):
    return any(
        row[1] == column
        for row in conn.execute(f"PRAGMA table_info({table})")
    )

# with repo.get_conn() as conn:
#     if not column_exists(conn, "deals", "turnover_range_raw"):
#         conn.execute("ALTER TABLE deals ADD COLUMN turnover_range_raw TEXT")
#
#     if not column_exists(conn, "deals", "sector_raw"):
#         conn.execute("ALTER TABLE deals ADD COLUMN sector_raw TEXT")
#
#     if not column_exists(conn, "deals", "location_raw"):
#         conn.execute("ALTER TABLE deals ADD COLUMN location_raw TEXT")
#
#     if not column_exists(conn, "deals", "content_hash"):
#         conn.execute("ALTER TABLE deals ADD COLUMN content_hash TEXT")
#
#     if not column_exists(conn, "deals", "drive_folder_id"):
#         conn.execute("ALTER TABLE deals ADD COLUMN drive_folder_id TEXT")
#
#     if not column_exists(conn, "deals", "detail_fetched_at"):
#         conn.execute("ALTER TABLE deals ADD COLUMN detail_fetched_at DATETIME")
#
#     if not column_exists(conn, "deals", "pdf_drive_url"):
#         conn.execute("ALTER TABLE deals ADD COLUMN pdf_drive_url TEXT")
#
#     if not column_exists(conn, "deals", "pdf_generated_at"):
#         conn.execute("ALTER TABLE deals ADD COLUMN pdf_generated_at DATETIME")
#
#     if not column_exists(conn, "deals", "pdf_error"):
#         conn.execute("ALTER TABLE deals ADD COLUMN pdf_error TEXT")
#
#     conn.commit()

with repo.get_conn() as conn:
    conn.execute("""
        DELETE FROM deal_artifacts
        WHERE deal_id IN (
            SELECT id FROM deals WHERE source = 'DealOpportunities'
        )
    """)

    conn.execute("""
        DELETE FROM deal_metrics
        WHERE deal_id IN (
            SELECT id FROM deals WHERE source = 'DealOpportunities'
        )
    """)

    conn.execute("""
        DELETE FROM deals WHERE source = 'DealOpportunities'
    """)

    conn.commit()