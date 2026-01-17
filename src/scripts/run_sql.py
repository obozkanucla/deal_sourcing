from pathlib import Path
from src.persistence.repository import SQLiteRepository

repo = SQLiteRepository(Path("db/deals.sqlite"))
def column_exists(conn, table, column):
    return any(
        row[1] == column
        for row in conn.execute(f"PRAGMA table_info({table})")
    )

with repo.get_conn() as conn:
    conn.execute("""SELECT
                      id,
                      source_listing_id,
                      status,
                      detail_fetch_reason,
                      detail_fetched_at
                    FROM deals
                    WHERE source = 'BusinessesForSale'
                      AND status = 'Lost'
                      AND detail_fetch_reason = 'listing_unavailable';""")
    conn.execute("""UPDATE deals
                    SET
                      status               = NULL,
                      detail_fetch_reason  = 'reverted_false_lost',
                      needs_detail_refresh = 1,
                      last_updated         = CURRENT_TIMESTAMP,
                      last_updated_source  = 'MANUAL_FIX'
                    WHERE source = 'BusinessesForSale'
                      AND status = 'Lost'
                      AND detail_fetch_reason = 'listing_unavailable';
    """)
    # conn.execute("""DELETE FROM deals
    #                 WHERE source = 'transworld_uk'
    #                   AND source_url IN (
    #                       SELECT source_url
    #                       FROM deals
    #                       WHERE source = 'transworld_uk'
    #                       GROUP BY source_url
    #                       HAVING COUNT(*) > 1
    #                   )
    #                   AND source_listing_id NOT LIKE 'TW-%';
    #              """)
    # conn.execute("""CREATE UNIQUE INDEX IF NOT EXISTS
    #                 ux_tw_source_url
    #                 ON deals(source, source_url)
    #                 WHERE source = 'transworld_uk';""")
    # conn.execute("""CREATE UNIQUE INDEX IF NOT EXISTS
    #                 ux_bb_source_url
    #                 ON deals(source, source_url)
    #                 WHERE source = 'BusinessBuyers';
    #                 """)
    # conn.execute("""CREATE UNIQUE INDEX IF NOT EXISTS
    #                 ux_b4s_source_url
    #                 ON deals(source, source_url)
    #                 WHERE source = 'BusinessesForSale';
    #              """)
    # conn.execute("""DELETE FROM deals
    #                 WHERE source = 'BusinessBuyers'
    #                   AND pdf_drive_url IS NULL
    #                   AND description IS NULL
    #                   AND industry IS NULL;
    #             """)
    # if not column_exists(conn, "deals", "added_at"):
    #     conn.execute("""ALTER TABLE deals
    #                     ADD COLUMN added_at DATE;
    #                     """)
    # conn.execute("""UPDATE deals
    #                 SET drive_folder_url = 'https://drive.google.com/drive/folders/' || drive_folder_id
    #                 WHERE source = 'transworld_uk'
    #                   AND drive_folder_id IS NOT NULL
    #                   AND drive_folder_url IS NULL;""")
    # conn.execute("""DELETE FROM deals
    #                 WHERE source = 'transworld_uk'
    #                 AND source_listing_id NOT LIKE 'TW-%'
    #                 AND EXISTS (
    #                 SELECT 1
    #                 FROM deals d2
    #                 WHERE d2.source = 'transworld_uk'
    #                 AND d2.source_listing_id LIKE 'TW-%'
    #                 AND d2.source_url = deals.source_url
    #                 );""")
    # conn.execute("""UPDATE deals
    #                 SET source_listing_id = 'TW-SOLD-' || source_listing_id
    #                 WHERE source = 'transworld_uk'
    #                   AND status = 'Lost'
    #                   AND source_listing_id NOT LIKE 'TW-%';""")
    # conn.execute("""UPDATE deals
    #                 SET
    #                     industry = NULL,
    #                     sector = NULL,
    #                     sector_source = NULL,
    #                     sector_inference_confidence = NULL,
    #                     sector_inference_reason = NULL,
    #                     drive_folder_id = NULL,
    #                     drive_folder_url = NULL,
    #                     pdf_drive_url = NULL,
    #                     needs_detail_refresh = 1,
    #                     detail_fetched_at = NULL,
    #                     detail_fetch_reason = NULL
    #                 WHERE source = 'Knightsbridge'
    #                   AND drive_folder_id IS NULL
    #                   AND status IS NULL;
    #                 """)
    # conn.execute("""DELETE FROM deal_artifacts WHERE deal_id IS NULL;""")
    # conn.execute("""DELETE FROM deal_artifacts
    #                 WHERE deal_id NOT IN (SELECT id FROM deals);""")
    # conn.execute("""DELETE FROM deals
    #                 WHERE source = 'Knightsbridge';""")
    # conn.execute("""DELETE FROM deal_artifacts
    #                 WHERE deal_id NOT IN (SELECT id FROM deals);""")

    # conn.execute("""UPDATE deals
    #                 SET first_seen = '2026-01-12 09:00:00'
    #                 WHERE source = 'HiltonSmythe';""")
    # conn.execute("""UPDATE deals
    #                 SET
    #                     detail_fetch_reason  = NULL,
    #                     last_updated         = CURRENT_TIMESTAMP,
    #                     last_updated_source  = 'AUTO'
    #                 WHERE source = 'Knightsbridge'
    #                   AND detail_fetch_reason = 'cannot access local variable ''e'' where it is not associated with a value'
    #                   AND industry IS NOT NULL
    #                   AND sector IS NOT NULL
    #                   AND needs_detail_refresh = 0;
    #              """)
    #
    # conn.execute("""UPDATE deals
    #                 SET
    #                     industry                    = 'Healthcare',
    #                     sector                      = 'Medical Devices / Equipment',
    #                     sector_source               = 'manual',
    #                     sector_inference_confidence = 1.0,
    #                     sector_inference_reason     = 'manual_final_resolution',
    #
    #                     detail_fetched_at           = CURRENT_TIMESTAMP,
    #                     needs_detail_refresh        = 0,
    #                     detail_fetch_reason         = NULL,
    #
    #                     last_updated                = CURRENT_TIMESTAMP,
    #                     last_updated_source         = 'AUTO'
    #                 WHERE source = 'Knightsbridge'
    #                   AND source_listing_id = '165799';
    #              """)
    #
    # conn.execute("""UPDATE deals
    #                 SET
    #                     industry                    = 'Business_Services',
    #                     sector                      = 'Facilities Management',
    #                     sector_source               = 'manual',
    #                     sector_inference_confidence = 1.0,
    #                     sector_inference_reason     = 'manual_final_resolution',
    #
    #                     detail_fetched_at           = CURRENT_TIMESTAMP,
    #                     needs_detail_refresh        = 0,
    #                     detail_fetch_reason         = NULL,
    #
    #                     last_updated                = CURRENT_TIMESTAMP,
    #                     last_updated_source         = 'AUTO'
    #                 WHERE source = 'Knightsbridge'
    #                   AND source_listing_id = '166105';
    #              """)
    #
    # conn.execute("""UPDATE deals
    #                 SET
    #                     detail_fetch_reason = NULL,
    #                     last_updated = CURRENT_TIMESTAMP,
    #                     last_updated_source = 'AUTO'
    #                 WHERE source = 'Knightsbridge'
    #                   AND detail_fetch_reason = 'MISSING_SECTOR_CANONICAL'
    #                   AND industry IS NOT NULL
    #                   AND sector IS NOT NULL;
    #              """)
    #
    # conn.execute("""UPDATE deals
    #                 SET
    #                     detail_fetched_at    = CURRENT_TIMESTAMP,
    #                     needs_detail_refresh = 0,
    #                     detail_fetch_reason  = 'REQUIRES_MANUAL_SECTOR_ASSIGNMENT',
    #                     last_updated         = CURRENT_TIMESTAMP,
    #                     last_updated_source  = 'AUTO'
    #                 WHERE source = 'Knightsbridge'
    #                   AND detail_fetch_reason = 'MISSING_SECTOR_CANONICAL'
    #                   AND detail_fetched_at IS NULL;
    #              """)

    # conn.execute("""ALTER TABLE deals ADD COLUMN lost_reason TEXT;""")
    # conn.execute(""" ALTER TABLE deals
    #     ADD COLUMN detail_fetch_reason TEXT;""")
    # conn.execute("""
    #                 UPDATE deals
    #                 SET last_updated_source = 'AUTO'
    #                 WHERE source = 'AxisPartnership'
    #                   AND detail_fetched_at IS NOT NULL
    #                   AND last_updated_source IS NULL;
    #             """)
    # conn.execute("""UPDATE deals
    #                 SET last_updated_source = 'AUTO'
    #                 WHERE source = 'BusinessesForSale'
    #                   AND detail_fetched_at IS NOT NULL
    #                   AND last_updated_source IS NULL;
    #             """)
    # conn.execute("""DROP TABLE IF EXISTS deal_artifacts;""")
    # conn.execute("""CREATE TABLE deal_artifacts (
    #                 id INTEGER PRIMARY KEY AUTOINCREMENT,
    #
    #                 -- canonical broker identity (AUTHORITATIVE)
    #                 source               TEXT NOT NULL,
    #                 source_listing_id    TEXT NOT NULL,
    #
    #                 -- optional convenience pointer (NON-AUTHORITATIVE)
    #                 deal_id              INTEGER NULL,
    #
    #                 -- artifact identity
    #                 artifact_type        TEXT NOT NULL,     -- pdf, html, snapshot
    #                 artifact_name        TEXT NOT NULL,
    #                 artifact_hash        TEXT NOT NULL,
    #
    #                 -- storage
    #                 drive_file_id        TEXT NOT NULL,
    #                 drive_url            TEXT NOT NULL,
    #
    #                 -- provenance
    #                 created_at           DATETIME NOT NULL,
    #                 created_by           TEXT NOT NULL,
    #                 extraction_version   TEXT NULL,
    #
    #                 -- safety
    #                 UNIQUE (
    #                     source,
    #                     source_listing_id,
    #                     artifact_type,
    #                     artifact_hash
    #                 )
    #             );""")
    # conn.execute("""
    #              CREATE INDEX idx_artifacts_lookup
    #                  ON deal_artifacts(source, source_listing_id);
    #     """)
    # conn.execute("""
    #              CREATE UNIQUE INDEX IF NOT EXISTS uniq_artifact_hash
    #                  ON deal_artifacts(artifact_hash);
    #              """)
    # conn.execute(f"-- ALTER TABLE deals ADD COLUMN needs_detail_refresh INTEGER DEFAULT 1;")
    # conn.execute(f"ALTER TABLE deals DROP COLUMN decision_reason;")
    # if not column_exists(conn, "deals", "revenue_k_effective"):
    #     conn.execute ("ALTER TABLE deals ADD COLUMN revenue_k_effective REAL;")
    # if not column_exists(conn, "deals", "ebitda_k_effective"):
    #     conn.execute ("ALTER TABLE deals ADD COLUMN ebitda_k_effective REAL;")
    # if not column_exists(conn, "deals", "asking_price_k_effective"):
    #     conn.execute ("ALTER TABLE deals ADD COLUMN asking_price_k_effective REAL;")
    # if not column_exists(conn, "deals", "revenue_k_manual"):
    #     conn.execute ("ALTER TABLE deals ADD COLUMN revenue_k_manual REAL;")
    # if not column_exists(conn, "deals", "ebitda_k_manual"):
    #     conn.execute ("ALTER TABLE deals ADD COLUMN ebitda_k_manual REAL;")
    # if not column_exists(conn, "deals", "asking_price_k_manual"):
    #     conn.execute ("ALTER TABLE deals ADD COLUMN asking_price_k_manual REAL;")

    # conn.execute("""
    # UPDATE deals
    #     SET
    #         industry = NULL,
    #         sector = NULL,
    #         title = NULL,
    #         description = NULL,
    #         location_raw = NULL,
    #         location = NULL,
    #         drive_folder_id = NULL,
    #         drive_folder_url = NULL,
    #         pdf_drive_url = NULL,
    #         pdf_generated_at = NULL,
    #         detail_fetched_at = NULL,
    #         last_updated = CURRENT_TIMESTAMP,
    #         last_updated_source = 'AUTO'
    #     WHERE source = 'DealOpportunities'
    #       AND source_listing_id IN (
    #         'S12257','S12261','S12267','S12268','S12272','S12275',
    #         'S12279','S12282','S12285','S12287','S12288','S12291',
    #         'S12300','S12313','S12315','S12316','S12319','S12320'
    #         )
    #              """)
    # conn.execute(
    #     """
    #     CREATE TABLE IF NOT EXISTS pipeline_snapshots (
    #         id INTEGER PRIMARY KEY AUTOINCREMENT,
    #         snapshot_year INTEGER NOT NULL,
    #         snapshot_week INTEGER NOT NULL,
    #         snapshot_key TEXT NOT NULL,
    #         industry TEXT NOT NULL,
    #         status TEXT NOT NULL,
    #         source TEXT NOT NULL,
    #         deal_count INTEGER NOT NULL,
    #         snapshot_run_date DATE NOT NULL
    #     )
    #     """
    # )
    # if not column_exists(conn, "deals", "pass_reason"):
    #     conn.execute ("ALTER TABLE deals ADD COLUMN pass_reason TEXT;")

    # if not column_exists(conn, "deals", "extracted_json"):
    #     conn.execute("ALTER TABLE deals ADD COLUMN extracted_json TEXT")
    #
    # if not column_exists(conn, "deals", "sector_source"):
    #     conn.execute("ALTER TABLE deals ADD COLUMN sector_source TEXT")
    #
    # if not column_exists(conn, "deals", "sector_inference_confidence"):
    #     conn.execute("ALTER TABLE deals ADD COLUMN sector_inference_confidence REAL")
    #
    # if not column_exists(conn, "deals", "sector_inference_reason"):
    #     conn.execute("ALTER TABLE deals ADD COLUMN sector_inference_reason REAL")
    #
    # if not column_exists(conn, "deals", "turnover_range_raw"):
    #     conn.execute("ALTER TABLE deals ADD COLUMN turnover_range_raw TEXT")
    #
    # if not column_exists(conn, "deals", "sector_raw"):
    #     conn.execute("ALTER TABLE deals ADD COLUMN sector_raw TEXT")
    #
    # if not column_exists(conn, "deals", "location_raw"):
    #     conn.execute("ALTER TABLE deals ADD COLUMN location_raw TEXT")
    #
    # if not column_exists(conn, "deals", "content_hash"):
    #     conn.execute("ALTER TABLE deals ADD COLUMN content_hash TEXT")
    #
    # if not column_exists(conn, "deals", "drive_folder_id"):
    #     conn.execute("ALTER TABLE deals ADD COLUMN drive_folder_id TEXT")
    #
    # if not column_exists(conn, "deals", "detail_fetched_at"):
    #     conn.execute("ALTER TABLE deals ADD COLUMN detail_fetched_at DATETIME")
    #
    # if not column_exists(conn, "deals", "pdf_drive_url"):
    #     conn.execute("ALTER TABLE deals ADD COLUMN pdf_drive_url TEXT")
    #
    # if not column_exists(conn, "deals", "pdf_generated_at"):
    #     conn.execute("ALTER TABLE deals ADD COLUMN pdf_generated_at DATETIME")
    #
    # if not column_exists(conn, "deals", "pdf_error"):
    #     conn.execute("ALTER TABLE deals ADD COLUMN pdf_error TEXT")
    conn.commit()

# with repo.get_conn() as conn:
#     conn.execute("""
#         DELETE FROM deal_artifacts
#         WHERE deal_id IN (
#             SELECT id FROM deals WHERE source = 'DealOpportunities'
#         )
#     """)
#
#     conn.execute("""
#         DELETE FROM deal_metrics
#         WHERE deal_id IN (
#             SELECT id FROM deals WHERE source = 'DealOpportunities'
#         )
#     """)
#
#     conn.execute("""
#         DELETE FROM deals WHERE source = 'DealOpportunities'
#     """)
#
#     conn.commit()