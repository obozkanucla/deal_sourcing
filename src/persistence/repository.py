import sqlite3
from pathlib import Path
from datetime import datetime, date
from typing import Optional

class SQLiteRepository:
    def __init__(self, db_path: Path):
        # ✅ force path relative to project root
        project_root = Path(__file__).resolve().parents[2]
        self.db_path = project_root / db_path
        print("📀 SQLite DB path:", self.db_path.resolve())
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

        self.DEALS_DB_COLUMNS = {
                                "source",
                                "source_listing_id",
                                "source_url",
                                "title",
                                "industry",
                                "sector",
                                "status",
                                "owner",
                                "priority",
                                "notes",
                                "last_touch",
                                "first_seen",
                                "last_seen",
                                "decision",
                                "decision_confidence",
                                "drive_folder_url",
                            }
    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            with open(Path(__file__).parent / "schema.sql") as f:
                conn.executescript(f.read())

    def fetch_all(self, sql: str, params=()):
        with self.get_conn() as conn:
            cur = conn.execute(sql, params)
            rows = cur.fetchall()
            return [dict(row) for row in rows]

    def execute(self, sql: str, params=()):
        with self.get_conn() as conn:
            conn.execute(sql, params)
            conn.commit()

    def get_conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row   # ✅ THIS IS THE KEY LINE
        return conn

    # ---------- DEALS ----------

    def deal_exists(self, source: str, listing_id: str) -> bool:
        with self.get_conn() as conn:
            cur = conn.execute(
                """
                SELECT 1 FROM deals
                WHERE source = ? AND source_listing_id = ?
                """,
                (source, listing_id),
            )
            return cur.fetchone() is not None

    def get_content_hash(self, source: str, listing_id: str):
        with self.get_conn() as conn:
            cur = conn.execute(
                """
                SELECT content_hash FROM deals
                WHERE source = ? AND source_listing_id = ?
                """,
                (source, listing_id),
            )
            row = cur.fetchone()
            return row[0] if row else None

    def upsert_deal(
        self,
        *,
        source,
        source_listing_id,
        source_url,
        content_hash,
        decision,
        decision_confidence,
        reasons,
        extracted_json,
        pdf_path
    ):
        now = datetime.utcnow().isoformat()
        today = date.today().isoformat()

        with self.get_conn() as conn:
            conn.execute(
                """
                INSERT INTO deals (
                    source,
                    source_listing_id,
                    source_url,
                    content_hash,
                    decision,
                    decision_confidence,
                    reasons,
                    extracted_json,
                    first_seen,
                    last_seen,
                    last_updated,
                    pdf_path
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source, source_listing_id) DO UPDATE SET
                    content_hash = excluded.content_hash,
                    decision = excluded.decision,
                    decision_confidence = excluded.decision_confidence,
                    reasons = excluded.reasons,
                    extracted_json = excluded.extracted_json,
                    last_seen = excluded.last_seen,
                    last_updated = excluded.last_updated,
                    pdf_path = excluded.pdf_path
                """,
                (
                    source,
                    source_listing_id,
                    source_url,
                    content_hash,
                    decision,
                    decision_confidence,
                    reasons,
                    extracted_json,
                    today,
                    today,
                    now,
                    pdf_path,
                ),
            )

    # ---------- RATE LIMITING ----------

    def get_clicks_used(self, broker: str, day: date) -> int:
        with self.get_conn() as conn:
            cur = conn.execute(
                """
                SELECT clicks_used FROM daily_clicks
                WHERE date = ? AND broker = ?
                """,
                (day.isoformat(), broker),
            )
            row = cur.fetchone()
            return row[0] if row else 0

    def increment_clicks(self, broker: str, day: date):
        with self.get_conn() as conn:
            conn.execute(
                """
                INSERT INTO daily_clicks (date, broker, clicks_used)
                VALUES (?, ?, 1)
                ON CONFLICT(date, broker)
                DO UPDATE SET clicks_used = clicks_used + 1
                """,
                (day.isoformat(), broker),
            )

    def upsert_index_only(
            self,
            *,
            source: str,
            source_listing_id: str,
            source_url: str,
            sector_raw: Optional[str] = None,
    ):
        now = datetime.utcnow().isoformat()
        today = date.today().isoformat()

        with self.get_conn() as conn:
            conn.execute(
                """
                INSERT INTO deals (source,
                                   source_listing_id,
                                   source_url,
                                   sector_raw,
                                   first_seen,
                                   last_seen,
                                   last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(source, source_listing_id)
                DO
                UPDATE SET
                    sector_raw = excluded.sector_raw,
                    last_seen = excluded.last_seen,
                    last_updated = excluded.last_updated
                """,
                (
                    source,
                    source_listing_id,
                    source_url,
                    sector_raw,
                    today,
                    today,
                    now,
                ),
            )

    def get_pending_index_records(self, source: str):
        """
        Return index-only deals that have not yet been processed
        (i.e. no content_hash yet).
        """
        with self.get_conn() as conn:
            cur = conn.execute(
                """
                SELECT source,
                       source_listing_id,
                       source_url
                FROM deals
                WHERE source = ?
                  AND content_hash IS NULL
                ORDER BY first_seen
                """,
                (source,),
            )

            return [
                {
                    "source": row[0],
                    "source_listing_id": row[1],
                    "source_url": row[2],
                }
                for row in cur.fetchall()
            ]

    def fetch_all_deals(self):
        conn = self.get_conn()
        conn.row_factory = sqlite3.Row

        rows = conn.execute(
            """
            SELECT             
                id,
                source,
                source_listing_id,
                source_url,
                title,
                industry,
                sector,
                status,
                owner,
                priority,
                notes,
                last_touch,
                first_seen,
                last_seen,
                last_updated,
                decision,
                decision_confidence,
                drive_folder_url,
                incorporation_year,
                -- base financials
                revenue_k,
                ebitda_k,
                asking_price_k,
    
                -- derived financials
                ebitda_margin,
                revenue_multiple,
                ebitda_multiple
            FROM deals
            ORDER BY first_seen ASC
            """
        ).fetchall()

        conn.close()
        return [dict(row) for row in rows]

    def update_human_fields(
            self,
            deal_id: str,
            status: str = None,
            owner: str = None,
            priority: str = None,
            notes: str = None,
            last_touch: str = None,
            manual_decision: str = None,
    ):
        with self.get_conn() as conn:
            conn.execute(
                """
                UPDATE deals
                SET status          = COALESCE(?, status),
                    owner           = COALESCE(?, owner),
                    priority        = COALESCE(?, priority),
                    notes           = COALESCE(?, notes),
                    last_touch      = COALESCE(?, last_touch),
                    manual_decision = COALESCE(?, manual_decision),
                    last_updated    = CURRENT_TIMESTAMP
                WHERE deal_id = ?
                """,
                (
                    status,
                    owner,
                    priority,
                    notes,
                    last_touch,
                    manual_decision,
                    deal_id,
                ),
            )

    def update_deal_fields(self, deal_id: int, updates: dict):
        safe_updates = {
            k: v
            for k, v in updates.items()
            if k in self.DEALS_DB_COLUMNS
        }

        if not safe_updates:
            return

        cols = ", ".join(f"{k} = ?" for k in safe_updates)
        values = list(safe_updates.values()) + [deal_id]

        sql = f"""
            UPDATE deals
            SET {cols},
                last_updated = CURRENT_TIMESTAMP
            WHERE id = ?
        """

        with self.get_conn() as conn:
            conn.execute(sql, values)
            conn.commit()

    def update_sector_inference(
            self,
            deal_id,
            industry,
            sector,
            source,
            reason,
            confidence,
    ):
        with self.get_conn() as conn:
            conn.execute(
                """
                UPDATE deals
                SET industry                    = ?,
                    sector                      = ?,
                    sector_source               = ?,
                    sector_inference_reason     = ?,
                    sector_inference_confidence = ?
                WHERE deal_id = ?
                """,
                (industry, sector, source, reason, confidence, deal_id),
            )

    def fetch_by_deal_id(self, deal_id: str):
        with self.get_conn() as conn:
            cur = conn.execute(
                "SELECT * FROM deals WHERE deal_id = ?",
                (deal_id,)
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def update_pdf_info(self, deal_id: str, pdf_path: str, pdf_drive_url: str):
        with self.get_conn() as conn:
            conn.execute(
                """
                UPDATE deals
                SET pdf_path      = ?,
                    pdf_drive_url = ?,
                    last_updated  = CURRENT_TIMESTAMP
                WHERE deal_id = ?
                """,
                (pdf_path, pdf_drive_url, deal_id),
            )

    def fetch_deals_needing_details(self, *, source: str, limit: int):
        return self.fetch_all(
            """
            SELECT *
            FROM deals
            WHERE source = ?
              AND (
                content_hash IS NULL
                    OR needs_detail_refresh = 1
                )
            ORDER BY last_seen DESC LIMIT ?
            """,
            (source, limit),
        )

    def fetch_deals_needing_details_for_source(self, source: str, limit: int):
        sql = """
              SELECT *
              FROM deals
              WHERE source = ?
                AND (
                  content_hash IS NULL
                      OR pdf_path IS NULL
                      OR needs_detail_refresh = 1
                  )
              ORDER BY last_seen DESC \
              """

        params = [source]

        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)

        return self.fetch_all(sql, tuple(params))

    def mark_detail_checked(self, deal_id: str, reason: str):
        with self.get_conn() as conn:
            conn.execute(
                """
                UPDATE deals
                SET detail_fetched_at    = CURRENT_TIMESTAMP,
                    detail_fetch_reason  = ?,
                    needs_detail_refresh = 0
                WHERE deal_id = ?
                """,
                (reason, deal_id),
            )

    def update_detail_fields(self, deal_id: str, fields: dict):
        if not fields:
            return

        fields["detail_fetched_at"] = datetime.utcnow().isoformat()
        fields["needs_detail_refresh"] = 0

        assignments = ", ".join(f"{k} = ?" for k in fields.keys())
        values = list(fields.values()) + [deal_id]

        sql = f"""
            UPDATE deals
            SET {assignments}
            WHERE deal_id = ?
        """

        with self.get_conn() as conn:
            conn.execute(sql, values)
            conn.commit()

    def upsert_legacy_deal(self, deal: dict):
        with self.get_conn() as conn:
            conn.execute(
                """
                INSERT INTO deals (
                    deal_id,
                    source,
                    source_url,
                    source_listing_id,
                    intermediary,
                    title,
                    industry,
                    sector,
                    sector_source,
                    location,
                    incorporation_year,
                    first_seen,
                    last_updated,
                    decision,
                    decision_reason,
                    notes,
                    revenue_k,
                    ebitda_k,
                    asking_price_k,
                    drive_folder_url
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(deal_id) DO UPDATE SET
                    intermediary = excluded.intermediary,
                    title = excluded.title,
                    industry = excluded.industry,
                    sector = excluded.sector,
                    sector_source = excluded.sector_source,
                    location = excluded.location,
                    incorporation_year = excluded.incorporation_year,
                    last_updated = excluded.last_updated,
                    decision = excluded.decision,
                    decision_reason = excluded.decision_reason,
                    notes = excluded.notes,
                    revenue_k = excluded.revenue_k,
                    ebitda_k = excluded.ebitda_k,
                    asking_price_k = excluded.asking_price_k,
                    drive_folder_url = excluded.drive_folder_url;
                """,
                (
                    deal["deal_id"],  # deal_id
                    deal["source"],  # source ("LegacySheet")
                    f"legacy://{deal['deal_id']}",  # source_url ✅ REQUIRED
                    deal["deal_id"],  # source_listing_id ✅ REQUIRED
                    deal["intermediary"],
                    deal["title"],
                    deal["industry"],
                    deal["sector"],
                    deal["sector_source"],  # "manual"
                    deal["location"],
                    deal["incorporation_year"],
                    deal["first_seen"],
                    deal["last_updated"],
                    deal["outcome"],
                    deal["outcome_reason"],
                    deal["notes"],
                    deal["revenue_k"],
                    deal["ebitda_k"],
                    deal["asking_price_k"],
                    deal["drive_folder_url"],
                ),
            )

    def upsert_intermediary(self, rec: dict):
        with self.get_conn() as conn:
            conn.execute(
                """
                INSERT INTO intermediaries (intermediary_id,
                                            name,
                                            website,
                                            last_checked,
                                            existing_relationship,
                                            relationship_owner,
                                            active,
                                            sector_focus,
                                            geography,
                                            category,
                                            notes,
                                            description,
                                            updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP) ON CONFLICT(intermediary_id) DO
                UPDATE SET
                    name = excluded.name,
                    website = excluded.website,
                    last_checked = excluded.last_checked,
                    existing_relationship = excluded.existing_relationship,
                    relationship_owner = excluded.relationship_owner,
                    active = excluded.active,
                    sector_focus = excluded.sector_focus,
                    geography = excluded.geography,
                    category = excluded.category,
                    notes = excluded.notes,
                    description = excluded.description,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    rec["intermediary_id"],
                    rec["name"],
                    rec["website"],
                    rec["last_checked"],
                    rec["existing_relationship"],
                    rec["relationship_owner"],
                    rec["active"],
                    rec["sector_focus"],
                    rec["geography"],
                    rec["category"],
                    rec["notes"],
                    rec["description"],
                ),
            )

    def insert_raw_deal(self, deal: dict):
        """
        Insert or update a raw deal (e.g. Dmitry sheets).
        Deduplicates on (source, source_listing_id).
        Preserves first_seen.
        """

        source_url = f"internal://{deal['source']}/{deal['source_listing_id']}"
        now = datetime.utcnow().isoformat()

        with self.get_conn() as conn:
            conn.execute(
                """
                INSERT INTO deals (
                    deal_id,
                    source,
                    source_url,
                    source_listing_id,
                    identity_method,
                    content_hash,
                    description,
                    sector_raw,
                    industry_raw,
                    location,
                    revenue_k,
                    ebitda_k,
                    asking_price_k,
                    notes,
                    first_seen,
                    last_seen,
                    manual_imported_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)

                ON CONFLICT(source, source_listing_id)
                DO UPDATE SET
                    last_seen = excluded.last_seen,
                    manual_imported_at = excluded.manual_imported_at
                """,
                (
                    deal["deal_id"],
                    deal["source"],                     # "Dmitry"
                    source_url,                          # synthetic URL
                    deal["source_listing_id"],           # Aug25-D / Oct25-D
                    deal["identity_method"],
                    deal["content_hash"],
                    deal["description"],
                    deal["sector_raw"],
                    deal["industry_raw"],
                    deal["location"],
                    deal["revenue_k"],
                    deal["ebitda_k"],
                    deal["asking_price_k"],
                    deal["notes"],
                    deal["first_seen"],                  # only used on INSERT
                    deal["last_seen"],                   # updated on re-seen
                    now,
                ),
            )

    def update_dmitry_seen(self, deal_id: str, seen_date: str):
        """
        Update last_seen for an existing Dmitry deal.
        first_seen must remain unchanged.
        """
        with self.get_conn() as conn:
            conn.execute(
                """
                UPDATE deals
                SET last_seen = ?,
                    last_updated = CURRENT_TIMESTAMP
                WHERE deal_id = ?
                """,
                (seen_date, deal_id),
            )
            conn.commit()

    def upsert_dmitry_seen(self, *, deal_id: str, first_seen: str, last_seen: str):
        with self.get_conn() as conn:
            conn.execute(
                """
                UPDATE deals
                SET first_seen   = ?,
                    last_seen    = ?,
                    last_updated = CURRENT_TIMESTAMP
                WHERE deal_id = ?
                """,
                (first_seen, last_seen, deal_id),
            )

    def update_dmitry_enrichment(
            self,
            *,
            deal_id: str,
            revenue_k: float | None,
            ebitda_k: float | None,
            decision: str | None,
            notes: str | None,
    ):
        with self.get_conn() as conn:
            conn.execute(
                """
                UPDATE deals
                SET revenue_k    = COALESCE(deals.revenue_k, ?),
                    ebitda_k     = COALESCE(deals.ebitda_k, ?),
                    decision     = COALESCE(deals.decision, ?),
                    notes        = COALESCE(deals.notes, ?),
                    last_updated = CURRENT_TIMESTAMP
                WHERE deal_id = ?
                """,
                (revenue_k, ebitda_k, decision, notes, deal_id),
            )

    def update_detail_fields_by_source(
            self,
            *,
            source: str,
            source_listing_id: str,
            fields: dict,
    ):
        if not fields:
            return

        assignments = ", ".join(f"{k} = ?" for k in fields.keys())
        values = list(fields.values()) + [source, source_listing_id]

        sql = f"""
            UPDATE deals
            SET {assignments},
                last_updated = CURRENT_TIMESTAMP
            WHERE source = ?
              AND source_listing_id = ?
        """

        with self.get_conn() as conn:
            conn.execute(sql, values)
            conn.commit()

    def fetch_by_source_and_listing(self, source: str, source_listing_id: str) -> dict | None:
        with self.get_conn() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM deals
                WHERE source = ?
                  AND source_listing_id = ?
                """,
                (source, source_listing_id),
            ).fetchone()

        return dict(row) if row else None

    def compute_deal_uid(self, deal: dict) -> str:
        return f"{deal['source']}:{deal['source_listing_id']}"

    def recalculate_financial_metrics(self):
        """
        Recalculate derived financial metrics from canonical stored values.
        Safe to run repeatedly.
        """
        with self.get_conn() as conn:
            conn.execute(
                """
                UPDATE deals
                SET ebitda_margin = CASE
                                        WHEN revenue_k IS NOT NULL
                                            AND revenue_k != 0
                    AND ebitda_k IS NOT NULL THEN ROUND((ebitda_k * 100.0) / revenue_k, 2)
                        ELSE NULL
                END
                ,

                    revenue_multiple = CASE
                        WHEN revenue_k IS NOT NULL
                         AND revenue_k != 0
                         AND asking_price_k IS NOT NULL
                        THEN ROUND(asking_price_k / revenue_k, 2)
                        ELSE NULL
                END
                ,

                    ebitda_multiple = CASE
                        WHEN ebitda_k IS NOT NULL
                         AND ebitda_k != 0
                         AND asking_price_k IS NOT NULL
                        THEN ROUND(asking_price_k / ebitda_k, 2)
                        ELSE NULL
                END
                """
            )
            conn.commit()