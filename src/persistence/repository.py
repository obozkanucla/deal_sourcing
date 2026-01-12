import sqlite3
from pathlib import Path
from datetime import datetime, date
from typing import Optional
from src.domain.deal_columns import DEAL_COLUMNS, sqlite_select_columns

class SQLiteRepository:
    def __init__(self, db_path: Path):
        # ✅ force path relative to project root
        project_root = Path(__file__).resolve().parents[2]
        self.db_path = project_root / db_path
        print("📀 SQLite DB path:", self.db_path.resolve())
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
        self.DEALS_DB_COLUMNS = {
            c.name
            for c in DEAL_COLUMNS
            if c.pull and not c.system
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
            source_url: str | None = None,
            sector_raw: str | None = None,
            location_raw: str | None = None,
            turnover_range_raw: str | None = None,
            first_seen: str | None = None,
            last_seen: str | None = None,
            last_updated: str | None = None,
            last_updated_source: str | None = None,
    ):
        now = datetime.utcnow().isoformat(timespec="seconds")

        with self.get_conn() as conn:
            if source == "BusinessesForSale":
                # 1️⃣ insert if new (guarded by uniq_b4s_source_url)
                conn.execute(
                    """
                    INSERT
                    OR IGNORE INTO deals (
                        source,
                        source_listing_id,
                        source_url,
                        sector_raw,
                        location_raw,
                        turnover_range_raw,
                        first_seen,
                        last_seen,
                        last_updated,
                        last_updated_source
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        source,
                        source_listing_id,
                        source_url,
                        sector_raw,
                        location_raw,
                        turnover_range_raw,
                        first_seen,
                        last_seen,
                        last_updated or now,
                        last_updated_source or "AUTO",
                    ),
                )

                # 2️⃣ always update the existing row
                conn.execute(
                    """
                    UPDATE deals
                    SET source_listing_id   = ?,
                        sector_raw          = COALESCE(?, sector_raw),
                        location_raw        = COALESCE(?, location_raw),
                        turnover_range_raw  = COALESCE(?, turnover_range_raw),
                        first_seen          = COALESCE(first_seen, ?),
                        last_seen           = ?,
                        last_updated        = ?,
                        last_updated_source = ?
                    WHERE source = 'BusinessesForSale'
                      AND source_url = ?
                    """,
                    (
                        source_listing_id,
                        sector_raw,
                        location_raw,
                        turnover_range_raw,
                        first_seen,
                        last_seen,
                        last_updated or now,
                        last_updated_source or "AUTO",
                        source_url,
                    ),
                )

            else:
                # keep existing ON CONFLICT logic for other brokers
                conn.execute(
                    """
                    INSERT INTO deals (...)
                    VALUES (...) ON CONFLICT(source, source_listing_id) DO
                    UPDATE...
                    """
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
        cols = sqlite_select_columns()
        print(cols)
        print(", ".join(cols))
        sql = f"""
            SELECT {", ".join(cols)}
            FROM deals
            ORDER BY first_seen ASC
        """

        with self.get_conn() as conn:
            rows = conn.execute(sql).fetchall()

        out = []
        for row in rows:
            d = dict(row)
            d["deal_uid"] = f"{d['source']}:{d['source_listing_id']}"
            out.append(d)

        return out

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

    def update_deal_fields(
            self,
            *,
            source: str,
            source_listing_id: str,
            updates: dict,
    ):
        safe_updates = {
            k: v for k, v in updates.items()
            if k in self.DEALS_DB_COLUMNS
        }
        if not safe_updates:
            return

        cols = ", ".join(f"{k} = ?" for k in safe_updates)
        values = list(safe_updates.values()) + [source, source_listing_id]

        sql = f"""
            UPDATE deals
            SET {cols},
                last_updated = CURRENT_TIMESTAMP,
                last_updated_source = 'MANUAL'
            WHERE source = ?
              AND source_listing_id = ?
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
            cur = conn.execute(
                """
                UPDATE deals
                SET industry                    = ?,
                    sector                      = ?,
                    sector_source               = ?,
                    sector_inference_reason     = ?,
                    sector_inference_confidence = ?,
                    last_updated                = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (industry, sector, source, reason, confidence, deal_id),
            )

            if cur.rowcount != 1:
                raise RuntimeError(
                    f"update_sector_inference affected {cur.rowcount} rows "
                    f"(expected 1, id={deal_id})"
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
                INSERT INTO deals (source,
                                   source_listing_id,
                                   source_url,
                                   title,
                                   industry,
                                   sector,
                                   location,
                                   incorporation_year,
                                   first_seen,
                                   last_updated,
                                   last_updated_source,
                                   status,
                                   decision,
                                   notes,
                                   revenue_k,
                                   ebitda_k,
                                   asking_price_k,
                                   drive_folder_url)
                VALUES (:source,
                        :source_listing_id,
                        :source_url,
                        :title,
                        :industry,
                        :sector,
                        :location,
                        :incorporation_year,
                        :first_seen,
                        :last_updated,
                        'MANUAL',
                        :status,
                        :decision,
                        :notes,
                        :revenue_k,
                        :ebitda_k,
                        :asking_price_k,
                        :drive_folder_url) ON CONFLICT (source, source_listing_id) DO
                UPDATE SET
                    title = excluded.title,
                    industry = excluded.industry,
                    sector = excluded.sector,
                    location = excluded.location,
                    incorporation_year = excluded.incorporation_year,

                    last_updated = excluded.last_updated,
                    last_updated_source = 'MANUAL',

                    status = excluded.status,
                    decision = excluded.decision,
                    notes = excluded.notes,

                    revenue_k = excluded.revenue_k,
                    ebitda_k = excluded.ebitda_k,
                    asking_price_k = excluded.asking_price_k,
                    drive_folder_url = excluded.drive_folder_url
                ;
                """,
                deal,
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
                    title,
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
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)

                ON CONFLICT(source, source_listing_id)
                DO UPDATE SET
                    last_seen = excluded.last_seen,
                    manual_imported_at = excluded.manual_imported_at
                """,
                (
                    deal["deal_id"],
                    deal["source"],                     # "Dmitry"
                    source_url,                         # synthetic URL
                    deal["source_listing_id"],          # Aug25-D / Oct25-D
                    deal["title"],
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
        Recalculate derived financial metrics from effective values.
        Safe to run repeatedly.
        """
        with self.get_conn() as conn:
            conn.execute(
                """
                UPDATE deals
                    SET
                        ebitda_margin = CASE
                            WHEN revenue_k_effective IS NOT NULL
                             AND revenue_k_effective != 0
                             AND ebitda_k_effective IS NOT NULL
                            THEN ROUND((ebitda_k_effective * 100.0) / revenue_k_effective, 2)
                            ELSE NULL
                        END,
                    
                        revenue_multiple = CASE
                            WHEN revenue_k_effective IS NOT NULL
                             AND revenue_k_effective != 0
                             AND asking_price_k_effective IS NOT NULL
                            THEN ROUND(asking_price_k_effective / revenue_k_effective, 2)
                            ELSE NULL
                        END,
                    
                        ebitda_multiple = CASE
                            WHEN ebitda_k_effective IS NOT NULL
                             AND ebitda_k_effective != 0
                             AND asking_price_k_effective IS NOT NULL
                            THEN ROUND(asking_price_k_effective / ebitda_k_effective, 2)
                            ELSE NULL
                        END,
                    
                        last_updated = CURRENT_TIMESTAMP,
                        last_updated_source = 'AUTO'
                    ;
                """
            )
            conn.commit()

    def fetch_deals_with_descriptions(
            self,
            *,
            sources: list[str] | None = None,
            only_missing_financials: bool = True,
    ):
        """
        Fetch deals that have usable descriptions
        and are candidates for financial extraction.
        """

        where = [
            "description IS NOT NULL",
            "TRIM(description) != ''",
        ]

        if only_missing_financials:
            where.append("""
                (
                    revenue_k IS NULL
                    OR ebitda_k IS NULL
                    OR asking_price_k IS NULL
                )
            """)

        params = []

        if sources:
            placeholders = ",".join("?" for _ in sources)
            where.append(f"source IN ({placeholders})")
            params.extend(sources)

        sql = f"""
            SELECT
                id,
                source,
                source_listing_id,
                description,
                revenue_k,
                ebitda_k,
                asking_price_k
            FROM deals
            WHERE {' AND '.join(where)}
            ORDER BY last_updated DESC
        """

        with self.get_conn() as conn:
            rows = conn.execute(sql, params).fetchall()

        return [dict(r) for r in rows]

    def update_deal_fields_auto(
            self,
            *,
            deal_id: int,
            updates: dict,
    ):
        safe_updates = {
            k: v for k, v in updates.items()
            if k in self.DEALS_DB_COLUMNS
        }
        if not safe_updates:
            return

        cols = ", ".join(f"{k} = ?" for k in safe_updates)
        values = list(safe_updates.values())

        sql = f"""
            UPDATE deals
            SET {cols},
                last_updated = CURRENT_TIMESTAMP,
                last_updated_source = 'AUTO'
            WHERE id = ?
        """

        with self.get_conn() as conn:
            conn.execute(sql, values + [deal_id])
            conn.commit()

    def upsert_deal_v2(self, deal: dict):
        """
        Canonical v2 upsert.
        Identity = (source, source_listing_id)
        Analyst-owned fields are never overwritten here.
        """

        cols = [
            "source",
            "source_listing_id",
            "source_url",

            "title",
            "industry",
            "sector",
            "location",
            "incorporation_year",

            "revenue_k",
            "ebitda_k",
            "asking_price_k",
            "profit_margin_pct",
            "revenue_growth_pct",
            "leverage_pct",

            "ebitda_margin",
            "revenue_multiple",
            "ebitda_multiple",

            "decision",

            "first_seen",
            "last_seen",
            "last_updated",
            "last_updated_source",

            "drive_folder_url",
        ]

        values = [deal.get(c) for c in cols]

        with self.get_conn() as conn:
            conn.execute(
                f"""
                INSERT INTO deals (
                    {", ".join(cols)}
                )
                VALUES (
                    {", ".join(["?"] * len(cols))}
                )
                ON CONFLICT(source, source_listing_id)
                DO UPDATE SET
                    title = excluded.title,
                    industry = excluded.industry,
                    sector = excluded.sector,
                    location = excluded.location,
                    incorporation_year = excluded.incorporation_year,

                    revenue_k = excluded.revenue_k,
                    ebitda_k = excluded.ebitda_k,
                    asking_price_k = excluded.asking_price_k,
                    profit_margin_pct = excluded.profit_margin_pct,
                    revenue_growth_pct = excluded.revenue_growth_pct,
                    leverage_pct = excluded.leverage_pct,

                    ebitda_margin = excluded.ebitda_margin,
                    revenue_multiple = excluded.revenue_multiple,
                    ebitda_multiple = excluded.ebitda_multiple,

                    decision = excluded.decision,

                    last_seen = excluded.last_seen,
                    last_updated = excluded.last_updated,
                    last_updated_source = excluded.last_updated_source,

                    drive_folder_url = excluded.drive_folder_url
                """,
                values,
            )
            conn.commit()

    def enrich_do_raw_fields(
            self,
            source: str,
            source_listing_id: str,
            *,
            location_raw: str | None,
            turnover_range_raw: str | None,
    ):
        with self.get_conn() as conn:
            conn.execute(
                """
                UPDATE deals
                SET location_raw        = COALESCE(location_raw, ?),
                    turnover_range_raw  = COALESCE(turnover_range_raw, ?),
                    last_updated        = CURRENT_TIMESTAMP,
                    last_updated_source = 'AUTO'
                WHERE source = ?
                  AND source_listing_id = ?
                """,
                (
                    location_raw,
                    turnover_range_raw,
                    source,
                    source_listing_id,
                ),
            )

    def recompute_effective_fields(self):
        """
        Recompute effective financial fields.
        Manual values override broker values.
        Safe, idempotent, non-destructive.
        """
        with self.get_conn() as conn:
            conn.execute(
                """
                UPDATE deals
                SET revenue_k_effective      =
                        COALESCE(revenue_k_manual, revenue_k),

                    ebitda_k_effective       =
                        COALESCE(ebitda_k_manual, ebitda_k),

                    asking_price_k_effective =
                        COALESCE(asking_price_k_manual, asking_price_k),

                    last_updated             = CURRENT_TIMESTAMP
                """
            )
            conn.commit()

    def get_deals_table_columns(self) -> set[str]:
        with self.get_conn() as conn:
            rows = conn.execute("PRAGMA table_info(deals)").fetchall()
        return {r["name"] for r in rows}

    def insert_status_history(self, deal_id, old_status, new_status):
        with self.get_conn() as conn:
            conn.execute(
                """
                INSERT INTO deal_status_history
                    (deal_id, old_status, new_status, changed_at)
                VALUES (?, ?, ?, datetime('now'))
                """,
                (deal_id, old_status, new_status)
            )

    def fetch_deals_for_enrichment(
            self,
            source: str,
            freshness_days: int = 14,
    ):
        with self.get_conn() as conn:
            conn.row_factory = sqlite3.Row
            return conn.execute(
                """
                SELECT id,
                       source_listing_id,
                       source_url,
                       title,
                       sector_raw
                FROM deals
                WHERE source = ?
                  AND (
                    needs_detail_refresh = 1
                        OR detail_fetched_at IS NULL
                        OR detail_fetched_at < datetime('now', ?)
                    )
                  AND (status IS NULL OR status NOT IN ('Pass', 'Lost'))
                ORDER BY source_listing_id;
                """,
                (source, f"-{freshness_days} days"),
            ).fetchall()