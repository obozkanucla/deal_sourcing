import sqlite3
from pathlib import Path
from datetime import datetime, date
from typing import Optional

class SQLiteRepository:
    def __init__(self, db_path: Path):
        # ✅ force path relative to project root
        project_root = Path(__file__).resolve().parents[2]
        self.db_path = project_root / db_path
        print("📀 SQLite DB path:", db_path.resolve())
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()


    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            with open(Path(__file__).parent / "schema.sql") as f:
                conn.executescript(f.read())

    def get_conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row   # ✅ THIS IS THE KEY LINE
        return conn

    # ---------- DEALS ----------

    def deal_exists(self, source: str, listing_id: str) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute(
                """
                SELECT 1 FROM deals
                WHERE source = ? AND source_listing_id = ?
                """,
                (source, listing_id),
            )
            return cur.fetchone() is not None

    def get_content_hash(self, source: str, listing_id: str):
        with sqlite3.connect(self.db_path) as conn:
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

        with sqlite3.connect(self.db_path) as conn:
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
        with sqlite3.connect(self.db_path) as conn:
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
        with sqlite3.connect(self.db_path) as conn:
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
            sector: Optional[str] = None,
    ):
        now = datetime.utcnow().isoformat()
        today = date.today().isoformat()

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO deals (source,
                                   source_listing_id,
                                   source_url,
                                   sector,
                                   first_seen,
                                   last_seen,
                                   last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(source, source_listing_id) DO
                UPDATE SET
                    last_seen = excluded.last_seen,
                    last_updated = excluded.last_updated
                """,
                (
                    source,
                    source_listing_id,
                    source_url,
                    sector,
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
        with sqlite3.connect(self.db_path) as conn:
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
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row

        rows = conn.execute(
            """
            SELECT deal_id,
                   source,
                   source_listing_id,
                   source_url,
                   sector,
                   decision,
                   decision_confidence,
                   first_seen,
                   last_seen,
                   last_updated
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

    def update_deal_fields(self, deal_id: str, fields: dict):
        if not fields:
            return

        assignments = ", ".join([f"{k} = ?" for k in fields.keys()])
        values = list(fields.values()) + [deal_id]

        sql = f"""
            UPDATE deals
            SET {assignments},
                last_updated = CURRENT_TIMESTAMP
            WHERE deal_id = ?
        """

        with self.get_conn() as conn:
            conn.execute(sql, values)
            conn.commit()

    def fetch_by_deal_id(self, deal_id: str):
        with self.get_conn() as conn:
            cur = conn.execute(
                "SELECT * FROM deals WHERE deal_id = ?",
                (deal_id,)
            )
            row = cur.fetchone()
            return dict(row) if row else None