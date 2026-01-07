from playwright.sync_api import TimeoutError

LOST_PHRASES = [
    "no longer available",
    "listing has been removed",
    "business has been sold",
    "opportunity withdrawn",
]

def page_indicates_lost(page) -> bool:
    html = page.content().lower()
    return any(p in html for p in LOST_PHRASES)


def handle_lost_deal(
    *,
    conn,
    deal_id: int,
    reason: str,
):
    conn.execute(
        """
        UPDATE deals
        SET
            status = 'Lost',
            needs_detail_refresh = 0,
            detail_fetch_reason = ?,
            last_updated = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (reason, deal_id),
    )