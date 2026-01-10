Valuable Commands
- ALTER TABLE deals DROP COLUMN decision_reason;
- PRAGMA table_info(deals);

- enrichment criteria
- WHERE source = ?
  AND (
        detail_fetched_at IS NULL
        OR needs_detail_refresh = 1
      )