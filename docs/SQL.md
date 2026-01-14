Valuable Commands
- ALTER TABLE deals DROP COLUMN decision_reason;
- PRAGMA table_info(deals);

- enrichment criteria
- WHERE source = ?
  AND (
        detail_fetched_at IS NULL
        OR needs_detail_refresh = 1
      )

SELECT COUNT(DISTINCT source_url) AS distinct_urls
FROM deals
WHERE source = 'BusinessBuyers';

SELECT COUNT(*) AS total_rows
FROM deals
WHERE source = 'BusinessBuyers';

SELECT
  COUNT(*) AS duplicate_urls
FROM (
  SELECT source_url
  FROM deals
  WHERE source = 'BusinessBuyers'
  GROUP BY source_url
  HAVING COUNT(*) > 1
);