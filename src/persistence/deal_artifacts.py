def record_deal_artifact(
    *,
    conn,
    source: str,
    source_listing_id: str,
    artifact_type: str,
    artifact_name: str,
    artifact_hash: str,
    drive_file_id: str,
    drive_url: str,
    created_by: str,
    extraction_version: str | None = None,
    deal_id: int | None = None,
):
    conn.execute(
        """
        INSERT OR IGNORE INTO deal_artifacts (
            source,
            source_listing_id,
            deal_id,
            artifact_type,
            artifact_name,
            artifact_hash,
            drive_file_id,
            drive_url,
            created_at,
            created_by,
            extraction_version
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?)
        """,
        (
            source,
            source_listing_id,
            deal_id,
            artifact_type,
            artifact_name,
            artifact_hash,
            drive_file_id,
            drive_url,
            created_by,
            extraction_version,
        ),
    )