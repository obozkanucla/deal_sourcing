def record_deal_artifact(
    *,
    conn,
    deal_id: int,
    broker: str,
    artifact_type: str,
    artifact_name: str,
    drive_file_id: str | None,
    drive_url: str,
    industry: str | None,
    sector: str | None,
    created_by: str,
):
    conn.execute(
        """
        INSERT INTO deal_artifacts (
            deal_id,
            broker,
            artifact_type,
            artifact_name,
            drive_file_id,
            drive_url,
            industry_at_create,
            sector_at_create,
            created_by
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            deal_id,
            broker,
            artifact_type,
            artifact_name,
            drive_file_id,
            drive_url,
            industry,
            sector,
            created_by,
        ),
    )