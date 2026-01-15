def record_deal_artifact(
    *,
    conn,
    source: str,
    source_listing_id: str,
    deal_id: int,
    artifact_type: str,
    artifact_name: str,
    artifact_hash: str,
    drive_file_id: str,
    drive_url: str,
    created_by: str,
    extraction_version: str | None = None,
):
    """
    Record a deal artifact.

    Invariants:
    - deal_id MUST be present and refer to an existing deal
    - artifact insertion must fail loudly on violations
    """

    # ------------------------------------------------------------------
    # HARD GUARDS — DO NOT REMOVE
    # ------------------------------------------------------------------
    if deal_id is None:
        raise RuntimeError(
            "ARTIFACT_INVARIANT_VIOLATION: deal_id is None "
            f"(source={source}, source_listing_id={source_listing_id}, "
            f"artifact_name={artifact_name})"
        )

    # Optional but recommended: ensure deal exists
    cur = conn.execute(
        "SELECT 1 FROM deals WHERE id = ?",
        (deal_id,),
    ).fetchone()

    if cur is None:
        raise RuntimeError(
            "ARTIFACT_INVARIANT_VIOLATION: deal_id does not exist "
            f"(deal_id={deal_id}, source={source}, "
            f"source_listing_id={source_listing_id})"
        )

    # ------------------------------------------------------------------
    # STRICT INSERT (NO OR IGNORE)
    # ------------------------------------------------------------------
    conn.execute(
        """
        INSERT INTO deal_artifacts (
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