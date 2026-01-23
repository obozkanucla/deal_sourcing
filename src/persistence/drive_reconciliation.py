from src.persistence.deal_artifacts import record_deal_artifact

def reconcile_drive_pdfs_for_deal(
    *,
    conn,
    deal,
    drive_files: list[dict],
    source: str,
    created_by: str,
):
    """
    Convert Drive PDFs into deal_artifacts rows.
    """
    for f in drive_files:
        name = f["name"].lower()

        if name.endswith("-listing.pdf"):
            artifact_type = "listing_pdf"
        elif name.endswith(".pdf"):
            artifact_type = "information_memorandum"
        else:
            continue

        existing = conn.execute(
            """
            SELECT 1 FROM deal_artifacts
            WHERE deal_id = ?
              AND artifact_type = ?
              AND drive_file_id = ?
            """,
            (deal["id"], artifact_type, f["id"]),
        ).fetchone()

        if existing:
            continue

        record_deal_artifact(
            conn=conn,
            source=source,
            source_listing_id=deal["source_listing_id"],
            deal_id=deal["id"],
            artifact_type=artifact_type,
            artifact_name=f["name"],
            artifact_hash=None,  # Drive-originated
            drive_file_id=f["id"],
            drive_url=f["webViewLink"],
            extraction_version="drive-reconcile",
            created_by=created_by,
        )

        if artifact_type == "information_memorandum":
            conn.execute(
                """
                UPDATE deals
                SET pdf_drive_url = ?,
                    last_updated = CURRENT_TIMESTAMP,
                    last_updated_source = 'AUTO'
                WHERE id = ?
                """,
                (f["webViewLink"], deal["id"]),
            )

    conn.commit()