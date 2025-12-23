"""
Authoritative mapping between (broker, sector) and Google Drive folder IDs.

Rules:
- These folders MUST already exist
- Code must never auto-create broker or sector folders
- Only deal-level folders are created dynamically
"""

BROKER_FOLDERS = {
    ("BusinessBuyers", "Healthcare"): "1wXZq2pFt1khin0l2d0CgvZOqu_a060IN",
    ("Dmitry", "Healthcare"): "17J7CipmRFWfW-kdIXscDiw7nX4a8crIl",
    # add more explicitly
}