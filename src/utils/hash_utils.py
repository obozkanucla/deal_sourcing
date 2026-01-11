# src/utils/hash_utils.py
from pathlib import Path
import hashlib

def compute_file_hash(path: Path) -> str:
    """
    Compute SHA-256 hash of a file.
    This is the canonical artifact identity.
    """
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()