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

# src/utils/hash_utils.py

import hashlib
import json
import re
from typing import Optional


def _normalize_text(s: str) -> str:
    """
    Normalize human-readable content so that
    presentation noise does not affect hashing.
    """
    s = s.lower()
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def compute_content_hash(
    *,
    title: str,
    description: str,
    location: Optional[str] = None,
) -> str:
    """
    Canonical hash of extracted business content.

    Contract:
    - title and description MUST be present
    - location is optional
    - hashing is semantic, not presentational
    - broker-agnostic meaning
    """

    payload = {
        "title": _normalize_text(title),
        "description": _normalize_text(description),
        "location": _normalize_text(location) if location else None,
    }

    blob = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()