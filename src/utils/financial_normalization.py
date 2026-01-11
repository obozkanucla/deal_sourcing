import re

def _normalize_money_to_k(raw: str | None) -> float | None:
    if not raw:
        return None

    s = raw.replace(",", "").strip()

    m = re.search(r"£?\s*([\d\.]+)\s*([mk])?", s, re.I)
    if not m:
        return None

    val = float(m.group(1))
    unit = (m.group(2) or "").lower()

    if unit == "m":
        return val * 1_000
    if unit == "k":
        return val

    # assume absolute number
    return val / 1_000


def _normalize_pct(raw: str | None) -> float | None:
    if not raw:
        return None

    m = re.search(r"([\d\.]+)\s*%", raw)
    if not m:
        return None

    # IMPORTANT: percentage points, NOT ratio
    return float(m.group(1))

def normalize_from_description(raw_number, value_k):
    """
    raw_number = matched integer from text, e.g. 334762
    value_k    = extractor output
    """
    # If raw number < 10m, assume it was absolute pounds
    if raw_number < 10_000_000:
        return raw_number / 1_000

    return value_k