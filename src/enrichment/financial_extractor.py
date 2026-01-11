import re
from typing import Dict, Optional


# ==========================================================
# NORMALISATION
# ==========================================================

def normalize_text(text: str) -> str:
    if not text:
        return ""

    text = text.lower()
    text = text.replace("million", "m")
    text = text.replace("millions", "m")
    text = text.replace("approx.", "approx")
    text = text.replace("approximately", "approx")
    text = text.replace("circa", "approx")
    return text


# ==========================================================
# REGEX PATTERNS
# ==========================================================

RE_REVENUE = re.compile(
    r"(revenue|turnover|sales)"
    r"(?:\s*(?:for|of|:|-))?"
    r"(?:\s*(?:ye|fy)?\s*\w*\s*\d{4})?"
    r"\s*£?\s*([\d,]+(?:\.\d+)?)\s*(m|k)?",
    re.I,
)

RE_EBITDA = re.compile(
    r"(ebitda)"
    r"(?:\s*(?:of|:|-))?"
    r"\s*£?\s*([\d,]+(?:\.\d+)?)\s*(m|k)?",
    re.I,
)

RE_ASKING = re.compile(
    r"(asking price|valuation|offers in excess of|price)"
    r"(?:\s*(?:of|around|approx|:|-))?"
    r"\s*£?\s*([\d,]+(?:\.\d+)?)\s*(m|k)?",
    re.I,
)


# ==========================================================
# CONFIDENCE
# ==========================================================

def confidence_from_match(match_text: str) -> str:
    t = match_text.lower()

    if any(x in t for x in ("of", ":", "-")):
        return "high"
    if "approx" in t:
        return "medium"
    return "low"


# ==========================================================
# MONEY NORMALISATION (SINGLE SOURCE OF TRUTH)
# ==========================================================

def normalize_from_description(raw: float, unit: Optional[str]) -> Optional[int]:
    """
    Convert raw extracted number into £k.

    Rules:
    - 'm' → millions
    - 'k' → thousands
    - no unit:
        - < £10m → assume absolute pounds
        - otherwise → discard (too ambiguous)
    """
    if unit == "m":
        return int(raw * 1_000)

    if unit == "k":
        return int(raw)

    # no unit → absolute pounds heuristic
    if raw < 10_000_000:
        return int(raw / 1_000)

    return None


# ==========================================================
# MAIN EXTRACTION
# ==========================================================

def extract_financial_metrics(description: str) -> Dict[str, dict]:
    """
    Extract financial metrics from free text.

    Returns:
        {
            "revenue_k": { "value": int, "confidence": str },
            "ebitda_k": { "value": int, "confidence": str },
            "asking_price_k": { "value": int, "confidence": str }
        }
    """
    if not description:
        return {}

    text = normalize_text(description)
    out: Dict[str, dict] = {}

    # --------------------------
    # Revenue / Turnover
    # --------------------------
    if m := RE_REVENUE.search(text):
        raw = float(m.group(2).replace(",", ""))
        unit = m.group(3)
        value = normalize_from_description(raw, unit)
        if value is not None:
            out["revenue_k"] = {
                "value": value,
                "confidence": confidence_from_match(m.group(0)),
            }

    # --------------------------
    # EBITDA
    # --------------------------
    if m := RE_EBITDA.search(text):
        raw = float(m.group(2).replace(",", ""))
        unit = m.group(3)
        value = normalize_from_description(raw, unit)
        if value is not None:
            out["ebitda_k"] = {
                "value": value,
                "confidence": confidence_from_match(m.group(0)),
            }

    # --------------------------
    # Asking price / valuation
    # --------------------------
    if m := RE_ASKING.search(text):
        raw = float(m.group(2).replace(",", ""))
        unit = m.group(3)
        value = normalize_from_description(raw, unit)
        if value is not None:
            out["asking_price_k"] = {
                "value": value,
                "confidence": confidence_from_match(m.group(0)),
            }

    return out