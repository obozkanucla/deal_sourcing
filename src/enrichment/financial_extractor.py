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
# MONEY PARSING
# ==========================================================

def money_to_k(value: str, unit: Optional[str]) -> Optional[int]:
    """
    Convert monetary expression to thousands (£k).
    """
    try:
        v = float(value.replace(",", ""))
    except Exception:
        return None

    # explicit units
    if unit == "m":
        return int(v * 1_000)

    if unit == "k":
        return int(v)

    # absolute £ amounts
    if v >= 1_000:
        return int(v / 1_000)

    # small ambiguous numbers → ignore
    return None


# ==========================================================
# REGEX PATTERNS
# ==========================================================

RE_REVENUE = re.compile(
    r"(revenue|turnover|sales)"
    r"(?:\s*(?:for|of|:|-))?"
    r"(?:\s*(?:ye|fy)?\s*\w*\s*\d{4})?"
    r"\s*£?\s*([\d,]+(?:\.\d+)?)\s*(m|k)?",
    re.I
)

RE_EBITDA = re.compile(
    r"(ebitda)"
    r"(?:\s*(?:of|:|-))?"
    r"\s*£?\s*([\d,]+(?:\.\d+)?)\s*(m|k)?",
    re.I
)

RE_ASKING = re.compile(
    r"(asking price|valuation|offers in excess of|price)"
    r"(?:\s*(?:of|around|approx|:|-))?"
    r"\s*£?\s*([\d,]+(?:\.\d+)?)\s*(m|k)?",
    re.I
)


# ==========================================================
# CONFIDENCE SCORING
# ==========================================================

def confidence_from_match(match_text: str) -> str:
    t = match_text.lower()

    if any(x in t for x in ("of", ":", "-")):
        return "high"
    if "approx" in t:
        return "medium"
    return "low"


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
        value = money_to_k(m.group(2), m.group(3))
        if value:
            out["revenue_k"] = {
                "value": value,
                "confidence": confidence_from_match(m.group(0)),
            }

    # --------------------------
    # EBITDA
    # --------------------------
    if m := RE_EBITDA.search(text):
        value = money_to_k(m.group(2), m.group(3))
        if value:
            out["ebitda_k"] = {
                "value": value,
                "confidence": confidence_from_match(m.group(0)),
            }

    # --------------------------
    # Asking price / valuation
    # --------------------------
    if m := RE_ASKING.search(text):
        value = money_to_k(m.group(2), m.group(3))
        if value:
            out["asking_price_k"] = {
                "value": value,
                "confidence": confidence_from_match(m.group(0)),
            }

    return out