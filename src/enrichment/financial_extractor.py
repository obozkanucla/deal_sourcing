import re
from typing import Dict, Optional


# ==========================================================
# NORMALISATION
# ==========================================================

def normalize_text(text: str) -> str:
    if not text:
        return ""

    text = text.lower()
    text = text.replace(",", "")
    text = text.replace("£", "£")
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
    try:
        v = float(value)
    except Exception:
        return None

    if unit == "m":
        return int(v * 1_000)
    if unit == "000":
        return int(v)

    # heuristic: large numbers without unit are probably full £
    if v > 100:
        return int(v / 1_000)

    return int(v)


# ==========================================================
# REGEX PATTERNS
# ==========================================================

RE_REVENUE = re.compile(
    r"(revenue|turnover|sales)\s*(of|around|approx)?\s*£?\s*([\d\.]+)\s*(m|000)?",
    re.I
)

RE_EBITDA = re.compile(
    r"(ebitda|operating profit|profit)\s*(of|around|approx)?\s*£?\s*([\d\.]+)\s*(m|000)?",
    re.I
)

RE_ASKING = re.compile(
    r"(asking price|price|valuation|offers in excess of)\s*(of|around|approx)?\s*£?\s*([\d\.]+)\s*(m|000)?",
    re.I
)


# ==========================================================
# CONFIDENCE SCORING
# ==========================================================

def confidence_from_match(match_text: str) -> str:
    """
    Very conservative confidence scoring.
    """
    if "of" in match_text:
        return "high"
    if "approx" in match_text:
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

    if m := RE_REVENUE.search(text):
        value = money_to_k(m.group(3), m.group(4))
        if value:
            out["revenue_k"] = {
                "value": value,
                "confidence": confidence_from_match(m.group(0)),
            }

    if m := RE_EBITDA.search(text):
        value = money_to_k(m.group(3), m.group(4))
        if value:
            out["ebitda_k"] = {
                "value": value,
                "confidence": confidence_from_match(m.group(0)),
            }

    if m := RE_ASKING.search(text):
        value = money_to_k(m.group(3), m.group(4))
        if value:
            out["asking_price_k"] = {
                "value": value,
                "confidence": confidence_from_match(m.group(0)),
            }

    return out