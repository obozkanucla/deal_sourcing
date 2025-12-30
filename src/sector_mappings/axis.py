"""
Axis Partnership → Canonical Industry / Sector Inference

Rules:
- Axis does NOT declare sectors
- Inference is heuristic and conservative
- Confidence is capped (< 0.6)
- This is the ONLY place Axis semantics live
"""

BROKER_NAME = "AxisPartnership"

def infer_axis_industry_sector(*, title: str | None, description: str | None):
    text = f"{title or ''} {description or ''}".lower()

    if any(k in text for k in ["clinic", "care", "medical", "health", "pharma"]):
        return {
            "industry": "Healthcare",
            "sector": "Healthcare Services",
            "confidence": 0.5,
            "reason": "Keyword match: healthcare-related terms",
        }

    if any(k in text for k in ["software", "saas", "it", "platform", "digital"]):
        return {
            "industry": "Technology",
            "sector": "Software / IT Services",
            "confidence": 0.55,
            "reason": "Keyword match: software / IT / digital services",
        }

    if any(k in text for k in ["manufacturing", "engineering", "factory", "industrial"]):
        return {
            "industry": "Industrials",
            "sector": "Manufacturing",
            "confidence": 0.55,
            "reason": "Keyword match: manufacturing / industrial terms",
        }

    if any(k in text for k in ["education", "training", "school", "academy"]):
        return {
            "industry": "Education",
            "sector": "Education Services",
            "confidence": 0.5,
            "reason": "Keyword match: education / training terms",
        }

    if any(k in text for k in ["consult", "outsourced", "services", "b2b"]):
        return {
            "industry": "Business_Services",
            "sector": "Professional Services",
            "confidence": 0.5,
            "reason": "Keyword match: professional / outsourced services",
        }

    return {
        "industry": "Other",
        "sector": "Miscellaneous",
        "confidence": 0.3,
        "reason": "No strong keyword signals; defaulted to Other",
    }