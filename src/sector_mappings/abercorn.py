ABERCORN_SECTOR_MAP = {
    # -------------------------------------------------
    # E-COMMERCE / RETAIL
    # -------------------------------------------------
    "ECA": ("Consumer_Retail", "E-commerce", 0.95, "E-commerce / online retail"),
    "RB":  ("Consumer_Retail", "Retail", 0.9, "Retail & wholesale"),
    "CV":  ("Consumer_Retail", "Automotive Retail", 0.9, "Car & vehicle sales"),
    "HF":  ("Consumer_Retail", "Fitness & Leisure", 0.9, "Health & fitness"),
    "LB":  ("Consumer_Retail", "Leisure", 0.9, "Leisure & entertainment"),
    "GR":  ("Consumer_Retail", "Automotive Services", 0.9, "Garage & MOT"),
    "FB":  ("Consumer_Retail", "Furniture Retail", 0.9, "Furniture & furnishings"),

    # -------------------------------------------------
    # CONSTRUCTION / BUILT ENVIRONMENT
    # -------------------------------------------------
    "CG": ("Construction_Built_Environment", "Construction", 0.95, "Construction & groundwork"),
    "HA": ("Construction_Built_Environment", "HVAC", 0.95, "Heating, plumbing & AC"),
    "WJ": ("Construction_Built_Environment", "Joinery", 0.95, "Joinery & carpentry"),
    "LS": ("Construction_Built_Environment", "Landscaping", 0.9, "Landscaping services"),
    "CC": ("Construction_Built_Environment", "Building Services", 0.9, "Cleaning & maintenance"),
    "DH": ("Construction_Built_Environment", "Building Products", 0.9, "Doors & hardware"),
    "DG": ("Construction_Built_Environment", "Glazing", 0.9, "Double glazing"),

    # -------------------------------------------------
    # BUSINESS SERVICES
    # -------------------------------------------------
    "RA": ("Business_Services", "Recruitment", 0.95, "Recruitment services"),
    "TC": ("Business_Services", "Consulting & Training", 0.9, "Training & consultancy"),
    "PS": ("Business_Services", "Printing", 0.9, "Printing & stationery"),
    "FR": ("Business_Services", "Franchise", 0.6, "Franchise business"),

    # -------------------------------------------------
    # HEALTHCARE / CARE
    # -------------------------------------------------
    "DC": ("Healthcare", "Care Services", 0.95, "Domiciliary & residential care"),
    "MC": ("Healthcare", "Medical Services", 0.9, "Clinics & medical services"),
    "FD": ("Healthcare", "Funeral Services", 0.9, "Funeral directors"),

    # -------------------------------------------------
    # INDUSTRIALS
    # -------------------------------------------------
    "HB": ("Consumer_Retail", "Health & Beauty Services", 0.95, "Hairdressers, salons and beauty businesses"),
    "MD": ("Industrials", "Manufacturing", 0.95, "Manufacturing"),
    "HR": ("Industrials", "Engineering", 0.9, "Engineering & machinery"),
    "ES": ("Industrials", "Electrical Services", 0.9, "Electrical services"),
    "CE": ("Industrials", "Catering Production", 0.85, "Catering production"),
    "KB": ("Construction_Built_Environment", "Kitchen & Bathroom Fit-Out", 0.95, "Kitchen and bathroom design, installation and fit-out businesses"),
    "LP": ("Construction_Built_Environment", "Commercial Property / Luxury Assets", 0.9, "Luxury outlets and commercial property businesses"),
    "REF": ("Other", "Miscellaneous", 0.2, "Abercorn internal reference / non-sector listing"),
    "EG": ("Consumer_Retail", "Leisure / Entertainment", 0.9, "Entertainment and leisure venues"),
    # -------------------------------------------------
    # FOOD & BEVERAGE
    # -------------------------------------------------
    "CFR": ("Food_Beverage", "Hospitality", 0.95, "Cafes, restaurants & food"),
    "LD":  ("Food_Beverage", "Food Services", 0.85, "Laundry / food-related services"),

    # -------------------------------------------------
    # PROPERTY
    # -------------------------------------------------
    "PR": ("Construction_Built_Environment", "Property Services", 0.9, "Property & estate agency"),

    # -------------------------------------------------
    # TECHNOLOGY
    # -------------------------------------------------
    "CN": ("Technology", "IT Services", 0.9, "Computer & networking"),

    # -------------------------------------------------
    # EDGE / SPECIAL
    # -------------------------------------------------
    "EG": ("Consumer_Retail", "Entertainment", 0.8, "Entertainment & gaming"),
    "TA": ("Consumer_Retail", "Travel", 0.9, "Travel & tourism"),
    "EB": ("Education", "Education Services", 0.9, "Education & training"),
    "FS": ("Financial_Services", "Financial Services", 0.9, "Financial services"),

    # -------------------------------------------------
    # JUNK / FALLBACK
    # -------------------------------------------------
    "REF": ("Other", None, 0.1, "Unclassified / junk reference"),
}

def resolve_abercorn_sector(prefix: str):
    if prefix not in ABERCORN_SECTOR_MAP:
        raise RuntimeError(f"UNMAPPED_ABERCORN_SECTOR: {prefix}")

    industry, sector, confidence, reason = ABERCORN_SECTOR_MAP[prefix]

    return (
        industry,
        sector,
        "broker",
        confidence,
        reason,
    )