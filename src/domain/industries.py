"""
Canonical Industry & Sector Taxonomy

Authoritative rules:
- Industries MUST match Drive folder configuration
- Sectors are informational / analytical (not infra-binding)
- No new industry may be introduced without Drive provisioning
"""

# ------------------------------------------------------------------
# CANONICAL INDUSTRIES (INFRA-BOUND)
# ------------------------------------------------------------------
# These MUST exist as Drive folders.
# This is the definitive industry universe.

CANONICAL_INDUSTRIES: set[str] = {
    "Business_Services",
    "Construction_Built_Environment",
    "Consumer_Retail",
    "Education",
    "Financial_Services",
    "Food_Beverage",
    "Healthcare",
    "Industrials",
    "Logistics_Distribution",
    "Other",
    "Technology",
    "Franchise_Businesses",
    "Agriculture"
}

# ------------------------------------------------------------------
# SECTOR TAXONOMY (NON-BINDING, FUTURE USE)
# ------------------------------------------------------------------
# Sectors may evolve.
# They do NOT imply Drive structure unless explicitly promoted.

INDUSTRY_SECTORS: dict[str, list[str]] = {
    "Business_Services": [
        "Consulting / Professional Services",
        "Facilities Management",
        "IT Services / Support",
        "Marketing / Advertising",
        "Recruitment / HR Services",
    ],

    "Construction_Built_Environment": [
        "Architecture / Design",
        "Building Materials",
        "Construction Contractors",
        "Facilities Management",
        "Property Development",
    ],

    "Consumer_Retail": [
        "Consumer Goods",
        "E-commerce",
        "Food & Beverage Retail",
        "Leisure / Hospitality",
        "Retail Stores",
    ],

    "Education": [
        "Educational Products",
        "Higher Education",
        "Schools / Colleges",
        "Training / E-learning",
    ],

    "Financial_Services": [
        "Accounting / Auditing",
        "Asset Management",
        "Banking",
        "FinTech",
        "Insurance",
    ],

    "Food_Beverage": [
        "Beverage Production (including Beer Distribution)",
        "Catering / Hospitality",
        "Food Production / Processing",
        "Food Retail",
    ],

    "Healthcare": [
        "Domiciliary Care / Home Healthcare",
        "Hospitals / Clinics",
        "Medical Devices / Equipment",
        "Pharmaceuticals",
        "Social Care",
    ],

    "Industrials": [
        "Chemicals",
        "Energy / Utilities",
        "Engineering",
        "Equipment Rental / Leasing",
        "Manufacturing",
    ],

    "Logistics_Distribution": [
        "Courier / Delivery Services",
        "Freight / Shipping",
        "Supply Chain Management",
        "Warehousing",
    ],

    "Technology": [
        "Cybersecurity",
        "Hardware",
        "IT Consulting",
        "Software / SaaS",
        "Telecommunications",
    ],
    "Agriculture": [
        "Agriculture / Primary Production",
        "Agriculture/Forestry/Fishing"
    ],
    "Other": [
        "Agriculture / Farming",
        "Government / Public Sector",
        "Media / Publishing",
        "Miscellaneous",
        "Non-Profit / Charities",
    ],
}

# ------------------------------------------------------------------
# VALIDATION HELPERS (USE THESE)
# ------------------------------------------------------------------

def assert_valid_industry(industry: str) -> None:
    if industry not in CANONICAL_INDUSTRIES:
        raise ValueError(
            f"Invalid industry '{industry}'. "
            f"Must be one of: {sorted(CANONICAL_INDUSTRIES)}"
        )


def is_known_sector(industry: str, sector: str) -> bool:
    return sector in INDUSTRY_SECTORS.get(industry, [])