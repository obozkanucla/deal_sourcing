"""
Knightsbridge → Canonical Industry / Sector Mapping

Rules:
- This file is the ONLY place Knightsbridge semantics live
- No inference, no scraping logic, no keyword guessing
- Confidence is intentionally high (broker-declared)
- Used during enrich + BAU backfill
"""

BROKER_NAME = "Knightsbridge"

# -------------------------------------------------------------------
# RAW KNIGHTSBRIDGE SECTOR → CANONICAL MAPPING
# -------------------------------------------------------------------
# Format:
#   raw_label: {
#       "industry": <canonical industry>,
#       "sector": <canonical sector>,
#       "confidence": float,
#       "reason": str
#   }
# -------------------------------------------------------------------

KNIGHTSBRIDGE_SECTOR_MAP = {

    # ---------------------------------------------------------------
    # BUSINESS SERVICES
    # ---------------------------------------------------------------
    "Advertising & Media": {
        "industry": "Business_Services",
        "sector": "Marketing / Advertising",
        "confidence": 0.95,
        "reason": "Knightsbridge sector explicitly advertising/media services",
    },
    "Consultancy": {
        "industry": "Business_Services",
        "sector": "Consulting / Professional Services",
        "confidence": 0.95,
        "reason": "Direct match to consulting services",
    },
    "Recruitment": {
        "industry": "Business_Services",
        "sector": "Recruitment / HR Services",
        "confidence": 0.95,
        "reason": "Direct recruitment services classification",
    },
    "Facilities Management": {
        "industry": "Business_Services",
        "sector": "Facilities Management",
        "confidence": 0.9,
        "reason": "Facilities and outsourced services",
    },
    "Health & Safety": {
        "industry": "Business_Services",
        "sector": "Consulting / Professional Services",
        "confidence": 0.85,
        "reason": "Health & safety compliance and advisory services",
    },
    "Events": {
        "industry": "Business_Services",
        "sector": "Marketing / Advertising",
        "confidence": 0.8,
        "reason": "Events, experiential and promotional services",
    },

    # ---------------------------------------------------------------
    # CONSTRUCTION & BUILT ENVIRONMENT
    # ---------------------------------------------------------------
    "Commercial": {
        "industry": "Business_Services",
        "sector": "Facilities Management",
        "confidence": 0.75,
        "reason": "Knightsbridge 'Commercial' heavily skewed to cleaning/FM",
    },
    "Construction & Building": {
        "industry": "Construction_Built_Environment",
        "sector": "Construction Contractors",
        "confidence": 0.95,
        "reason": "Direct construction and building services",
    },
    "Architecture": {
        "industry": "Construction_Built_Environment",
        "sector": "Architecture / Design",
        "confidence": 0.95,
        "reason": "Architectural design services",
    },
    "Civil Engineering": {
        "industry": "Construction_Built_Environment",
        "sector": "Construction Contractors",
        "confidence": 0.95,
        "reason": "Civil engineering contracting",
    },
    "Electrical/Electricians": {
        "industry": "Construction_Built_Environment",
        "sector": "Construction Contractors",
        "confidence": 0.9,
        "reason": "Electrical installation and contracting",
    },
    "Refrigeration & Air Conditioning": {
        "industry": "Construction_Built_Environment",
        "sector": "Construction Contractors",
        "confidence": 0.9,
        "reason": "Mechanical and HVAC contracting services",
    },
    "Fire & Security": {
        "industry": "Construction_Built_Environment",
        "sector": "Construction Contractors",
        "confidence": 0.9,
        "reason": "Fire alarms, security systems, and building safety installation",
    },
    "Flooring Services": {
        "industry": "Construction_Built_Environment",
        "sector": "Construction Contractors",
        "confidence": 0.9,
        "reason": "Specialist flooring installation and refurbishment",
    },
    "Glass Related": {
        "industry": "Construction_Built_Environment",
        "sector": "Building Materials",
        "confidence": 0.85,
        "reason": "Glazing, glass fabrication and installation services",
    },
    "Gardening & Landscaping": {
        "industry": "Construction_Built_Environment",
        "sector": "Facilities Management",
        "confidence": 0.85,
        "reason": "Grounds maintenance and landscaping services",
    },

    # ---------------------------------------------------------------
    # INDUSTRIALS
    # ---------------------------------------------------------------
    "Fabrications": {
        "industry": "Industrials",
        "sector": "Engineering",
        "confidence": 0.95,
        "reason": "Fabrication, machining and engineering workshops",
    },
    "Manufacturing": {
        "industry": "Industrials",
        "sector": "Manufacturing",
        "confidence": 0.95,
        "reason": "Explicit manufacturing classification",
    },
    "Machinery": {
        "industry": "Industrials",
        "sector": "Equipment Rental / Leasing",
        "confidence": 0.85,
        "reason": "Machinery sales, hire, and servicing",
    },
    "Environmental/Energy": {
        "industry": "Industrials",
        "sector": "Energy / Utilities",
        "confidence": 0.9,
        "reason": "Environmental services and energy-related operations",
    },
    "Waste Management & Recycling": {
        "industry": "Industrials",
        "sector": "Energy / Utilities",
        "confidence": 0.9,
        "reason": "Waste handling, recycling and environmental services",
    },

    # ---------------------------------------------------------------
    # TECHNOLOGY
    # ---------------------------------------------------------------
    "IT Services & Support": {
        "industry": "Technology",
        "sector": "IT Services / Support",
        "confidence": 0.95,
        "reason": "Managed IT and technical support services",
    },
    "IT Consultancy": {
        "industry": "Technology",
        "sector": "IT Consulting",
        "confidence": 0.95,
        "reason": "IT advisory and systems consulting",
    },
    "Software": {
        "industry": "Technology",
        "sector": "Software / SaaS",
        "confidence": 0.95,
        "reason": "Software-led and SaaS businesses",
    },
    "Telecommunications": {
        "industry": "Technology",
        "sector": "Telecommunications",
        "confidence": 0.95,
        "reason": "Telecom operators and connectivity services",
    },
    "Audio & Visual": {
        "industry": "Technology",
        "sector": "Hardware",
        "confidence": 0.85,
        "reason": "AV systems, installation, and technology services",
    },

    # ---------------------------------------------------------------
    # HEALTHCARE
    # ---------------------------------------------------------------
    "Medical & Education": {
        "industry": "Healthcare",
        "sector": "Medical Devices / Equipment",
        "confidence": 0.75,
        "reason": "Mixed category with healthcare equipment bias",
    },
    "Medical Service": {
        "industry": "Healthcare",
        "sector": "Hospitals / Clinics",
        "confidence": 0.9,
        "reason": "Direct healthcare service providers",
    },
    "Funeral Services": {
        "industry": "Healthcare",
        "sector": "Social Care",
        "confidence": 0.85,
        "reason": "End-of-life and funeral care services",
    },
    "Mobility Equipment": {
        "industry": "Healthcare",
        "sector": "Medical Devices / Equipment",
        "confidence": 0.9,
        "reason": "Mobility aids and healthcare equipment supply",
    },

    # ---------------------------------------------------------------
    # LOGISTICS & DISTRIBUTION
    # ---------------------------------------------------------------
    "Transport, Haulage & Logistics": {
        "industry": "Logistics_Distribution",
        "sector": "Freight / Shipping",
        "confidence": 0.95,
        "reason": "Transport and logistics operators",
    },
    "Removals & Storage": {
        "industry": "Logistics_Distribution",
        "sector": "Warehousing",
        "confidence": 0.9,
        "reason": "Storage and removals businesses",
    },
    "Import & Distribution": {
        "industry": "Logistics_Distribution",
        "sector": "Supply Chain Management",
        "confidence": 0.9,
        "reason": "Importers, wholesalers, and distributors",
    },

    # ---------------------------------------------------------------
    # CONSUMER / RETAIL
    # ---------------------------------------------------------------
    "Food Related": {
        "industry": "Food_Beverage",
        "sector": "Food Production / Processing",
        "confidence": 0.85,
        "reason": "Food-related commercial operations",
    },
    "Wholesale & Retail": {
        "industry": "Consumer_Retail",
        "sector": "Retail Stores",
        "confidence": 0.95,
        "reason": "Wholesale and retail operations",
    },
    "Breweries & Distilleries": {
        "industry": "Food_Beverage",
        "sector": "Beverage Production (including Beer Distribution)",
        "confidence": 0.95,
        "reason": "Alcohol production and distillation businesses",
    },
    "Leisure & Lifestyle": {
        "industry": "Consumer_Retail",
        "sector": "Leisure / Hospitality",
        "confidence": 0.9,
        "reason": "Consumer-facing leisure businesses",
    },

    # ---------------------------------------------------------------
    # EDUCATION
    # ---------------------------------------------------------------
    "Education Services": {
        "industry": "Education",
        "sector": "Schools / Colleges",
        "confidence": 0.85,
        "reason": "Education and learning service providers",
    },
    "School/Training Centre": {
        "industry": "Education",
        "sector": "Schools / Colleges",
        "confidence": 0.9,
        "reason": "Schools, academies, and training centres",
    },

    "Training": {
        "industry": "Education",
        "sector": "Training / E-learning",
        "confidence": 0.9,
        "reason": "Standalone training providers explicitly classified by Knightsbridge",
    },

    # ---------------------------------------------------------------
    # FINANCIAL SERVICES
    # ---------------------------------------------------------------
    "Financial Services": {
        "industry": "Financial_Services",
        "sector": "Accounting / Auditing",
        "confidence": 0.85,
        "reason": "Financial advisory, accounting and related services",
    },
    "Corporate": {
        "industry": "Business_Services",
        "sector": "Consulting / Professional Services",
        "confidence": 0.7,
        "reason": "Generic corporate services classification",
    },

    # ---------------------------------------------------------------
    # OTHER / EDGE
    # ---------------------------------------------------------------
    "Miscellaneous": {
        "industry": "Other",
        "sector": "Miscellaneous",
        "confidence": 0.6,
        "reason": "Knightsbridge catch-all category",
    },
}