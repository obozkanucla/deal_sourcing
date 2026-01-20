def canonicalize_sector_key(s: str) -> str:
    return (
        s.replace("\xa0", " ")
         .replace("&amp;", "&")
         .strip()
         .lower()
    )

KNIGHTSBRIDGE_SECTOR_MAP = {
    # ------------------------------------------------------------------
    # AGRICULTURE
    # ------------------------------------------------------------------
    "Agriculture/Forestry/Fishing": {
        "industry": "Agriculture",
        "sector": "Agriculture / Primary Production",
        "confidence": 0.9,
        "reason": "Primary agriculture, forestry and fishing activities",
    },

    # ------------------------------------------------------------------
    # BUSINESS / SERVICES (AGGREGATES)
    # ------------------------------------------------------------------
    "Commercial": {
        "industry": "Business_Services",
        "sector": None,
        "confidence": 0.2,
        "reason": "Broker top-level commercial aggregation bucket",
    },
    "Services": {
        "industry": "Other",
        "sector": None,
        "confidence": 0.2,
        "reason": "Generic broker services aggregation bucket",
    },

    # ------------------------------------------------------------------
    # CARE / HEALTH
    # ------------------------------------------------------------------
    "Care": {
        "industry": "Healthcare",
        "sector": None,
        "confidence": 0.3,
        "reason": "Broker taxonomy migration – top-level care bucket",
    },
    "Child Care": {
        "industry": "Healthcare",
        "sector": "Childcare Services",
        "confidence": 0.85,
        "reason": "Early years and childcare providers",
    },
    "Healthcare": {
        "industry": "Healthcare",
        "sector": None,
        "confidence": 0.4,
        "reason": "Broker healthcare aggregation bucket",
    },
    "Health & Beauty": {
        "industry": "Consumer_Retail",
        "sector": "Health & Beauty Services",
        "confidence": 0.8,
        "reason": "Consumer-facing health and beauty businesses",
    },
    "Kennels": {
        "industry": "Consumer_Retail",
        "sector": "Pet Services",
        "confidence": 0.85,
        "reason": "Pet boarding and kennel services",
    },

    # ------------------------------------------------------------------
    # FOOD & BEVERAGE
    # ------------------------------------------------------------------
    "Food & Drink": {
        "industry": "Food_Beverage",
        "sector": "Food & Beverage",
        "confidence": 0.9,
        "reason": "Food and beverage businesses",
    },
    "Catering": {
        "industry": "Food_Beverage",
        "sector": "Catering Services",
        "confidence": 0.8,
        "reason": "Catering and contract food services",
    },

    # ------------------------------------------------------------------
    # CONSTRUCTION / BUILT ENVIRONMENT
    # ------------------------------------------------------------------
    "Construction": {
        "industry": "Construction_Built_Environment",
        "sector": "Construction Contractors",
        "confidence": 0.85,
        "reason": "Top-level construction category introduced by broker",
    },

    # ------------------------------------------------------------------
    # INDUSTRIALS
    # ------------------------------------------------------------------
    "Manufacturing": {
        "industry": "Industrials",
        "sector": "Manufacturing",
        "confidence": 0.95,
        "reason": "Explicit manufacturing classification",
    },
    "Engineering": {
        "industry": "Industrials",
        "sector": "Engineering",
        "confidence": 0.9,
        "reason": "General engineering businesses",
    },

    # ------------------------------------------------------------------
    # FACILITIES / WASTE
    # ------------------------------------------------------------------
    "Facilities & Waste Management": {
        "industry": "Business_Services",
        "sector": "Facilities Management",
        "confidence": 0.9,
        "reason": "Facilities and waste management services",
    },

    # ------------------------------------------------------------------
    # CONSUMER / LEISURE
    # ------------------------------------------------------------------
    "Leisure & Lifestyle": {
        "industry": "Consumer_Retail",
        "sector": "Leisure / Hospitality",
        "confidence": 0.9,
        "reason": "Consumer-facing leisure businesses",
    },
    "License & Leisure": {
        "industry": "Consumer_Retail",
        "sector": "Licensed Leisure",
        "confidence": 0.85,
        "reason": "Licensed leisure and hospitality venues",
    },
    "Motor Related": {
        "industry": "Consumer_Retail",
        "sector": "Automotive Services",
        "confidence": 0.85,
        "reason": "Automotive sales, repair and related services",
    },

    # ------------------------------------------------------------------
    # RETAIL / DISTRIBUTION
    # ------------------------------------------------------------------
    "Retail": {
        "industry": "Consumer_Retail",
        "sector": "Retail Stores",
        "confidence": 0.9,
        "reason": "Retail-focused businesses",
    },
    "Retail/Wholesale/Distribution": {
        "industry": "Consumer_Retail",
        "sector": "Wholesale / Distribution",
        "confidence": 0.85,
        "reason": "Combined retail and distribution activities",
    },
    "E-Commerce": {
        "industry": "Consumer_Retail",
        "sector": "E-commerce",
        "confidence": 0.95,
        "reason": "Online retail and e-commerce businesses",
    },

    # ------------------------------------------------------------------
    # TECHNOLOGY
    # ------------------------------------------------------------------
    "Technology": {
        "industry": "Technology",
        "sector": None,
        "confidence": 0.4,
        "reason": "Top-level technology aggregation bucket",
    },

    # ------------------------------------------------------------------
    # TRANSPORT / LOGISTICS
    # ------------------------------------------------------------------
    "Transport/Logistics/Storage": {
        "industry": "Logistics_Distribution",
        "sector": "Logistics / Storage",
        "confidence": 0.9,
        "reason": "Transport, logistics and storage services",
    },

    # ------------------------------------------------------------------
    # PROPERTY
    # ------------------------------------------------------------------
    "Property": {
        "industry": "Construction_Built_Environment",
        "sector": "Property Services",
        "confidence": 0.85,
        "reason": "Property-related operating businesses",
    },

    # ------------------------------------------------------------------
    # OTHER
    # ------------------------------------------------------------------
    "Miscellaneous": {
        "industry": "Other",
        "sector": "Miscellaneous",
        "confidence": 0.6,
        "reason": "Explicit broker miscellaneous category",
    },
    # ------------------------------------------------------------------
    # BUSINESS SERVICES
    # ------------------------------------------------------------------
    "Advertising & Media": {
        "industry": "Business_Services",
        "sector": "Marketing / Advertising",
        "confidence": 0.95,
        "reason": "Advertising and media services explicitly broker-declared",
    },
    "Business Services": {
        "industry": "Business_Services",
        "sector": "Outsourced Business Services",
        "confidence": 0.9,
        "reason": "Generic business services classification",
    },
    "Print, Publishing, Media & Marketing": {
        "industry": "Business_Services",
        "sector": "Marketing / Advertising",
        "confidence": 0.95,
        "reason": "Knightsbridge combined print, publishing, media and marketing services",
    },
    "Consultancy": {
        "industry": "Business_Services",
        "sector": "Consulting / Professional Services",
        "confidence": 0.95,
        "reason": "Direct consulting services",
    },
    "Corporate": {
        "industry": "Business_Services",
        "sector": "Consulting / Professional Services",
        "confidence": 0.7,
        "reason": "Generic corporate services bucket used by broker",
    },
    "Recruitment": {
        "industry": "Business_Services",
        "sector": "Recruitment / HR Services",
        "confidence": 0.95,
        "reason": "Staffing and recruitment services",
    },
    "Facilities Management": {
        "industry": "Business_Services",
        "sector": "Facilities Management",
        "confidence": 0.95,
        "reason": "Facilities and outsourced services",
    },
    "Health & Safety": {
        "industry": "Business_Services",
        "sector": "Compliance / Advisory Services",
        "confidence": 0.9,
        "reason": "Health & safety advisory and compliance services",
    },
    "Events": {
        "industry": "Business_Services",
        "sector": "Marketing / Events",
        "confidence": 0.85,
        "reason": "Events and experiential services",
    },
    "Service": {
        "industry": "Other",
        "sector": "General Services",
        "confidence": 0.6,
        "reason": "Non-specific broker service category",
    },
    "Service (Other)": {
        "industry": "Other",
        "sector": "Other Services",
        "confidence": 0.6,
        "reason": "Explicit broker catch-all",
    },



    # ------------------------------------------------------------------
    # CONSTRUCTION & BUILT ENVIRONMENT
    # ------------------------------------------------------------------
    "Construction & Building": {
        "industry": "Construction_Built_Environment",
        "sector": "Construction Contractors",
        "confidence": 0.95,
        "reason": "Construction and building services",
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
    "Plumbing & Heating": {
        "industry": "Construction_Built_Environment",
        "sector": "Construction Contractors",
        "confidence": 0.9,
        "reason": "Plumbing and heating contractors",
    },
    "Fire & Security": {
        "industry": "Construction_Built_Environment",
        "sector": "Construction Contractors",
        "confidence": 0.9,
        "reason": "Fire, security and building systems",
    },
    "Flooring Services": {
        "industry": "Construction_Built_Environment",
        "sector": "Construction Contractors",
        "confidence": 0.9,
        "reason": "Specialist flooring installation",
    },
    "Gardening & Landscaping": {
        "industry": "Construction_Built_Environment",
        "sector": "Facilities Management",
        "confidence": 0.85,
        "reason": "Grounds maintenance and landscaping",
    },
    "Glass Related": {
        "industry": "Construction_Built_Environment",
        "sector": "Building Materials",
        "confidence": 0.85,
        "reason": "Glazing and glass services",
    },
    "Refrigeration & Air Conditioning": {
        "industry": "Construction_Built_Environment",
        "sector": "Construction Contractors",
        "confidence": 0.9,
        "reason": "HVAC installation and servicing",
    },
    "Windows & Doors": {
        "industry": "Construction_Built_Environment",
        "sector": "Building Materials",
        "confidence": 0.9,
        "reason": "Window and door manufacturing/installation",
    },
    "Cleaning Company": {
        "industry": "Business_Services",
        "sector": "Facilities Management",
        "confidence": 0.95,
        "reason": "Commercial cleaning services",
    },

    # ------------------------------------------------------------------
    # INDUSTRIALS
    # ------------------------------------------------------------------
    "Manufacturing": {
        "industry": "Industrials",
        "sector": "Manufacturing",
        "confidence": 0.95,
        "reason": "Explicit manufacturing classification",
    },
    "Engineering": {
        "industry": "Industrials",
        "sector": "Engineering",
        "confidence": 0.9,
        "reason": "General engineering businesses",
    },
    "Fabrications": {
        "industry": "Industrials",
        "sector": "Engineering",
        "confidence": 0.95,
        "reason": "Fabrication and machining workshops",
    },
    "Machinery": {
        "industry": "Industrials",
        "sector": "Equipment Sales / Hire",
        "confidence": 0.85,
        "reason": "Machinery sales, hire and servicing",
    },
    "Environmental/Energy": {
        "industry": "Industrials",
        "sector": "Energy / Utilities",
        "confidence": 0.9,
        "reason": "Environmental and energy services",
    },
    "Waste management & Recycling": {
        "industry": "Industrials",
        "sector": "Energy / Utilities",
        "confidence": 0.9,
        "reason": "Waste management and recycling services",
    },

    # ------------------------------------------------------------------
    # TECHNOLOGY
    # ------------------------------------------------------------------
    "IT Technology & Web": {
        "industry": "Technology",
        "sector": "IT Services / Software",
        "confidence": 0.9,
        "reason": "IT, web and digital technology businesses",
    },
    "IT Consultancy": {
        "industry": "Technology",
        "sector": "IT Consulting",
        "confidence": 0.95,
        "reason": "IT advisory and consultancy",
    },
    "IT Services & Support": {
        "industry": "Technology",
        "sector": "IT Services / Support",
        "confidence": 0.95,
        "reason": "Managed IT services and support",
    },
    "Software": {
        "industry": "Technology",
        "sector": "Software / SaaS",
        "confidence": 0.95,
        "reason": "Software-led businesses",
    },
    "Web Design & Development": {
        "industry": "Technology",
        "sector": "Software / Digital Services",
        "confidence": 0.9,
        "reason": "Web and digital development services",
    },
    "Telecommunications": {
        "industry": "Technology",
        "sector": "Telecommunications",
        "confidence": 0.95,
        "reason": "Telecoms and connectivity services",
    },
    "Audio & Visual": {
        "industry": "Technology",
        "sector": "Hardware / Systems",
        "confidence": 0.85,
        "reason": "AV systems and technology services",
    },

    # ------------------------------------------------------------------
    # LOGISTICS & DISTRIBUTION
    # ------------------------------------------------------------------
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
        "reason": "Removals and storage businesses",
    },
    "Import & Distribution": {
        "industry": "Logistics_Distribution",
        "sector": "Wholesale / Distribution",
        "confidence": 0.9,
        "reason": "Importers and distributors",
    },

    # ------------------------------------------------------------------
    # CONSUMER / RETAIL
    # ------------------------------------------------------------------
    "E-commerce": {
        "industry": "Consumer_Retail",
        "sector": "E-commerce",
        "confidence": 0.95,
        "reason": "Online retail and e-commerce businesses",
    },
    "Wholesale & Retail": {
        "industry": "Consumer_Retail",
        "sector": "Retail Stores",
        "confidence": 0.95,
        "reason": "Wholesale and retail operations",
    },
    "Leisure & Lifestyle": {
        "industry": "Consumer_Retail",
        "sector": "Leisure / Hospitality",
        "confidence": 0.9,
        "reason": "Consumer-facing leisure businesses",
    },
    "Vending Rounds": {
        "industry": "Consumer_Retail",
        "sector": "Retail Distribution",
        "confidence": 0.85,
        "reason": "Vending and route-based retail",
    },

    # ------------------------------------------------------------------
    # FOOD & BEVERAGE
    # ------------------------------------------------------------------
    "Food & Drink": {
        "industry": "Food_Beverage",
        "sector": "Food & Beverage",
        "confidence": 0.9,
        "reason": "Food and beverage businesses",
    },
    "Food Related": {
        "industry": "Food_Beverage",
        "sector": "Food Production / Services",
        "confidence": 0.85,
        "reason": "Food-related commercial activities",
    },
    "Breweries & Distilleries": {
        "industry": "Food_Beverage",
        "sector": "Beverage Production",
        "confidence": 0.95,
        "reason": "Alcohol production businesses",
    },

    # ------------------------------------------------------------------
    # HEALTHCARE
    # ------------------------------------------------------------------
    "Medical & Education": {
        "industry": "Healthcare",
        "sector": "Medical Devices / Services",
        "confidence": 0.75,
        "reason": "Mixed medical and education classification",
    },
    "Medical Service": {
        "industry": "Healthcare",
        "sector": "Healthcare Services",
        "confidence": 0.95,
        "reason": "Direct healthcare service providers",
    },
    "Care": {
        "industry": "Healthcare",
        "sector": None,
        "confidence": 0.3,
        "reason": "Broker taxonomy migration – top-level care bucket",
    },
    "Mobility Equipment": {
        "industry": "Healthcare",
        "sector": "Medical Devices",
        "confidence": 0.9,
        "reason": "Mobility and healthcare equipment",
    },
    "Funeral Services": {
        "industry": "Healthcare",
        "sector": "Social Care",
        "confidence": 0.85,
        "reason": "Funeral and end-of-life services",
    },

    # ------------------------------------------------------------------
    # EDUCATION
    # ------------------------------------------------------------------
    "Education Services": {
        "industry": "Education",
        "sector": "Education Services",
        "confidence": 0.9,
        "reason": "Education service providers",
    },
    "School/Training Centre": {
        "industry": "Education",
        "sector": "Schools / Training",
        "confidence": 0.9,
        "reason": "Schools and training centres",
    },
    "Training": {
        "industry": "Education",
        "sector": "Training / E-learning",
        "confidence": 0.9,
        "reason": "Standalone training providers",
    },

    # ------------------------------------------------------------------
    # FINANCIAL SERVICES
    # ------------------------------------------------------------------
    "Financial Services": {
        "industry": "Financial_Services",
        "sector": "Financial Advisory / Accounting",
        "confidence": 0.9,
        "reason": "Financial services firms",
    },
    "Professional & Financial Services": {
        "industry": "Business_Services",
        "sector": "Consulting / Professional Services",
        "confidence": 0.85,
        "reason": "Combined professional and financial services",
    },
    "Professional & Legal Services": {
        "industry": "Business_Services",
        "sector": "Legal / Professional Services",
        "confidence": 0.9,
        "reason": "Legal and professional services firms",
    },

    # ------------------------------------------------------------------
    # OTHER / EDGE
    # ------------------------------------------------------------------
    "Miscellaneous": {
        "industry": "Other",
        "sector": "Miscellaneous",
        "confidence": 0.6,
        "reason": "Explicit broker miscellaneous category",
    },
}

_CANONICAL_KB_MAP = {
    canonicalize_sector_key(k): v
    for k, v in KNIGHTSBRIDGE_SECTOR_MAP.items()
}

def resolve_knightsbridge_sector(sector_raw: str | None):
    if not sector_raw:
        return (
            "Other",
            None,
            "inferred",
            0.2,
            "Knightsbridge listing missing sector at source",
        )

    key = canonicalize_sector_key(sector_raw)
    mapping = _CANONICAL_KB_MAP.get(key)

    if not mapping:
        raise RuntimeError(f"UNMAPPED_KNIGHTSBRIDGE_SECTOR: {sector_raw}")

    return (
        mapping["industry"],
        mapping["sector"],
        "broker",
        mapping["confidence"],
        mapping["reason"],
    )