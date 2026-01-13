from typing import Optional
from src.domain.industries import assert_valid_industry

# ------------------------------------------------------------------
# EXHAUSTIVE TRANSWORLD → CANONICAL INDUSTRY MAP
# ------------------------------------------------------------------

TRANSWORLD_CATEGORY_MAP: dict[str, tuple[str, str]] = {

    # -----------------------------
    # FINANCIAL SERVICES
    # -----------------------------
    "Accounting": ("Financial_Services", "Accounting / Auditing"),
    "Financial": ("Financial_Services", "Financial Services"),
    "Financial Related": ("Financial_Services", "Financial Services"),
    "Insurance": ("Financial_Services", "Insurance"),

    # -----------------------------
    # BUSINESS SERVICES
    # -----------------------------
    "Business Services": ("Business_Services", "Consulting / Professional Services"),
    "Consultancy": ("Business_Services", "Consulting / Professional Services"),
    "Consulting Business": ("Business_Services", "Consulting / Professional Services"),
    "Advertisement & Media Services": ("Business_Services", "Marketing / Advertising"),
    "Advertising": ("Business_Services", "Marketing / Advertising"),
    "Sales & Marketing": ("Business_Services", "Marketing / Advertising"),
    "Communications": ("Business_Services", "Marketing / Advertising"),
    "Personnel Services": ("Business_Services", "Recruitment / HR Services"),
    "Service Businesses": ("Business_Services", "Consulting / Professional Services"),
    "Professnl Practices": ("Business_Services", "Consulting / Professional Services"),
    "Personal Services": ("Business_Services", "Consulting / Professional Services"),
    "Cleaning": ("Business_Services", "Facilities Management"),
    "Cleaning/Clothing": ("Business_Services", "Facilities Management"),
    "Security Related": ("Business_Services", "Facilities Management"),
    "Pest Control": ("Business_Services", "Facilities Management"),
    "Locksmith": ("Business_Services", "Facilities Management"),
    "Repair": ("Business_Services", "Facilities Management"),

    # -----------------------------
    # TECHNOLOGY
    # -----------------------------
    "Information Technology": ("Technology", "IT Consulting"),
    "IT Services": ("Technology", "IT Consulting"),
    "Internet Related": ("Technology", "Software / SaaS"),
    "Electronics/Computer": ("Technology", "Hardware"),
    "Consumer Durables & IT": ("Technology", "Hardware"),
    "Telephone & Related": ("Technology", "Telecommunications"),

    # -----------------------------
    # INDUSTRIALS
    # -----------------------------
    "Engineering": ("Industrials", "Engineering"),
    "Manufacturing": ("Industrials", "Manufacturing"),
    "Machine Shop": ("Industrials", "Manufacturing"),
    "Metal Related": ("Industrials", "Manufacturing"),
    "Automotive": ("Industrials", "Manufacturing"),
    "Aviation": ("Industrials", "Engineering"),
    "Marine Related": ("Industrials", "Engineering"),
    "Energy and Power": ("Industrials", "Energy / Utilities"),
    "Equipment S & S": ("Industrials", "Equipment Rental / Leasing"),
    "Glass": ("Industrials", "Manufacturing"),
    "Firearms": ("Industrials", "Manufacturing"),

    # -----------------------------
    # CONSTRUCTION / BUILT ENVIRONMENT
    # -----------------------------
    "Construction": ("Construction_Built_Environment", "Construction Contractors"),
    "Building and Construction Businesses": ("Construction_Built_Environment", "Construction Contractors"),
    "Building Materials": ("Construction_Built_Environment", "Building Materials"),
    "Flooring": ("Construction_Built_Environment", "Construction Contractors"),
    "Interior Design/Dec": ("Construction_Built_Environment", "Architecture / Design"),
    "Commercial Property": ("Construction_Built_Environment", "Property Development"),
    "Real Estate": ("Construction_Built_Environment", "Property Development"),
    "Real Estate Related": ("Construction_Built_Environment", "Property Development"),
    "Real Property Rltd": ("Construction_Built_Environment", "Property Development"),
    "Rental Business": ("Construction_Built_Environment", "Property Development"),

    # -----------------------------
    # CONSUMER & RETAIL
    # -----------------------------
    "Retail Businesses": ("Consumer_Retail", "Retail Stores"),
    "Retail Miscellaneous": ("Consumer_Retail", "Retail Stores"),
    "Convenience Stores": ("Consumer_Retail", "Retail Stores"),
    "Supermarkets & Marts": ("Consumer_Retail", "Food & Beverage Retail"),
    "Food Business Retail": ("Consumer_Retail", "Food & Beverage Retail"),
    "Clothing": ("Consumer_Retail", "Retail Stores"),
    "Shoes/Footwear": ("Consumer_Retail", "Retail Stores"),
    "Jewelry": ("Consumer_Retail", "Retail Stores"),
    "Beauty/Personal Care": ("Consumer_Retail", "Retail Stores"),
    "Beauty Salons & Supplies": ("Consumer_Retail", "Retail Stores"),
    "Furniture Retail": ("Consumer_Retail", "Retail Stores"),
    "Books, Toys & Gifts": ("Consumer_Retail", "Retail Stores"),
    "Cards/Gifts/Books": ("Consumer_Retail", "Retail Stores"),
    "Toys": ("Consumer_Retail", "Retail Stores"),
    "Office Supplies": ("Consumer_Retail", "Retail Stores"),
    "Mail Order": ("Consumer_Retail", "E-commerce"),

    # -----------------------------
    # FOOD & BEVERAGE
    # -----------------------------
    "Restaurants": ("Food_Beverage", "Catering / Hospitality"),
    "Catering": ("Food_Beverage", "Catering / Hospitality"),
    "Ice Cream Yogurt Ice": ("Food_Beverage", "Food Retail"),
    "Liquor Related Biz": ("Food_Beverage", "Food & Beverage Retail"),

    # -----------------------------
    # HEALTHCARE
    # -----------------------------
    "Care Businesses": ("Healthcare", "Domiciliary Care / Home Healthcare"),
    "Child Care": ("Healthcare", "Social Care"),
    "Care": ("Healthcare", "Domiciliary Care / Home Healthcare"),
    "Health Care & Fitness": ("Healthcare", "Healthcare Services"),
    "Medical Related Biz": ("Healthcare", "Medical Devices / Equipment"),
    "Dental Related": ("Healthcare", "Medical Devices / Equipment"),
    "Fitness": ("Healthcare", "Healthcare Services"),

    # -----------------------------
    # LOGISTICS & DISTRIBUTION
    # -----------------------------
    "Logistics": ("Logistics_Distribution", "Supply Chain Management"),
    "Distribution": ("Logistics_Distribution", "Warehousing"),
    "Distribution Businesses": ("Logistics_Distribution", "Warehousing"),
    "Dealers & Distributors": ("Logistics_Distribution", "Warehousing"),
    "Delivery": ("Logistics_Distribution", "Courier / Delivery Services"),
    "Transportation": ("Logistics_Distribution", "Freight / Shipping"),
    "Motor and Transport Businesses": ("Logistics_Distribution", "Freight / Shipping"),
    "Moving": ("Logistics_Distribution", "Freight / Shipping"),
    "Import/Export": ("Logistics_Distribution", "Freight / Shipping"),
    "Pack/Ship/Postal": ("Logistics_Distribution", "Courier / Delivery Services"),
    "Routes": ("Logistics_Distribution", "Courier / Delivery Services"),

    # -----------------------------
    # EDUCATION
    # -----------------------------
    "Educational/School": ("Education", "Schools / Colleges"),
    "K-12 Education": ("Education", "Schools / Colleges"),
    "Higher Education": ("Education", "Higher Education"),
    "Early Education": ("Education", "Schools / Colleges"),
    "Vocational Training": ("Education", "Training / E-learning"),
    "Online Education": ("Education", "Training / E-learning"),
    "Coaching & Tutoring": ("Education", "Training / E-learning"),
    "Education Consultants": ("Education", "Training / E-learning"),

    # -----------------------------
    # LEISURE / MEDIA
    # -----------------------------
    "Entertainment and Leisure businesses": ("Consumer_Retail", "Leisure / Hospitality"),
    "Recreation": ("Consumer_Retail", "Leisure / Hospitality"),
    "Travel": ("Consumer_Retail", "Leisure / Hospitality"),
    "Music": ("Consumer_Retail", "Leisure / Hospitality"),
    "Photography": ("Consumer_Retail", "Leisure / Hospitality"),
    "Publishing": ("Other", "Media / Publishing"),
    "Newspaper/Magazines": ("Other", "Media / Publishing"),
    "Art": ("Other", "Media / Publishing"),
    "Antiques": ("Other", "Media / Publishing"),

    # -----------------------------
    # AGRICULTURE / OTHER
    # -----------------------------
    "Agricultural": ("Other", "Agriculture / Farming"),
    "Agricultural and Gardening Businesses": ("Other", "Agriculture / Farming"),
    "Animal and Pet Businesses": ("Other", "Miscellaneous"),
    "Animals/Pets": ("Other", "Miscellaneous"),
    "Lawn/Landscaping": ("Other", "Miscellaneous"),
    "Water Related": ("Other", "Miscellaneous"),
    "Environmental Rltd": ("Other", "Miscellaneous"),
    "Mobile Homes": ("Other", "Miscellaneous"),
    "Vending Related": ("Other", "Miscellaneous"),
    "Upholstery/Fabrics": ("Other", "Miscellaneous"),
    "Signs": ("Other", "Miscellaneous"),
    "CountUpdate": ("Other", "Miscellaneous"),
    "Miscellaneous/Other": ("Other", "Miscellaneous"),
    "Home Based Business": ("Other", "Miscellaneous"),
    "Start Up Businesses": ("Other", "Miscellaneous"),
    "New Franchises": ("Other", "Miscellaneous"),
}

# ------------------------------------------------------------------
# TITLE-BASED FALLBACK (USED ONLY IF sector_raw IS EMPTY)
# ------------------------------------------------------------------

TITLE_CATEGORY_HINTS: dict[str, str] = {
    "restaurant": "Restaurants",
    "dining": "Restaurants",
    "cafe": "Restaurants",
    "bar": "Restaurants",
    "venue": "Restaurants",
    "care": "Care Businesses",
    "domiciliary": "Care Businesses",
    "healthcare": "Health Care & Fitness",
    "health": "Health Care & Fitness",
    "property": "Real Estate",
    "real estate": "Real Estate",
    "manufacturing": "Manufacturing",
    "engineering": "Engineering",
    "automotive": "Automotive",
    "logistics": "Logistics",
    "distribution": "Distribution",
    "vineyard": "Liquor Related Biz",
    "promotional merchandise": "Sales & Marketing",
}

# ------------------------------------------------------------------
# PUBLIC API
# ------------------------------------------------------------------

def infer_category_from_title(title: str) -> Optional[str]:
    t = title.lower()
    for keyword, category in TITLE_CATEGORY_HINTS.items():
        if keyword in t:
            return category
    return None


def map_transworld_category(sector_raw: str | None, title: str | None) -> dict:
    if sector_raw:
        mapped = TRANSWORLD_CATEGORY_MAP.get(sector_raw)
        if mapped:
            industry, sector = mapped
            assert_valid_industry(industry)
            return {
                "industry": industry,
                "sector": sector,
                "confidence": 0.95,
                "reason": "Broker category (Transworld)",
            }

    if title:
        inferred = infer_category_from_title(title)
        if inferred:
            mapped = TRANSWORLD_CATEGORY_MAP.get(inferred)
            if mapped:
                industry, sector = mapped
                assert_valid_industry(industry)
                return {
                    "industry": industry,
                    "sector": sector,
                    "confidence": 0.60,
                    "reason": "Title keyword inference",
                }

    return {
        "industry": "Other",
        "sector": "Miscellaneous",
        "confidence": 0.0,
        "reason": "Unmapped category",
    }