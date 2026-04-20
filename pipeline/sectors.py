"""Canonical sector taxonomy + normalization.

LLMs emit sector as free text ("fintech", "financial services", "payments",
"SaaS platform for HR", etc). For analytics we want a fixed vocabulary: 24
buckets that cover ~95% of Indian startup output, plus an explicit "Other".

`normalize_sector()` is cheap: exact-match on aliases first, then rapidfuzz
token_set_ratio against known terms, with a conservative threshold. Anything
genuinely ambiguous → "other" rather than a wrong bucket.
"""
from __future__ import annotations

from rapidfuzz import fuzz, process

# Canonical slugs → display labels. Stored slug = what goes in the DB.
SECTOR_LABELS: dict[str, str] = {
    "fintech": "FinTech",
    "saas": "SaaS",
    "healthtech": "HealthTech",
    "edtech": "EdTech",
    "agritech": "AgriTech",
    "d2c": "D2C",
    "ecommerce": "E-Commerce",
    "logistics": "Logistics",
    "mobility": "Mobility",
    "climatetech": "ClimateTech",
    "gaming": "Gaming",
    "ai_ml": "AI/ML",
    "cybersecurity": "Cybersecurity",
    "deeptech": "DeepTech",
    "media": "Media & Content",
    "foodtech": "FoodTech",
    "proptech": "PropTech",
    "hrtech": "HRTech",
    "legaltech": "LegalTech",
    "retail": "Retail",
    "traveltech": "TravelTech",
    "spacetech": "SpaceTech",
    "web3": "Web3",
    "manufacturing": "Manufacturing",
    "other": "Other",
}

# Alias → canonical slug. Keys are lowercase normalized phrases the LLM might
# emit. The fuzzy fallback handles near-misses ("fin-tech", "financetech").
SECTOR_ALIASES: dict[str, str] = {
    # fintech family
    "fintech": "fintech",
    "financial technology": "fintech",
    "financial services": "fintech",
    "finance": "fintech",
    "payments": "fintech",
    "payment": "fintech",
    "lending": "fintech",
    "credit": "fintech",
    "banking": "fintech",
    "neobank": "fintech",
    "insurtech": "fintech",
    "insurance": "fintech",
    "wealthtech": "fintech",
    "investtech": "fintech",
    "crypto": "fintech",
    # saas
    "saas": "saas",
    "software": "saas",
    "enterprise software": "saas",
    "b2b saas": "saas",
    "productivity": "saas",
    "collaboration": "saas",
    "devtools": "saas",
    "developer tools": "saas",
    "no-code": "saas",
    "low-code": "saas",
    # healthtech
    "healthtech": "healthtech",
    "health tech": "healthtech",
    "healthcare": "healthtech",
    "health": "healthtech",
    "medtech": "healthtech",
    "medical devices": "healthtech",
    "pharma": "healthtech",
    "biotech": "healthtech",
    "biotechnology": "healthtech",
    "wellness": "healthtech",
    "mental health": "healthtech",
    "diagnostics": "healthtech",
    # edtech
    "edtech": "edtech",
    "education": "edtech",
    "e-learning": "edtech",
    "learning": "edtech",
    "upskilling": "edtech",
    "test prep": "edtech",
    # agritech
    "agritech": "agritech",
    "agriculture": "agritech",
    "agri": "agritech",
    "farming": "agritech",
    "agtech": "agritech",
    # d2c
    "d2c": "d2c",
    "dtc": "d2c",
    "direct-to-consumer": "d2c",
    "direct to consumer": "d2c",
    "consumer brand": "d2c",
    "consumer goods": "d2c",
    "fmcg": "d2c",
    "beauty": "d2c",
    "personal care": "d2c",
    "apparel": "d2c",
    # ecommerce
    "ecommerce": "ecommerce",
    "e-commerce": "ecommerce",
    "marketplace": "ecommerce",
    "online retail": "ecommerce",
    # logistics
    "logistics": "logistics",
    "supply chain": "logistics",
    "shipping": "logistics",
    "freight": "logistics",
    "delivery": "logistics",
    "last-mile": "logistics",
    "warehousing": "logistics",
    # mobility
    "mobility": "mobility",
    "electric vehicle": "mobility",
    "electric vehicles": "mobility",
    "ev": "mobility",
    "automotive": "mobility",
    "ride-hailing": "mobility",
    "transportation": "mobility",
    # climatetech
    "climatetech": "climatetech",
    "climate tech": "climatetech",
    "cleantech": "climatetech",
    "clean tech": "climatetech",
    "renewable energy": "climatetech",
    "solar": "climatetech",
    "sustainability": "climatetech",
    "green energy": "climatetech",
    "energy": "climatetech",
    # gaming
    "gaming": "gaming",
    "games": "gaming",
    "esports": "gaming",
    "fantasy sports": "gaming",
    # ai_ml
    "ai": "ai_ml",
    "ai/ml": "ai_ml",
    "artificial intelligence": "ai_ml",
    "machine learning": "ai_ml",
    "genai": "ai_ml",
    "generative ai": "ai_ml",
    "llm": "ai_ml",
    # cybersecurity
    "cybersecurity": "cybersecurity",
    "cyber security": "cybersecurity",
    "security": "cybersecurity",
    "infosec": "cybersecurity",
    # deeptech
    "deeptech": "deeptech",
    "deep tech": "deeptech",
    "hardware": "deeptech",
    "robotics": "deeptech",
    "iot": "deeptech",
    "semiconductor": "deeptech",
    "semiconductors": "deeptech",
    # media
    "media": "media",
    "content": "media",
    "ott": "media",
    "creator economy": "media",
    "entertainment": "media",
    "streaming": "media",
    "social media": "media",
    # foodtech
    "foodtech": "foodtech",
    "food tech": "foodtech",
    "food": "foodtech",
    "food delivery": "foodtech",
    "cloud kitchen": "foodtech",
    "restaurant tech": "foodtech",
    "beverage": "foodtech",
    # proptech
    "proptech": "proptech",
    "real estate": "proptech",
    "property": "proptech",
    "housing": "proptech",
    "construction tech": "proptech",
    # hrtech
    "hrtech": "hrtech",
    "hr tech": "hrtech",
    "hr": "hrtech",
    "human resources": "hrtech",
    "recruitment": "hrtech",
    "hiring": "hrtech",
    "staffing": "hrtech",
    # legaltech
    "legaltech": "legaltech",
    "legal tech": "legaltech",
    "legal": "legaltech",
    "regtech": "legaltech",
    "compliance": "legaltech",
    # retail
    "retail": "retail",
    "omnichannel retail": "retail",
    # traveltech
    "traveltech": "traveltech",
    "travel tech": "traveltech",
    "travel": "traveltech",
    "tourism": "traveltech",
    "hospitality": "traveltech",
    # spacetech
    "spacetech": "spacetech",
    "space tech": "spacetech",
    "space": "spacetech",
    "aerospace": "spacetech",
    "satellite": "spacetech",
    "defence": "spacetech",
    "defense": "spacetech",
    # web3
    "web3": "web3",
    "web 3": "web3",
    "blockchain": "web3",
    "nft": "web3",
    # manufacturing
    "manufacturing": "manufacturing",
    "industrial": "manufacturing",
    "industry 4.0": "manufacturing",
}

_FUZZY_THRESHOLD = 85  # only map if we're reasonably sure


def normalize_sector(raw: str | None) -> str | None:
    """Map a free-text sector string → canonical slug, or None if empty."""
    if not raw:
        return None
    norm = raw.strip().lower()
    if not norm:
        return None
    # exact alias hit
    if norm in SECTOR_ALIASES:
        return SECTOR_ALIASES[norm]
    # strip common decoration: "fintech platform" → "fintech"
    for suffix in (" platform", " startup", " company", " industry", " sector", " space"):
        if norm.endswith(suffix):
            stripped = norm[: -len(suffix)].strip()
            if stripped in SECTOR_ALIASES:
                return SECTOR_ALIASES[stripped]
    # fuzzy fallback against aliases
    match = process.extractOne(norm, SECTOR_ALIASES.keys(), scorer=fuzz.token_set_ratio)
    if match and match[1] >= _FUZZY_THRESHOLD:
        return SECTOR_ALIASES[match[0]]
    return "other"


def sector_label(slug: str | None) -> str:
    """Display label for a canonical slug."""
    if not slug:
        return ""
    return SECTOR_LABELS.get(slug, slug.replace("_", " ").title())


def sector_choices_for_prompt() -> str:
    """Human-readable list injected into the LLM system prompt."""
    return ", ".join(sorted(SECTOR_LABELS.keys()))
