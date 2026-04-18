"""Deterministic extraction of funding fields from raw text.

Used when:
  1. OPENROUTER_API_KEY is not configured
  2. The LLM call fails
  3. The LLM response has confidence below a threshold

Aims for decent recall on common patterns in Indian startup news headlines,
not perfect coverage. The LLM is the primary path.
"""
from __future__ import annotations

import re
from datetime import date

from dateutil import parser as dateparse

from config.schemas import Currency, EnrichmentResult, FundingStage, Investor

# --- amount + currency --------------------------------------------------------

# $80 Mn / $8 million / $2.5M
_USD_RE = re.compile(
    r"\$\s*(?P<num>\d+(?:\.\d+)?)\s*(?P<unit>mn|m|million|bn|b|billion)\b",
    re.IGNORECASE,
)
# USD 8 million
_USD_WORD_RE = re.compile(
    r"\bUSD\s*(?P<num>\d+(?:\.\d+)?)\s*(?P<unit>mn|m|million|bn|b|billion)?",
    re.IGNORECASE,
)
# Rs 150 Cr / ₹22 Cr / INR 50 crore / Rs. 5 lakh
_INR_RE = re.compile(
    r"(?:₹|Rs\.?|INR)\s*(?P<num>\d+(?:[.,]\d+)?)\s*"
    r"(?P<unit>cr|crore|crores|lakh|lakhs|lac|lacs|mn|million|bn|billion)\b",
    re.IGNORECASE,
)

_USD_PER_INR = 1 / 83.0
_USD_PER_EUR = 1 / 0.92
_USD_PER_GBP = 1 / 0.79


def _unit_multiplier(unit: str) -> float:
    u = unit.lower()
    if u in ("mn", "m", "million"):
        return 1_000_000
    if u in ("bn", "b", "billion"):
        return 1_000_000_000
    if u in ("cr", "crore", "crores"):
        return 10_000_000
    if u in ("lakh", "lakhs", "lac", "lacs"):
        return 100_000
    return 1.0


def extract_amount(text: str) -> tuple[float | None, Currency | None, float | None]:
    """Return (amount_native, currency, amount_usd) if found."""
    # Try USD forms first (Indian press often quotes USD in parentheses)
    for regex, ccy in (
        (_USD_RE, Currency.USD),
        (_USD_WORD_RE, Currency.USD),
    ):
        m = regex.search(text)
        if m:
            num = float(m.group("num"))
            unit = m.group("unit") or "million"
            native = num * _unit_multiplier(unit)
            return native, ccy, native  # already USD

    m = _INR_RE.search(text)
    if m:
        num = float(m.group("num").replace(",", ""))
        native = num * _unit_multiplier(m.group("unit"))
        return native, Currency.INR, round(native * _USD_PER_INR, 2)

    return None, None, None


# --- stage --------------------------------------------------------------------

_STAGE_PATTERNS: list[tuple[re.Pattern[str], FundingStage]] = [
    (re.compile(r"\bpre[-\s]?series\s*a\b", re.I), FundingStage.PRE_SERIES_A),
    (re.compile(r"\bseries\s*a\b", re.I), FundingStage.SERIES_A),
    (re.compile(r"\bseries\s*b\b", re.I), FundingStage.SERIES_B),
    (re.compile(r"\bseries\s*c\b", re.I), FundingStage.SERIES_C),
    (re.compile(r"\bseries\s*d\b", re.I), FundingStage.SERIES_D),
    (re.compile(r"\bseries\s*(e|f|g|h)\b", re.I), FundingStage.SERIES_E_PLUS),
    (re.compile(r"\bpre[-\s]?seed\b", re.I), FundingStage.PRE_SEED),
    (re.compile(r"\bseed\s*(round|funding|investment)?\b", re.I), FundingStage.SEED),
    (re.compile(r"\bbridge\s*(round|funding)?\b", re.I), FundingStage.BRIDGE),
    (re.compile(r"\bdebt\s*(funding|round|financing)\b", re.I), FundingStage.DEBT),
    (re.compile(r"\bgrant\b", re.I), FundingStage.GRANT),
    (re.compile(r"\bIPO\b"), FundingStage.IPO),
    (re.compile(r"\bacqui(?:res|sition|red)\b", re.I), FundingStage.ACQUISITION),
]


def extract_stage(text: str) -> FundingStage:
    for pat, stage in _STAGE_PATTERNS:
        if pat.search(text):
            return stage
    return FundingStage.UNDISCLOSED


# --- company name from title --------------------------------------------------

_RAISE_VERBS = r"(?:raises|raised|bags|secures|secured|mops[-\s]up|closes|closed|nets)"
_TITLE_COMPANY_RE = re.compile(
    rf"^\s*(?P<name>[A-Z][\w&.\-]*(?:\s+[A-Z][\w&.\-]*){{0,4}})\s+{_RAISE_VERBS}\b",
    re.IGNORECASE,
)


def extract_company_name(title: str) -> str | None:
    """Best-effort: the capitalized word(s) before 'raises/bags/etc.'"""
    m = _TITLE_COMPANY_RE.search(title)
    if m:
        return m.group("name").strip()
    # Fallback: first 1-4 capitalized words if title starts with them
    toks = title.split()
    head = []
    for t in toks[:5]:
        if not t:
            break
        if t[0].isupper() or t[0].isdigit():
            head.append(t)
        else:
            break
    return " ".join(head) if head else None


# --- investors ----------------------------------------------------------------

_LED_BY_RE = re.compile(
    r"led\s+by\s+([A-Z][\w&.\-]*(?:\s+[A-Z][\w&.\-]*){0,4}"
    r"(?:\s+Ventures|\s+Capital|\s+Partners|\s+Fund|\s+Investments)?)",
)
_PARTICIPATION_RE = re.compile(
    r"(?:participation\s+from|with\s+participation\s+from|alongside|joined\s+by)\s+"
    r"([A-Z][\w&.\-]*(?:\s+[A-Z][\w&.\-]*){0,4})",
)


def extract_investors(text: str) -> list[Investor]:
    seen: dict[str, Investor] = {}
    m = _LED_BY_RE.search(text)
    if m:
        name = m.group(1).strip().rstrip(".,;")
        seen[name.lower()] = Investor(name=name, lead=True)
    for m in _PARTICIPATION_RE.finditer(text):
        name = m.group(1).strip().rstrip(".,;")
        key = name.lower()
        if key not in seen:
            seen[key] = Investor(name=name, lead=False)
    return list(seen.values())


# --- announced_on -------------------------------------------------------------


def extract_announced_on(
    published_at_hint: date | None,
    text: str,
) -> date | None:
    # Prefer the publish date of the article — news is typically same-day
    if published_at_hint:
        return published_at_hint
    # Try inline dates like "April 14, 2026" or "14 April 2026"
    m = re.search(
        r"\b(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{4})\b",
        text,
    )
    if m:
        try:
            return dateparse.parse(m.group(1)).date()
        except Exception:
            return None
    return None


# --- top-level ----------------------------------------------------------------


def regex_extract(
    title: str,
    text: str,
    published_at: date | None = None,
) -> EnrichmentResult:
    amount, currency, amount_usd = extract_amount(text + "\n" + title)
    stage = extract_stage(text + "\n" + title)
    company = extract_company_name(title)
    investors = extract_investors(text)
    announced_on = extract_announced_on(published_at, text)

    # Confidence heuristic: 0.2 baseline, +0.2 each for company/amount/stage/investors
    score = 0.2
    if company:
        score += 0.2
    if amount is not None:
        score += 0.2
    if stage is not FundingStage.UNDISCLOSED:
        score += 0.2
    if investors:
        score += 0.2
    score = min(score, 0.85)  # cap — regex is never fully trusted

    return EnrichmentResult(
        company_name=company,
        stage=stage,
        amount=amount,
        currency=currency,
        amount_usd=amount_usd,
        announced_on=announced_on,
        investors=investors,
        summary=None,
        confidence=score,
        extraction_method="regex_fallback",
    )
