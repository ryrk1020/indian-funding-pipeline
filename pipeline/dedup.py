"""Cross-source deduplication for funding rounds.

The deterministic `round_id = sha256(company|date|amount_usd)` already collapses
exact matches. This module catches near-matches: same event reported with
slight variations.

Typical variations we see in Indian startup news:
  - "BlueStone" vs "Bluestone" vs "Bluestone Jewellery Pvt Ltd"
  - amount in USD vs INR (Inc42 writes $18M, Entrackr writes Rs 150 Cr)
  - announced_on off by 1 day across timezones

Match rules (all three must hold):
  1. Normalized company names fuzzy-match (rapidfuzz token_set_ratio ≥ 88)
  2. announced_on within ±3 days (or either side missing)
  3. amount_usd within 15% (or either side missing)

If a match is found, the new round adopts the existing round_id. Upsert then
merges sources and investors onto the original.
"""
from __future__ import annotations

import re
import sqlite3
from datetime import date

from rapidfuzz import fuzz

from config.schemas import FundingRound

_SUFFIX_RE = re.compile(
    r"\b("
    r"private\s+limited|pvt\.?\s*ltd\.?|"
    r"limited|ltd\.?|"
    r"incorporated|inc\.?|"
    r"corporation|corp\.?|"
    r"technologies|tech|"
    r"labs|solutions|"
    r"india"
    r")\b",
    re.IGNORECASE,
)
_PUNCT_RE = re.compile(r"[^\w\s]")


def normalize_company_name(name: str) -> str:
    """Lowercase, strip punctuation + common corporate suffixes, collapse spaces."""
    n = name.lower()
    n = _SUFFIX_RE.sub(" ", n)
    n = _PUNCT_RE.sub(" ", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n


def _dates_close(a: date | None, b: date | None, window_days: int = 3) -> bool:
    if a is None or b is None:
        return True
    return abs((a - b).days) <= window_days


def _amounts_close(a: float | None, b: float | None, rel_tol: float = 0.15) -> bool:
    if a is None or b is None:
        return True
    if a == 0 and b == 0:
        return True
    larger = max(a, b)
    return abs(a - b) / larger <= rel_tol


def find_existing_round_id(
    conn: sqlite3.Connection,
    candidate: FundingRound,
    name_threshold: int = 88,
) -> str | None:
    """Return an existing round_id to merge into, or None for a new round.

    Scans the funding_rounds table. O(N) is fine — for a portfolio project with
    hundreds of rounds, a SQL prefilter by first letter + fuzzy compare is cheap.
    """
    cand_norm = normalize_company_name(candidate.company.name)
    if not cand_norm:
        return None

    # Full scan — prefiltering on first-letter doesn't work when names have
    # stopword prefixes ("The Hosteller" vs "Hosteller" differ on char[0]).
    # For the portfolio-scale corpus (hundreds of rounds) this is fine.
    cur = conn.execute(
        "SELECT round_id, company_name, announced_on, amount_usd FROM funding_rounds"
    )
    best_id: str | None = None
    best_score = 0
    for row in cur.fetchall():
        existing_name = row["company_name"] or ""
        existing_norm = normalize_company_name(existing_name)
        if not existing_norm:
            continue
        score = fuzz.token_set_ratio(cand_norm, existing_norm)
        if score < name_threshold:
            continue
        existing_date = (
            date.fromisoformat(row["announced_on"]) if row["announced_on"] else None
        )
        if not _dates_close(candidate.announced_on, existing_date):
            continue
        if not _amounts_close(candidate.amount_usd, row["amount_usd"]):
            continue
        if score > best_score:
            best_score = score
            best_id = row["round_id"]
    return best_id
