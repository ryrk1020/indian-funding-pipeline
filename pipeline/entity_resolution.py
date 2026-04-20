"""Canonicalize company and investor names across sources.

Why: Inc42 says "Hocco", Entrackr says "Hocco Ice Cream Pvt Ltd", VCCircle
says "Hocco Inc." — three rows, one company. Same for investors (Accel vs
Accel Partners vs Accel India).

Strategy: an alias table per entity type. On each insert we:
  1. Normalize the raw name (lowercase, strip punct + corporate suffixes).
  2. Look up the normalized form in the alias table. Hit → use canonical.
  3. Else fuzzy-match against known canonicals (rapidfuzz token_set_ratio).
     Strong match (≥92) → register alias + use canonical. Else this becomes
     a new canonical entity.

The alias table is opaque in the DB (no FK), deliberately — it's a resolver,
not a relationship. Operators can hand-edit rows to fix misresolution.
"""
from __future__ import annotations

import re
import sqlite3
from datetime import UTC, datetime

from rapidfuzz import fuzz, process

from pipeline.dedup import normalize_company_name

# Investors rarely carry "Pvt Ltd" but do carry "Ventures", "Capital", etc.
# We don't strip those — "Accel" and "Accel Ventures" may be different firms.
# Keep investor normalization light: lowercase + punct + whitespace.
_INVESTOR_PUNCT_RE = re.compile(r"[^\w\s&+\-]")
_INVESTOR_SUFFIX_RE = re.compile(
    r"\b(llp|llc|pvt\.?\s*ltd\.?|private\s+limited|limited|ltd\.?|inc\.?|corp\.?|corporation)\b",
    re.IGNORECASE,
)

COMPANY_FUZZY_THRESHOLD = 92


def normalize_investor_name(name: str) -> str:
    """Lowercase, strip legal suffixes, collapse whitespace."""
    n = name.lower()
    n = _INVESTOR_SUFFIX_RE.sub(" ", n)
    n = _INVESTOR_PUNCT_RE.sub(" ", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n


def _iso_now() -> str:
    return datetime.now(UTC).replace(tzinfo=None).isoformat()


def _record_alias(conn: sqlite3.Connection, table: str, alias_norm: str, canonical: str) -> None:
    if not alias_norm or not canonical:
        return
    conn.execute(
        f"INSERT OR IGNORE INTO {table} (alias_norm, canonical, created_at) VALUES (?, ?, ?)",
        (alias_norm, canonical, _iso_now()),
    )


def _known_canonicals(conn: sqlite3.Connection, kind: str) -> list[str]:
    if kind == "company":
        rows = conn.execute(
            "SELECT DISTINCT company_name FROM funding_rounds WHERE company_name IS NOT NULL"
        ).fetchall()
        return [r["company_name"] for r in rows if r["company_name"]]
    if kind == "investor":
        rows = conn.execute(
            "SELECT DISTINCT name FROM round_investors WHERE name IS NOT NULL"
        ).fetchall()
        return [r["name"] for r in rows if r["name"]]
    return []


def resolve_company(conn: sqlite3.Connection, raw_name: str) -> str:
    """Return the canonical company name to use for `raw_name`.

    Does NOT modify funding_rounds; callers use the returned name when
    inserting. Registers an alias row as a side effect so subsequent lookups
    are O(1).
    """
    raw_name = raw_name.strip()
    if not raw_name:
        return raw_name

    alias_norm = normalize_company_name(raw_name)
    if not alias_norm:
        return raw_name

    # 1. alias table hit?
    row = conn.execute(
        "SELECT canonical FROM company_aliases WHERE alias_norm = ?", (alias_norm,)
    ).fetchone()
    if row:
        return row["canonical"]

    # 2. fuzzy match against existing canonicals
    candidates = _known_canonicals(conn, "company")
    if candidates:
        norm_to_canon = {normalize_company_name(c): c for c in candidates if c}
        match = process.extractOne(
            alias_norm, norm_to_canon.keys(), scorer=fuzz.token_set_ratio
        )
        if match and match[1] >= COMPANY_FUZZY_THRESHOLD:
            canonical = norm_to_canon[match[0]]
            _record_alias(conn, "company_aliases", alias_norm, canonical)
            return canonical

    # 3. new entity — raw_name IS the canonical. Record alias so next spelling
    #    variant resolves back here.
    _record_alias(conn, "company_aliases", alias_norm, raw_name)
    return raw_name


def resolve_investor(conn: sqlite3.Connection, raw_name: str) -> str:
    """Return the canonical investor name for `raw_name`.

    Investor resolution is intentionally tighter than company resolution:
    only exact matches after normalization (plus the seed alias table)
    collapse two strings to one canonical. Fuzzy matching is too permissive
    here — "IIFL" would absorb "IIFL. Aliste Technologies" under
    token_set_ratio, and investor identity hinges on the full name.
    """
    raw_name = raw_name.strip()
    if not raw_name:
        return raw_name

    alias_norm = normalize_investor_name(raw_name)
    if not alias_norm:
        return raw_name

    row = conn.execute(
        "SELECT canonical FROM investor_aliases WHERE alias_norm = ?", (alias_norm,)
    ).fetchone()
    if row:
        return row["canonical"]

    # Exact-normalized match against existing canonicals (catches case differences
    # and trailing legal suffixes like "LLP", "Pvt Ltd" that we strip).
    candidates = _known_canonicals(conn, "investor")
    for c in candidates:
        if normalize_investor_name(c) == alias_norm:
            _record_alias(conn, "investor_aliases", alias_norm, c)
            return c

    _record_alias(conn, "investor_aliases", alias_norm, raw_name)
    return raw_name


# Seed aliases for clear cases where the canonical may not yet exist in
# funding_rounds but we still want consistent naming. Applied on first run.
SEED_INVESTOR_ALIASES: dict[str, str] = {
    "accel": "Accel",
    "accel partners": "Accel",
    "accel india": "Accel",
    "peak xv": "Peak XV Partners",
    "peak xv partners": "Peak XV Partners",
    "sequoia capital india": "Peak XV Partners",
    "sequoia india": "Peak XV Partners",
    "blume": "Blume Ventures",
    "blume ventures": "Blume Ventures",
    "matrix partners india": "Z47",
    "matrix partners": "Z47",
    "z47": "Z47",
    "elevation capital": "Elevation Capital",
    "saif partners": "Elevation Capital",
    "3one4 capital": "3one4 Capital",
    "3one4": "3one4 Capital",
    "lightspeed india partners": "Lightspeed India",
    "lightspeed india": "Lightspeed India",
    "lightspeed venture partners": "Lightspeed",
    "nexus venture partners": "Nexus Venture Partners",
    "nexus venture": "Nexus Venture Partners",
    "ivycap ventures": "IvyCap Ventures",
    "ivycap": "IvyCap Ventures",
    "kalaari capital": "Kalaari Capital",
    "kalaari": "Kalaari Capital",
    "tiger global": "Tiger Global",
    "tiger global management": "Tiger Global",
}


def seed_aliases(conn: sqlite3.Connection) -> int:
    """Populate the investor_aliases table with well-known canonicalizations.

    Idempotent — uses INSERT OR IGNORE. Called once at startup by storage.
    """
    inserted = 0
    for raw_alias, canonical in SEED_INVESTOR_ALIASES.items():
        cur = conn.execute(
            "INSERT OR IGNORE INTO investor_aliases (alias_norm, canonical, created_at) "
            "VALUES (?, ?, ?)",
            (raw_alias, canonical, _iso_now()),
        )
        inserted += cur.rowcount or 0
    return inserted
