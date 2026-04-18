"""Detect when a source's HTML structure changes underneath us.

Each scraper depends on a small set of "anchor selectors" — the CSS hooks that
extract the body, title, date, and author. If a site redesigns, those selectors
stop matching and the scraper silently returns empty strings. This module
catches that drift before it corrupts the output.

Approach:
  1. `probe_selectors(html, source)` counts how many times each anchor matches.
  2. On the first successful run, counts are stored as the baseline.
  3. On later runs, any anchor that drops from ≥1 to 0 is flagged as drift.

The baseline is stored per-source in the `schema_baselines` table. It's updated
only when a run succeeds with all anchors present (conservative — we don't
learn a broken page as the new normal).
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field

from bs4 import BeautifulSoup
from loguru import logger

# Anchor selectors per source. These are the elements each scraper depends on.
# If any of these vanish on a real article page, the scraper is broken.
ANCHOR_SELECTORS: dict[str, list[str]] = {
    "inc42": [
        "h1.entry-title, h1",
        "div.entry-content, article .entry-content",
        'meta[property="article:published_time"]',
    ],
    "entrackr": [
        "h1",
        'script[type="application/ld+json"]',
        "div.entry-content, article p",
    ],
    "yourstory": [
        "h1",
        'script[type="application/ld+json"]',
        "article, main",
    ],
}


@dataclass
class DriftReport:
    source: str
    current: dict[str, int] = field(default_factory=dict)
    baseline: dict[str, int] = field(default_factory=dict)
    drifted: list[str] = field(default_factory=list)  # selectors that regressed

    @property
    def has_drift(self) -> bool:
        return bool(self.drifted)


def probe_selectors(html: str, source: str) -> dict[str, int]:
    """Return {selector: hit_count} for the source's anchor selectors."""
    if not html:
        return {}
    selectors = ANCHOR_SELECTORS.get(source, [])
    soup = BeautifulSoup(html, "lxml")
    return {sel: len(soup.select(sel)) for sel in selectors}


def load_baseline(conn: sqlite3.Connection, source: str) -> dict[str, int]:
    cur = conn.execute(
        "SELECT selector_key, baseline_hits FROM schema_baselines WHERE source = ?",
        (source,),
    )
    return {row["selector_key"]: row["baseline_hits"] for row in cur.fetchall()}


def save_baseline(
    conn: sqlite3.Connection,
    source: str,
    counts: dict[str, int],
    updated_at: str,
) -> None:
    for selector, hits in counts.items():
        conn.execute(
            """
            INSERT INTO schema_baselines (source, selector_key, baseline_hits, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(source, selector_key) DO UPDATE SET
                baseline_hits = excluded.baseline_hits,
                updated_at = excluded.updated_at
            """,
            (source, selector, hits, updated_at),
        )


def check_drift(
    conn: sqlite3.Connection,
    source: str,
    html: str,
) -> DriftReport:
    """Probe current HTML, compare against baseline, return a report.

    Drift criterion: a selector whose baseline was ≥1 but now returns 0. We
    don't flag increases — sites often add more DOM, that's fine.
    """
    report = DriftReport(source=source)
    report.current = probe_selectors(html, source)
    report.baseline = load_baseline(conn, source)
    for selector, baseline_hits in report.baseline.items():
        if baseline_hits >= 1 and report.current.get(selector, 0) == 0:
            report.drifted.append(selector)
    if report.drifted:
        logger.warning(
            "[schema-drift] {}: {} selector(s) regressed — {}",
            source, len(report.drifted), report.drifted,
        )
    return report


def update_baseline_if_healthy(
    conn: sqlite3.Connection,
    source: str,
    html: str,
    updated_at: str,
) -> bool:
    """Update baseline only if every anchor currently has ≥1 hit.

    Returns True if baseline was updated.
    """
    counts = probe_selectors(html, source)
    if not counts or any(v == 0 for v in counts.values()):
        return False
    save_baseline(conn, source, counts, updated_at)
    return True
