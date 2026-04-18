"""SQLite persistence. Synchronous — SQLite is cheap and our writes are batched.

Tables:
  articles       — one row per scraped article (raw text + metadata)
  funding_rounds — deduplicated canonical rounds (post-enrichment)
  round_sources  — M:N join (round_id ↔ article_url)
  investors      — investors per round
  run_log        — one row per (run_id, source)

Article URL is the natural idempotency key. `INSERT OR REPLACE` keeps re-runs safe.
"""
from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Any

from loguru import logger

from config.schemas import ArticleRaw, FundingRound, RunRecord
from config.settings import settings

SCHEMA = """
CREATE TABLE IF NOT EXISTS articles (
    article_id     TEXT PRIMARY KEY,
    source         TEXT NOT NULL,
    url            TEXT NOT NULL UNIQUE,
    title          TEXT NOT NULL,
    author         TEXT,
    published_at   TEXT,
    fetched_at     TEXT NOT NULL,
    text           TEXT NOT NULL,
    html           TEXT,
    enrichment_json TEXT,
    enrichment_status TEXT DEFAULT 'pending'   -- pending | ok | failed | fallback
);
CREATE INDEX IF NOT EXISTS idx_articles_source ON articles(source);
CREATE INDEX IF NOT EXISTS idx_articles_status ON articles(enrichment_status);

CREATE TABLE IF NOT EXISTS funding_rounds (
    round_id       TEXT PRIMARY KEY,
    company_name   TEXT NOT NULL,
    company_json   TEXT NOT NULL,
    stage          TEXT,
    amount         REAL,
    currency       TEXT,
    amount_usd     REAL,
    announced_on   TEXT,
    summary        TEXT,
    confidence     REAL,
    extraction_method TEXT,
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_rounds_company ON funding_rounds(company_name);
CREATE INDEX IF NOT EXISTS idx_rounds_announced ON funding_rounds(announced_on);

CREATE TABLE IF NOT EXISTS round_sources (
    round_id     TEXT NOT NULL,
    article_url  TEXT NOT NULL,
    PRIMARY KEY (round_id, article_url),
    FOREIGN KEY (round_id)    REFERENCES funding_rounds(round_id) ON DELETE CASCADE,
    FOREIGN KEY (article_url) REFERENCES articles(url)            ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS round_investors (
    round_id   TEXT NOT NULL,
    name       TEXT NOT NULL,
    lead       INTEGER DEFAULT 0,
    PRIMARY KEY (round_id, name),
    FOREIGN KEY (round_id) REFERENCES funding_rounds(round_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS run_log (
    run_id              TEXT NOT NULL,
    source              TEXT NOT NULL,
    started_at          TEXT NOT NULL,
    finished_at         TEXT,
    articles_seen       INTEGER DEFAULT 0,
    articles_new        INTEGER DEFAULT 0,
    articles_failed     INTEGER DEFAULT 0,
    enriched_ok         INTEGER DEFAULT 0,
    enriched_failed     INTEGER DEFAULT 0,
    schema_drift_flag   INTEGER DEFAULT 0,
    error               TEXT,
    PRIMARY KEY (run_id, source)
);

CREATE TABLE IF NOT EXISTS schema_baselines (
    source          TEXT NOT NULL,
    selector_key    TEXT NOT NULL,
    baseline_hits   INTEGER NOT NULL,
    updated_at      TEXT NOT NULL,
    PRIMARY KEY (source, selector_key)
);
"""


def _iso(dt: datetime | date | None) -> str | None:
    if dt is None:
        return None
    return dt.isoformat()


class Storage:
    def __init__(self, db_path: Path | str | None = None) -> None:
        self.db_path = Path(db_path) if db_path else settings.db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init()

    def _init(self) -> None:
        with self.connect() as c:
            c.executescript(SCHEMA)

    @contextmanager
    def connect(self):
        conn = sqlite3.connect(self.db_path, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        try:
            yield conn
        finally:
            conn.close()

    # --- articles -----------------------------------------------------------
    def upsert_article(self, a: ArticleRaw) -> bool:
        """Return True if inserted new, False if already present."""
        with self.connect() as c:
            cur = c.execute("SELECT 1 FROM articles WHERE url = ?", (str(a.url),))
            existed = cur.fetchone() is not None
            c.execute(
                """
                INSERT INTO articles (article_id, source, url, title, author,
                    published_at, fetched_at, text, html, enrichment_status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE(
                    (SELECT enrichment_status FROM articles WHERE url = ?), 'pending'))
                ON CONFLICT(url) DO UPDATE SET
                    title = excluded.title,
                    author = excluded.author,
                    published_at = excluded.published_at,
                    fetched_at = excluded.fetched_at,
                    text = excluded.text,
                    html = excluded.html
                """,
                (
                    a.article_id,
                    a.source,
                    str(a.url),
                    a.title,
                    a.author,
                    _iso(a.published_at),
                    _iso(a.fetched_at),
                    a.text,
                    a.html,
                    str(a.url),
                ),
            )
            return not existed

    def pending_articles(self, source: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        q = "SELECT * FROM articles WHERE enrichment_status IN ('pending','failed')"
        params: list[Any] = []
        if source:
            q += " AND source = ?"
            params.append(source)
        q += " ORDER BY fetched_at DESC LIMIT ?"
        params.append(limit)
        with self.connect() as c:
            return [dict(r) for r in c.execute(q, params).fetchall()]

    def mark_enrichment(
        self,
        url: str,
        status: str,
        payload: dict[str, Any] | None = None,
    ) -> None:
        with self.connect() as c:
            c.execute(
                "UPDATE articles SET enrichment_status = ?, enrichment_json = ? WHERE url = ?",
                (status, json.dumps(payload) if payload else None, url),
            )

    # --- funding rounds -----------------------------------------------------
    def upsert_round(
        self,
        r: FundingRound,
        source_urls: Iterable[str],
        dedup: bool = True,
    ) -> str:
        """Insert or merge a round. Returns the effective round_id (may differ
        from r.round_id if fuzzy-merged into an existing row).
        """
        with self.connect() as c:
            if dedup:
                from pipeline.dedup import find_existing_round_id
                existing = find_existing_round_id(c, r)
                if existing and existing != r.round_id:
                    logger.info(
                        "dedup: merging '{}' into existing round_id={}",
                        r.company.name, existing,
                    )
                    r = r.model_copy(update={"round_id": existing})
            c.execute(
                """
                INSERT INTO funding_rounds
                    (round_id, company_name, company_json, stage, amount, currency,
                     amount_usd, announced_on, summary, confidence, extraction_method,
                     created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(round_id) DO UPDATE SET
                    company_json = excluded.company_json,
                    stage = excluded.stage,
                    amount = excluded.amount,
                    currency = excluded.currency,
                    amount_usd = excluded.amount_usd,
                    announced_on = excluded.announced_on,
                    summary = excluded.summary,
                    confidence = MAX(excluded.confidence, funding_rounds.confidence),
                    extraction_method = excluded.extraction_method,
                    updated_at = excluded.updated_at
                """,
                (
                    r.round_id,
                    r.company.name,
                    r.company.model_dump_json(),
                    r.stage.value if r.stage else None,
                    r.amount,
                    r.currency.value if r.currency else None,
                    r.amount_usd,
                    _iso(r.announced_on),
                    r.summary,
                    r.confidence,
                    r.extraction_method,
                    _iso(r.created_at),
                    _iso(r.updated_at),
                ),
            )
            for url in source_urls:
                c.execute(
                    "INSERT OR IGNORE INTO round_sources (round_id, article_url) VALUES (?, ?)",
                    (r.round_id, url),
                )
            for inv in r.investors:
                c.execute(
                    "INSERT OR IGNORE INTO round_investors (round_id, name, lead) VALUES (?, ?, ?)",
                    (r.round_id, inv.name, 1 if inv.lead else 0),
                )
        return r.round_id

    # --- run log ------------------------------------------------------------
    def record_run(self, rec: RunRecord) -> None:
        with self.connect() as c:
            c.execute(
                """
                INSERT INTO run_log
                    (run_id, source, started_at, finished_at, articles_seen,
                     articles_new, articles_failed, enriched_ok, enriched_failed,
                     schema_drift_flag, error)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id, source) DO UPDATE SET
                    finished_at = excluded.finished_at,
                    articles_seen = excluded.articles_seen,
                    articles_new = excluded.articles_new,
                    articles_failed = excluded.articles_failed,
                    enriched_ok = excluded.enriched_ok,
                    enriched_failed = excluded.enriched_failed,
                    schema_drift_flag = excluded.schema_drift_flag,
                    error = excluded.error
                """,
                (
                    rec.run_id,
                    rec.source,
                    _iso(rec.started_at),
                    _iso(rec.finished_at),
                    rec.articles_seen,
                    rec.articles_new,
                    rec.articles_failed,
                    rec.enriched_ok,
                    rec.enriched_failed,
                    1 if rec.schema_drift_flag else 0,
                    rec.error,
                ),
            )
            logger.debug("run_log recorded: {} / {}", rec.run_id, rec.source)
