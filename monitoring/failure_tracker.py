"""Aggregate run_log to surface sources that are silently breaking.

A scraper can fail in two ways: loudly (exception, `error` column set) or
quietly (runs succeed but `articles_seen=0`, `articles_new=0` for days).
This module surfaces both.

The `health` function produces a per-source report used by `pipeline run health`.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from pipeline.storage import Storage


def _utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


@dataclass
class SourceHealth:
    source: str
    runs_last_7d: int
    errors_last_7d: int
    zero_article_runs: int  # runs where articles_seen == 0 and no error
    consecutive_failures: int  # current streak from most-recent run backwards
    last_success_at: str | None
    last_drift_flag: bool
    avg_articles_per_run: float

    @property
    def status(self) -> str:
        if self.consecutive_failures >= 3:
            return "CRITICAL"
        if self.last_drift_flag:
            return "DRIFT"
        if self.zero_article_runs >= 2:
            return "STALE"
        if self.errors_last_7d > 0:
            return "WARN"
        return "OK"


def health(storage: Storage, window_days: int = 7) -> list[SourceHealth]:
    cutoff = (_utcnow() - timedelta(days=window_days)).isoformat()
    with storage.connect() as c:
        sources = [
            r["source"] for r in c.execute(
                "SELECT DISTINCT source FROM run_log"
            ).fetchall()
        ]
        out: list[SourceHealth] = []
        for src in sources:
            rows = c.execute(
                """
                SELECT started_at, finished_at, articles_seen, articles_new,
                       articles_failed, error, schema_drift_flag
                FROM run_log
                WHERE source = ? AND started_at >= ?
                ORDER BY started_at DESC
                """,
                (src, cutoff),
            ).fetchall()
            if not rows:
                continue
            errors = sum(1 for r in rows if r["error"])
            zero_runs = sum(
                1 for r in rows
                if not r["error"] and (r["articles_seen"] or 0) == 0
            )
            streak = 0
            for r in rows:
                if r["error"] or (r["articles_seen"] or 0) == 0:
                    streak += 1
                else:
                    break
            last_ok = next(
                (r["finished_at"] for r in rows
                 if not r["error"] and (r["articles_seen"] or 0) > 0),
                None,
            )
            avg = sum((r["articles_seen"] or 0) for r in rows) / len(rows)
            out.append(SourceHealth(
                source=src,
                runs_last_7d=len(rows),
                errors_last_7d=errors,
                zero_article_runs=zero_runs,
                consecutive_failures=streak,
                last_success_at=last_ok,
                last_drift_flag=bool(rows[0]["schema_drift_flag"]),
                avg_articles_per_run=round(avg, 1),
            ))
    return out
