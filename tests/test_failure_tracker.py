"""failure_tracker.health aggregates run_log correctly."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

from config.schemas import RunRecord
from monitoring.failure_tracker import health
from pipeline.storage import Storage


def _utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _run(source: str, started_at: datetime, **kw) -> RunRecord:
    return RunRecord(
        run_id=started_at.strftime("%Y%m%dT%H%M%S"),
        source=source,
        started_at=started_at,
        finished_at=started_at + timedelta(seconds=10),
        **kw,
    )


def test_healthy_source_reports_ok(tmp_path) -> None:
    s = Storage(db_path=tmp_path / "h.db")
    now = _utcnow()
    for i in range(3):
        s.record_run(_run("inc42", now - timedelta(hours=i), articles_seen=20, articles_new=5))
    [h] = health(s)
    assert h.source == "inc42"
    assert h.status == "OK"
    assert h.errors_last_7d == 0
    assert h.consecutive_failures == 0


def test_consecutive_errors_mark_critical(tmp_path) -> None:
    s = Storage(db_path=tmp_path / "h2.db")
    now = _utcnow()
    # Older runs were fine, most-recent 3 errored → streak = 3 → CRITICAL
    s.record_run(_run("inc42", now - timedelta(hours=10), articles_seen=15))
    for i in range(3):
        s.record_run(_run(
            "inc42", now - timedelta(hours=i),
            articles_seen=0, error="TimeoutError: fetch failed",
        ))
    [h] = health(s)
    assert h.status == "CRITICAL"
    assert h.consecutive_failures == 3
    assert h.errors_last_7d == 3


def test_stale_source_with_zero_articles(tmp_path) -> None:
    s = Storage(db_path=tmp_path / "h3.db")
    now = _utcnow()
    for i in range(2):
        s.record_run(_run("yourstory", now - timedelta(hours=i), articles_seen=0))
    [h] = health(s)
    assert h.status in ("STALE", "CRITICAL")  # 2 zero-article runs
    assert h.zero_article_runs == 2


def test_drift_flag_bubbles_up(tmp_path) -> None:
    s = Storage(db_path=tmp_path / "h4.db")
    now = _utcnow()
    s.record_run(_run(
        "entrackr", now, articles_seen=10, articles_new=3, schema_drift_flag=True,
    ))
    [h] = health(s)
    assert h.status == "DRIFT"
    assert h.last_drift_flag is True
