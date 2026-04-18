"""CLI entrypoint.

    python -m pipeline.run                         # scrape all enabled sources
    python -m pipeline.run scrape --source inc42   # scrape one source
    python -m pipeline.run scrape --limit 5
    python -m pipeline.run list                    # show configured sources
    python -m pipeline.run enrich                  # LLM-enrich pending articles
    python -m pipeline.run enrich --limit 10 --use-regex-only
"""
from __future__ import annotations

import asyncio
import sys
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

import typer
from loguru import logger
from rich.console import Console
from rich.table import Table

from config.schemas import RunRecord
from config.settings import settings
from pipeline.enricher import EnrichmentError, OpenRouterEnricher
from pipeline.fetcher import Fetcher
from pipeline.regex_fallback import regex_extract
from pipeline.registry import load_source_configs, make_scraper
from pipeline.storage import Storage
from pipeline.validator import build_funding_round

app = typer.Typer(add_completion=False, help="Funding pipeline runner.")
console = Console()

LOW_CONFIDENCE_THRESHOLD = 0.35


def _check_and_update_drift(storage: Storage, source: str, html: str) -> bool:
    """Probe the first article's HTML. Returns True if drift detected."""
    from monitoring.schema_drift import check_drift, update_baseline_if_healthy
    with storage.connect() as c:
        report = check_drift(c, source, html)
        if not report.has_drift:
            update_baseline_if_healthy(c, source, html, _utcnow().isoformat())
    return report.has_drift


def _utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _configure_logging(level: str) -> None:
    logger.remove()
    logger.add(
        sys.stderr,
        level=level.upper(),
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <7}</level> | {message}",
    )


# =============================================================================
# SCRAPE
# =============================================================================


async def _scrape_source(
    name: str,
    storage: Storage,
    run_id: str,
    limit: int | None,
) -> RunRecord:
    configs = load_source_configs()
    cfg = configs.get(name)
    if cfg is None:
        raise typer.BadParameter(f"unknown source: {name}")
    if not cfg.enabled:
        logger.warning("[{}] disabled — skipping", name)
        return RunRecord(run_id=run_id, source=name, started_at=_utcnow(), finished_at=_utcnow())

    scraper = make_scraper(name, cfg)
    rec = RunRecord(run_id=run_id, source=name, started_at=_utcnow())
    drift_checked = False

    try:
        async with Fetcher() as fetcher:
            async for article in scraper.crawl(fetcher, limit=limit):
                rec.articles_seen += 1
                try:
                    if storage.upsert_article(article):
                        rec.articles_new += 1
                    if not drift_checked and article.html:
                        rec.schema_drift_flag = _check_and_update_drift(
                            storage, name, article.html,
                        )
                        drift_checked = True
                except Exception as e:
                    rec.articles_failed += 1
                    logger.exception("[{}] upsert failed for {}: {}", name, article.url, e)
    except Exception as e:
        rec.error = f"{type(e).__name__}: {e}"
        logger.exception("[{}] run crashed: {}", name, e)
    finally:
        rec.articles_failed += scraper.stats.articles_failed
        rec.finished_at = _utcnow()
        storage.record_run(rec)
    return rec


def _print_scrape_summary(records: list[RunRecord]) -> None:
    t = Table(title="Scrape summary", show_lines=False)
    for col in ("source", "seen", "new", "failed", "elapsed"):
        t.add_column(col, justify="right" if col != "source" else "left")
    for r in records:
        elapsed = (
            f"{(r.finished_at - r.started_at).total_seconds():.1f}s"
            if r.finished_at else "—"
        )
        t.add_row(r.source, str(r.articles_seen), str(r.articles_new), str(r.articles_failed), elapsed)
    console.print(t)


@app.command()
def scrape(
    source: str | None = typer.Option(None, help="Single source name; default = all enabled."),
    limit: int | None = typer.Option(None, help="Cap articles per source."),
    log_level: str = typer.Option(settings.pipeline_log_level, "--log-level"),
) -> None:
    """Scrape configured sources into the articles table."""
    _configure_logging(log_level)
    storage = Storage()
    run_id = _utcnow().strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]
    configs = load_source_configs()
    targets = [source] if source else [n for n, c in configs.items() if c.enabled]

    logger.info("run_id={} | db={} | sources={}", run_id, settings.db_path, targets)
    records = [asyncio.run(_scrape_source(n, storage, run_id, limit)) for n in targets]
    _print_scrape_summary(records)


@app.command("list")
def list_sources() -> None:
    """List configured sources."""
    configs = load_source_configs()
    t = Table(title="Configured sources")
    for col in ("name", "kind", "enabled", "base_url"):
        t.add_column(col)
    for n, c in configs.items():
        t.add_row(n, c.kind, "yes" if c.enabled else "no", c.base_url)
    console.print(t)


# =============================================================================
# ENRICH
# =============================================================================


@dataclass
class EnrichStats:
    total: int = 0
    llm_ok: int = 0
    llm_failed: int = 0
    regex_fallback: int = 0
    rounds_written: int = 0
    skipped_no_company: int = 0


async def _enrich_articles(
    storage: Storage,
    *,
    limit: int,
    source: str | None,
    use_regex_only: bool,
) -> EnrichStats:
    rows = storage.pending_articles(source=source, limit=limit)
    logger.info("enrich: {} pending article(s){}", len(rows),
                f" [source={source}]" if source else "")
    stats = EnrichStats(total=len(rows))
    if not rows:
        return stats

    enricher: OpenRouterEnricher | None = None
    if not use_regex_only:
        if not settings.openrouter_api_key:
            logger.warning("OPENROUTER_API_KEY not set — running regex-only")
            use_regex_only = True
        else:
            try:
                enricher = OpenRouterEnricher()
            except Exception as e:  # missing deps / bad config
                logger.warning("enricher init failed ({}), regex-only", e)
                use_regex_only = True

    # LLM calls are the bottleneck — run them concurrently under a semaphore.
    # DB writes are fast and naturally serialize on SQLite's writer lock.
    sem = asyncio.Semaphore(settings.pipeline_max_concurrency)
    stats_lock = asyncio.Lock()

    async def _process_one(row: dict) -> None:
        url = row["url"]
        title = row["title"]
        text = row["text"]
        published_at = None
        if row.get("published_at"):
            try:
                from dateutil import parser as dp
                published_at = dp.isoparse(row["published_at"]).date()
            except Exception:
                pass

        async with sem:
            result = None
            if enricher is not None:
                try:
                    result = await enricher.enrich(title, text)
                    async with stats_lock:
                        stats.llm_ok += 1
                    if result.confidence < LOW_CONFIDENCE_THRESHOLD:
                        logger.info(
                            "enrich: low confidence ({:.2f}) — merging regex for {}",
                            result.confidence, url,
                        )
                        rx = regex_extract(title, text, published_at=published_at)
                        result = _merge_llm_regex(result, rx)
                except EnrichmentError as e:
                    async with stats_lock:
                        stats.llm_failed += 1
                        stats.regex_fallback += 1
                    logger.warning("enrich: LLM failed ({}), falling back for {}", e, url)
                    result = regex_extract(title, text, published_at=published_at)
            else:
                result = regex_extract(title, text, published_at=published_at)
                async with stats_lock:
                    stats.regex_fallback += 1

            fr = build_funding_round(
                result,
                article_url=url,
                fallback_company=None,
                fallback_announced=published_at,
            )
            if fr is None:
                async with stats_lock:
                    stats.skipped_no_company += 1
                storage.mark_enrichment(url, "failed", result.model_dump(mode="json"))
                return
            storage.upsert_round(fr, source_urls=[url])
            storage.mark_enrichment(
                url,
                "fallback" if result.extraction_method != "llm" else "ok",
                result.model_dump(mode="json"),
            )
            async with stats_lock:
                stats.rounds_written += 1

    await asyncio.gather(*(_process_one(r) for r in rows), return_exceptions=False)
    return stats


def _merge_llm_regex(llm, rx):
    """LLM takes priority; regex fills only null gaps.

    Confidence: LLM's low score is a real signal (the model *decided* this isn't
    a funding round). Taking max() lets regex's title-heuristic 0.80 mask that.
    Use a weighted blend (0.6 LLM + 0.3 regex, capped at 0.75) so merged rows
    stay visibly below trustworthy rounds (≥0.9).
    """
    for field in ("company_name", "stage", "amount", "currency", "amount_usd",
                  "announced_on", "location", "summary"):
        if getattr(llm, field) in (None, ""):
            setattr(llm, field, getattr(rx, field))
    if not llm.investors and rx.investors:
        llm.investors = rx.investors
    llm.confidence = min(0.75, 0.6 * llm.confidence + 0.3 * rx.confidence)
    llm.extraction_method = "llm+regex"
    return llm


@app.command()
def enrich(
    limit: int = typer.Option(50, help="Max articles to enrich this run."),
    source: str | None = typer.Option(None, help="Restrict to one source."),
    use_regex_only: bool = typer.Option(False, "--use-regex-only", help="Skip the LLM call."),
    log_level: str = typer.Option(settings.pipeline_log_level, "--log-level"),
) -> None:
    """Enrich pending articles into funding_rounds rows."""
    _configure_logging(log_level)
    storage = Storage()
    stats = asyncio.run(_enrich_articles(
        storage, limit=limit, source=source, use_regex_only=use_regex_only,
    ))
    t = Table(title="Enrichment summary")
    for col, _val in (
        ("articles processed", stats.total),
        ("llm ok", stats.llm_ok),
        ("llm failed", stats.llm_failed),
        ("regex fallback", stats.regex_fallback),
        ("rounds written", stats.rounds_written),
        ("skipped (no company)", stats.skipped_no_company),
    ):
        t.add_column(col, justify="right")
    t.add_row(*[str(v) for _, v in (
        ("articles processed", stats.total),
        ("llm ok", stats.llm_ok),
        ("llm failed", stats.llm_failed),
        ("regex fallback", stats.regex_fallback),
        ("rounds written", stats.rounds_written),
        ("skipped (no company)", stats.skipped_no_company),
    )])
    console.print(t)


# =============================================================================
# EXPORT
# =============================================================================


@app.command()
def export(
    fmt: str = typer.Option("all", "--format", help="csv | json | xlsx | html | sheets | all"),
    out_dir: str = typer.Option("data/exports", "--out", help="Directory for CSV/JSON/XLSX/HTML."),
    min_confidence: float = typer.Option(0.0, help="Skip rounds below this confidence."),
    log_level: str = typer.Option(settings.pipeline_log_level, "--log-level"),
) -> None:
    """Export funding_rounds to CSV / JSON / XLSX / HTML dashboard / Google Sheets."""
    _configure_logging(log_level)
    from pathlib import Path

    from pipeline.dashboard import export_dashboard
    from pipeline.exporter import export_csv, export_json, export_xlsx

    storage = Storage()
    stamp = _utcnow().strftime("%Y%m%d")
    out_path = Path(out_dir)
    results: list[tuple[str, str, int]] = []

    if fmt in ("csv", "all"):
        p = out_path / f"rounds_{stamp}.csv"
        n = export_csv(storage, p, min_confidence=min_confidence)
        results.append(("csv", str(p), n))
    if fmt in ("json", "all"):
        p = out_path / f"rounds_{stamp}.json"
        n = export_json(storage, p, min_confidence=min_confidence)
        results.append(("json", str(p), n))
    if fmt in ("xlsx", "all"):
        p = out_path / f"rounds_{stamp}.xlsx"
        n = export_xlsx(storage, p, min_confidence=min_confidence)
        results.append(("xlsx", str(p), n))
    if fmt in ("html", "all"):
        p = out_path / "dashboard.html"
        n = export_dashboard(storage, p, min_confidence=min_confidence)
        results.append(("html", str(p), n))
    if fmt in ("sheets", "all"):
        sa = settings.google_service_account_file
        sid = settings.google_sheet_id
        if not sa or not sid:
            if fmt == "sheets":
                console.print(
                    "[red]GOOGLE_SERVICE_ACCOUNT_FILE and GOOGLE_SHEET_ID "
                    "must be set in .env[/red]"
                )
                raise typer.Exit(1)
            logger.info("sheets: skipped (GOOGLE_SERVICE_ACCOUNT_FILE / GOOGLE_SHEET_ID not set)")
        else:
            from pipeline.sheets_export import SheetsExportError, export_sheets
            try:
                n = export_sheets(storage, sid, sa, min_confidence=min_confidence)
                results.append(("sheets", f"sheet:{sid}", n))
            except SheetsExportError as e:
                console.print(f"[red]sheets export failed: {e}[/red]")
                if fmt == "sheets":
                    raise typer.Exit(1) from e

    t = Table(title="Export summary")
    for col in ("format", "destination", "rows"):
        t.add_column(col)
    for f, dest, n in results:
        t.add_row(f, dest, str(n))
    console.print(t)


# =============================================================================
# HEALTH
# =============================================================================


@app.command()
def health(
    window_days: int = typer.Option(7, help="Lookback window."),
    log_level: str = typer.Option(settings.pipeline_log_level, "--log-level"),
) -> None:
    """Per-source scraper health: errors, stale runs, drift, failure streaks."""
    _configure_logging(log_level)
    from monitoring.failure_tracker import health as compute_health
    reports = compute_health(Storage(), window_days=window_days)
    if not reports:
        console.print("[yellow]No run history yet — run `scrape` first.[/yellow]")
        return
    t = Table(title=f"Source health (last {window_days}d)")
    for col in ("source", "status", "runs", "errors", "zero-runs",
                "streak", "avg articles", "last success", "drift"):
        t.add_column(col)
    status_style = {
        "OK": "green", "WARN": "yellow", "STALE": "yellow",
        "DRIFT": "magenta", "CRITICAL": "red",
    }
    for r in reports:
        t.add_row(
            r.source,
            f"[{status_style.get(r.status,'white')}]{r.status}[/]",
            str(r.runs_last_7d),
            str(r.errors_last_7d),
            str(r.zero_article_runs),
            str(r.consecutive_failures),
            str(r.avg_articles_per_run),
            (r.last_success_at or "—")[:19],
            "yes" if r.last_drift_flag else "no",
        )
    console.print(t)


# =============================================================================
# Default: bare `python -m pipeline.run` runs a full scrape
# =============================================================================


@app.callback(invoke_without_command=True)
def _default(ctx: typer.Context) -> None:
    if ctx.invoked_subcommand is None:
        scrape(source=None, limit=None, log_level=settings.pipeline_log_level)


if __name__ == "__main__":
    app()
