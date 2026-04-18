"""Dashboard renderer: template loads, data embedded, key markers present."""
from __future__ import annotations

from datetime import date, datetime

from config.schemas import ArticleRaw, Company, FundingRound, FundingStage, Investor
from pipeline.dashboard import export_dashboard, render_dashboard
from pipeline.storage import Storage


def _seed(tmp_path) -> Storage:
    s = Storage(db_path=tmp_path / "d.db")
    s.upsert_article(ArticleRaw(
        source="inc42", url="https://inc42.com/a", title="t", text="x",
        fetched_at=datetime(2026, 4, 17),
    ))
    s.upsert_round(FundingRound(
        round_id="abc" * 6,
        company=Company(name="Hocco", sector="Food"),
        stage=FundingStage.SERIES_C,
        amount_usd=12_000_000,
        announced_on=date(2026, 4, 17),
        investors=[Investor(name="Sixth Sense", lead=True)],
        sources=["https://inc42.com/a"],
        confidence=0.95,
        summary="Hocco raised Series C.",
    ), source_urls=["https://inc42.com/a"], dedup=False)
    return s


def test_render_contains_data_and_scaffold(tmp_path) -> None:
    html = render_dashboard(_seed(tmp_path))
    assert "<!DOCTYPE html>" in html
    # Data embedded
    assert "Hocco" in html
    assert "Series C" in html
    assert "const ROUNDS =" in html
    assert "const META =" in html
    # Scaffold
    assert "Indian Startup Funding Intelligence" in html
    assert "chart-timeline" in html
    assert "chart-stages" in html
    assert "chart-sectors" in html
    assert "chart-investors" in html
    assert "chart-distribution" in html
    # Alpine + ECharts loaded via CDN
    assert "alpinejs" in html
    assert "echarts" in html


def test_export_writes_file(tmp_path) -> None:
    s = _seed(tmp_path)
    out = tmp_path / "dashboard.html"
    n = export_dashboard(s, out)
    assert n == 1
    assert out.exists()
    assert out.stat().st_size > 10_000


def test_min_confidence_filters(tmp_path) -> None:
    s = _seed(tmp_path)
    s.upsert_round(FundingRound(
        round_id="lowconf" * 2 + "aa",
        company=Company(name="LowConfCo"),
        stage=FundingStage.SEED,
        amount_usd=500_000,
        announced_on=date(2026, 4, 14),
        sources=["https://inc42.com/a"],
        confidence=0.20,
    ), source_urls=["https://inc42.com/a"], dedup=False)
    high = render_dashboard(s, min_confidence=0.8)
    assert "LowConfCo" not in high
    assert "Hocco" in high
