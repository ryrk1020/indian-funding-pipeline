"""Offline tests for the Inc42 scraper — no network."""
from __future__ import annotations

from pathlib import Path

import pytest

from config.schemas import ArticleRaw
from sources.base_scraper import SourceConfig
from sources.inc42 import Inc42Scraper

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def scraper() -> Inc42Scraper:
    cfg = SourceConfig(
        name="inc42",
        kind="static",
        base_url="https://inc42.com",
        list_urls=["https://inc42.com/post-sitemap54.xml"],
        pagination={"sitemaps": ["https://inc42.com/post-sitemap54.xml"], "max_pages": 50},
    )
    return Inc42Scraper(cfg)


def test_sitemap_filters_to_funding_like_urls(scraper: Inc42Scraper) -> None:
    html = (FIXTURES / "inc42_sitemap_sample.xml").read_text(encoding="utf-8")
    urls = scraper.parse_list(html, "https://inc42.com/post-sitemap54.xml")
    # TraqCheck "raises", Palmonas "raises", Ola "ipo" → keep. The loopworm
    # feature story and generic tech news should be dropped.
    assert any("traqcheck" in u for u in urls)
    assert any("palmonas" in u for u in urls)
    assert any("ola-electric-ipo" in u for u in urls)
    assert not any("loopworm" in u for u in urls)
    assert not any("unveils-new-office" in u for u in urls)


def test_sitemap_ordered_most_recent_first(scraper: Inc42Scraper) -> None:
    html = (FIXTURES / "inc42_sitemap_sample.xml").read_text(encoding="utf-8")
    urls = scraper.parse_list(html, "x")
    # TraqCheck (2026-04-14) comes before Palmonas (2026-04-09)
    assert urls.index(next(u for u in urls if "traqcheck" in u)) < urls.index(
        next(u for u in urls if "palmonas" in u)
    )


def test_parse_detail_extracts_title_and_body(scraper: Inc42Scraper) -> None:
    html = (FIXTURES / "inc42_article_sample.html").read_text(encoding="utf-8")
    url = "https://inc42.com/buzz/traqcheck-raises-8-mn-to-build-ai-agents-for-recruitment/"
    article: ArticleRaw = scraper.parse_detail(html, url)

    assert article.source == "inc42"
    assert str(article.url) == url
    assert "TraqCheck" in article.title
    assert "Series A" in article.text
    assert "IvyCap" in article.text
    assert article.author == "Sample Reporter"
    assert article.published_at is not None
    assert article.published_at.year == 2026
    # scripts stripped
    assert "dataLayer" not in article.text
