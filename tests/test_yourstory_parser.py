"""Offline tests for the YourStory scraper — no network."""
from __future__ import annotations

from pathlib import Path

import pytest

from sources.base_scraper import SourceConfig
from sources.yourstory import YourStoryScraper

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def scraper() -> YourStoryScraper:
    cfg = SourceConfig(
        name="yourstory",
        kind="dynamic",
        base_url="https://yourstory.com",
        list_urls=["https://yourstory.com/category/funding"],
        pagination={"max_scrolls": 5, "max_articles": 40},
    )
    return YourStoryScraper(cfg)


def test_listing_keeps_article_paths_only(scraper: YourStoryScraper) -> None:
    html = (FIXTURES / "yourstory_listing_sample.html").read_text(encoding="utf-8")
    urls = scraper.parse_list(html, "https://yourstory.com/category/funding")
    assert any("traqcheck-raises-8-million" in u for u in urls)
    assert any("the-hosteller-raises-rs-150-cr" in u for u in urls)
    assert any("gobblecube-15-million-series-a" in u for u in urls)
    assert any("razorpay-acquires-billme" in u for u in urls)


def test_listing_drops_category_topic_video_company(scraper: YourStoryScraper) -> None:
    html = (FIXTURES / "yourstory_listing_sample.html").read_text(encoding="utf-8")
    urls = scraper.parse_list(html, "x")
    assert not any("/category/" in u for u in urls)
    assert not any("/topic/" in u for u in urls)
    assert not any("/video/" in u for u in urls)
    assert not any("/companies/" in u for u in urls)
    # herstory is a sibling section — also exclude
    assert not any("/herstory/" in u for u in urls)


def test_detail_parses_title_date_body(scraper: YourStoryScraper) -> None:
    html = (FIXTURES / "yourstory_article_sample.html").read_text(encoding="utf-8")
    url = "https://yourstory.com/2026/04/traqcheck-raises-8-million-led-by-ivycap-ventures"
    art = scraper.parse_detail(html, url)

    assert "TraqCheck" in art.title
    assert art.published_at is not None
    assert art.published_at.year == 2026 and art.published_at.month == 4
    assert "$8 million" in art.text
    assert "IvyCap" in art.text
