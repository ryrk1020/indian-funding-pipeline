"""Offline tests for the Entrackr scraper — no network."""
from __future__ import annotations

from pathlib import Path

import pytest

from sources.base_scraper import SourceConfig
from sources.entrackr import EntrackrScraper

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def scraper() -> EntrackrScraper:
    cfg = SourceConfig(
        name="entrackr",
        kind="dynamic",
        base_url="https://entrackr.com",
        list_urls=["https://entrackr.com/"],
        pagination={"max_scrolls": 5, "max_articles": 50},
    )
    return EntrackrScraper(cfg)


def test_listing_keeps_funding_urls(scraper: EntrackrScraper) -> None:
    html = (FIXTURES / "entrackr_listing_sample.html").read_text(encoding="utf-8")
    urls = scraper.parse_list(html, "https://entrackr.com/")
    assert any("polaris-raises-80-mn" in u for u in urls)
    assert any("hosteller-raises-rs-150-cr-in-series-b-round" in u for u in urls)
    assert any("cohoma-coffee-raises-rs-5-cr-seed-round" in u for u in urls)
    assert any("aliste-technologies-raises-rs-30-cr-in-pre-series-a" in u for u in urls)


def test_listing_drops_non_funding_and_category_pages(scraper: EntrackrScraper) -> None:
    html = (FIXTURES / "entrackr_listing_sample.html").read_text(encoding="utf-8")
    urls = scraper.parse_list(html, "https://entrackr.com/")
    assert not any("bigbasket-appoints" in u for u in urls)
    assert not any("ola-electric-exec-moves" in u for u in urls)
    assert not any("/category/" in u for u in urls)
    assert not any("/tag/" in u for u in urls)
    assert not any(u.endswith("/about") for u in urls)


def test_detail_parses_title_date_author_body(scraper: EntrackrScraper) -> None:
    html = (FIXTURES / "entrackr_article_sample.html").read_text(encoding="utf-8")
    url = "https://entrackr.com/snippets/polaris-raises-80-mn-from-british-international-investment-11731748"
    art = scraper.parse_detail(html, url)

    assert "Polaris" in art.title
    assert art.published_at is not None
    assert art.published_at.year == 2026 and art.published_at.month == 4
    assert art.author == "Shashank Pathak"
    assert "Rs 710 crore" in art.text
    assert "dataLayer" not in art.text
