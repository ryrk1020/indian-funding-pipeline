"""Smoke tests for the MoneyControl scraper parsers. No network."""
from __future__ import annotations

from sources.base_scraper import SourceConfig
from sources.moneycontrol import MoneyControlScraper


def _mk() -> MoneyControlScraper:
    cfg = SourceConfig(
        name="moneycontrol",
        kind="static",
        base_url="https://www.moneycontrol.com",
        list_urls=["https://www.moneycontrol.com/news/business/startup"],
        pagination={"max_pages": 2, "max_articles": 20},
    )
    return MoneyControlScraper(cfg)


def test_parse_list_filters_to_funding_article_urls() -> None:
    html = """
    <html><body>
      <a href="https://www.moneycontrol.com/news/business/startup/hocco-raises-18-mn-series-c-1234.html">1</a>
      <a href="https://www.moneycontrol.com/news/business/startup/ev-maker-bags-funding-from-accel-5678.html">2</a>
      <a href="https://www.moneycontrol.com/news/business/markets/sensex-hits-high-9999.html">skip</a>
      <a href="https://www.moneycontrol.com/news/business/startup/about-us">skip no id</a>
      <a href="/some/relative/link">skip relative</a>
    </body></html>
    """
    s = _mk()
    out = s.parse_list(html, s.config.list_urls[0])
    assert "https://www.moneycontrol.com/news/business/startup/hocco-raises-18-mn-series-c-1234.html" in out
    assert "https://www.moneycontrol.com/news/business/startup/ev-maker-bags-funding-from-accel-5678.html" in out
    assert all("markets/sensex" not in u for u in out)


def test_parse_detail_extracts_og_title_and_body() -> None:
    html = """
    <html><head>
      <meta property="og:title" content="Hocco raises $18M Series C led by Accel" />
      <title>ignored</title>
      <script type="application/ld+json">{"datePublished":"2026-04-14T09:00:00+05:30"}</script>
    </head><body>
      <div class="content_wrapper">
        <p>Hocco, a food brand, raised $18M.</p>
        <p>The round was led by Accel.</p>
      </div>
    </body></html>
    """
    s = _mk()
    a = s.parse_detail(html, "https://www.moneycontrol.com/news/business/startup/hocco-1234.html")
    assert a.title.startswith("Hocco raises")
    assert "Hocco, a food brand, raised $18M." in a.text
    assert "The round was led by Accel." in a.text
    assert a.published_at is not None
    assert a.published_at.year == 2026 and a.published_at.month == 4


def test_iter_list_urls_paginates() -> None:
    s = _mk()
    urls = list(s.iter_list_urls())
    # max_pages=2 → [base, base/page-2]
    assert urls[0] == "https://www.moneycontrol.com/news/business/startup"
    assert urls[-1].endswith("/page-2")
