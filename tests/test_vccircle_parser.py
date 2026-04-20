"""Smoke tests for the VCCircle scraper parsers. No network."""
from __future__ import annotations

from sources.base_scraper import SourceConfig
from sources.vccircle import VCCircleScraper


def _mk() -> VCCircleScraper:
    cfg = SourceConfig(
        name="vccircle",
        kind="static",
        base_url="https://www.vccircle.com",
        list_urls=["https://www.vccircle.com/sector/tmt"],
        pagination={"max_pages": 2, "max_articles": 20},
    )
    return VCCircleScraper(cfg)


def test_parse_list_filters_to_article_paths() -> None:
    html = """
    <html><body>
      <a href="/sector/tmt/hocco-raises-series-c-round">1</a>
      <a href="https://www.vccircle.com/sector/consumer/bags-funding-round">2</a>
      <a href="/tag/funding/">skip tag</a>
      <a href="/author/jane">skip author</a>
      <a href="/newsletter/subscribe">skip newsletter</a>
      <a href="/events/tmt-summit">skip events</a>
      <a href="/sector/tmt">skip too shallow</a>
      <a href="https://unrelated.com/x">skip external</a>
    </body></html>
    """
    s = _mk()
    out = s.parse_list(html, s.config.list_urls[0])
    assert "https://www.vccircle.com/sector/tmt/hocco-raises-series-c-round" in out
    assert "https://www.vccircle.com/sector/consumer/bags-funding-round" in out
    assert all("/tag/" not in u for u in out)
    assert all("/author/" not in u for u in out)


def test_parse_detail_with_field_body_and_jsonld() -> None:
    html = """
    <html><head>
      <meta property="og:title" content="TraqCheck raises Series A" />
      <script type="application/ld+json">[{"datePublished":"2026-04-14T09:00:00Z"}]</script>
    </head><body>
      <div class="field--name-body">
        <p>TraqCheck raised $8M.</p>
        <p>Led by IvyCap Ventures.</p>
      </div>
    </body></html>
    """
    s = _mk()
    a = s.parse_detail(html, "https://www.vccircle.com/sector/tmt/traqcheck-series-a")
    assert a.title == "TraqCheck raises Series A"
    assert "TraqCheck raised $8M." in a.text
    assert a.published_at is not None
