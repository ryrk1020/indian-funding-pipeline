"""Inc42 scraper.

Strategy:
  - Listing via WordPress sitemaps (post-sitemap*.xml). The HTML category page is
    client-rendered and returns no useful data, but the sitemap is static, large,
    and dated. We filter the sitemap to likely-funding articles using URL-slug
    keywords, then scrape detail pages statically.

Detail pages: Next.js-rendered but OpenGraph + `.entry-content` are present in
the initial HTML, so HTTPX + BeautifulSoup is enough.
"""
from __future__ import annotations

import re
from collections.abc import Iterable
from datetime import datetime

from bs4 import BeautifulSoup
from dateutil import parser as dateparse
from loguru import logger

from config.schemas import ArticleRaw
from sources.base_scraper import BaseScraper, SourceConfig

FUNDING_KEYWORDS = (
    "rais", "raises", "raised", "funding", "seed", "series-a", "series-b",
    "series-c", "series-d", "pre-series", "bags", "mops-up", "secures",
    "closes", "mn-from", "cr-from", "mn-round", "cr-round", "-mn-", "-cr-",
    "acquires", "acquisition", "ipo", "debt-funding", "bridge-round",
)


def _looks_like_funding(url: str) -> bool:
    low = url.lower()
    return any(k in low for k in FUNDING_KEYWORDS)


class Inc42Scraper(BaseScraper):
    """Static scraper using XML sitemaps for listing."""

    def __init__(self, config: SourceConfig) -> None:
        super().__init__(config)
        # Which sitemap files to walk. Configured via pagination.sitemaps list
        # (defaults to the currently-freshest post-sitemap54.xml).
        self._sitemap_urls: list[str] = config.pagination.get(
            "sitemaps", ["https://inc42.com/post-sitemap54.xml"]
        )
        self._max_articles: int = int(config.pagination.get("max_pages", 50)) * 20

    # BaseScraper treats iter_list_urls() as producing list-page URLs; we reuse
    # the abstraction but the "list pages" are sitemap XML files.
    def iter_list_urls(self) -> Iterable[str]:
        yield from self._sitemap_urls

    def parse_list(self, html: str, list_url: str) -> list[str]:
        # Extract <loc> URLs with <lastmod>, sort by date desc, filter for
        # funding-ish slugs, cap to max_articles.
        pairs = re.findall(
            r"<url>\s*<loc>([^<]+)</loc>\s*<lastmod>([^<]+)</lastmod>", html
        )
        pairs.sort(key=lambda p: p[1], reverse=True)
        funding = [u for u, _ in pairs if _looks_like_funding(u)]
        logger.debug(
            "[inc42] sitemap {} -> {} total, {} funding candidates",
            list_url, len(pairs), len(funding),
        )
        return funding[: self._max_articles]

    def parse_detail(self, html: str, url: str) -> ArticleRaw:
        soup = BeautifulSoup(html, "lxml")

        def og(prop: str) -> str | None:
            tag = soup.find("meta", attrs={"property": prop})
            return tag.get("content") if tag and tag.get("content") else None

        title = og("og:title") or (soup.title.string.strip() if soup.title else "")
        title = re.sub(r"\s+", " ", title or "").strip()

        pub_raw = og("article:published_time")
        published_at: datetime | None = None
        if pub_raw:
            try:
                published_at = dateparse.isoparse(pub_raw)
            except Exception:
                logger.debug("[inc42] bad published_at '{}' on {}", pub_raw, url)

        # Author: Inc42 puts it as JSON-LD or in a byline <a rel="author">
        author = None
        author_tag = soup.find("a", attrs={"rel": "author"})
        if author_tag and author_tag.get_text(strip=True):
            author = author_tag.get_text(strip=True)

        # Main content
        body_el = soup.find("div", class_="entry-content")
        if body_el is None:
            # fallback: article tag
            body_el = soup.find("article")
        text = ""
        if body_el is not None:
            # strip scripts/styles/figure captions
            for junk in body_el(["script", "style", "noscript", "iframe"]):
                junk.decompose()
            # join paragraph-ish text
            paragraphs = [
                p.get_text(" ", strip=True)
                for p in body_el.find_all(["p", "li", "h2", "h3", "blockquote"])
            ]
            text = "\n\n".join(t for t in paragraphs if t)
        if not text:
            text = og("og:description") or title

        return ArticleRaw(
            source=self.config.name,
            url=url,
            title=title or url,
            published_at=published_at,
            author=author,
            html=html,
            text=text,
        )

    # Override: Inc42 sitemaps are XML, not rendered. Always static.
    @property
    def is_dynamic(self) -> bool:  # type: ignore[override]
        return False
