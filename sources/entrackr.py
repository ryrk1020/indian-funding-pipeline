"""Entrackr scraper.

Strategy:
  - Listing: Playwright renders the homepage and performs N infinite-scroll
    passes. This exercises the JS-rendering path. Links are filtered down to
    article-shaped URLs whose slugs contain funding keywords.
  - Detail: Plain HTTPX + BeautifulSoup. OpenGraph gives title/summary, a
    regex over the embedded JSON-LD gives the published timestamp, and the
    `<article>` element contains the body.

Why homepage and not /category/funding-news: the category page caches only a
handful of items client-side regardless of scroll; the homepage yields 30+
articles per scroll cycle and filtering by slug keywords is cheap and precise.
"""
from __future__ import annotations

import json
import re
from collections.abc import Iterable
from datetime import datetime

from bs4 import BeautifulSoup
from dateutil import parser as dateparse
from loguru import logger

from config.schemas import ArticleRaw
from pipeline.fetcher import Fetcher
from sources.base_scraper import BaseScraper, SourceConfig

FUNDING_KEYWORDS = (
    "rais", "raises", "raised", "funding", "seed", "series-a", "series-b",
    "series-c", "series-d", "series-e", "pre-series", "bags", "secures",
    "mops-up", "closes", "-mn-", "-cr-", "-million", "-crore", "round",
    "debt-funding", "bridge-round", "acquires", "acquisition",
)

ARTICLE_PATH_PREFIXES = ("snippets/", "news/", "fintrackr/", "news-analysis/", "report/", "exclusive/")


def _looks_like_funding(url: str) -> bool:
    low = url.lower()
    path = low.replace("https://entrackr.com/", "").strip("/")
    if not any(path.startswith(p) for p in ARTICLE_PATH_PREFIXES):
        return False
    return any(k in low for k in FUNDING_KEYWORDS)


class EntrackrScraper(BaseScraper):
    def __init__(self, config: SourceConfig) -> None:
        super().__init__(config)
        self._max_scrolls: int = int(config.pagination.get("max_scrolls", 5))
        self._scroll_pause_ms: int = int(config.pagination.get("scroll_pause_ms", 1500))
        self._max_articles: int = int(config.pagination.get("max_articles", 50))

    def iter_list_urls(self) -> Iterable[str]:
        yield from self.config.list_urls

    # We override _fetch_html for the listing to pass scroll params.
    async def _fetch_html(self, fetcher: Fetcher, url: str) -> str:
        if url in self.config.list_urls:
            res = await fetcher.fetch_rendered(
                url,
                scroll_times=self._max_scrolls,
                scroll_pause_ms=self._scroll_pause_ms,
            )
            return res.html
        # detail — static is fine
        res = await fetcher.fetch(url)
        return res.html

    def parse_list(self, html: str, list_url: str) -> list[str]:
        # Grab every <a href> and filter. BS4 is fine; DOM is fully rendered.
        soup = BeautifulSoup(html, "lxml")
        hrefs = [a.get("href") for a in soup.find_all("a", href=True)]
        urls: list[str] = []
        for h in hrefs:
            if not h:
                continue
            if h.startswith("/"):
                h = "https://entrackr.com" + h
            if "entrackr.com" not in h:
                continue
            h = h.split("?")[0].split("#")[0]
            if _looks_like_funding(h):
                urls.append(h)
        # dedupe, preserve order
        seen: set[str] = set()
        out: list[str] = []
        for u in urls:
            if u not in seen:
                seen.add(u)
                out.append(u)
            if len(out) >= self._max_articles:
                break
        logger.debug("[entrackr] listing {} -> {} funding URLs", list_url, len(out))
        return out

    def parse_detail(self, html: str, url: str) -> ArticleRaw:
        soup = BeautifulSoup(html, "lxml")

        def og(prop: str) -> str | None:
            tag = soup.find("meta", attrs={"property": prop})
            return tag.get("content") if tag and tag.get("content") else None

        title = og("og:title") or (soup.title.string.strip() if soup.title else "")
        title = re.sub(r"\s+", " ", title or "").strip()

        published_at: datetime | None = None
        # JSON-LD blocks — look for any datePublished
        for block in soup.find_all("script", attrs={"type": "application/ld+json"}):
            raw = block.string or block.get_text() or ""
            try:
                obj = json.loads(raw)
            except Exception:
                # some blocks are arrays / malformed; try regex fallback
                m = re.search(r'"datePublished"\s*:\s*"([^"]+)"', raw)
                if m:
                    try:
                        published_at = dateparse.isoparse(m.group(1))
                        break
                    except Exception:
                        pass
                continue
            if isinstance(obj, dict) and obj.get("datePublished"):
                try:
                    published_at = dateparse.isoparse(obj["datePublished"])
                    break
                except Exception:
                    pass

        # Author — Entrackr merges author and timestamp into one div.author.
        author: str | None = None
        author_el = soup.select_one(".author")
        if author_el:
            raw_author = author_el.get_text(" ", strip=True)
            # Typical: "Shashank Pathak16 Apr 202614:17IST" → name is the leading
            # run of letters/spaces, before the first digit.
            m = re.match(r"^([A-Za-z][A-Za-z .'\-]{1,60}?)(?=\s*\d)", raw_author)
            if m:
                author = m.group(1).strip()

        body_el = soup.find("article")
        text = ""
        if body_el is not None:
            for junk in body_el(["script", "style", "noscript", "iframe", "aside", "nav", "header", "footer"]):
                junk.decompose()
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

    @property
    def is_dynamic(self) -> bool:  # type: ignore[override]
        # Only the listing is dynamic; detail uses HTTPX. We manually dispatch
        # in _fetch_html above, so return False so the base class doesn't force
        # Playwright for everything.
        return False
