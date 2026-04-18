"""YourStory scraper.

Strategy:
  - Listing: Playwright renders `/category/funding` with N scroll passes to
    load additional stories. Article links match `/YYYY/MM/slug`.
  - Detail: Static HTTPX. OG tags + JSON-LD `datePublished` + `<article>` body.
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

# Main-site articles only: /YYYY/MM/slug. Sibling sections (herstory,
# socialstory, enterprise-story, ai-story, ys-life, hindi, tamil, smbstory)
# have their own pipelines; we don't mix them into the funding feed.
_ARTICLE_PATH_RE = re.compile(r"^/\d{4}/\d{2}/[a-z0-9-]{10,}/?$")
_NON_FUNDING_SECTION_RE = re.compile(
    r"^/(video|podcast|topic|author|tag|category|brands|events|techsparks|"
    r"companies|herstory|socialstory|enterprise-story|ai-story|ys-life|"
    r"hindi|tamil|smbstory)/"
)


class YourStoryScraper(BaseScraper):
    def __init__(self, config: SourceConfig) -> None:
        super().__init__(config)
        self._max_scrolls: int = int(config.pagination.get("max_scrolls", 6))
        self._scroll_pause_ms: int = int(config.pagination.get("scroll_pause_ms", 1500))
        self._max_articles: int = int(config.pagination.get("max_articles", 40))

    def iter_list_urls(self) -> Iterable[str]:
        yield from self.config.list_urls

    async def _fetch_html(self, fetcher: Fetcher, url: str) -> str:
        # YourStory 403s plain httpx (TLS fingerprint / anti-bot). Listing and
        # detail both go through Playwright. Detail pages don't need scrolling.
        if url in self.config.list_urls:
            res = await fetcher.fetch_rendered(
                url,
                scroll_times=self._max_scrolls,
                scroll_pause_ms=self._scroll_pause_ms,
            )
        else:
            res = await fetcher.fetch_rendered(url, scroll_times=0)
        return res.html

    def parse_list(self, html: str, list_url: str) -> list[str]:
        soup = BeautifulSoup(html, "lxml")
        urls: list[str] = []
        for a in soup.find_all("a", href=True):
            h = a.get("href") or ""
            if h.startswith("/"):
                path = h
                h = "https://yourstory.com" + h
            elif h.startswith("https://yourstory.com"):
                path = h.replace("https://yourstory.com", "")
            else:
                continue
            path = path.split("?")[0].split("#")[0]
            h = h.split("?")[0].split("#")[0]
            if _NON_FUNDING_SECTION_RE.match(path):
                continue
            if _ARTICLE_PATH_RE.match(path):
                urls.append(h)
        # dedupe preserving order
        seen: set[str] = set()
        out: list[str] = []
        for u in urls:
            if u not in seen:
                seen.add(u)
                out.append(u)
            if len(out) >= self._max_articles:
                break
        logger.debug("[yourstory] listing {} -> {} candidates", list_url, len(out))
        return out

    def parse_detail(self, html: str, url: str) -> ArticleRaw:
        soup = BeautifulSoup(html, "lxml")

        def og(prop: str) -> str | None:
            tag = soup.find("meta", attrs={"property": prop})
            return tag.get("content") if tag and tag.get("content") else None

        title = og("og:title") or (soup.title.string.strip() if soup.title else "")
        title = re.sub(r"\s+", " ", title or "").strip()

        published_at: datetime | None = None
        for block in soup.find_all("script", attrs={"type": "application/ld+json"}):
            raw = block.string or block.get_text() or ""
            candidate = None
            try:
                obj = json.loads(raw)
                if isinstance(obj, dict) and obj.get("datePublished"):
                    candidate = obj["datePublished"]
                elif isinstance(obj, list):
                    for o in obj:
                        if isinstance(o, dict) and o.get("datePublished"):
                            candidate = o["datePublished"]
                            break
            except Exception:
                m = re.search(r'"datePublished"\s*:\s*"([^"]+)"', raw)
                if m:
                    candidate = m.group(1)
            if candidate:
                try:
                    published_at = dateparse.isoparse(candidate)
                    break
                except Exception:
                    continue

        # Author — YourStory uses various layouts; try a few common patterns.
        author: str | None = None
        for sel in ['a[rel="author"]', "[itemprop=author]", ".author-name", ".byline a", ".author a"]:
            el = soup.select_one(sel)
            if el and el.get_text(strip=True):
                author = re.sub(r"\s+", " ", el.get_text(strip=True))
                break

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
        return False  # listing is dynamic; dispatch handled in _fetch_html
