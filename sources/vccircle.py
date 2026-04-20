"""VCCircle scraper.

VCCircle is a PE/VC specialist — shorter articles, structured reporting of
round size + stage + investors. Higher signal-to-noise than mainstream press.

Strategy:
  - Listing: static HTTPX on the startup/funding category. Pagination via
    `?page=N` query param.
  - Detail: static HTTPX + BeautifulSoup. OG tags + `.field--name-body` body +
    JSON-LD datePublished.

Note: VCCircle has partial paywalls on some deep-dive pieces. The free snippet
(~300 words) is usually enough to identify company + amount + stage, which is
all we need for structured extraction.
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
from sources.base_scraper import BaseScraper, SourceConfig

FUNDING_KEYWORDS = (
    "rais", "raises", "raised", "funding", "seed", "series-a", "series-b",
    "series-c", "series-d", "series-e", "pre-series", "bags", "secures",
    "mops-up", "closes", "-mn-", "-cr-", "-million", "-crore", "round",
    "debt-funding", "bridge-round", "acquires", "acquisition", "invest",
    "backs", "led-by",
)

_NON_ARTICLE_SECTION_RE = re.compile(
    r"^https?://(www\.)?vccircle\.com/"
    r"(tag|topic|author|page|category|user|search|events|newsletter|subscribe)/",
    re.IGNORECASE,
)


def _looks_like_funding(url: str) -> bool:
    low = url.lower()
    return any(k in low for k in FUNDING_KEYWORDS)


class VCCircleScraper(BaseScraper):
    def __init__(self, config: SourceConfig) -> None:
        super().__init__(config)
        self._max_pages: int = int(config.pagination.get("max_pages", 3))
        self._max_articles: int = int(config.pagination.get("max_articles", 60))

    def iter_list_urls(self) -> Iterable[str]:
        for base in self.config.list_urls:
            yield base
            for page in range(1, self._max_pages):
                # VCCircle uses Drupal-style `?page=N` (0-indexed; page=1 = second).
                sep = "&" if "?" in base else "?"
                yield f"{base}{sep}page={page}"

    def parse_list(self, html: str, list_url: str) -> list[str]:
        soup = BeautifulSoup(html, "lxml")
        urls: list[str] = []
        for a in soup.find_all("a", href=True):
            h = (a.get("href") or "").strip()
            if h.startswith("/"):
                h = "https://www.vccircle.com" + h
            if "vccircle.com" not in h:
                continue
            h = h.split("?")[0].split("#")[0]
            if _NON_ARTICLE_SECTION_RE.match(h):
                continue
            # Article URLs are slugs 3+ path segments deep under a category.
            path = h.replace("https://www.vccircle.com", "").replace(
                "https://vccircle.com", ""
            ).strip("/")
            if path.count("/") < 2:
                continue
            if not _looks_like_funding(h):
                continue
            urls.append(h)
        seen: set[str] = set()
        out: list[str] = []
        for u in urls:
            if u not in seen:
                seen.add(u)
                out.append(u)
            if len(out) >= self._max_articles:
                break
        logger.debug("[vccircle] {} -> {} funding URLs", list_url, len(out))
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
            candidate: str | None = None
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

        author: str | None = None
        for sel in (".field--name-field-author a", ".author a", "[rel=author]"):
            el = soup.select_one(sel)
            if el and el.get_text(strip=True):
                author = re.sub(r"\s+", " ", el.get_text(strip=True))
                break

        body_el = (
            soup.select_one(".field--name-body")
            or soup.select_one(".node__content")
            or soup.find("article")
        )
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
        return False
