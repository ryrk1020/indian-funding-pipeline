"""Abstract base for every source scraper.

Contract:
  - iter_list_urls()         → yields list-page URLs (pagination handled here)
  - parse_list(html, url)    → returns candidate article URLs on that list page
  - parse_detail(html, url)  → returns one ArticleRaw
The base class orchestrates fetching, rate limiting, and turning failures into
logged events instead of crashes.
"""
from __future__ import annotations

import abc
from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass, field
from typing import Any

from loguru import logger

from config.schemas import ArticleRaw
from pipeline.fetcher import Fetcher, FetchError


@dataclass
class SourceConfig:
    name: str
    kind: str  # "static" | "dynamic"
    base_url: str
    list_urls: list[str]
    pagination: dict[str, Any] = field(default_factory=dict)
    selectors: dict[str, str] = field(default_factory=dict)
    rate_limit_per_sec: float = 1.0
    enabled: bool = True


@dataclass
class ScrapeStats:
    list_pages: int = 0
    candidate_urls: int = 0
    articles_ok: int = 0
    articles_failed: int = 0


class BaseScraper(abc.ABC):
    """Subclasses implement the three parse_* methods + iter_list_urls."""

    config: SourceConfig

    def __init__(self, config: SourceConfig) -> None:
        self.config = config
        self.stats = ScrapeStats()

    # ---- subclass contract -------------------------------------------------
    @abc.abstractmethod
    def iter_list_urls(self) -> Iterable[str]:
        """Yield list-page URLs in crawl order. Pagination lives here."""

    @abc.abstractmethod
    def parse_list(self, html: str, list_url: str) -> list[str]:
        """Given a list page HTML, return candidate article URLs."""

    @abc.abstractmethod
    def parse_detail(self, html: str, url: str) -> ArticleRaw:
        """Given an article page HTML, return an ArticleRaw."""

    # ---- fetch knobs (override per source) --------------------------------
    @property
    def is_dynamic(self) -> bool:
        return self.config.kind == "dynamic"

    async def _fetch_html(self, fetcher: Fetcher, url: str) -> str:
        if self.is_dynamic:
            pag = self.config.pagination or {}
            scrolls = int(pag.get("max_scrolls", 0)) if pag.get("strategy") == "infinite_scroll" else 0
            res = await fetcher.fetch_rendered(url, scroll_times=scrolls)
        else:
            res = await fetcher.fetch(url)
        return res.html

    # ---- orchestration -----------------------------------------------------
    async def crawl(
        self,
        fetcher: Fetcher,
        *,
        limit: int | None = None,
    ) -> AsyncIterator[ArticleRaw]:
        """Walk list pages → yield ArticleRaw per article. Errors are logged, not raised."""
        fetcher.rate_limiter.set_host_rate(
            host=self.config.base_url.split("://", 1)[-1].split("/")[0],
            per_sec=self.config.rate_limit_per_sec,
        )

        seen: set[str] = set()
        produced = 0
        for list_url in self.iter_list_urls():
            self.stats.list_pages += 1
            try:
                html = await self._fetch_html(fetcher, list_url)
            except FetchError as e:
                logger.error("[{}] list fetch failed {}: {}", self.config.name, list_url, e)
                continue
            try:
                candidates = self.parse_list(html, list_url)
            except Exception as e:
                logger.exception("[{}] parse_list crashed on {}: {}", self.config.name, list_url, e)
                continue
            logger.info(
                "[{}] list {} → {} candidates", self.config.name, list_url, len(candidates)
            )
            for art_url in candidates:
                if art_url in seen:
                    continue
                seen.add(art_url)
                self.stats.candidate_urls += 1
                try:
                    detail_html = await self._fetch_html(fetcher, art_url)
                    article = self.parse_detail(detail_html, art_url)
                except FetchError as e:
                    self.stats.articles_failed += 1
                    logger.warning("[{}] detail fetch failed {}: {}", self.config.name, art_url, e)
                    continue
                except Exception as e:
                    self.stats.articles_failed += 1
                    logger.exception("[{}] parse_detail failed {}: {}", self.config.name, art_url, e)
                    continue
                self.stats.articles_ok += 1
                produced += 1
                yield article
                if limit is not None and produced >= limit:
                    return
