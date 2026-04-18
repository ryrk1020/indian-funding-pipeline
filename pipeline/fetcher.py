"""HTTP fetching with retries, rate limiting, and a uniform interface.

Keeps Playwright optional (imported lazily) so static-only runs don't pay the cost.
"""
from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

import httpx
from loguru import logger
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from config.settings import settings


class FetchError(RuntimeError):
    pass


_TRANSIENT = (httpx.TimeoutException, httpx.TransportError, httpx.RemoteProtocolError)


@dataclass
class FetchResult:
    url: str
    status: int
    html: str
    final_url: str


class RateLimiter:
    """Simple per-host token bucket. Async-safe."""

    def __init__(self, per_host_rate: float = 1.0) -> None:
        self.default_rate = per_host_rate
        self._rates: dict[str, float] = {}
        self._last: dict[str, float] = defaultdict(lambda: 0.0)
        self._lock = asyncio.Lock()

    def set_host_rate(self, host: str, per_sec: float) -> None:
        self._rates[host] = per_sec

    async def acquire(self, url: str) -> None:
        host = urlparse(url).netloc
        rate = self._rates.get(host, self.default_rate)
        if rate <= 0:
            return
        min_gap = 1.0 / rate
        async with self._lock:
            now = time.monotonic()
            wait = self._last[host] + min_gap - now
            if wait > 0:
                await asyncio.sleep(wait)
            self._last[host] = time.monotonic()


class Fetcher:
    """Async HTTPX-based fetcher. Use `fetch()` for static pages.

    For dynamic sources, use `fetch_rendered()` which lazily spins a Playwright
    browser once per process and reuses it.
    """

    def __init__(
        self,
        *,
        timeout: float | None = None,
        user_agent: str | None = None,
        rate_limit_per_host: float | None = None,
    ) -> None:
        self._timeout = timeout or settings.pipeline_request_timeout
        self._ua = user_agent or settings.pipeline_user_agent
        self.rate_limiter = RateLimiter(
            rate_limit_per_host or settings.pipeline_rate_limit_per_host
        )
        self._client: httpx.AsyncClient | None = None
        self._browser: Any = None
        self._pw: Any = None

    async def __aenter__(self) -> Fetcher:
        self._client = httpx.AsyncClient(
            timeout=self._timeout,
            headers={
                "User-Agent": self._ua,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-IN,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "X-Crawler-Identity": "funding-pipeline/0.1 (+https://github.com/ryrk1020/funding-pipeline)",
            },
            follow_redirects=True,
            http2=False,
        )
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._client is not None:
            await self._client.aclose()
        if self._browser is not None:
            await self._browser.close()
        if self._pw is not None:
            await self._pw.stop()

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        retry=retry_if_exception_type(_TRANSIENT),
        reraise=True,
    )
    async def _get(self, url: str) -> httpx.Response:
        assert self._client is not None, "use Fetcher as async context manager"
        r = await self._client.get(url)
        if r.status_code >= 500:
            raise httpx.HTTPStatusError(
                f"{r.status_code} on {url}", request=r.request, response=r
            )
        return r

    async def fetch(self, url: str) -> FetchResult:
        await self.rate_limiter.acquire(url)
        try:
            r = await self._get(url)
        except Exception as e:
            logger.warning("fetch failed: {} -> {}", url, e)
            raise FetchError(str(e)) from e
        if r.status_code >= 400:
            raise FetchError(f"HTTP {r.status_code} on {url}")
        return FetchResult(url=url, status=r.status_code, html=r.text, final_url=str(r.url))

    async def fetch_rendered(
        self,
        url: str,
        *,
        wait_selector: str | None = None,
        scroll_times: int = 0,
        scroll_pause_ms: int = 800,
    ) -> FetchResult:
        """Render via Playwright. Imported lazily."""
        await self.rate_limiter.acquire(url)
        if self._browser is None:
            from playwright.async_api import async_playwright

            self._pw = await async_playwright().start()
            self._browser = await self._pw.chromium.launch(headless=True)

        context = await self._browser.new_context(user_agent=self._ua)
        page = await context.new_page()
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=self._timeout * 1000)
            if wait_selector:
                try:
                    await page.wait_for_selector(wait_selector, timeout=10_000)
                except Exception:
                    logger.debug("wait_selector timeout on {}", url)
            for _ in range(scroll_times):
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(scroll_pause_ms)
            html = await page.content()
            final_url = page.url
        finally:
            await context.close()
        return FetchResult(url=url, status=200, html=html, final_url=final_url)
