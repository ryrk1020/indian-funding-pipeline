"""Probe YourStory funding listing."""
from __future__ import annotations

import asyncio
from collections import Counter

LIST_URLS = [
    "https://yourstory.com/category/funding",
    "https://yourstory.com/tag/funding",
    "https://yourstory.com/",
]


async def run(page, url: str, scrolls: int = 8) -> list[str]:
    await page.goto(url, wait_until="domcontentloaded", timeout=30000)
    prev = 0
    for _ in range(scrolls):
        h = await page.evaluate("document.body.scrollHeight")
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(1800)
        if h == prev:
            break
        prev = h
    hrefs = await page.eval_on_selector_all(
        "a[href]", "els => els.map(e => e.getAttribute('href'))"
    )
    internal = []
    for h in hrefs:
        if not h:
            continue
        if h.startswith("/"):
            h = "https://yourstory.com" + h
        if "yourstory.com" not in h:
            continue
        internal.append(h.split("?")[0].split("#")[0])
    articleish = []
    for u in internal:
        path = u.replace("https://yourstory.com", "").strip("/")
        if not path or path.startswith(("category/", "tag/", "topic/", "author/", "about", "contact", "companies", "techsparks", "brands/", "events/")):
            continue
        last = path.split("/")[-1]
        if last.count("-") >= 3 and len(last) > 20:
            articleish.append(u)
    return list(dict.fromkeys(articleish))


async def main() -> None:
    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124"
        )
        for url in LIST_URLS:
            page = await ctx.new_page()
            try:
                uniq = await run(page, url)
                print(f"\n=== {url} — {len(uniq)} article-ish")
                for u in uniq[:12]:
                    print(" ", u)
                ctr = Counter(u.replace("https://yourstory.com/", "").split("/")[0] for u in uniq)
                print("prefixes:", ctr.most_common())
            except Exception as e:
                print(f"=== {url} FAILED: {e}")
            await page.close()
        await ctx.close()
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
