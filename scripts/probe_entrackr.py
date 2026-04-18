"""One-off probe — aggressive scroll + multiple listing URL candidates."""
from __future__ import annotations

import asyncio
from collections import Counter

LIST_URLS = [
    "https://entrackr.com/category/funding-news",
    "https://entrackr.com/category/news/funding-news",
    "https://entrackr.com/",
]


async def count_after_scrolls(page, url: str, n_scrolls: int) -> tuple[int, list[str]]:
    await page.goto(url, wait_until="domcontentloaded", timeout=30000)
    prev_h = 0
    for _ in range(n_scrolls):
        h = await page.evaluate("document.body.scrollHeight")
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(1800)
        if h == prev_h:
            break
        prev_h = h

    hrefs = await page.eval_on_selector_all(
        "a[href]", "els => els.map(e => e.getAttribute('href'))"
    )
    internal = []
    for h in hrefs:
        if not h:
            continue
        if h.startswith("/"):
            h = f"https://entrackr.com{h}"
        if "entrackr.com" not in h:
            continue
        internal.append(h)

    articleish = []
    for u in internal:
        path = u.replace("https://entrackr.com", "").strip("/")
        if not path or path.startswith(("category/", "tag/", "author/", "about", "contact", "subscribe", "login", "signup")):
            continue
        if "?" in path or "#" in path:
            continue
        last = path.split("/")[-1]
        if last.count("-") >= 2 and len(last) > 15:
            articleish.append(u)

    uniq = list(dict.fromkeys(articleish))
    return len(uniq), uniq


async def main() -> None:
    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124"
        )
        for url in LIST_URLS:
            page = await ctx.new_page()
            n, uniq = await count_after_scrolls(page, url, 10)
            print(f"\n=== {url} — {n} article-ish hrefs")
            for u in uniq[:15]:
                print(" ", u)
            ctr = Counter(u.replace("https://entrackr.com/", "").split("/")[0] for u in uniq)
            print("prefixes:", ctr.most_common())
            await page.close()
        await ctx.close()
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
