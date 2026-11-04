"""Diagnostic probe: what does the OddsPortal EPL 2024-2025 page actually render?"""

import asyncio
import sys

from playwright.async_api import async_playwright

URL = "https://www.oddsportal.com/football/england/premier-league-2024-2025/results/"


async def main() -> None:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            locale="en-GB",
            timezone_id="Europe/London",
        )
        page = await ctx.new_page()
        try:
            await page.goto(URL, timeout=45000, wait_until="domcontentloaded")
        except Exception as exc:
            print(f"[goto] exception: {exc}")
        await page.wait_for_timeout(6000)  # let JS settle
        title = await page.title()
        print(f"[title] {title!r}")
        html = await page.content()
        print(f"[html length] {len(html)}")
        low = html.lower()
        for marker in ("cloudflare", "challenge", "cf-", "captcha", "attention required", "just a moment", "access denied", "geo"):
            if marker in low:
                idx = low.find(marker)
                print(f"[marker] {marker!r} found at {idx}: ...{html[max(0, idx - 60):idx + 120]!r}...")
        anchors = await page.locator("a[href*='/football/england/premier-league']").count()
        print(f"[anchors] premier-league links: {anchors}")
        body_text = (await page.locator("body").inner_text())[:600]
        print(f"[body text head] {body_text!r}")
        await page.screenshot(path="data/smoke_test/page_probe.png", full_page=False)
        with open("data/smoke_test/page_dump.html", "w", encoding="utf-8") as fh:
            fh.write(html)
        print("[done] screenshot + html dump saved")
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
