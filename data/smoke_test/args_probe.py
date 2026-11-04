"""Probe: scraper's DOCKER browser args vs clean launch — which renders the page?"""
import asyncio
from playwright.async_api import async_playwright

URL = "https://www.oddsportal.com/football/england/premier-league-2024-2025/results/"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
DOCKER_ARGS = ["--disable-dev-shm-usage", "--no-sandbox", "--headless", "--disable-gpu",
               "--disable-background-networking", "--disable-popup-blocking", "--disable-extensions",
               "--disable-blink-features=AutomationControlled", "--disable-features=IsolateOrigins,site-per-process",
               "--mute-audio", "--window-size=1280,720"]

async def probe(label, args):
    async with async_playwright() as p:
        b = await p.chromium.launch(headless=True, args=args)
        ctx = await b.new_context(user_agent=UA, locale="en-GB", timezone_id="Europe/London")
        page = await ctx.new_page()
        try:
            await page.goto(URL, timeout=45000, wait_until="domcontentloaded")
            await page.wait_for_timeout(5000)
            anchors = await page.locator("a[href*='premier-league']").count()
            height = await page.evaluate("document.body.scrollHeight")
            print(f"[{label}] anchors={anchors} page_height={height}")
        except Exception as e:
            print(f"[{label}] EXC: {e}")
        await b.close()

async def main():
    await probe("scraper_docker_args", DOCKER_ARGS)
    await probe("clean_no_headless_arg", [a for a in DOCKER_ARGS if a != "--headless"])

asyncio.run(main())
