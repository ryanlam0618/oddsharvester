"""Probe: use PlaywrightManager itself — does ITS context render the page?"""
import asyncio
from playwright.async_api import async_playwright

URL = "https://www.oddsportal.com/football/england/premier-league-2024-2025/results/"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
DOCKER_ARGS = ["--disable-dev-shm-usage", "--no-sandbox", "--headless", "--disable-gpu",
               "--disable-background-networking", "--disable-popup-blocking", "--disable-extensions",
               "--disable-blink-features=AutomationControlled", "--disable-features=IsolateOrigins,site-per-process",
               "--mute-audio", "--window-size=1280,720"]

async def pm_probe():
    from oddsharvester.core.playwright_manager import PlaywrightManager
    pm = PlaywrightManager()
    await pm.initialize(headless=True, user_agent=UA, locale="en-GB", timezone_id="Europe/London")
    page = pm.page
    try:
        await page.goto(URL, timeout=45000, wait_until="domcontentloaded")
        await page.wait_for_timeout(6000)
        anchors = await page.locator("a[href*='premier-league']").count()
        height = await page.evaluate("document.body.scrollHeight")
        rows = await page.locator("[class*='eventRow']").count()
        print(f"[PlaywrightManager] anchors={anchors} height={height} eventRows={rows}")
        await page.screenshot(path="data/smoke_test/pm_probe.png")
    except Exception as e:
        print(f"[PlaywrightManager] EXC: {e}")
    await pm.cleanup() if hasattr(pm, "cleanup") else None

async def main():
    await pm_probe()

asyncio.run(main())
