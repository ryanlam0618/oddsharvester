"""Bisect: which PlaywrightManager component breaks the render?"""
import asyncio
import sys

import oddsharvester.core.playwright_manager as pwm

URL = "https://www.oddsportal.com/football/england/premier-league-2024-2025/results/"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

MODE = sys.argv[1] if len(sys.argv) > 1 else "baseline"

if MODE in ("no_route", "neither"):
    pwm.PlaywrightManager._block_unneeded_resources = lambda self, page: print(f"[{MODE}] route blocking DISABLED")
if MODE in ("no_stealth", "neither"):
    pwm.STEALTH_SCRIPT = "// stealth disabled for bisect"

from oddsharvester.core.playwright_manager import PlaywrightManager


async def main():
    pm = PlaywrightManager()
    await pm.initialize(headless=True, user_agent=UA, locale="en-GB", timezone_id="Europe/London")
    page = pm.page
    try:
        await page.goto(URL, timeout=45000, wait_until="domcontentloaded")
        await page.wait_for_timeout(6000)
        anchors = await page.locator("a[href*='premier-league']").count()
        height = await page.evaluate("document.body.scrollHeight")
        rows = await page.locator("[class*='eventRow']").count()
        print(f"[{MODE}] anchors={anchors} height={height} eventRows={rows}")
    except Exception as e:
        print(f"[{MODE}] EXC: {e}")


asyncio.run(main())
