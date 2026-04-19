import logging

from playwright.async_api import Page

from oddsharvester.core.odds_portal_selectors import OddsPortalSelectors
from oddsharvester.utils.constants import (
    ODDS_HISTORY_HOVER_WAIT_MS,
    ODDS_HISTORY_PRE_WAIT_MS,
    ODDS_MOVEMENT_SELECTOR_TIMEOUT_MS,
)


class OddsHistoryExtractor:
    """Handles extraction of odds history data by hovering over bookmaker odds."""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    async def _clear_bookie_overlay(self, page: Page) -> int:
        """Best-effort cleanup for bookmaker overlay/modal layers that intercept pointer events."""
        dismissed = 0
        try:
            for selector in OddsPortalSelectors.BOOKIE_OVERLAY_DISMISS_SELECTORS:
                try:
                    buttons = await page.query_selector_all(selector)
                    for button in buttons:
                        try:
                            if await button.is_visible():
                                await button.click(timeout=1000)
                                dismissed += 1
                        except Exception:
                            continue
                except Exception:
                    continue

            removed = await page.evaluate(
                """
                (selectors) => {
                    let count = 0;
                    for (const selector of selectors) {
                        for (const el of document.querySelectorAll(selector)) {
                            try {
                                el.remove();
                                count += 1;
                            } catch {}
                        }
                    }
                    return count;
                }
                """,
                OddsPortalSelectors.BOOKIE_OVERLAY_SELECTORS,
            )
            dismissed += int(removed or 0)
        except Exception as e:
            self.logger.debug(f"Bookie overlay cleanup skipped due to error: {e}")

        if dismissed:
            self.logger.info(f"Dismissed {dismissed} overlay element(s)")
        return dismissed

    async def extract_odds_history_for_bookmaker(self, page: Page, bookmaker_name: str) -> list[str]:
        """
        Hover on odds for a specific bookmaker to trigger and capture the odds history modal.

        Args:
            page (Page): Playwright page instance.
            bookmaker_name (str): Name of the bookmaker to match.

        Returns:
            List[str]: List of raw HTML content from modals triggered by hovering over matched odds blocks.
        """
        self.logger.info(f"Extracting odds history for bookmaker: {bookmaker_name}")
        await page.wait_for_timeout(ODDS_HISTORY_PRE_WAIT_MS)

        modals_data = []

        try:
            rows = await page.query_selector_all(OddsPortalSelectors.BOOKMAKER_ROW_CSS)

            for row in rows:
                try:
                    logo_img = await row.query_selector(OddsPortalSelectors.BOOKMAKER_LOGO_CSS)

                    if logo_img:
                        title = await logo_img.get_attribute("title")

                        if title and bookmaker_name.lower() in title.lower():
                            self.logger.info(f"Found matching bookmaker row: {title}")
                            odds_blocks = await row.query_selector_all(OddsPortalSelectors.ODDS_BLOCK_CSS)

                            for odds in odds_blocks:
                                try:
                                    await self._clear_bookie_overlay(page)
                                    await odds.scroll_into_view_if_needed()
                                    await odds.hover(timeout=5000)
                                except Exception as hover_error:
                                    self.logger.warning(
                                        f"Hover failed for bookmaker '{bookmaker_name}', retrying after overlay cleanup: {hover_error}"
                                    )
                                    await self._clear_bookie_overlay(page)
                                    try:
                                        await page.evaluate(
                                            """
                                            (el) => {
                                                el.dispatchEvent(new MouseEvent('mouseover', { bubbles: true }));
                                                el.dispatchEvent(new MouseEvent('mouseenter', { bubbles: true }));
                                            }
                                            """,
                                            odds,
                                        )
                                    except Exception as js_hover_error:
                                        self.logger.warning(
                                            f"JS hover fallback failed for bookmaker '{bookmaker_name}': {js_hover_error}"
                                        )
                                        continue

                                await page.wait_for_timeout(ODDS_HISTORY_HOVER_WAIT_MS)

                                try:
                                    odds_movement_element = await page.wait_for_selector(
                                        OddsPortalSelectors.ODDS_MOVEMENT_HEADER,
                                        timeout=ODDS_MOVEMENT_SELECTOR_TIMEOUT_MS,
                                    )
                                except Exception:
                                    self.logger.debug(
                                        f"Odds movement modal not found for bookmaker '{bookmaker_name}' on this odds block."
                                    )
                                    continue

                                modal_wrapper = await odds_movement_element.evaluate_handle("node => node.parentElement")
                                modal_element = modal_wrapper.as_element()

                                if modal_element:
                                    html = await modal_element.inner_html()
                                    modals_data.append(html)
                                    await self._clear_bookie_overlay(page)
                                else:
                                    self.logger.warning(
                                        "Unable to retrieve odds' evolution modal: modal_element is None"
                                    )

                except Exception as e:
                    self.logger.warning(f"Failed to process a bookmaker row: {e}")
        except Exception as e:
            self.logger.warning(f"Failed to extract odds history for bookmaker {bookmaker_name}: {e}")

        return modals_data
