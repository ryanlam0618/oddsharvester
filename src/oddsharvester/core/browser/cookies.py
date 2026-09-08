"""See module docstring in core/browser/__init__.py."""

import logging

from playwright.async_api import Page
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from oddsharvester.core.odds_portal_selectors import OddsPortalSelectors
from oddsharvester.utils.constants import COOKIE_BANNER_TIMEOUT_MS


class CookieDismisser:
    """Dismiss the cookie consent banner if present on the page."""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    async def dismiss(
        self,
        page: Page,
        selector: str | None = None,
        timeout: int = COOKIE_BANNER_TIMEOUT_MS,
    ) -> bool:
        """Dismiss the cookie banner if it appears.

        Returns True if a banner was found and dismissed, False otherwise (banner absent or click failed).
        """
        if selector is None:
            selector = OddsPortalSelectors.COOKIE_BANNER

        try:
            self.logger.info("Checking for cookie banner...")
            await page.wait_for_selector(selector, timeout=timeout)
            self.logger.info("Cookie banner found. Dismissing it.")
            await page.click(selector)
            return True

        except PlaywrightTimeoutError:
            self.logger.info("No cookie banner detected.")
            return False

        except Exception as e:
            self.logger.error(f"Error while dismissing cookie banner: {e}")
            return False
