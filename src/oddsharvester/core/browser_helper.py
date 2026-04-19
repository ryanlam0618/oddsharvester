from enum import Enum
import logging
import re
import time

from playwright.async_api import Error as PlaywrightError
from playwright.async_api import Page
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from oddsharvester.core.odds_portal_selectors import OddsPortalSelectors
from oddsharvester.utils.bookies_filter_enum import BookiesFilter
from oddsharvester.utils.constants import (
    BOOKIES_FILTER_TIMEOUT_MS,
    COOKIE_BANNER_TIMEOUT_MS,
    DEFAULT_MARKET_TIMEOUT_MS,
    DROPDOWN_WAIT_MS,
    FALLBACK_VERIFY_WAIT_MS,
    MARKET_TAB_TIMEOUT_MS,
    MAX_SCROLL_ATTEMPTS,
    PERIOD_SELECTOR_TIMEOUT_MS,
    SCROLL_PAUSE_S,
    SCROLL_TIMEOUT_S,
    SCROLL_UNTIL_CLICK_PAUSE_S,
    SCROLL_UNTIL_CLICK_TIMEOUT_S,
    TAB_SWITCH_WAIT_MS,
)


class BrowserHelper:
    """
    A helper class for managing common browser interactions using Playwright.

    This class provides high-level methods for:
    - Cookie banner management
    - Market navigation (including hidden markets)
    - Scrolling operations
    - Element interaction utilities
    """

    def __init__(self):
        """
        Initialize the BrowserHelper class.
        """
        self.logger = logging.getLogger(self.__class__.__name__)

    # =============================================================================
    # COOKIE BANNER MANAGEMENT
    # =============================================================================

    async def dismiss_cookie_banner(
        self, page: Page, selector: str | None = None, timeout: int = COOKIE_BANNER_TIMEOUT_MS
    ):
        """
        Dismiss the cookie banner if it appears on the page.

        Args:
            page (Page): The Playwright page instance to interact with.
            selector (str): The CSS selector for the cookie banner's accept button.
            timeout (int): Maximum time to wait for the banner (default: 10000ms).

        Returns:
            bool: True if the banner was dismissed, False otherwise.
        """
        selectors = [selector] if selector else OddsPortalSelectors.COOKIE_BANNER_SELECTORS

        try:
            self.logger.info("Checking for cookie banner...")

            banner_present = await self._has_cookie_banner(page)
            if not banner_present:
                self.logger.info("No cookie banner detected.")
                return False

            for candidate in selectors:
                try:
                    button = await page.query_selector(candidate)
                    if not button or not await button.is_visible():
                        continue

                    text = (await button.text_content() or "").strip()
                    self.logger.info(
                        f"Cookie banner found via '{candidate}'. Dismissing it."
                        + (f" (text='{text[:60]}')" if text else "")
                    )
                    if await self._safe_click(page=page, element=button, reason=f"cookie banner via {candidate}"):
                        await page.wait_for_timeout(800)
                        if not await self._has_cookie_banner(page):
                            return True
                except Exception as inner_error:
                    self.logger.debug(f"Cookie banner selector '{candidate}' failed: {inner_error}")
                    continue

            self.logger.info("Cookie banner still present after click attempts. Applying hard cleanup.")
            await self._force_accept_and_remove_cookie_banner(page)
            await page.wait_for_timeout(500)

            if not await self._has_cookie_banner(page):
                self.logger.info("Cookie banner removed by hard cleanup.")
                return True

            self.logger.info("Cookie banner still present after dismiss attempts.")
            return False

        except PlaywrightTimeoutError:
            self.logger.info("No cookie banner detected.")
            return False

        except Exception as e:
            self.logger.error(f"Error while dismissing cookie banner: {e}")
            return False

    async def _force_accept_and_remove_cookie_banner(self, page: Page) -> None:
        """Apply a hard fallback for stubborn OneTrust/cookie banners."""
        try:
            await page.evaluate(
                """
                (config) => {
                    const clickIfPresent = (selector) => {
                        const el = document.querySelector(selector);
                        if (!el) return false;
                        try { el.click(); } catch (_) {}
                        try { el.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true })); } catch (_) {}
                        return true;
                    };

                    for (const selector of config.acceptSelectors) {
                        clickIfPresent(selector);
                    }

                    for (const selector of config.removeSelectors) {
                        for (const el of document.querySelectorAll(selector)) {
                            try { el.remove(); } catch (_) {}
                        }
                    }

                    for (const el of document.querySelectorAll('[style*="overflow: hidden"], [style*="overflow:hidden"]')) {
                        try { el.style.removeProperty('overflow'); } catch (_) {}
                    }

                    document.documentElement.classList.remove('onetrust-consent-sdk', 'onetrust-lock', 'modal-open');
                    document.body.classList.remove('onetrust-consent-sdk', 'onetrust-lock', 'modal-open');
                    document.documentElement.style.removeProperty('overflow');
                    document.body.style.removeProperty('overflow');
                    document.documentElement.style.removeProperty('pointer-events');
                    document.body.style.removeProperty('pointer-events');
                }
                """,
                {
                    "acceptSelectors": OddsPortalSelectors.COOKIE_BANNER_SELECTORS,
                    "removeSelectors": OddsPortalSelectors.COOKIE_BANNER_CONTAINER_SELECTORS
                    + [".onetrust-pc-dark-filter", "#onetrust-banner-sdk", "#onetrust-consent-sdk"],
                },
            )
        except Exception as e:
            self.logger.debug(f"Hard cookie cleanup failed: {e}")

    async def _has_cookie_banner(self, page: Page) -> bool:
        """Return True only when a known cookie/consent surface is visibly present."""
        try:
            for selector in OddsPortalSelectors.COOKIE_BANNER_PRESENCE_SELECTORS:
                try:
                    elements = await page.query_selector_all(selector)
                    for element in elements[:5]:
                        if await element.is_visible():
                            return True
                except PlaywrightError:
                    continue
            return False
        except Exception:
            return False

    async def _is_inside_cookie_banner(self, page: Page, element) -> bool:
        """Check whether an element belongs to a visible cookie banner/consent surface."""
        try:
            return bool(
                await element.evaluate(
                    """
                    (el, selectors) => {
                        for (const selector of selectors) {
                            const container = el.closest(selector);
                            if (!container) continue;
                            const style = window.getComputedStyle(container);
                            const visible = style && style.display !== 'none' && style.visibility !== 'hidden';
                            if (visible) return true;
                        }
                        return false;
                    }
                    """,
                    OddsPortalSelectors.COOKIE_BANNER_CONTAINER_SELECTORS,
                )
            )
        except Exception:
            return False

    async def dismiss_overlays(self, page: Page) -> bool:
        """Try to dismiss or neutralize obstructive overlays/modals that intercept clicks."""
        dismissed_any = False

        try:
            if await self._has_cookie_banner(page):
                await self.dismiss_cookie_banner(page)

            for dismiss_selector in OddsPortalSelectors.OVERLAY_DISMISS_SELECTORS:
                try:
                    elements = await page.query_selector_all(dismiss_selector)
                    for element in elements[:5]:
                        try:
                            if not await element.is_visible():
                                continue

                            if await self._is_inside_cookie_banner(page, element):
                                continue

                            text = (await element.text_content() or "").strip()
                            self.logger.info(
                                f"Attempting overlay dismiss via '{dismiss_selector}'"
                                + (f" (text='{text[:60]}')" if text else "")
                            )
                            if await self._safe_click(page=page, element=element, reason="overlay dismiss"):
                                dismissed_any = True
                                await page.wait_for_timeout(500)
                        except Exception as click_error:
                            self.logger.debug(f"Overlay dismiss click failed: {click_error}")
                except Exception as selector_error:
                    self.logger.debug(f"Overlay dismiss selector failed '{dismiss_selector}': {selector_error}")

            # Hide lingering overlays that still intercept pointer events.
            await page.evaluate(
                """
                (selectors) => {
                    for (const selector of selectors) {
                        for (const el of document.querySelectorAll(selector)) {
                            const style = window.getComputedStyle(el);
                            const isVisible = style && style.display !== 'none' && style.visibility !== 'hidden';
                            if (isVisible) {
                                el.setAttribute('data-oh-hidden-overlay', '1');
                                el.style.setProperty('display', 'none', 'important');
                                el.style.setProperty('visibility', 'hidden', 'important');
                                el.style.setProperty('pointer-events', 'none', 'important');
                            }
                        }
                    }
                }
                """,
                OddsPortalSelectors.OVERLAY_SELECTORS,
            )

            return dismissed_any
        except Exception as e:
            self.logger.debug(f"Overlay dismissal encountered an issue: {e}")
            return dismissed_any

    # =============================================================================
    # BOOKMAKER FILTER MANAGEMENT
    # =============================================================================

    async def ensure_bookies_filter_selected(self, page: Page, desired_filter: BookiesFilter) -> bool:
        """
        Ensure the desired bookmaker filter is selected on the page.

        This method:
        1. Checks if the bookies filter nav is present
        2. Reads the currently selected filter
        3. If it matches desired filter, does nothing
        4. Otherwise, clicks the desired filter option
        5. Waits for the selection to update

        Args:
            page (Page): The Playwright page instance.
            desired_filter (BookiesFilter): The desired bookmaker filter to select.

        Returns:
            bool: True if the desired filter is selected, False otherwise.
        """
        try:
            display_label = BookiesFilter.get_display_label(desired_filter)
            self.logger.info(f"Ensuring bookmaker filter is set to: {display_label}")

            # Check if bookies filter nav exists
            filter_container = await page.query_selector(OddsPortalSelectors.BOOKIES_FILTER_CONTAINER)
            if not filter_container:
                self.logger.warning("Bookies filter navigation not found on page. Skipping filter selection.")
                return False

            # Get current selected filter
            current_filter = await self._get_current_bookies_filter(page)
            if current_filter:
                self.logger.info(f"Current bookmaker filter: {current_filter}")

                # If already selected, do nothing
                if current_filter == desired_filter.value:
                    self.logger.info(f"Bookmaker filter already set to '{desired_filter.value}'. No action needed.")

                    return True

            # Click the desired filter
            filter_selector = OddsPortalSelectors.get_bookies_filter_selector(desired_filter.value)

            self.logger.info(f"Clicking bookmaker filter: {BookiesFilter.get_display_label(desired_filter)}")
            filter_element = await page.query_selector(filter_selector)

            if not filter_element:
                self.logger.error(f"Bookmaker filter element not found for: {desired_filter.value}")
                return False

            await filter_element.click()

            # Wait for selection to update using robust wait condition
            try:
                active_class = OddsPortalSelectors.BOOKIES_FILTER_ACTIVE_CLASS
                await page.wait_for_function(
                    f"""
                    () => {{
                        const container = document.querySelector('[data-testid="bookies-filter-nav"]');
                        if (!container) return false;
                        const activeElement = container.querySelector('.{active_class}');
                        if (!activeElement) return false;
                        return activeElement.getAttribute('data-testid') === '{desired_filter.value}';
                    }}
                    """,
                    timeout=BOOKIES_FILTER_TIMEOUT_MS,
                )
                display_label = BookiesFilter.get_display_label(desired_filter)
                self.logger.info(f"Successfully set bookmaker filter to: {display_label}")
                return True

            except Exception as wait_error:
                self.logger.warning(f"Wait condition failed: {wait_error}. Verifying selection...")

                # Fallback: verify the selection after a short delay
                await page.wait_for_timeout(FALLBACK_VERIFY_WAIT_MS)
                new_filter = await self._get_current_bookies_filter(page)
                if new_filter == desired_filter.value:
                    self.logger.info(f"Bookmaker filter successfully set to: {desired_filter.value}")
                    return True
                else:
                    self.logger.error(f"Failed to set bookmaker filter to: {desired_filter.value}")
                    return False

        except Exception as e:
            self.logger.error(f"Error setting bookmaker filter: {e}")
            return False

    async def _get_current_bookies_filter(self, page: Page) -> str | None:
        """
        Get the currently selected bookmaker filter.

        Args:
            page (Page): The Playwright page instance.

        Returns:
            str | None: The data-testid of the currently selected filter, or None if not found.
        """
        try:
            # Find the active element within the bookies filter container
            active_selector = (
                f"{OddsPortalSelectors.BOOKIES_FILTER_CONTAINER} .{OddsPortalSelectors.BOOKIES_FILTER_ACTIVE_CLASS}"
            )
            active_element = await page.query_selector(active_selector)

            if active_element:
                data_testid = await active_element.get_attribute("data-testid")
                return data_testid

            self.logger.warning("No active bookmaker filter found")
            return None

        except Exception as e:
            self.logger.error(f"Error getting current bookmaker filter: {e}")
            return None

    # =============================================================================
    # PERIOD SELECTION MANAGEMENT
    # =============================================================================

    async def ensure_period_selected(self, page: Page, desired_period: Enum) -> bool:
        """
        Ensure the desired match period is selected on the page.

        This method:
        1. Checks if the period selector nav is present
        2. Reads the currently selected period
        3. If it matches desired period, does nothing
        4. Otherwise, clicks the desired period option
        5. Waits for the selection to update

        Args:
            page (Page): The Playwright page instance.
            desired_period: The desired period enum to select.

        Returns:
            bool: True if the desired period is selected, False otherwise.
        """
        try:
            # All period enums have get_display_label method
            display_label = desired_period.get_display_label(desired_period)
            self.logger.info(f"Ensuring match period is set to: {display_label}")

            # Check if period selector nav exists
            period_container = await page.query_selector(OddsPortalSelectors.PERIOD_SELECTOR_CONTAINER)
            if not period_container:
                self.logger.warning("Period selector navigation not found on page. Skipping period selection.")
                return False

            # Get current selected period
            current_period = await self._get_current_period(page)
            if current_period:
                self.logger.info(f"Current match period: {current_period}")

                # If already selected, do nothing
                if current_period == display_label:
                    self.logger.info(f"Match period already set to '{display_label}'. No action needed.")
                    return True

            # Click the desired period
            self.logger.info(f"Clicking match period: {display_label}")

            # Find the period element by text within the container
            period_element = await page.query_selector(
                f"{OddsPortalSelectors.PERIOD_SELECTOR_CONTAINER} div:has-text('{display_label}')"
            )

            if not period_element:
                self.logger.error(f"Period element not found for: {display_label}")
                return False

            await period_element.click()

            # Wait for selection to update using robust wait condition
            try:
                active_class = OddsPortalSelectors.PERIOD_ACTIVE_CLASS
                await page.wait_for_function(
                    f"""
                    () => {{
                        const container = document.querySelector('[data-testid="kickoff-events-nav"]');
                        if (!container) return false;
                        const activeElement = container.querySelector('.{active_class}');
                        if (!activeElement) return false;
                        return activeElement.textContent.trim() === '{display_label}';
                    }}
                    """,
                    timeout=PERIOD_SELECTOR_TIMEOUT_MS,
                )
                self.logger.info(f"Successfully set match period to: {display_label}")
                return True

            except Exception as wait_error:
                self.logger.warning(f"Wait condition failed: {wait_error}. Verifying selection...")

                # Fallback: verify the selection after a short delay
                await page.wait_for_timeout(FALLBACK_VERIFY_WAIT_MS)
                new_period = await self._get_current_period(page)
                if new_period == display_label:
                    self.logger.info(f"Match period successfully set to: {display_label}")
                    return True
                else:
                    self.logger.error(f"Failed to set match period to: {display_label}")
                    return False

        except Exception as e:
            self.logger.error(f"Error setting match period: {e}")
            return False

    async def _get_current_period(self, page: Page) -> str | None:
        """
        Get the currently selected match period.

        Args:
            page (Page): The Playwright page instance.

        Returns:
            str | None: The text of the currently selected period, or None if not found.
        """
        try:
            # Find the active element within the period selector container
            active_selector = (
                f"{OddsPortalSelectors.PERIOD_SELECTOR_CONTAINER} .{OddsPortalSelectors.PERIOD_ACTIVE_CLASS}"
            )
            active_element = await page.query_selector(active_selector)

            if active_element:
                period_text = await active_element.text_content()
                return period_text.strip() if period_text else None

            self.logger.warning("No active period found")
            return None

        except Exception as e:
            self.logger.error(f"Error getting current period: {e}")
            return None

    # =============================================================================
    # MARKET NAVIGATION
    # =============================================================================

    async def navigate_to_market_tab(self, page: Page, market_tab_name: str, timeout=MARKET_TAB_TIMEOUT_MS):
        """
        Navigate to a specific market tab by its name.
        Now supports hidden markets under the "More" dropdown.

        Args:
            page: The Playwright page instance.
            market_tab_name: The name of the market tab to navigate to (e.g., 'Over/Under', 'Draw No Bet').
            timeout: Timeout in milliseconds.

        Returns:
            bool: True if the market tab was successfully selected, False otherwise.
        """
        self.logger.info(f"Attempting to navigate to market tab: {market_tab_name}")
        await self.dismiss_overlays(page)

        # First attempt: Try to find the market directly in visible tabs
        market_found = False
        for selector in OddsPortalSelectors.MARKET_TAB_SELECTORS:
            if await self._wait_and_click(page=page, selector=selector, text=market_tab_name, timeout=timeout):
                market_found = True
                break

        if market_found:
            # Verify that the tab is actually active
            if await self._verify_tab_is_active(page, market_tab_name):
                self.logger.info(f"Successfully navigated to {market_tab_name} tab (directly visible).")
                return True
            else:
                self.logger.warning(f"Tab {market_tab_name} was clicked but is not active.")

        # Second attempt: Try to find the market in the "More" dropdown
        self.logger.info(f"Market '{market_tab_name}' not found in visible tabs. Checking 'More' dropdown...")
        if await self._click_more_if_market_hidden(page, market_tab_name, timeout):
            # Verify that the tab is actually active
            if await self._verify_tab_is_active(page, market_tab_name):
                self.logger.info(f"Successfully navigated to {market_tab_name} tab (via 'More' dropdown).")
                return True
            else:
                self.logger.warning(f"Tab {market_tab_name} was clicked but is not active.")

        self.logger.error(
            f"Failed to find or click the {market_tab_name} tab (searched visible tabs and 'More' dropdown)."
        )
        return False

    # =============================================================================
    # SCROLLING OPERATIONS
    # =============================================================================

    async def scroll_until_loaded(
        self,
        page: Page,
        timeout=SCROLL_TIMEOUT_S,
        scroll_pause_time=SCROLL_PAUSE_S,
        max_scroll_attempts=MAX_SCROLL_ATTEMPTS,
        content_check_selector: str | None = None,
    ):
        """
        Scrolls down the page until no new content is loaded or a timeout is reached.

        This method is useful for pages that load content dynamically as the user scrolls.
        It attempts to scroll the page to the bottom multiple times, waiting for a specified
        interval between scrolls. Scrolling stops when no new content is detected, a timeout
        occurs, or the maximum number of scroll attempts is reached.

        Args:
            page (Page): The Playwright page instance to interact with.
            timeout (int): The maximum time (in seconds) to attempt scrolling (default: 30).
            scroll_pause_time (int): The time (in seconds) to pause between scrolls (default: 3).
            max_scroll_attempts (int): The maximum number of attempts to detect new content (default: 5).
            content_check_selector (str): Optional CSS selector to check for new content after scrolling.

        Returns:
            bool: True if scrolling completed successfully, False otherwise.
        """
        self.logger.info("Will scroll to the bottom of the page to load all content.")
        end_time = time.time() + timeout
        last_height = await page.evaluate("document.body.scrollHeight")
        last_element_count = 0
        stable_count_attempts = 0

        # Get initial element count if selector is provided
        if content_check_selector:
            initial_elements = await page.query_selector_all(content_check_selector)
            last_element_count = len(initial_elements)
            self.logger.info(f"Initial element count: {last_element_count}")

        self.logger.info(f"Initial page height: {last_height}")

        scroll_step = 500
        current_scroll_pos = 0

        while time.time() < end_time:
            # Scroll incrementally to trigger lazy-loading content
            page_height = await page.evaluate("document.body.scrollHeight")
            if current_scroll_pos < page_height:
                current_scroll_pos = min(current_scroll_pos + scroll_step, page_height)
                await page.evaluate(f"window.scrollTo(0, {current_scroll_pos})")
            else:
                # Already at bottom, nudge to trigger any remaining loads
                await page.evaluate(f"window.scrollTo(0, {page_height})")
            await page.wait_for_timeout(scroll_pause_time * 1000)

            new_height = await page.evaluate("document.body.scrollHeight")
            new_element_count = 0

            # Count elements if selector is provided
            if content_check_selector:
                elements = await page.query_selector_all(content_check_selector)
                new_element_count = len(elements)
                self.logger.info(f"Current element count: {new_element_count} (height: {new_height})")

                # Check if element count is stable
                if new_element_count == last_element_count and new_height == last_height:
                    stable_count_attempts += 1
                    self.logger.debug(f"Content stable. Attempt {stable_count_attempts}/{max_scroll_attempts}.")

                    if stable_count_attempts >= max_scroll_attempts:
                        self.logger.info(f"Content stabilized at {new_element_count} elements. Scrolling complete.")
                        return True
                else:
                    stable_count_attempts = 0  # Reset if content changed
                    last_element_count = new_element_count
            else:
                # Fallback to height-based detection
                if new_height == last_height:
                    stable_count_attempts += 1
                    self.logger.debug(f"Height stable. Attempt {stable_count_attempts}/{max_scroll_attempts}.")

                    if stable_count_attempts >= max_scroll_attempts:
                        self.logger.info("Page height stabilized. Scrolling complete.")
                        return True
                else:
                    stable_count_attempts = 0

            last_height = new_height

        self.logger.info("Reached scrolling timeout. Stopping scroll.")
        return False

    async def scroll_until_visible_and_click_parent(
        self,
        page,
        selector,
        text: str | None = None,
        timeout=SCROLL_UNTIL_CLICK_TIMEOUT_S,
        scroll_pause_time=SCROLL_UNTIL_CLICK_PAUSE_S,
    ):
        """
        Scrolls the page until an element matching the selector and text is visible, then clicks its parent element.

        Args:
            page (Page): The Playwright page instance.
            selector (str): The CSS selector of the element.
            text (str): Optional. The text content to match.
            timeout (int): Timeout in seconds (default: 20).
            scroll_pause_time (int): Pause time in seconds between scrolls (default: 3).

        Returns:
            bool: True if the parent element was clicked successfully, False otherwise.
        """
        end_time = time.time() + timeout

        while time.time() < end_time:
            elements = await page.query_selector_all(selector)

            for element in elements:
                if text:
                    element_text = await element.text_content()

                    if element_text and text in element_text:
                        bounding_box = await element.bounding_box()

                        if bounding_box:
                            self.logger.info(f"Element with text '{text}' is visible. Clicking its parent.")
                            parent_element = await element.evaluate_handle("element => element.parentElement")
                            await parent_element.click()
                            return True
                else:
                    bounding_box = await element.bounding_box()
                    if bounding_box:
                        self.logger.info("Element is visible. Clicking its parent.")
                        parent_element = await element.evaluate_handle("element => element.parentElement")
                        await parent_element.click()
                        return True

            await page.evaluate("window.scrollBy(0, 500);")
            await page.wait_for_timeout(scroll_pause_time * 1000)

        self.logger.warning(
            f"Failed to find and click parent of element matching selector '{selector}' with text '{text}' "
            f"within timeout."
        )
        return False

    # =============================================================================
    # PRIVATE HELPER METHODS
    # =============================================================================

    async def _wait_and_click(
        self, page: Page, selector: str, text: str | None = None, timeout: float = DEFAULT_MARKET_TIMEOUT_MS
    ):
        """
        Waits for a selector and optionally clicks an element based on its text.

        Args:
            page (Page): The Playwright page instance to interact with.
            selector (str): The CSS selector to wait for.
            text (str): Optional. The text of the element to click.
            timeout (float): The waiting time for the element to click.

        Returns:
            bool: True if the element is clicked successfully, False otherwise.
        """
        try:
            await page.wait_for_selector(selector=selector, timeout=timeout)

            if text:
                return await self._click_by_text(page=page, selector=selector, text=text)
            else:
                # Click the first element matching the selector
                element = await page.query_selector(selector)
                await element.click()
                return True

        except Exception as e:
            self.logger.error(f"Error waiting for or clicking selector '{selector}': {e}")
            return False

    async def _click_by_text(self, page: Page, selector: str, text: str) -> bool:
        """
        Attempts to click an element based on its text content.

        This method searches for all elements matching a specific selector, retrieves their
        text content, and checks if the provided text is a substring of the element's text.
        If a match is found, the method clicks the element.

        Args:
            page (Page): The Playwright page instance to interact with.
            selector (str): The CSS selector for the elements to search (e.g., '.btn', 'div').
            text (str): The text content to match as a substring.

        Returns:
            bool: True if an element with the matching text was successfully clicked, False otherwise.

        Raises:
            Exception: Logs the error and returns False if an issue occurs during execution.
        """
        try:
            elements = await page.query_selector_all(selector)

            for element in elements:
                element_text = await element.text_content()
                if element_text and self._text_matches(element_text, text):
                    return await self._safe_click(page=page, element=element, reason=f"click text '{text}'")

            self.logger.info(f"Element with text '{text}' not found.")
            return False

        except Exception as e:
            self.logger.error(f"Error clicking element with text '{text}': {e}")
            return False

    async def _click_more_if_market_hidden(
        self, page: Page, market_tab_name: str, timeout: int = MARKET_TAB_TIMEOUT_MS
    ):
        """
        Attempts to find and click a market tab hidden in the "More" dropdown.

        Args:
            page (Page): The Playwright page instance.
            market_tab_name (str): The name of the market tab to find.
            timeout (int): Timeout in milliseconds.

        Returns:
            bool: True if the market was found and clicked in the "More" dropdown, False otherwise.
        """
        try:
            more_clicked = False
            await self.dismiss_overlays(page)
            for selector in OddsPortalSelectors.MORE_BUTTON_SELECTORS:
                try:
                    more_elements = await page.query_selector_all(selector)
                    for more_element in more_elements:
                        text = (await more_element.text_content() or "").strip()
                        normalized = self._normalize_text(text)
                        if normalized in {"more", "...", "more..."} or normalized.startswith("more "):
                            self.logger.info(f"Clicking 'More' button: '{text.strip()}'")
                            if await self._safe_click(page=page, element=more_element, reason="More button"):
                                more_clicked = True
                                break
                    if more_clicked:
                        break
                except Exception as e:
                    self.logger.debug(f"Exception while searching for 'More' button with selector '{selector}': {e}")
                    continue

            if not more_clicked:
                self.logger.warning("Could not find or click 'More' button")
                return False

            await page.wait_for_timeout(DROPDOWN_WAIT_MS)

            dropdown_selectors = OddsPortalSelectors.get_dropdown_selectors_for_market(market_tab_name)
            for selector in dropdown_selectors:
                try:
                    dropdown_elements = await page.query_selector_all(selector)
                    for dropdown_element in dropdown_elements:
                        text = await dropdown_element.text_content()
                        if text and self._text_matches(text, market_tab_name):
                            self.logger.info(f"Found '{market_tab_name}' in dropdown. Clicking...")
                            if await self._safe_click(page=page, element=dropdown_element, reason=f"dropdown market {market_tab_name}"):
                                return True
                except Exception as e:
                    self.logger.debug(
                        f"Exception while searching for market '{market_tab_name}' in dropdown with selector "
                        f"'{selector}': {e}"
                    )
                    continue

            self.logger.info("Debugging dropdown content:")
            dropdown_items = await page.query_selector_all(OddsPortalSelectors.DROPDOWN_DEBUG_ELEMENTS)
            for item in dropdown_items[:10]:  # Limit to first 10 items
                try:
                    text = await item.text_content()
                    if text and text.strip():
                        self.logger.info(f"  Dropdown item: '{text.strip()}'")
                except Exception as e:
                    self.logger.debug(f"Exception while logging dropdown item: {e}")
                    continue

            return False

        except Exception as e:
            self.logger.error(f"Error in _click_more_if_market_hidden: {e}")
            return False

    async def _verify_tab_is_active(self, page: Page, market_tab_name: str) -> bool:
        """
        Verify that a market tab is actually active after clicking.

        Args:
            page (Page): The Playwright page instance.
            market_tab_name (str): The name of the market tab to verify.

        Returns:
            bool: True if the tab is active, False otherwise.
        """
        try:
            # Wait a bit for the tab switch to complete
            await page.wait_for_timeout(TAB_SWITCH_WAIT_MS)

            # Check for active tab indicators
            active_selectors = ["li.active", "li[class*='active']", ".active", "[class*='active']"]

            for selector in active_selectors:
                try:
                    active_element = await page.query_selector(selector)
                    if active_element:
                        text = await active_element.text_content()
                        if text and self._text_matches(text, market_tab_name):
                            self.logger.info(f"Tab '{market_tab_name}' is confirmed active")
                            return True
                except Exception as e:
                    self.logger.debug(f"Exception checking active selector '{selector}': {e}")
                    continue

            # Alternative: check if the market name appears in the current URL or page content
            page_content = await page.content()
            if market_tab_name and market_tab_name.lower() in page_content.lower():
                self.logger.info(f"Market '{market_tab_name}' found in page content")
                return True

            self.logger.warning(f"Tab '{market_tab_name}' is not confirmed as active")
            return False

        except Exception as e:
            self.logger.error(f"Error verifying tab is active: {e}")
            return False

    def _normalize_text(self, text: str | None) -> str:
        """Normalize text for safer UI matching."""
        if not text:
            return ""
        normalized = re.sub(r"\s+", " ", text).strip().lower()
        return normalized.replace("\u00a0", " ")

    def _text_matches(self, candidate: str | None, target: str | None) -> bool:
        """Prefer exact-ish normalized matching over loose substring matching."""
        candidate_norm = self._normalize_text(candidate)
        target_norm = self._normalize_text(target)
        if not candidate_norm or not target_norm:
            return False
        if candidate_norm == target_norm:
            return True
        candidate_tokens = [token.strip() for token in re.split(r"[|/\\-]", candidate_norm) if token.strip()]
        return target_norm in candidate_tokens

    async def _safe_click(self, page: Page, element, reason: str = "element") -> bool:
        """Click robustly when normal clicks are intercepted by overlays or animations."""
        try:
            try:
                await element.scroll_into_view_if_needed()
            except Exception:
                pass

            await self.dismiss_overlays(page)

            try:
                await element.click(timeout=3000)
                return True
            except Exception as click_error:
                self.logger.debug(f"Normal click failed for {reason}: {click_error}")

            try:
                await element.click(timeout=3000, force=True)
                return True
            except Exception as force_error:
                self.logger.debug(f"Forced click failed for {reason}: {force_error}")

            try:
                await element.evaluate("el => el.click()")
                return True
            except Exception as eval_error:
                self.logger.debug(f"DOM click failed for {reason}: {eval_error}")

            return False
        except Exception as e:
            self.logger.debug(f"Safe click failed for {reason}: {e}")
            return False
