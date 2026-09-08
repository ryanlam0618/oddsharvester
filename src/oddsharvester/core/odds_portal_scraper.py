import asyncio
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from enum import Enum
import random
import re
from typing import Any
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from playwright.async_api import Page

from oddsharvester.core.base_scraper import BaseScraper, _parse_date_header
from oddsharvester.core.browser.pagination import WalkVerdict
from oddsharvester.core.exceptions import PageNotFoundError
from oddsharvester.core.odds_portal_selectors import OddsPortalSelectors
from oddsharvester.core.scrape_result import ErrorType, FailedUrl, ScrapeResult, ScrapeStats
from oddsharvester.core.url_builder import URLBuilder, normalize_inplay_match_url
from oddsharvester.utils.bookies_filter_enum import BookiesFilter
from oddsharvester.utils.constants import (
    DEFAULT_REQUEST_DELAY_S,
    GOTO_TIMEOUT_LONG_MS,
    GOTO_TIMEOUT_MS,
    LISTING_PAGE_RETRY_ATTEMPTS,
    LISTING_PAGE_RETRY_DELAY_S,
    MAX_PAGINATION_PAGES,
    ODDSPORTAL_BASE_URL,
    PAGE_COLLECTION_DELAY_MAX_MS,
    PAGE_COLLECTION_DELAY_MIN_MS,
    RESULTS_PAGE_SIZE,
)


@dataclass
class MatchDataResult:
    """Result of extracting match data from results pages."""

    matches: list[dict[str, Any]] = field(default_factory=list)
    successful_pages: int = 0
    failed_pages: list[int] = field(default_factory=list)

    @property
    def total_pages(self) -> int:
        return self.successful_pages + len(self.failed_pages)


@dataclass
class LinkCollectionResult:
    """Result of collecting match links from pages."""

    links: list[str] = field(default_factory=list)
    successful_pages: int = 0
    failed_pages: list[int] = field(default_factory=list)

    @property
    def total_pages(self) -> int:
        return self.successful_pages + len(self.failed_pages)


class OddsPortalScraper(BaseScraper):
    """
    Main class that manages the scraping workflow from OddsPortal.
    """

    async def start_playwright(
        self,
        headless: bool = True,
        browser_user_agent: str | None = None,
        browser_locale_timezone: str | None = None,
        browser_timezone_id: str | None = None,
        proxy_manager=None,
    ):
        """Initializes Playwright using PlaywrightManager."""
        await self.playwright_manager.initialize(
            headless=headless,
            user_agent=browser_user_agent,
            locale=browser_locale_timezone,
            timezone_id=browser_timezone_id,
            proxy_manager=proxy_manager,
        )

    async def stop_playwright(self):
        """Stops Playwright and cleans up resources."""
        await self.playwright_manager.cleanup()

    async def scrape_historic(
        self,
        sport: str,
        league: str,
        season: str,
        markets: list[str] | None = None,
        scrape_odds_history: bool = False,
        target_bookmaker: str | None = None,
        max_pages: int | None = None,
        bookies_filter: BookiesFilter = BookiesFilter.ALL,
        period: Enum | None = None,
        request_delay: float = DEFAULT_REQUEST_DELAY_S,
        concurrent_scraping_task: int = 3,
        links_only: bool = False,
        checkpoint_file_path: str | None = None,
        checkpoint_storage_type: str = "local",
        checkpoint_storage_format: str = "json",
    ) -> ScrapeResult:
        """
        Scrapes historical odds data.

        Args:
            sport (str): The sport to scrape.
            league (str): The league to scrape.
            season (str): The season to scrape.
            markets (Optional[List[str]]): List of markets.
            scrape_odds_history (bool): Whether to scrape and attach odds history.
            target_bookmaker (str): If set, only scrape odds for this bookmaker.
            max_pages (Optional[int]): Maximum number of pages to scrape (default is None for all pages).
            links_only (bool): If True, stop after link collection and return the links (no odds scraping).
            checkpoint_file_path (Optional[str]): Fork feature — persist each match immediately
                after it is scraped so long season runs survive a crash.
            checkpoint_storage_type (str): Storage backend used for checkpoints.
            checkpoint_storage_format (str): Storage format used for checkpoints.

        Returns:
            ScrapeResult: Contains successful results, failed URLs, and statistics.
        """
        current_page = self.playwright_manager.page
        if not current_page:
            raise RuntimeError("Playwright has not been initialized. Call `start_playwright()` first.")

        base_url = URLBuilder.get_historic_matches_url(
            sport=sport, league=league, season=season, base_url=self.base_url
        )
        self.logger.info(f"Starting historic scraping for {sport} - {league} - {season}")
        self.logger.info(f"Base URL: {base_url}")
        self.logger.info(f"Max pages parameter: {max_pages}")

        # Fork anti-detection: set consent cookies + block OneTrust BEFORE navigation.
        if self.browser_helper is not None:
            self.logger.info("Setting consent cookies in browser context...")
            await self.browser_helper.set_consent_cookies_for_context(current_page.context)
        self.logger.info("Blocking OneTrust scripts to prevent bot detection...")
        await self.playwright_manager.block_one_trust_for_page(current_page)

        # Navigate to the base URL
        self.logger.info("Navigating to base URL...")
        await current_page.goto(base_url)
        self._assert_season_page_reached(requested_url=base_url, landed_url=current_page.url)
        await self._prepare_page_for_scraping(page=current_page)

        # Analyze pagination and determine pages to scrape
        self.logger.info("Step 1: Analyzing pagination information...")
        pages_to_scrape = await self._get_pagination_info(page=current_page, max_pages=max_pages)

        # Fork: season-aware row filtering during link collection.
        season_year: int | None = None
        season_end_year: int | None = None
        m = re.match(r"^(\d{4})-(\d{4})$", season or "")
        if m:
            season_year = int(m.group(1))
            season_end_year = int(m.group(2))

        # Collect match links from all pages
        self.logger.info("Step 2: Collecting match links from all pages...")
        link_result = await self._collect_match_links(
            base_url=base_url,
            pages_to_scrape=pages_to_scrape,
            page_limit=self._effective_page_limit(max_pages),
            max_pages=max_pages,
            season_year=season_year,
            season_end_year=season_end_year,
        )

        if link_result.failed_pages:
            self.logger.warning(f"Failed to collect links from pages: {link_result.failed_pages}")

        if links_only:
            self.logger.info(f"Links-only mode: returning {len(link_result.links)} match links without odds.")
            return self._links_only_result(
                rows=[{"match_link": link} for link in link_result.links],
                context={"sport": sport, "league": league, "season": season},
                failed_page_urls=[
                    f"{base_url}{OddsPortalSelectors.page_fragment(p)}" for p in link_result.failed_pages
                ],
            )

        # Extract odds from all collected links
        self.logger.info("Step 3: Extracting odds from collected match links...")
        self.logger.info(f"Total unique matches to process: {len(link_result.links)}")

        result = await self.extract_match_odds(
            sport=sport,
            match_links=link_result.links,
            markets=markets,
            scrape_odds_history=scrape_odds_history,
            target_bookmaker=target_bookmaker,
            concurrent_scraping_task=concurrent_scraping_task,
            preview_submarkets_only=self.preview_submarkets_only,
            bookies_filter=bookies_filter,
            period=period,
            request_delay=request_delay,
            checkpoint_file_path=checkpoint_file_path,
            checkpoint_storage_type=checkpoint_storage_type,
            checkpoint_storage_format=checkpoint_storage_format,
            season_year=season_year,
            season_end_year=season_end_year,
        )

        for row in result.success:
            row["season"] = season

        # A failed listing page loses an entire page of matches that were never
        # discovered, so it cannot show up as a per-match failure. Surface it here
        # or the run reports 100% success on an incomplete dataset.
        if link_result.failed_pages:
            listing_failures = self._listing_page_failures(base_url, link_result.failed_pages)
            result.failed.extend(listing_failures)
            result.stats.failed += len(listing_failures)
            result.stats.total_urls += len(listing_failures)

        return result

    async def scrape_upcoming(
        self,
        sport: str,
        date: str,
        league: str | None = None,
        markets: list[str] | None = None,
        scrape_odds_history: bool = False,
        target_bookmaker: str | None = None,
        bookies_filter: BookiesFilter = BookiesFilter.ALL,
        period: Enum | None = None,
        request_delay: float = DEFAULT_REQUEST_DELAY_S,
        concurrent_scraping_task: int = 3,
        include_started: bool = False,
        kickoff_within_hours: float | None = None,
        links_only: bool = False,
    ) -> ScrapeResult:
        """
        Scrapes upcoming match odds.

        Args:
            sport (str): The sport to scrape.
            date (str): The date to scrape.
            league (Optional[str]): The league to scrape.
            markets (Optional[List[str]]): List of markets.
            scrape_odds_history (bool): Whether to scrape and attach odds history.
            target_bookmaker (str): If set, only scrape odds for this bookmaker.
            include_started (bool): If True, also return matches that have
                already started or finished. Default False keeps the listing
                page's true "upcoming" semantics (GitHub issue #58).
            kickoff_within_hours (Optional[float]): If set, only scrape matches
                kicking off within this many hours from now, cutting request
                volume by skipping far-off matches (GitHub issue #77).
            links_only (bool): If True, stop after link collection and return the links (no odds scraping).

        Returns:
            ScrapeResult: Contains successful results, failed URLs, and statistics.
        """
        current_page = self.playwright_manager.page
        if not current_page:
            raise RuntimeError("Playwright has not been initialized. Call `start_playwright()` first.")

        url = URLBuilder.get_upcoming_matches_url(sport=sport, date=date, league=league, base_url=self.base_url)
        self.logger.info(f"Fetching upcoming odds from {url}")

        await current_page.goto(url, timeout=GOTO_TIMEOUT_MS, wait_until="domcontentloaded")
        await self._prepare_page_for_scraping(page=current_page)

        # Scroll to load all matches due to lazy loading
        self.logger.info("Scrolling page to load all upcoming matches...")
        await self.scroller.scroll_until_loaded(
            page=current_page,
            timeout=30,
            scroll_pause_time=2,
            max_scroll_attempts=3,
            content_check_selector=OddsPortalSelectors.LISTING_ROW_SELECTOR,
        )

        # League page shows all upcoming dates; when a specific date is requested,
        # post-filter links by the date-header rendered above each row group.
        date_filter = None
        if league and date:
            try:
                date_filter = datetime.strptime(date, "%Y%m%d").date()
                self.logger.info(f"Applying date filter for league page: {date_filter.isoformat()}")
            except ValueError:
                self.logger.warning(f"Could not parse date '{date}' for filtering; returning all league matches.")

        rows = await self.extract_match_rows(
            page=current_page,
            date_filter=date_filter,
            skip_started=not include_started,
            kickoff_within_hours=kickoff_within_hours,
            collect_kickoff=links_only,
        )

        if not rows:
            self.logger.warning("No match links found for upcoming matches.")
            return ScrapeResult()

        if links_only:
            self.logger.info(f"Links-only mode: returning {len(rows)} match links without odds.")
            return self._links_only_result(
                rows=rows,
                context={"sport": sport, "league": league, "date": date, "season": None},
            )

        match_links = [row["match_link"] for row in rows]

        return await self.extract_match_odds(
            sport=sport,
            match_links=match_links,
            markets=markets,
            scrape_odds_history=scrape_odds_history,
            target_bookmaker=target_bookmaker,
            concurrent_scraping_task=concurrent_scraping_task,
            preview_submarkets_only=self.preview_submarkets_only,
            bookies_filter=bookies_filter,
            period=period,
            request_delay=request_delay,
        )

    async def scrape_live(
        self,
        sport: str,
        league: str | None = None,
        markets: list[str] | None = None,
        match_links: list[str] | None = None,
        target_bookmaker: str | None = None,
        bookies_filter: BookiesFilter = BookiesFilter.ALL,
        request_delay: float = DEFAULT_REQUEST_DELAY_S,
        concurrent_scraping_task: int = 3,
        links_only: bool = False,
    ) -> ScrapeResult:
        """
        Scrapes a one-shot snapshot of in-play odds for currently live matches.

        Listing source is /inplay-odds/live-now/<sport>/; each match is scraped
        on its in-play view (per-bookmaker live odds plus live score/period).
        When `match_links` is given the listing is skipped and those URLs are
        normalized to their in-play form, which is the building block for
        external re-sampling of a known match.

        Args:
            sport (str): The sport to scrape.
            league (Optional[str]): Single league slug filter, applied after listing.
            markets (Optional[List[str]]): List of markets.
            match_links (Optional[List[str]]): Scrape these matches directly.
            target_bookmaker (str): If set, only scrape odds for this bookmaker.
            links_only (bool): If True, return collected live links without odds.

        Returns:
            ScrapeResult: Contains successful results, failed URLs, and statistics.
        """
        current_page = self.playwright_manager.page
        if not current_page:
            raise RuntimeError("Playwright has not been initialized. Call `start_playwright()` first.")

        if match_links:
            links = [normalize_inplay_match_url(link) for link in match_links]
            await current_page.goto(ODDSPORTAL_BASE_URL, timeout=GOTO_TIMEOUT_LONG_MS, wait_until="domcontentloaded")
            await self._prepare_page_for_scraping(page=current_page)
        else:
            url = URLBuilder.get_live_matches_url(sport=sport, base_url=self.base_url)
            self.logger.info(f"Fetching live matches from {url}")

            await current_page.goto(url, timeout=GOTO_TIMEOUT_MS, wait_until="domcontentloaded")
            await self._prepare_page_for_scraping(page=current_page)
            await self.scroller.scroll_until_loaded(
                page=current_page,
                timeout=30,
                scroll_pause_time=2,
                max_scroll_attempts=3,
                content_check_selector=OddsPortalSelectors.LISTING_ROW_SELECTOR,
            )

            rows = await self.extract_live_match_links(page=current_page, sport=sport, league=league)
            if not rows:
                self.logger.info("No live matches found on the live-now listing.")
                return ScrapeResult()
            links = [row["match_link"] for row in rows]

        if links_only:
            self.logger.info(f"Links-only mode: returning {len(links)} live match links without odds.")
            return self._links_only_result(
                rows=[{"match_link": link} for link in links],
                context={"sport": sport, "league": league},
            )

        result = await self.extract_match_odds(
            sport=sport,
            match_links=links,
            markets=markets,
            scrape_odds_history=False,
            target_bookmaker=target_bookmaker,
            concurrent_scraping_task=concurrent_scraping_task,
            preview_submarkets_only=self.preview_submarkets_only,
            bookies_filter=bookies_filter,
            period=None,
            request_delay=request_delay,
            live_mode=True,
        )

        ended = [d for d in result.success if d.get("_live_ended")]
        if ended:
            self.logger.info(f"{len(ended)} matches ended between listing and scrape; dropped from output.")
            result.success = [d for d in result.success if not d.get("_live_ended")]
            result.stats.successful -= len(ended)
            result.stats.total_urls -= len(ended)

        return result

    async def scrape_matches(
        self,
        match_links: list[str],
        sport: str,
        markets: list[str] | None = None,
        scrape_odds_history: bool = False,
        target_bookmaker: str | None = None,
        bookies_filter: BookiesFilter = BookiesFilter.ALL,
        period: Enum | None = None,
        request_delay: float = DEFAULT_REQUEST_DELAY_S,
        concurrent_scraping_task: int = 3,
    ) -> ScrapeResult:
        """
        Scrapes match odds from a list of specific match URLs.

        Args:
            match_links (List[str]): List of URLs of matches to scrape.
            sport (str): The sport to scrape.
            markets (List[str] | None): List of betting markets to scrape. Defaults to None.
            scrape_odds_history (bool): Whether to scrape and attach odds history.
            target_bookmaker (str): If set, only scrape odds for this bookmaker.

        Returns:
            ScrapeResult: Contains successful results, failed URLs, and statistics.
        """
        current_page = self.playwright_manager.page
        if not current_page:
            raise RuntimeError("Playwright has not been initialized. Call `start_playwright()` first.")

        # Block OneTrust scripts BEFORE navigation to prevent bot detection
        await self.playwright_manager.block_one_trust_for_page(current_page)

        await current_page.goto(ODDSPORTAL_BASE_URL, timeout=GOTO_TIMEOUT_LONG_MS, wait_until="domcontentloaded")
        await self._prepare_page_for_scraping(page=current_page)
        return await self.extract_match_odds(
            sport=sport,
            match_links=match_links,
            markets=markets,
            scrape_odds_history=scrape_odds_history,
            target_bookmaker=target_bookmaker,
            concurrent_scraping_task=concurrent_scraping_task,
            preview_submarkets_only=self.preview_submarkets_only,
            bookies_filter=bookies_filter,
            period=period,
            request_delay=request_delay,
        )

    async def _prepare_page_for_scraping(self, page: Page):
        """
        Prepares the Playwright page for scraping by setting odds format and dismissing banners.

        Args:
            page: Playwright page instance.
        """
        # Block OneTrust scripts to prevent bot detection (before any navigation)
        await self.playwright_manager.block_one_trust_for_page(page)

        if self.browser_helper is not None:
            # Set consent cookies before dismissing banner (needed for each page navigation)
            await self.browser_helper.set_consent_cookie_via_page_js(page)

        await self.set_odds_format(page=page)
        if self.browser_helper is not None:
            await self.browser_helper.dismiss_cookie_banner(page=page)
            await self.browser_helper.dismiss_overlays(page=page)
            # Simulate human behavior after page preparation (synced from SofaScore)
            await self.browser_helper.humanize_page(page=page)
        await self.cookie_dismisser.dismiss(page=page)

    @staticmethod
    def _effective_page_limit(max_pages: int | None) -> int:
        """Explicit --max-pages overrides the default safety cap."""
        return max_pages if max_pages else MAX_PAGINATION_PAGES

    @staticmethod
    def _listing_page_failures(base_url: str, failed_pages: list[int]) -> list[FailedUrl]:
        """Build the failure entries for listing pages that could not be collected."""
        return [
            FailedUrl(
                url=f"{base_url}{OddsPortalSelectors.page_fragment(page)}",
                error_type=ErrorType.LISTING_PAGE,
                error_message="Failed to collect links from listing page",
            )
            for page in failed_pages
        ]

    def _links_only_result(
        self,
        rows: list[dict],
        context: dict,
        failed_page_urls: list[str] | None = None,
    ) -> ScrapeResult:
        """Builds a ScrapeResult carrying collected match rows instead of odds data.

        Each row must carry `match_link`. Any other key it holds is appended
        after the context columns, so the link stays first and per-row extras last.
        """
        failed_page_urls = failed_page_urls or []
        success = [
            {"match_link": row["match_link"], **context, **{k: v for k, v in row.items() if k != "match_link"}}
            for row in rows
        ]
        failed = [
            FailedUrl(
                url=url,
                error_type=ErrorType.LISTING_PAGE,
                error_message="Failed to collect links from listing page",
            )
            for url in failed_page_urls
        ]
        return ScrapeResult(
            success=success,
            failed=failed,
            stats=ScrapeStats(
                total_urls=len(success) + len(failed),
                successful=len(success),
                failed=len(failed),
            ),
        )

    async def _get_pagination_info(self, page: Page, max_pages: int | None) -> list[int]:
        """
        Extracts pagination details from the page.

        Args:
            page: Playwright page instance.
            max_pages (Optional[int]): Maximum pages to scrape.

        Returns:
            List[int]: List of pages to scrape.
        """
        self.logger.info("Analyzing pagination information...")

        total_pages = await self.pagination_walker.read_widget(page=page)

        if not total_pages:
            self.logger.info(
                "No pagination widget on this page: either a single-page league or a degraded response. "
                "The walk will determine the page count."
            )
            return [1]

        self.logger.info(f"Raw pagination pages found: {total_pages}")

        # Check for gaps in pagination (e.g., [1,2,3,4,5,6,7,8,9,10,27] -> missing 11-26)
        pages_to_scrape = self._fill_pagination_gaps(total_pages)

        # Apply page limit: explicit --max-pages overrides the default safety cap
        effective_limit = self._effective_page_limit(max_pages)
        if len(pages_to_scrape) > effective_limit:
            self.logger.warning(
                f"Pagination has {len(pages_to_scrape)} pages, limiting to {effective_limit} "
                f"({'--max-pages' if max_pages else 'safety cap'})."
            )
            pages_to_scrape = pages_to_scrape[:effective_limit]
        else:
            self.logger.info(f"Will scrape all {len(pages_to_scrape)} pages (limit: {effective_limit})")

        self.logger.info(f"Final pages to scrape: {pages_to_scrape}")
        return pages_to_scrape

    def _fill_pagination_gaps(self, raw_pages: list[int]) -> list[int]:
        """
        Sort, deduplicate, and fill gaps in discovered pagination pages.

        OddsPortal renders pagination with an ellipsis for large page ranges
        (e.g. ``[1,2,3,...,28]``), so the HTML only contains the endpoints.
        This determines the maximum under the page cap; the walk is what
        actually ensures intermediate pages are scraped.

        Args:
            raw_pages (List[int]): Raw page numbers found in pagination.

        Returns:
            List[int]: Contiguous list of pages from 1..max.
        """
        if len(raw_pages) <= 1:
            return raw_pages

        max_page = max(raw_pages)
        all_pages = list(range(1, max_page + 1))
        self.logger.info(
            f"Pagination HTML showed {sorted(set(raw_pages))}, "
            f"filling to contiguous range 1..{max_page} ({len(all_pages)} pages)"
        )

        return all_pages

    @staticmethod
    def _assert_season_page_reached(requested_url: str, landed_url: str) -> None:
        """
        Fail a season whose URL was redirected away instead of scraping what it landed on.

        A season that does not exist under the requested slug does not always answer with a
        not-found page: OddsPortal redirects some of them to the league's current fixtures,
        which would otherwise be collected and labelled with the requested season (gotcha 4).
        """
        if urlparse(requested_url).path.rstrip("/") == urlparse(landed_url).path.rstrip("/"):
            return

        raise PageNotFoundError(
            f"Season page redirected to {landed_url}; the season does not exist under this league slug.",
            url=requested_url,
        )

    async def _collect_match_links(
        self,
        base_url: str,
        pages_to_scrape: list[int],
        page_limit: int = MAX_PAGINATION_PAGES,
        max_pages: int | None = None,
        season_year: int | None = None,
        season_end_year: int | None = None,
    ) -> LinkCollectionResult:
        """
        Walks listing pages, collecting match links.

        `pages_to_scrape` is a floor, not a plan: only its maximum is used, and the
        walk always visits 1, 2, 3... contiguously from there, continuing while pages
        come back full (issue #79). See gotchas 2 and 17.

        Args:
            base_url (str): The base URL of the historic matches.
            pages_to_scrape (List[int]): Only the maximum is used, as the walk's floor.
            page_limit (int): Hard bound on how many pages the walk may visit.
            max_pages (Optional[int]): The user-supplied --max-pages, if any; distinguishes
                an intentional limit from the default safety cap in the truncation warning.
            season_year (Optional[int]): Start year of the season (e.g. 2016 for "2016-2017").
                If not provided, extracted from base_url.
            season_end_year (Optional[int]): End year of the season (e.g. 2017 for "2016-2017").
                If not provided, extracted from base_url.

        Returns:
            LinkCollectionResult: Contains links found and tracking of successful/failed pages.
        """
        planned_max = max(pages_to_scrape) if pages_to_scrape else 1
        self.logger.info(f"Starting collection of match links from a floor of {planned_max} page(s)")


        if season_year is None or season_end_year is None:
            # Try to extract season from URL: .../laliga-2016-2017/...
            m = re.search(r"/-(\d{4})-\d{4}/", base_url)
            if m:
                season_year = int(m.group(1))
                season_end_year = season_year + 1

        self.logger.info(f"Starting collection of match links from {len(pages_to_scrape)} pages")
        if season_year is not None:
            self.logger.info(f"Season filtering: {season_year}-{season_end_year}")
        self.logger.info(f"Pages to process: {pages_to_scrape}")

        result = LinkCollectionResult()
        all_links = []
        frontier = planned_max
        observed_max: int | None = None
        page_number = 1
        attempt = 1

        while page_number <= page_limit:
            self.logger.info(f"Processing page {page_number} (frontier: {frontier}, limit: {page_limit})")
            tab = None
            past_frontier = page_number >= frontier

            try:
                tab = await self.playwright_manager.context.new_page()

                # Block OneTrust scripts BEFORE navigation on new tab
                await self.playwright_manager.block_one_trust_for_page(tab)

                self.logger.debug(f"Created new tab for page {page_number}")

                page_url = f"{base_url}{OddsPortalSelectors.page_fragment(page_number)}"
                self.logger.info(f"Navigating to: {page_url}")
                await tab.goto(page_url, timeout=GOTO_TIMEOUT_MS, wait_until="domcontentloaded")
                delay = random.randint(PAGE_COLLECTION_DELAY_MIN_MS, PAGE_COLLECTION_DELAY_MAX_MS)  # noqa: S311
                await tab.wait_for_timeout(delay)

                self.logger.info(f"Scrolling page {page_number} to load all matches...")
                scroll_success = await self.scroller.scroll_until_loaded(
                    page=tab,
                    timeout=30,
                    scroll_pause_time=2,
                    max_scroll_attempts=3,
                    content_check_selector=OddsPortalSelectors.LISTING_ROW_SELECTOR,
                )
                if not scroll_success:
                    self.logger.warning(f"Scrolling may not have completed for page {page_number}")

                self.logger.info(f"Extracting match links from page {page_number}...")
                links = await self.extract_match_links(
                    page=tab,
                    season_year=season_year,
                    season_end_year=season_end_year,
                )

                # Read on every page, not just empty ones: the tab is already loaded, so
                # this costs no request, and a widget missing from page 1 is often present
                # on page 2. That is how the true count is recovered (issue #79).
                widget_pages = await self.pagination_walker.read_widget(page=tab)
                if widget_pages:
                    observed_max = max(observed_max or 0, max(widget_pages))
                    frontier = max(frontier, observed_max)
                past_frontier = page_number >= frontier

                verdict = self.pagination_walker.decide(
                    requested_page=page_number,
                    link_count=len(links),
                    frontier=frontier,
                    observed_max=observed_max,
                    scroll_ok=scroll_success,
                )

                if verdict is WalkVerdict.PAGE_FAILED:
                    # The truncation is transient, so fetch the page again before writing it
                    # off: losing it costs a full re-run of the season to recover one page.
                    if attempt <= LISTING_PAGE_RETRY_ATTEMPTS:
                        self.logger.warning(
                            f"Page {page_number} returned {len(links)} of {RESULTS_PAGE_SIZE} links; re-fetching it."
                        )
                        attempt += 1
                        await asyncio.sleep(LISTING_PAGE_RETRY_DELAY_S)
                        continue

                    result.failed_pages.append(page_number)
                    self.logger.warning(
                        f"Page {page_number} returned {len(links)} of {RESULTS_PAGE_SIZE} links after "
                        f"{attempt} attempts; treating it as failed."
                    )
                    # Keep what it did render: the page is already reported and the exit code
                    # already non-zero, so dropping real rows only costs a re-scrape of links
                    # the user is holding. They dedupe on match_link, which is unique.
                    all_links.extend(links)
                    if past_frontier:
                        break
                    attempt = 1
                    page_number += 1
                    continue

                all_links.extend(links)
                # A zero-link STOP_COMPLETE past the planned floor is the widget-corroboration
                # page confirming the season already ended; it rendered nothing, so it was not
                # collected and must not inflate successful_pages (that count feeds the
                # widget-mismatch warning below).
                phantom_page = verdict is WalkVerdict.STOP_COMPLETE and not links and page_number > planned_max
                if not phantom_page:
                    result.successful_pages += 1
                self.logger.info(f"Extracted {len(links)} links from page {page_number}")

                if verdict is WalkVerdict.STOP_COMPLETE:
                    break

            except Exception as e:
                result.failed_pages.append(page_number)
                self.logger.error(f"Error processing page {page_number}: {e}")
                # Deliberate: a timeout past the frontier stops the walk same as PAGE_FAILED,
                # even though a timeout doesn't prove the page is absent. Loud failure, not silent.
                if past_frontier:
                    break

            finally:
                if tab:
                    await tab.close()

            attempt = 1
            page_number += 1

        pages_walked = result.total_pages
        if pages_walked > planned_max:
            self.logger.warning(
                f"Pagination widget reported {planned_max} page(s) but the walk collected {pages_walked} page(s). "
                "The widget read was incomplete; walked past it to avoid truncation."
            )
        if page_number > page_limit:
            if max_pages:
                self.logger.warning(
                    f"Walk stopped at the {page_limit}-page limit set by --max-pages. Result is intentionally "
                    "truncated."
                )
            else:
                self.logger.warning(
                    f"Walk stopped at the {page_limit}-page safety cap. The season may be incomplete; "
                    "raise --max-pages to collect the rest."
                )

        result.links = list(dict.fromkeys(all_links))
        self.logger.info("Collection Summary:")
        self.logger.info(f"   - Pages planned from widget: {planned_max}")
        self.logger.info(f"   - Pages actually walked: {pages_walked}")
        self.logger.info(f"   - Successful pages: {result.successful_pages}")
        self.logger.info(f"   - Failed pages: {len(result.failed_pages)}")
        self.logger.info(f"   - Total links found: {len(all_links)}")
        self.logger.info(f"   - Unique links: {len(result.links)}")

        if result.failed_pages:
            self.logger.warning(f"Failed to collect links from pages: {result.failed_pages}")

        return result

    async def _extract_matches_from_results_page(
        self,
        base_url: str,
        pages_to_scrape: list[int],
        season_year: int | None = None,
        season_end_year: int | None = None,
        sport: str = "football",
    ) -> MatchDataResult:
        """
        Extract match data directly from results page event rows.

        This method extracts all match data (teams, scores, date, odds) directly
        from the results page without visiting individual h2h pages. This solves
        the problem of h2h pages always showing the most recent match instead
        of the specific season's match.

        Args:
            base_url (str): The base URL of the historic matches.
            pages_to_scrape (List[int]): Pages to scrape.
            season_year (Optional[int]): Start year of the season (e.g. 2016 for "2016-2017").
            season_end_year (Optional[int]): End year of the season (e.g. 2017 for "2016-2017").
            sport (str): The sport being scraped (default: football).

        Returns:
            MatchDataResult: Contains match data and tracking of successful/failed pages.
        """

        if season_year is None or season_end_year is None:
            # Try to extract season from URL: .../laliga-2016-2017/...
            m = re.search(r"/-(\d{4})-\d{4}/", base_url)
            if m:
                season_year = int(m.group(1))
                season_end_year = season_year + 1

        self.logger.info(f"Starting extraction of match data from {len(pages_to_scrape)} pages")
        if season_year is not None:
            self.logger.info(f"Season filtering: {season_year}-{season_end_year}")
        self.logger.info(f"Pages to process: {pages_to_scrape}")

        result = MatchDataResult()
        all_matches: list[dict[str, Any]] = []

        # Get timezone for date parsing
        tz_name = getattr(self.playwright_manager, "timezone_id", None)

        for i, page_number in enumerate(pages_to_scrape, 1):
            self.logger.info(f"Processing page {i}/{len(pages_to_scrape)}: {page_number}")
            tab = None

            try:
                tab = await self.playwright_manager.context.new_page()

                # Block OneTrust scripts BEFORE navigation on new tab
                await self.playwright_manager.block_one_trust_for_page(tab)

                self.logger.debug(f"Created new tab for page {page_number}")

                page_url = f"{base_url}#/page/{page_number}"
                self.logger.info(f"Navigating to: {page_url}")
                await tab.goto(page_url, timeout=GOTO_TIMEOUT_MS, wait_until="domcontentloaded")
                delay = random.randint(PAGE_COLLECTION_DELAY_MIN_MS, PAGE_COLLECTION_DELAY_MAX_MS)  # noqa: S311
                self.logger.debug(f"Waiting {delay}ms before processing...")
                await tab.wait_for_timeout(delay)

                self.logger.info(f"Scrolling page {page_number} to load all matches...")
                scroller = self.browser_helper if self.browser_helper is not None else self.scroller
                scroll_success = await scroller.scroll_until_loaded(
                    page=tab,
                    timeout=30,
                    scroll_pause_time=2,
                    max_scroll_attempts=3,
                    content_check_selector="div[class*='eventRow']",
                )

                if scroll_success:
                    self.logger.debug(f"Successfully scrolled page {page_number}")
                else:
                    self.logger.warning(f"Scrolling may not have completed for page {page_number}")

                # Extract match data from this page
                self.logger.info(f"Extracting match data from page {page_number}...")
                matches = await self._extract_match_data_from_event_rows(
                    page=tab,
                    sport=sport,
                    season_year=season_year,
                    season_end_year=season_end_year,
                    tz_name=tz_name,
                )
                all_matches.extend(matches)
                result.successful_pages += 1
                self.logger.info(f"Extracted {len(matches)} matches from page {page_number}")

            except Exception as e:
                result.failed_pages.append(page_number)
                self.logger.error(f"Error processing page {page_number}: {e}")

            finally:
                if tab:
                    await tab.close()
                    self.logger.debug(f"Closed tab for page {page_number}")

        result.matches = all_matches
        self.logger.info("Extraction Summary:")
        self.logger.info(f"   - Total pages processed: {len(pages_to_scrape)}")
        self.logger.info(f"   - Successful pages: {result.successful_pages}")
        self.logger.info(f"   - Failed pages: {len(result.failed_pages)}")
        self.logger.info(f"   - Total matches extracted: {len(all_matches)}")

        if result.failed_pages:
            self.logger.warning(f"Failed to extract from pages: {result.failed_pages}")

        return result

    async def _extract_match_data_from_event_rows(
        self,
        page: Page,
        sport: str,
        season_year: int | None = None,
        season_end_year: int | None = None,
        tz_name: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Extract match data from event rows on a results page.

        Args:
            page (Page): A Playwright Page instance.
            sport (str): The sport being scraped.
            season_year (Optional[int]): Start year of the season.
            season_end_year (Optional[int]): End year of the season.
            tz_name (Optional[str]): Timezone name for date parsing.

        Returns:
            List[Dict]: List of match data dictionaries.
        """
        from oddsharvester.core.odds_portal_selectors import OddsPortalSelectors

        try:
            html_content = await page.content()
            soup = BeautifulSoup(html_content, "lxml")
            event_rows = soup.find_all(class_=re.compile(OddsPortalSelectors.EVENT_ROW_CLASS_PATTERN))
            self.logger.info(f"Found {len(event_rows)} event rows on page")

            matches: list[dict[str, Any]] = []
            current_row_date = None
            filtered_out_count = 0
            seen_h2h: set[str] = set()  # Deduplicate by h2h URL

            for row in event_rows:
                # Parse date header if present
                header_el = row.find(attrs={"data-testid": "date-header"})
                if header_el is not None:
                    header_text = header_el.get_text(" ", strip=True)
                    parsed = _parse_date_header(header_text, tz_name=tz_name, season_year=season_year)
                    if parsed is not None:
                        current_row_date = parsed

                # Season range filtering
                if season_year is not None and season_end_year is not None and current_row_date is not None:
                    season_start = date(season_year, 8, 1)
                    season_end = date(season_end_year, 5, 31)
                    if not (season_start <= current_row_date <= season_end):
                        filtered_out_count += 1
                        continue

                # Extract match data from this row
                match_data = self._parse_event_row_for_match_data(
                    row=row,
                    sport=sport,
                    match_date=current_row_date,
                )

                if match_data and match_data.get("h2h_url"):
                    # Deduplicate by h2h URL
                    h2h = match_data["h2h_url"]
                    if h2h not in seen_h2h:
                        seen_h2h.add(h2h)
                        matches.append(match_data)

            if filtered_out_count > 0:
                self.logger.info(
                    f"Filtered out {filtered_out_count} rows outside season range "
                    f"({season_year}-{season_end_year})"
                )

            self.logger.info(f"Extracted {len(matches)} unique matches from event rows")
            return matches

        except Exception as e:
            self.logger.error(f"Error extracting match data from event rows: {e}")
            return []

    def _parse_event_row_for_match_data(
        self,
        row,
        sport: str,
        match_date: date | None,
    ) -> dict[str, Any] | None:
        """
        Parse a single event row to extract match data.

        Args:
            row: BeautifulSoup element representing an event row.
            sport (str): The sport being scraped.
            match_date (Optional[date]): The date parsed from date header.

        Returns:
            Optional[Dict]: Match data dictionary or None if parsing fails.
        """
        try:
            # Get h2h link
            h2h_link = row.find("a", href=re.compile(r"/football/h2h/"))
            if not h2h_link:
                return None

            h2h_href = h2h_link.get("href", "")
            h2h_url = f"{ODDSPORTAL_BASE_URL}{h2h_href}" if not h2h_href.startswith("http") else h2h_href

            # Extract teams and scores from participants
            # Structure: <a title="TeamName"><p class="participant-name">TeamName</p><div>Score</div></a>
            participants_el = row.find(attrs={"data-testid": "event-participants"})
            home_team = None
            away_team = None
            home_score = None
            away_score = None

            if participants_el:
                # Find all team <a> elements (they have title attribute)
                team_links = participants_el.find_all("a", title=True)
                for i, team_link in enumerate(team_links[:2]):  # Take first 2 teams
                    team_name = team_link.get("title", "")
                    if not team_name:
                        # Fallback to participant-name class
                        name_el = team_link.find(class_="participant-name")
                        if name_el:
                            team_name = name_el.get_text(strip=True)

                    # Extract score from the team link (it's in a sibling div)
                    # The score div is a direct child of the <a> element, after <p class="participant-name">
                    score_div = team_link.find("div", class_=re.compile(r"font-bold"))
                    if score_div:
                        try:
                            score = int(score_div.get_text(strip=True))
                        except ValueError:
                            score = None
                    else:
                        score = None

                    if i == 0:
                        home_team = team_name
                        home_score = score
                    elif i == 1:
                        away_team = team_name
                        away_score = score

            # Alternative: extract scores from the score separator div
            # Structure: <div class="score-center"><div>3</div><a>–</a><div>1</div></div>
            if home_score is None or away_score is None:
                score_center = participants_el.find("div", class_=re.compile(r"relative"))
                if score_center:
                    score_text = score_center.get_text(strip=True)
                    score_match = re.search(r"(\d+)[\s\-–]+(\d+)", score_text)
                    if score_match:
                        if home_score is None:
                            home_score = int(score_match.group(1))
                        if away_score is None:
                            away_score = int(score_match.group(2))

            # Extract basic odds (1/X/2) from odd containers
            # Structure: Each odds value is in a div with class containing 'next-m:min-w-[80%]...font-bold...'
            # The containers appear in order: 1 (home win), X (draw), 2 (away win)
            # Odd container types:
            #   - odd-container-winning: home win odds (lowest = winning pick)
            #   - odd-container-default: draw or away win odds
            odds_1 = None
            odds_x = None
            odds_2 = None

            # Find all odd containers by data-testid
            odd_containers = row.find_all(attrs={"data-testid": re.compile(r"odd-container")})

            if odd_containers:
                # Extract odds values from the first occurrence of each container type
                # The order in the HTML is: winning (1), default (X), default (2)
                # But we need to filter out duplicates (each appears twice for desktop/mobile)
                odds_values = []
                seen_texts = set()
                for el in odd_containers:
                    text = el.get_text(strip=True)
                    # Only take the first occurrence of each unique value
                    if text not in seen_texts:
                        try:
                            val = float(text)
                            if 1.0 <= val <= 50.0:  # Filter for reasonable odds values
                                odds_values.append(val)
                                seen_texts.add(text)
                        except ValueError:
                            continue

                # Map odds to 1, X, 2
                # Based on the HTML structure:
                # - First unique odds value = 1 (home win)
                # - Second unique odds value = X (draw)
                # - Third unique odds value = 2 (away win)
                if len(odds_values) >= 3:
                    odds_1 = odds_values[0]
                    odds_x = odds_values[1]
                    odds_2 = odds_values[2]

            # Get league name from breadcrumb
            league_name = None
            breadcrumb = row.find(attrs={"data-testid": "header-tournament-item"})
            if breadcrumb:
                league_name = breadcrumb.get_text(strip=True)

            # Get match status (Finished, Live, etc.)
            status = None
            status_el = row.find(text=re.compile(r"^(Finished|Live|Cancelled|Postponed|In Progress)"))
            if status_el:
                status = status_el.strip()
            else:
                # Try to find status in time-item div
                time_item = row.find(attrs={"data-testid": "time-item"})
                if time_item:
                    time_text = time_item.get_text(strip=True)
                    if "Finished" in time_text:
                        status = "Finished"
                    elif "Live" in time_text:
                        status = "Live"

            # Build match data dictionary
            match_data: dict[str, Any] = {
                "scraped_date": datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S %Z"),
                "sport": sport,
                "match_link": h2h_url,
                "h2h_url": h2h_url,
            }

            # Add date if available
            if match_date:
                match_data["match_date"] = match_date.strftime("%Y-%m-%d")

            # Add teams
            if home_team:
                match_data["home_team"] = home_team
            if away_team:
                match_data["away_team"] = away_team

            # Add scores
            if home_score is not None:
                match_data["home_score"] = home_score
            if away_score is not None:
                match_data["away_score"] = away_score

            # Add league
            if league_name:
                match_data["league_name"] = league_name

            # Add basic odds as a sub-dictionary
            odds_data = {}
            if odds_1 is not None:
                odds_data["1"] = odds_1
            if odds_x is not None:
                odds_data["X"] = odds_x
            if odds_2 is not None:
                odds_data["2"] = odds_2

            if odds_data:
                match_data["odds"] = odds_data

            # Add status
            if status:
                match_data["status"] = status

            return match_data

        except Exception as e:
            self.logger.debug(f"Error parsing event row: {e}")
            return None

    async def _click_betting_tab(self, page: Page, tab_name: str) -> bool:
        """
        Click on a betting tab (e.g., 'Over/Under', 'Asian Handicap').

        Args:
            page: Playwright page object.
            tab_name: Name of the tab to click ('Over/Under', 'Asian Handicap', etc.)

        Returns:
            bool: True if tab was found and clicked, False otherwise.
        """
        try:
            # Find the tab element that contains the tab name text
            # Use partial match (.includes()) to find the right element
            result = await page.evaluate(
                f"""
                () => {{
                    const nav = document.querySelector('[data-testid="bet-types-nav"]');
                    if (!nav) return {{ found: false, error: 'nav not found' }};


                    // Find div elements that contain the tab name
                    const divs = nav.querySelectorAll('div');
                    for (const div of divs) {{
                        const text = div.textContent;
                        if (text && text.includes("{tab_name}")) {{
                            div.click();
                            return {{ found: true, text: text.trim() }};
                        }}
                    }}


                    return {{ found: false, error: 'Tab not found: {tab_name}' }};
                }}
                """
            )

            if result.get("found"):
                self.logger.debug(f"Clicked tab: {tab_name}")
                # Wait for content to update after clicking
                await page.wait_for_timeout(3000)
                return True
            else:
                self.logger.warning(f"Could not click tab '{tab_name}': {result.get('error')}")
                return False

        except Exception as e:
            self.logger.error(f"Error clicking betting tab '{tab_name}': {e}")
            return False

    async def _click_ah_tab(self, page: Page) -> bool:
        """
        Click on the Asian Handicap tab using exact text match.

        Unlike OU, AH requires exact text match because the partial match
        clicks on the parent nav element which doesn't switch to AH tab.

        Args:
            page: Playwright page object.

        Returns:
            bool: True if tab was found and clicked, False otherwise.
        """
        try:
            result = await page.evaluate(
                """
                () => {
                    const allElements = document.querySelectorAll('*');
                    for (const el of allElements) {
                        if (el.textContent.trim() === 'Asian Handicap') {
                            el.click();
                            return { found: true };
                        }
                    }
                    return { found: false, error: 'AH tab not found' };
                }
                """
            )

            if result.get("found"):
                self.logger.debug("Clicked AH tab")
                await page.wait_for_timeout(3000)
                return True
            else:
                self.logger.warning(f"Could not click AH tab: {result.get('error')}")
                return False

        except Exception as e:
            self.logger.error(f"Error clicking AH tab: {e}")
            return False

    async def _extract_ou_odds(self, page: Page) -> list[dict[str, Any]]:
        """
        Extract Over/Under odds from the current h2h page.

        The OU table shows multiple lines with their Over and Under odds:
        - Line 3.5: Over @ 1.78, Under @ 4.98
        - Line 3.0: Over @ 1.70, Under @ 4.80

        Args:
            page: Playwright page object (should be on Over/Under tab).

        Returns:
            List of dicts with 'line', 'over', 'under' keys.
        """
        try:
            ou_data = await page.evaluate(
                r"""
                () => {
                    const results = [];

                    // Find all odd-container elements
                    const oddContainers = document.querySelectorAll('[data-testid="odd-container"]');
                    const values = [];

                    oddContainers.forEach((el) => {
                        const text = el.textContent.trim();
                        const val = parseFloat(text);
                        if (!isNaN(val) && val >= 1.0 && val <= 20.0) {
                            values.push(val);
                        }
                    });

                    // The first 3 values are 1X2 odds, skip them
                    // Then OU odds follow in groups of 3: Over, Line, Under
                    for (let i = 3; i + 2 < values.length; i += 3) {
                        const over = values[i];
                        const line = values[i + 1];
                        const under = values[i + 2];

                        // Only include if it looks like a valid OU pattern
                        // Line should be between 2.0 and 5.0
                        if (line >= 2.0 && line <= 5.0) {
                            results.push({
                                over: over,
                                line: line,
                                under: under
                            });
                        }
                    }

                    // Remove duplicates based on line value
                    const unique = [];
                    const seen = new Set();
                    for (const r of results) {
                        const key = r.line.toFixed(2);
                        if (!seen.has(key)) {
                            seen.add(key);
                            unique.push(r);
                        }
                    }

                    return unique.sort((a, b) => a.line - b.line);
                }
                """
            )

            if ou_data:
                self.logger.debug(f"Extracted {len(ou_data)} OU lines")
                # Return only the first few most relevant lines (usually the main lines)
                return ou_data[:10]
            else:
                return []

        except Exception as e:
            self.logger.error(f"Error extracting OU odds: {e}")
            return []

    async def _extract_ah_odds(self, page: Page) -> list[dict[str, Any]]:
        """
        Extract Asian Handicap odds from the current h2h page.

        The AH row structure shows:
        - P element: contains the P_value (away odds)
        - Parent text: "Asian Handicap -1.75 AH -1.75 PP.PPHH.HHP%"
        - Pattern: P_value (2-3 digits), Home odds (starts with "1."), Payout (XX.X%)

        For example "103.991.3098.1%":
        - P_value: 10.39 (from P element)
        - Home: 1.30 (parsed from "1.30")
        - Payout: 98.1%

        Args:
            page: Playwright page object (should be on Asian Handicap tab).

        Returns:
            List of dicts with 'handicap', 'home', 'away' keys.
        """
        try:
            ah_data = await page.evaluate(
                """
                () => {
                    const results = [];

                    // Find P elements with class containing "height-content"
                    const allPElements = document.querySelectorAll('p');

                    for (const p of allPElements) {
                        // Check if class name contains "height-content"
                        if (!p.className.includes('height-content')) continue;

                        const text = p.textContent.trim();
                        const pValue = parseFloat(text);

                        // Only process odds-like values (1.5 - 10)
                        if (isNaN(pValue) || pValue < 1.5 || pValue > 10) continue;

                        // Walk up to find the AH row
                        let el = p;
                        for (let i = 0; i < 8; i++) {
                            el = el.parentElement;
                            if (!el) break;

                            const parentText = el.textContent || '';
                            if (!parentText.includes('Asian Handicap -')) continue;

                            // Extract handicap
                            const handicapMatch = parentText.match(/Asian Handicap\\s+(-[0-9]+\\.?[0-9]*)/);
                            if (!handicapMatch) break;

                            const handicap = parseFloat(handicapMatch[1]);

                            // Extract home odds and payout from parent text
                            // Pattern: "...PP.PPHH.HHP%" where:
                            // - PP.PP = P_value (first digits of away odds)
                            // - HH = home odds digits (exactly 2 digits after "1.")
                            // - P% = payout (number ending with %)
                            //
                            // Example: "89.171.0896.6%" -> P=8.91, Home=1.08, Payout=96.6%
                            // Example: "122.491.6599.2%" -> P=12.24, Home=1.65, Payout=99.2%

                            // Extract home odds from the text after "AH"
                            // The pattern after AH is: "-X PP.PPHH.HHP%"
                            // Where PP.PP = away odds, HH = home odds, P% = payout
                            // Strategy: match away odds, then find home odds (1.XX) after it

                            const ahPos = parentText.indexOf('AH');
                            if (ahPos < 0) break;

                            // Find the first "%" after "AH"
                            const pctPos = parentText.indexOf('%', ahPos);
                            if (pctPos < 0) break;

                            // Extract the section between "AH" and "%" (this row's odds only)
                            const oddsSection = parentText.substring(ahPos, pctPos);

                            // Find home odds: match away odds (digits.digits) then find 1.XX after it
                            // Pattern: "AH" + space + negative number + space + away_odds + anything + 1.XX
                            const homeOddsMatch = oddsSection.match(/(^|[0-9])(1\\.\\d{2})/);
                            if (!homeOddsMatch) break;

                            // Parse the home odds from the match
                            const homeOdds = parseFloat(homeOddsMatch[2]);

                            // Use P element value as away odds
                            const awayOdds = pValue;

                            // Validate odds range
                            if (homeOdds >= 1.0 && homeOdds <= 5.0 &&
                                awayOdds >= 1.0 && awayOdds <= 15.0) {
                                results.push({
                                    handicap: handicap,
                                    home: homeOdds,
                                    away: awayOdds
                                });
                            }
                            break;
                        }
                    }

                    // Remove duplicates based on handicap value
                    const unique = [];
                    const seen = new Set();
                    for (const r of results) {
                        const key = r.handicap.toFixed(2);
                        if (!seen.has(key)) {
                            seen.add(key);
                            unique.push(r);
                        }
                    }

                    return unique.sort((a, b) => a.handicap - b.handicap);
                }
                """
            )

            if ah_data:
                self.logger.debug(f"Extracted {len(ah_data)} AH lines")
                return ah_data[:10]
            else:
                return []

        except Exception as e:
            self.logger.error(f"Error extracting AH odds: {e}")
            return []

    async def scrape_match_odds(self, h2h_url: str) -> dict[str, Any]:
        """
        Scrape all odds (1X2, Over/Under, Asian Handicap) from a match's h2h page.

        Args:
            h2h_url: The h2h page URL for the match.

        Returns:
            Dict containing odds data with keys:
            - '1X2': { '1': float, 'X': float, '2': float }
            - 'over_under': [ { 'line': float, 'over': float, 'under': float }, ... ]
            - 'asian_handicap': [ { 'handicap': float, 'home': float, 'away': float }, ... ]
        """
        current_page = self.playwright_manager.page
        if not current_page:
            raise RuntimeError("Playwright has not been initialized.")

        odds_data: dict[str, Any] = {
            "1X2": {},
            "over_under": [],
            "asian_handicap": []
        }

        try:
            # Set consent cookies
            if self.browser_helper is not None:
                await self.browser_helper.set_consent_cookies_for_context(current_page.context)

            # Navigate to h2h page
            self.logger.debug(f"Navigating to h2h page: {h2h_url}")
            await current_page.goto(h2h_url, timeout=60000, wait_until="domcontentloaded")
            await current_page.wait_for_timeout(5000)

            # Extract 1X2 odds (default view)
            html = await current_page.content()
            soup = BeautifulSoup(html, "lxml")

            # Find 1X2 odds in the default view
            odd_containers = soup.find_all(attrs={"data-testid": re.compile(r"odd-container")})
            if odd_containers:
                odds_values = []
                seen_texts = set()
                for el in odd_containers:
                    text = el.get_text(strip=True)
                    if text not in seen_texts:
                        try:
                            val = float(text)
                            if 1.0 <= val <= 50.0:
                                odds_values.append(val)
                                seen_texts.add(text)
                        except ValueError:
                            continue

                if len(odds_values) >= 3:
                    odds_data["1X2"] = {
                        "1": odds_values[0],
                        "X": odds_values[1],
                        "2": odds_values[2]
                    }

            # Extract Over/Under odds
            if await self._click_betting_tab(current_page, "Over/Under"):
                ou_odds = await self._extract_ou_odds(current_page)
                if ou_odds:
                    odds_data["over_under"] = ou_odds

            # Extract Asian Handicap odds (uses special tab click method)
            if await self._click_ah_tab(current_page):
                ah_odds = await self._extract_ah_odds(current_page)
                if ah_odds:
                    odds_data["asian_handicap"] = ah_odds

        except Exception as e:
            self.logger.error(f"Error scraping match odds from {h2h_url}: {e}")

        return odds_data

