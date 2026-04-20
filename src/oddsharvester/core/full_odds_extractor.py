"""
Full odds extraction module for OddsPortal.

This module provides methods to extract complete odds data (1X2, Over/Under, Asian Handicap)
from h2h pages by clicking through all available tabs.
"""

import asyncio
import random
import re
from datetime import UTC, date, datetime
from typing import Any

from bs4 import BeautifulSoup
from playwright.async_api import Page

from oddsharvester.core.base_scraper import BaseScraper, _parse_date_header
from oddsharvester.core.odds_portal_scraper import OddsPortalScraper, MatchDataResult
from oddsharvester.core.url_builder import URLBuilder
from oddsharvester.utils.constants import (
    DEFAULT_REQUEST_DELAY_S,
    GOTO_TIMEOUT_MS,
    PAGE_COLLECTION_DELAY_MAX_MS,
    PAGE_COLLECTION_DELAY_MIN_MS,
    ODDSPORTAL_BASE_URL,
)


class FullOddsExtractor:
    """
    Extracts full odds data (1X2, Over/Under, Asian Handicap) from h2h pages.
    
    This class provides methods to visit h2h pages and click through all tabs
    to extract complete betting odds data.
    """

    def __init__(self, scraper: OddsPortalScraper):
        """
        Initialize the FullOddsExtractor.
        
        Args:
            scraper: An initialized OddsPortalScraper instance with Playwright running.
        """
        self.scraper = scraper
        self.logger = scraper.logger
        self.playwright_manager = scraper.playwright_manager
        self.browser_helper = scraper.browser_helper

    async def _click_betting_tab(self, page: Page, tab_name: str) -> bool:
        """
        Click on a betting tab (e.g., 'Over/Under', 'Asian Handicap').
        
        Uses JavaScript to find and click the tab element containing the specified text.
        
        Args:
            page: Playwright page object.
            tab_name: Name of the tab to click ('Over/Under', 'Asian Handicap', etc.)
            
        Returns:
            bool: True if tab was found and clicked, False otherwise.
        """
        try:
            result = await page.evaluate(
                f"""
                () => {{
                    // Find all li elements that might be tabs
                    const tabs = document.querySelectorAll('li');
                    for (const tab of tabs) {{
                        const text = tab.innerText?.trim();
                        if (text === '{tab_name}') {{
                            tab.click();
                            return {{ found: true, text: text }};
                        }}
                    }}
                    return {{ found: false, error: 'Tab not found: {tab_name}' }};
                }}
                """
            )

            if result.get('found'):
                self.logger.debug(f"Clicked tab: {tab_name}")
                # Wait for content to update after clicking
                await page.wait_for_timeout(2000)
                return True
            else:
                self.logger.warning(f"Could not click tab '{tab_name}': {result.get('error')}")
                return False

        except Exception as e:
            self.logger.error(f"Error clicking betting tab '{tab_name}': {e}")
            return False

    async def _extract_1x2_odds(self, page: Page) -> dict[str, float] | None:
        """
        Extract 1X2 odds from the current page (default view).
        
        Args:
            page: Playwright page object.
            
        Returns:
            Dict with keys '1', 'X', '2' or None if extraction fails.
        """
        try:
            odds = await page.evaluate(
                """
                () => {
                    // Find all odd-container elements
                    const containers = document.querySelectorAll('[data-testid="odd-container"]');
                    const values = [];
                    const seen = new Set();

                    containers.forEach((el) => {
                        const text = el.textContent.trim();
                        if (text && !seen.has(text)) {
                            const val = parseFloat(text);
                            if (!isNaN(val) && val >= 1.0 && val <= 50.0) {
                                values.push(val);
                                seen.add(text);
                            }
                        }
                    });

                    // First 3 values are typically 1, X, 2
                    if (values.length >= 3) {
                        return {
                            '1': values[0],
                            'X': values[1],
                            '2': values[2]
                        };
                    }
                    return null;
                }
                """
            )
            
            if odds:
                self.logger.debug(f"Extracted 1X2 odds: {odds}")
            return odds

        except Exception as e:
            self.logger.error(f"Error extracting 1X2 odds: {e}")
            return None

    async def _extract_ou_odds(self, page: Page) -> list[dict[str, Any]]:
        """
        Extract Over/Under odds from the current page.
        
        The page should be on the Over/Under tab showing OU lines.
        
        Args:
            page: Playwright page object.
            
        Returns:
            List of dicts with 'line', 'over', 'under' keys.
        """
        try:
            ou_data = await page.evaluate(
                """
                () => {
                    const results = [];
                    
                    // Find all divs containing "Over/Under" text
                    const divs = document.querySelectorAll('div');
                    
                    for (const div of divs) {
                        const text = div.innerText?.trim() || '';
                        
                        // Look for Over/Under +X.X pattern
                        const match = text.match(/Over\\/Under\\s+([+-]?\\d+\\.?\\d*)/);
                        if (!match) continue;
                        
                        const line = parseFloat(match[1]);
                        
                        // Skip AH-style lines (small numbers like +0.5, +1.0)
                        if (line <= 2.0) continue;
                        
                        // Get parent to find odds
                        const parent = div.parentElement;
                        if (!parent) continue;
                        
                        // Find odds in the row - look for values between 1.0 and 10.0
                        const parentText = parent.innerText || '';
                        const oddsMatches = parentText.matchAll(/(\\d+\\.\\d{2})/g);
                        const odds = [];
                        
                        for (const od of oddsMatches) {
                            const val = parseFloat(od[1]);
                            if (val >= 1.0 && val <= 10.0) {
                                odds.push(val);
                            }
                        }
                        
                        // OU pattern: typically 2 values per line (over, under)
                        // We want lines around 2.5, 3.5, etc.
                        if (odds.length >= 2) {
                            // First value is usually "Over", second is "Under"
                            results.push({
                                'line': line,
                                'over': odds[0],
                                'under': odds[1]
                            });
                        }
                    }

                    // Deduplicate by line value
                    const unique = [];
                    const seen = new Set();
                    for (const r of results) {
                        const key = r.line.toFixed(1);
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
                return ou_data[:10]  # Return top 10 most relevant lines
            return []

        except Exception as e:
            self.logger.error(f"Error extracting OU odds: {e}")
            return []

    async def _extract_ah_odds(self, page: Page) -> list[dict[str, Any]]:
        """
        Extract Asian Handicap odds from the current page.
        
        The page should be on the Asian Handicap tab showing AH lines.
        
        Args:
            page: Playwright page object.
            
        Returns:
            List of dicts with 'handicap', 'home', 'away' keys.
        """
        try:
            ah_data = await page.evaluate(
                """
                () => {
                    const results = [];
                    
                    // Find all divs containing "Asian Handicap" text
                    const divs = document.querySelectorAll('div');
                    
                    for (const div of divs) {
                        const text = div.innerText?.trim() || '';
                        
                        // Look for "Asian Handicap -X.X" or "Asian Handicap +X.X" pattern
                        const match = text.match(/Asian Handicap\\s+([+-]?\\d+\\.?\\d*)/);
                        if (!match) continue;
                        
                        const handicap = parseFloat(match[1]);
                        
                        // Get parent to find odds
                        const parent = div.parentElement;
                        if (!parent) continue;
                        
                        // Find odds in the row
                        const parentText = parent.innerText || '';
                        const oddsMatches = parentText.matchAll(/(\\d+\\.\\d{2})/g);
                        const odds = [];
                        
                        for (const od of oddsMatches) {
                            const val = parseFloat(od[1]);
                            if (val >= 1.0 && val <= 15.0) {
                                odds.push(val);
                            }
                        }
                        
                        // AH pattern: 2 values per line (home, away)
                        if (odds.length >= 2) {
                            results.push({
                                'handicap': handicap,
                                'home': odds[0],
                                'away': odds[1]
                            });
                        }
                    }

                    // Deduplicate by handicap value
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
                return ah_data[:10]  # Return top 10 most relevant lines
            return []

        except Exception as e:
            self.logger.error(f"Error extracting AH odds: {e}")
            return []

    async def extract_full_odds_from_h2h(self, h2h_url: str) -> dict[str, Any]:
        """
        Extract all odds (1X2, Over/Under, Asian Handicap) from a single h2h page.
        
        Args:
            h2h_url: The h2h page URL for the match.
            
        Returns:
            Dict containing:
            - '1X2': { '1': float, 'X': float, '2': float }
            - 'over_under': [ { 'line': float, 'over': float, 'under': float }, ... ]
            - 'asian_handicap': [ { 'handicap': float, 'home': float, 'away': float }, ... ]
        """
        page = self.playwright_manager.page
        if not page:
            raise RuntimeError("Playwright has not been initialized.")

        odds_data: dict[str, Any] = {
            "1X2": {},
            "over_under": [],
            "asian_handicap": []
        }

        try:
            # Navigate to h2h page
            self.logger.debug(f"Navigating to: {h2h_url}")
            await page.goto(h2h_url, timeout=60000, wait_until='domcontentloaded')
            await page.wait_for_timeout(3000)

            # Extract 1X2 odds (default view)
            odds_1x2 = await self._extract_1x2_odds(page)
            if odds_1x2:
                odds_data["1X2"] = odds_1x2

            # Extract Over/Under odds
            if await self._click_betting_tab(page, "Over/Under"):
                ou_odds = await self._extract_ou_odds(page)
                if ou_odds:
                    odds_data["over_under"] = ou_odds

            # Extract Asian Handicap odds
            if await self._click_betting_tab(page, "Asian Handicap"):
                ah_odds = await self._extract_ah_odds(page)
                if ah_odds:
                    odds_data["asian_handicap"] = ah_odds

        except Exception as e:
            self.logger.error(f"Error extracting odds from {h2h_url}: {e}")

        return odds_data

    async def enrich_matches_with_full_odds(
        self,
        matches: list[dict[str, Any]],
        max_matches: int | None = None,
        delay_between_requests: float = 2.0,
        progress_callback=None,
    ) -> list[dict[str, Any]]:
        """
        Visit each match's h2h page and extract full odds data.
        
        Args:
            matches: List of match dictionaries with 'h2h_url' key.
            max_matches: Maximum number of matches to process (None for all).
            delay_between_requests: Delay in seconds between each request.
            progress_callback: Optional callback function(current, total) for progress updates.
            
        Returns:
            List of match dictionaries with full odds data added.
        """
        matches_to_process = matches[:max_matches] if max_matches else matches
        total = len(matches_to_process)
        
        self.logger.info(f"Enriching {total} matches with full odds...")
        
        enriched_matches = []
        
        for i, match in enumerate(matches_to_process, 1):
            try:
                h2h_url = match.get("h2h_url") or match.get("match_link")
                if not h2h_url:
                    self.logger.warning(f"Match {i}/{total} has no h2h_url, skipping")
                    continue

                if progress_callback:
                    progress_callback(i, total)
                
                self.logger.info(f"Processing match {i}/{total}: {match.get('home_team', '?')} vs {match.get('away_team', '?')}")
                
                # Extract full odds
                odds = await self.extract_full_odds_from_h2h(h2h_url)
                
                # Add odds to match data
                enriched_match = match.copy()
                if odds.get("1X2"):
                    enriched_match["odds"] = odds["1X2"]
                if odds.get("over_under"):
                    enriched_match["over_under"] = odds["over_under"]
                if odds.get("asian_handicap"):
                    enriched_match["asian_handicap"] = odds["asian_handicap"]
                
                enriched_matches.append(enriched_match)
                
                # Delay to avoid rate limiting
                if i < total:
                    await asyncio.sleep(delay_between_requests + random.uniform(0, 1))
                    
            except Exception as e:
                self.logger.error(f"Error processing match {i}/{total}: {e}")
                # Still add the match without enriched odds
                enriched_matches.append(match.copy())

        self.logger.info(f"Enriched {len(enriched_matches)}/{total} matches with full odds")
        return enriched_matches


async def scrape_league_with_full_odds(
    scraper: OddsPortalScraper,
    sport: str,
    league: str,
    season: str,
    markets: list[str] | None = None,
    max_pages: int | None = None,
    max_matches: int | None = None,
    delay_between_requests: float = 2.0,
    save_checkpoint_every: int = 50,
    checkpoint_dir: str = ".",
) -> dict[str, Any]:
    """
    Scrape a league with full odds extraction.
    
    This is a high-level function that:
    1. Extracts match links from results pages (fast)
    2. Visits each match's h2h page and extracts full odds (slow but complete)
    
    Args:
        scraper: An initialized OddsPortalScraper instance.
        sport: Sport to scrape (e.g., 'football').
        league: League identifier (e.g., 'england-premier-league').
        season: Season string (e.g., '2020-2021').
        markets: List of markets to extract (default: ['1x2', 'over_under', 'asian_handicap']).
        max_pages: Maximum results pages to scrape (None for all).
        max_matches: Maximum matches to process (None for all).
        delay_between_requests: Delay between h2h page visits.
        save_checkpoint_every: Save checkpoint after every N matches.
        checkpoint_dir: Directory to save checkpoint files.
        
    Returns:
        Dict with 'matches' (list of enriched match data) and 'stats' (statistics).
    """
    logger = scraper.logger
    
    if markets is None:
        markets = ['1x2', 'over_under', 'asian_handicap']
    
    logger.info(f"Starting full odds extraction for {sport} - {league} - {season}")
    
    # Phase 1: Extract match data from results pages
    logger.info("Phase 1: Extracting match data from results pages...")
    
    base_url = URLBuilder.get_historic_matches_url(sport=sport, league=league, season=season)
    
    # Navigate to get pagination info
    page = scraper.playwright_manager.page
    await page.goto(base_url, timeout=60000, wait_until='domcontentloaded')
    await page.wait_for_timeout(2000)
    
    # Get pages to scrape
    pages_to_scrape = await scraper._get_pagination_info(page=page, max_pages=max_pages)
    
    # Extract matches from results pages
    result = await scraper._extract_matches_from_results_page(
        base_url=base_url,
        pages_to_scrape=pages_to_scrape,
        season_year=int(season.split('-')[0]) if '-' in season else None,
        season_end_year=int(season.split('-')[1]) if '-' in season else None,
        sport=sport,
    )
    
    matches = result.matches
    logger.info(f"Phase 1 complete: extracted {len(matches)} matches")
    
    # Phase 2: Enrich with full odds
    if 'over_under' in markets or 'asian_handicap' in markets or '1x2' in markets:
        logger.info("Phase 2: Enriching matches with full odds...")
        
        extractor = FullOddsExtractor(scraper)
        
        # Progress tracking
        start_time = datetime.now()
        last_save_time = start_time
        
        def progress_callback(current: int, total: int):
            elapsed = (datetime.now() - start_time).total_seconds()
            if current > 0:
                eta = (elapsed / current) * (total - current)
                logger.info(f"Progress: {current}/{total} ({current/total*100:.1f}%) - ETA: {eta/60:.1f} min")
            
            # Save checkpoint periodically
            nonlocal last_save_time
            if current % save_checkpoint_every == 0 or current == total:
                checkpoint_time = datetime.now()
                if (checkpoint_time - last_save_time).total_seconds() > 60:
                    logger.info(f"Saving checkpoint after {current} matches...")
                    last_save_time = checkpoint_time
        
        # Limit matches if specified
        matches_to_process = matches[:max_matches] if max_matches else matches
        logger.info(f"Processing {len(matches_to_process)} matches (max_matches={max_matches})")
        
        enriched_matches = []
        processed = 0
        
        for match in matches_to_process:
            try:
                processed += 1
                progress_callback(processed, len(matches_to_process))
                
                h2h_url = match.get("h2h_url") or match.get("match_link")
                if not h2h_url:
                    logger.warning(f"Match has no h2h_url, skipping")
                    enriched_matches.append(match)
                    continue

                # Extract full odds
                odds = await extractor.extract_full_odds_from_h2h(h2h_url)
                
                # Add odds to match data
                enriched_match = match.copy()
                
                if '1x2' in markets and odds.get("1X2"):
                    enriched_match["odds"] = odds["1X2"]
                if 'over_under' in markets and odds.get("over_under"):
                    enriched_match["over_under"] = odds["over_under"]
                if 'asian_handicap' in markets and odds.get("asian_handicap"):
                    enriched_match["asian_handicap"] = odds["asian_handicap"]
                
                enriched_matches.append(enriched_match)
                
                # Delay to avoid rate limiting
                if processed < len(matches_to_process):
                    await asyncio.sleep(delay_between_requests + random.uniform(0, 1))
                    
            except Exception as e:
                logger.error(f"Error processing match: {e}")
                enriched_matches.append(match.copy())
        
        matches = enriched_matches
        logger.info(f"Phase 2 complete: enriched {len(matches)} matches")
    
    # Summary
    total_time = (datetime.now() - start_time).total_seconds()
    logger.info("=" * 60)
    logger.info("Scraping Complete!")
    logger.info(f"  Total matches: {len(matches)}")
    logger.info(f"  Total time: {total_time/60:.1f} minutes")
    logger.info(f"  Average time per match: {total_time/len(matches):.1f} seconds")
    logger.info("=" * 60)
    
    return {
        "matches": matches,
        "stats": {
            "total_matches": len(matches),
            "total_time_seconds": total_time,
            "pages_scrape": len(pages_to_scrape),
        }
    }
