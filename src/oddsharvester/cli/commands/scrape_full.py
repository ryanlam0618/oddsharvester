"""CLI command for scraping historical matches with full odds (1X2, OU, AH)."""

import asyncio
import logging
import os
import sys

import click

from oddsharvester.cli.options import common_options
from oddsharvester.cli.validators import validate_max_pages, validate_season
from oddsharvester.core.browser_helper import BrowserHelper
from oddsharvester.core.full_odds_extractor import FullOddsExtractor
from oddsharvester.core.odds_portal_market_extractor import OddsPortalMarketExtractor
from oddsharvester.core.odds_portal_scraper import OddsPortalScraper
from oddsharvester.core.playwright_manager import PlaywrightManager
from oddsharvester.core.sport_market_registry import SportMarketRegistrar
from oddsharvester.core.url_builder import URLBuilder
from oddsharvester.storage.storage_manager import store_data
from oddsharvester.utils.sport_market_constants import Sport

logger = logging.getLogger(__name__)


@click.command("scrape-full")
@common_options
@click.option(
    "--season",
    required=True,
    callback=validate_season,
    help="Season to scrape (YYYY, YYYY-YYYY, or 'current').",
)
@click.option(
    "--max-pages",
    type=int,
    callback=validate_max_pages,
    help="Maximum number of pages to scrape.",
)
@click.option(
    "--max-matches",
    type=int,
    default=None,
    help="Maximum number of matches to process (None for all).",
)
@click.option(
    "--delay",
    type=float,
    default=2.0,
    help="Delay between match visits in seconds (default: 2.0).",
)
@click.option(
    "--save-every",
    type=int,
    default=50,
    help="Save checkpoint every N matches (default: 50).",
)
@click.option(
    "--include-1x2",
    is_flag=True,
    default=True,
    help="Include 1X2 odds (default: true).",
)
@click.option(
    "--include-ou",
    is_flag=True,
    default=True,
    help="Include Over/Under odds (default: true).",
)
@click.option(
    "--include-ah",
    is_flag=True,
    default=True,
    help="Include Asian Handicap odds (default: true).",
)
@click.pass_context
def scrape_full(ctx, **kwargs):
    """
    Scrape historical odds with FULL market data (1X2, Over/Under, Asian Handicap).
    
    Unlike 'historic' which only extracts 1X2 from results pages, this command
    visits each match's h2h page and clicks through all tabs to extract complete odds.
    
    Examples:
    
        # Scrape Premier League with all markets
        oddsharvester scrape-full -s football -l england-premier-league --season 2020-2021
        
        # Scrape only 1X2 and Over/Under, skip Asian Handicap
        oddsharvester scrape-full -s football -l england-premier-league --season 2020-2021 --no-include-ah
        
        # Limit to 100 matches with custom delay
        oddsharvester scrape-full -s football -l england-premier-league --season 2020-2021 --max-matches 100 --delay 3.0
    """
    # Normalize sport enum to string
    sport_value = kwargs["sport"]
    sport_str = sport_value.value if isinstance(sport_value, Sport) else sport_value
    
    storage = kwargs["storage"]
    storage_format = kwargs["storage_format"]
    league = kwargs["leagues"][0] if kwargs.get("leagues") else None
    season = kwargs.get("season")
    max_pages = kwargs.get("max_pages")
    max_matches = kwargs.get("max_matches")
    delay = kwargs.get("delay", 2.0)
    save_every = kwargs.get("save_every", 50)
    
    include_markets = []
    if kwargs.get("include_1x2"):
        include_markets.append("1x2")
    if kwargs.get("include_ou"):
        include_markets.append("over_under")
    if kwargs.get("include_ah"):
        include_markets.append("asian_handicap")
    
    if not include_markets:
        click.echo("Error: At least one market must be enabled (--include-1x2, --include-ou, --include-ah)", err=True)
        sys.exit(1)
    
    logger.info(f"Starting full odds scraping: {sport_str} - {league} - {season}")
    logger.info(f"Markets: {include_markets}")
    logger.info(f"Max matches: {max_matches or 'all'}")
    logger.info(f"Delay between matches: {delay}s")
    
    try:
        # Initialize components
        SportMarketRegistrar.register_all_markets()
        playwright_manager = PlaywrightManager()
        browser_helper = BrowserHelper()
        market_extractor = OddsPortalMarketExtractor(browser_helper=browser_helper)
        
        scraper = OddsPortalScraper(
            playwright_manager=playwright_manager,
            browser_helper=browser_helper,
            market_extractor=market_extractor,
            preview_submarkets_only=kwargs.get("preview_submarkets_only", False),
        )
        
        async def run_full_scrape():
            try:
                # Start Playwright
                await scraper.start_playwright(headless=not kwargs.get("headless", False))
                
                # Create extractor
                extractor = FullOddsExtractor(scraper)
                
                # Phase 1: Extract match data from results pages (fast)
                logger.info("Phase 1: Extracting match data from results pages...")
                
                base_url = URLBuilder.get_historic_matches_url(sport=sport_str, league=league, season=season)
                
                page = scraper.playwright_manager.page
                await page.goto(base_url, timeout=60000, wait_until="domcontentloaded")
                await page.wait_for_timeout(2000)
                
                pages_to_scrape = await scraper._get_pagination_info(page=page, max_pages=max_pages)
                
                season_year = int(season.split("-")[0]) if season and "-" in season else None
                season_end_year = int(season.split("-")[1]) if season and "-" in season else None
                
                result = await scraper._extract_matches_from_results_page(
                    base_url=base_url,
                    pages_to_scrape=pages_to_scrape,
                    season_year=season_year,
                    season_end_year=season_end_year,
                    sport=sport_str,
                )
                
                matches = result.matches
                logger.info(f"Phase 1 complete: extracted {len(matches)} matches from {len(pages_to_scrape)} pages")
                
                if not matches:
                    logger.error("No matches found!")
                    return None
                
                # Phase 2: Enrich with full odds (slow but complete)
                if include_markets:
                    logger.info("Phase 2: Enriching matches with full odds...")
                    
                    enriched_matches = []
                    processed = 0
                    total = min(len(matches), max_matches) if max_matches else len(matches)
                    
                    for match in matches[:max_matches] if max_matches else matches:
                        try:
                            processed += 1
                            logger.info(f"Processing match {processed}/{total}: {match.get('home_team', '?')} vs {match.get('away_team', '?')}")
                            
                            h2h_url = match.get("h2h_url") or match.get("match_link")
                            if not h2h_url:
                                logger.warning(f"Match has no h2h_url, skipping")
                                enriched_matches.append(match)
                                continue
                            
                            # Extract full odds
                            odds = await extractor.extract_full_odds_from_h2h(h2h_url)
                            
                            # Add odds to match data
                            enriched_match = match.copy()
                            
                            if "1x2" in include_markets and odds.get("1X2"):
                                enriched_match["odds"] = odds["1X2"]
                            if "over_under" in include_markets and odds.get("over_under"):
                                enriched_match["over_under"] = odds["over_under"]
                            if "asian_handicap" in include_markets and odds.get("asian_handicap"):
                                enriched_match["asian_handicap"] = odds["asian_handicap"]
                            
                            enriched_matches.append(enriched_match)
                            
                            # Save checkpoint periodically
                            if processed % save_every == 0:
                                checkpoint_file = f"checkpoint_{processed}.json"
                                store_data(
                                    storage_type="local",
                                    data=enriched_matches,
                                    storage_format="json",
                                    file_path=checkpoint_file,
                                )
                                logger.info(f"Saved checkpoint: {checkpoint_file}")
                            
                            # Delay to avoid rate limiting
                            if processed < total:
                                await asyncio.sleep(delay)
                                
                        except Exception as e:
                            logger.error(f"Error processing match: {e}")
                            enriched_matches.append(match.copy())
                    
                    matches = enriched_matches
                    logger.info(f"Phase 2 complete: enriched {len(matches)} matches")
                
                return matches
                
            finally:
                await scraper.stop_playwright()
        
        # Run the async scraping
        scraped_data = asyncio.run(run_full_scrape())
        
        if scraped_data:
            # Save results
            file_path = kwargs.get("file_path")
            if not file_path:
                file_path = f"full_odds_{league}_{season.replace('-', '_')}.json"
            
            store_data(
                storage_type=storage.value if storage else "local",
                data=scraped_data,
                storage_format=storage_format.value if storage_format else "json",
                file_path=file_path,
            )
            
            click.echo(f"Successfully scraped {len(scraped_data)} matches with full odds")
            click.echo(f"Saved to: {file_path}")
            
            # Show sample
            if scraped_data:
                sample = scraped_data[0]
                click.echo(f"\nSample match data:")
                click.echo(f"  Teams: {sample.get('home_team', '?')} vs {sample.get('away_team', '?')}")
                click.echo(f"  Date: {sample.get('match_date', '?')}")
                if sample.get("odds"):
                    click.echo(f"  1X2: {sample['odds']}")
                if sample.get("over_under"):
                    click.echo(f"  Over/Under: {len(sample['over_under'])} lines")
                if sample.get("asian_handicap"):
                    click.echo(f"  Asian Handicap: {len(sample['asian_handicap'])} lines")
        else:
            logger.error("Scraper did not return valid data.")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Error during scraping: {e}", exc_info=True)
        sys.exit(1)