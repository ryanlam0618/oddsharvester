#!/usr/bin/env python3
"""
Scrape all specified leagues and seasons with full odds extraction.

Usage:
    python3 scrape_all_leagues.py [--cookies COOKIES] [--delay DELAY] [--max-per-season MAX]

This script will scrape:
- 18 competitions × 10 seasons (2015/16 to 2024/25)
"""

import asyncio
import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f'scrape_all_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)
logger = logging.getLogger(__name__)

# Define all leagues and competitions to scrape
LEAGUES = [
    # European Top Leagues
    {"sport": "football", "league": "england-premier-league", "name": "Premier League"},
    {"sport": "football", "league": "spain-laliga", "name": "La Liga"},
    {"sport": "football", "league": "italy-serie-a", "name": "Serie A"},
    {"sport": "football", "league": "france-ligue-1", "name": "Ligue 1"},
    {"sport": "football", "league": "germany-bundesliga", "name": "Bundesliga"},
    
    # Asian Leagues
    {"sport": "football", "league": "japan-j-league", "name": "J1 League"},
    {"sport": "football", "league": "china-super-league", "name": "Chinese Super League"},
    {"sport": "football", "league": "south-korea-k-league-1", "name": "K League 1"},
    {"sport": "football", "league": "australia-a-league", "name": "A-League Men"},
    
    # European Competitions
    {"sport": "football", "league": "europe-champions-league", "name": "UEFA Champions League"},
    {"sport": "football", "league": "europe-europa-league", "name": "UEFA Europa League"},
    {"sport": "football", "league": "europe-conference-league", "name": "UEFA Europa Conference League"},
    {"sport": "football", "league": "europe-afc-champions-league", "name": "AFC Champions League"},
    
    # Domestic Cups
    {"sport": "football", "league": "england-fa-cup", "name": "FA Cup"},
    {"sport": "football", "league": "italy-coppa-italia", "name": "Coppa Italia"},
    {"sport": "football", "league": "spain-copa-del-rey", "name": "Copa del Rey"},
    {"sport": "football", "league": "france-coupe-de-france", "name": "Coupe de France"},
    {"sport": "football", "league": "germany-dfb-pokal", "name": "DFB Pokal"},
]

# Seasons from 2015/16 to 2024/25
SEASONS = [
    "2015-2016", "2016-2017", "2017-2018", "2018-2019", "2019-2020",
    "2020-2021", "2021-2022", "2022-2023", "2023-2024", "2024-2025"
]


def run_scrape(sport: str, league: str, season: str, cookies: str = None, delay: float = 2.0) -> dict:
    """
    Run a single scrape command.
    
    Returns:
        dict with 'success', 'output', 'error', 'league', 'season'
    """
    cmd = [
        "docker", "run", "--rm",
        "-v", f"{os.getcwd()}:/app",
        "-w", "/app",
        "-e", f"PYTHONPATH=/app/src",
        "oddsharvester:latest",
        "python3", "-m", "oddsharvester.cli.cli",
        "scrape-full",
        "-s", sport,
        "-l", league,
        "--season", season,
        "--delay", str(delay),
        "--no-headless",  # Run in headless mode
        "-o", f"data/{league}_{season.replace('-', '_')}.json"
    ]
    
    if cookies and os.path.exists(cookies):
        cmd.extend(["--cookies", cookies])
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600  # 10 minutes timeout per league-season
        )
        
        return {
            "success": result.returncode == 0,
            "output": result.stdout,
            "error": result.stderr,
            "league": league,
            "season": season,
            "name": next((l["name"] for l in LEAGUES if l["league"] == league), league)
        }
    except subprocess.TimeoutExpired:
        return {
            "success": False,
            "output": "",
            "error": "Timeout after 10 minutes",
            "league": league,
            "season": season,
            "name": next((l["name"] for l in LEAGUES if l["league"] == league), league)
        }
    except Exception as e:
        return {
            "success": False,
            "output": "",
            "error": str(e),
            "league": league,
            "season": season,
            "name": next((l["name"] for l in LEAGUES if l["league"] == league), league)
        }


def get_completed_count(results_file: str) -> set:
    """Get set of already completed (league, season) tuples."""
    completed = set()
    data_dir = "data"
    
    if not os.path.exists(data_dir):
        return completed
    
    for f in os.listdir(data_dir):
        if f.endswith('.json'):
            # Parse filename: league_season.json
            parts = f.replace('.json', '').split('_')
            if len(parts) >= 2:
                # Try to find a season pattern (YYYY-YYYY)
                for i, part in enumerate(parts):
                    if '-' in part and part.count('-') == 1:
                        season = part
                        league = '_'.join(parts[:i])
                        completed.add((league, season))
                        break
    
    return completed


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Scrape all leagues with full odds")
    parser.add_argument("--cookies", default="/home/openclaw/.openclaw/workspace/projects/OddsHarvester/cookies.json",
                        help="Path to cookies file")
    parser.add_argument("--delay", type=float, default=2.0,
                        help="Delay between matches in seconds")
    parser.add_argument("--max-per-season", type=int, default=None,
                        help="Max matches per season (for testing)")
    parser.add_argument("--resume", action="store_true",
                        help="Resume from last checkpoint")
    parser.add_argument("--leagues", nargs="+", 
                        help="Specific leagues to scrape (by name)")
    
    args = parser.parse_args()
    
    # Filter leagues if specified
    leagues_to_scrape = LEAGUES
    if args.leagues:
        leagues_to_scrape = [l for l in LEAGUES if any(
            name.lower() in l["name"].lower() or l["league"].lower() in name.lower()
            for name in args.leagues
        )]
        if not leagues_to_scrape:
            logger.error(f"No leagues match: {args.leagues}")
            return
    
    # Create data directory
    os.makedirs("data", exist_ok=True)
    
    # Get already completed
    completed = get_completed_count("data") if args.resume else set()
    logger.info(f"Found {len(completed)} already completed league-seasons")
    
    # Stats
    total = len(leagues_to_scrape) * len(SEASONS)
    done = 0
    success = 0
    failed = []
    
    start_time = time.time()
    
    logger.info(f"Starting scrape of {len(leagues_to_scrape)} leagues × {len(SEASONS)} seasons = {total} tasks")
    logger.info(f"Using cookies: {args.cookies if os.path.exists(args.cookies) else 'None (may fail for Asian leagues)'}")
    logger.info("=" * 70)
    
    for league_info in leagues_to_scrape:
        league = league_info["league"]
        name = league_info["name"]
        
        for season in SEASONS:
            task_id = f"{name} {season}"
            
            # Check if already completed
            if (league, season) in completed:
                logger.info(f"[{done+1}/{total}] Skipping (already done): {task_id}")
                done += 1
                continue
            
            logger.info(f"[{done+1}/{total}] Scraping: {task_id}")
            
            # Run scrape
            result = run_scrape(
                sport="football",
                league=league,
                season=season,
                cookies=args.cookies if os.path.exists(args.cookies) else None,
                delay=args.delay
            )
            
            if result["success"]:
                success += 1
                logger.info(f"✅ Success: {task_id}")
            else:
                failed.append((task_id, result["error"]))
                logger.error(f"❌ Failed: {task_id} - {result['error'][:100]}")
            
            done += 1
            
            # Log progress
            elapsed = time.time() - start_time
            rate = done / elapsed if elapsed > 0 else 0
            remaining = (total - done) / rate if rate > 0 else 0
            
            logger.info(f"   Progress: {done}/{total} ({100*done/total:.1f}%) | "
                       f"Elapsed: {elapsed/3600:.1f}h | "
                       f"Est. remaining: {remaining/3600:.1f}h")
    
    # Summary
    logger.info("=" * 70)
    logger.info("SCRAPING COMPLETE")
    logger.info(f"Total: {total}")
    logger.info(f"Success: {success}")
    logger.info(f"Failed: {len(failed)}")
    
    if failed:
        logger.info("\nFailed tasks:")
        for task, error in failed:
            logger.info(f"  - {task}: {error[:100]}")
    
    elapsed = time.time() - start_time
    logger.info(f"\nTotal time: {elapsed/3600:.2f} hours")


if __name__ == "__main__":
    main()