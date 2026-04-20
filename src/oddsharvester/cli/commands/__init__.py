"""CLI commands for OddsHarvester."""

from oddsharvester.cli.commands.historic import historic
from oddsharvester.cli.commands.scrape_full import scrape_full
from oddsharvester.cli.commands.upcoming import upcoming

__all__ = ["historic", "scrape_full", "upcoming"]
