"""
Odds Parser with fixed modal parsing.

This module handles parsing of odds data from HTML content.
"""

from datetime import UTC, datetime, timezone
import logging
import re
from typing import Any

from bs4 import BeautifulSoup, Tag

from oddsharvester.core.odds_portal_selectors import OddsPortalSelectors

_FRACTIONAL_RE = re.compile(r"^(\d+)/(\d+)$")
_logger = logging.getLogger(__name__)


def parse_odds_value(text: str) -> float:
    """Parse an odds string that may be decimal (``1.80``) or fractional (``4/5``).

    Fractional odds are converted to decimal: numerator / denominator + 1.
    """
    m = _FRACTIONAL_RE.match(text)
    if m:
        decimal = int(m.group(1)) / int(m.group(2)) + 1
        _logger.debug(f"Converted fractional odds '{text}' -> {decimal:.4f}")
        return decimal
    return float(text)


class OddsParser:
    """Handles parsing of odds data from HTML content."""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def parse_market_odds(
        self, html_content: str, period: str, odds_labels: list, target_bookmaker: str | None = None
    ) -> list[dict[str, Any]]:
        """
        Parses odds for a given market type in a generic way.

        Args:
            html_content (str): The HTML content of the page.
            period (str): The match period (e.g., "FullTime").
            odds_labels (list): A list of labels defining the expected odds columns (e.g., ["odds_over", "odds_under"]).
            target_bookmaker (str, optional): If set, only parse odds for this bookmaker.

        Returns:
            list[dict]: A list of dictionaries containing bookmaker odds.
        """
        self.logger.info("Parsing odds from HTML content.")
        soup = BeautifulSoup(html_content, "html.parser")

        # Try broader "border-black-borders" pattern first as it works better
        bookmaker_blocks = soup.find_all("div", class_=re.compile(OddsPortalSelectors.BOOKMAKER_ROW_CLASS))

        if not bookmaker_blocks:
            # Fallback to broader selector
            bookmaker_blocks = soup.find_all("div", class_=re.compile(OddsPortalSelectors.BOOKMAKER_ROW_FALLBACK_CLASS))

        if not bookmaker_blocks:
            self.logger.warning("No bookmaker blocks found.")
            return []

        odds_data = []
        for block in bookmaker_blocks:
            try:
                bookmaker_name = self._extract_bookmaker_name(block)

                if not bookmaker_name or (target_bookmaker and bookmaker_name.lower() != target_bookmaker.lower()):
                    continue

                odds_blocks = block.find_all("div", class_=re.compile(OddsPortalSelectors.ODDS_BLOCK_CLASS_PATTERN))

                if len(odds_blocks) < len(odds_labels):
                    self.logger.warning(f"Incomplete odds data for bookmaker: {bookmaker_name}. Skipping...")
                    continue

                extracted_odds = {label: odds_blocks[i].get_text(strip=True) for i, label in enumerate(odds_labels)}

                for key, value in extracted_odds.items():
                    extracted_odds[key] = re.sub(r"(\d+\.\d+)\1", r"\1", value)

                extracted_odds["bookmaker_name"] = bookmaker_name
                extracted_odds["period"] = period
                odds_data.append(extracted_odds)

            except Exception as e:
                self.logger.error(f"Error parsing odds: {e}")
                continue

        self.logger.info(f"Successfully parsed odds for {len(odds_data)} bookmakers.")
        return odds_data

    def parse_odds_history_modal(self, modal_html: str, reference_match_date: str | None = None) -> dict[str, Any]:
        """
        Parses the HTML content of an odds history modal.

        The modal HTML format is:
        "Odds movement | 15 Mar, 01:27 | 1.39 | +0.03 | Opening odds: | 08 Mar, 01:32 | 1.36"

        This gives us:
        - Current odds with timestamp (close to match time)
        - Change from opening
        - Opening odds with timestamp

        Args:
            modal_html (str): Raw HTML from the modal.
            reference_match_date (str | None): Match datetime string used to infer
                the correct calendar year for modal timestamps that omit a year.

        Returns:
            dict: Parsed odds history data with current_odds, change, opening_odds, and timestamps.
        """
        self.logger.info("Parsing modal content for odds history.")
        
        result = {
            'current_odds': None,
            'current_timestamp': None,
            'change': None,
            'opening_odds': None,
            'opening_timestamp': None,
            'closing_odds': None,  # Alias for current_odds (same thing)
            'closing_timestamp': None,  # Alias for current_timestamp
        }

        try:
            soup = BeautifulSoup(modal_html, "html.parser")
            
            # Get raw text
            text = soup.get_text(separator=" | ", strip=True)
            
            # Pattern: Odds movement | 15 Mar, 01:27 | 1.39 | +0.03 | Opening odds: | 08 Mar, 01:32 | 1.36
            pattern = r"(\d{1,2}\s+\w{3},?\s+\d{2}:\d{2})\s*\|\s*(\d+\.\d+)\s*\|\s*([+-]\d+\.\d+)\s*\|\s*Opening odds:\s*\|\s*(\d{1,2}\s+\w{3},?\s+\d{2}:\d{2})\s*\|\s*(\d+\.\d+)"
            match = re.search(pattern, text)
            
            if match:
                current_ts, current_odds_str, change, opening_ts, opening_odds_str = match.groups()
                
                result['current_timestamp'] = self._parse_timestamp(current_ts, reference_match_date)
                result['current_odds'] = parse_odds_value(current_odds_str)
                result['change'] = change
                result['opening_timestamp'] = self._parse_timestamp(opening_ts, reference_match_date)
                result['opening_odds'] = parse_odds_value(opening_odds_str)
                
                # Alias for clarity
                result['closing_odds'] = result['current_odds']
                result['closing_timestamp'] = result['current_timestamp']
                
                self.logger.info(
                    f"Parsed odds history: current={result['current_odds']} ({result['current_timestamp']}), "
                    f"opening={result['opening_odds']} ({result['opening_timestamp']}), change={change}"
                )
            else:
                self.logger.warning(f"Could not parse odds history modal with pattern. Text: {text[:100]}")
                
        except Exception as e:
            self.logger.error(f"Failed to parse odds history modal: {e}")
            
        return result

    def _parse_timestamp(self, time_text: str, reference_match_date: str | None = None) -> str:
        """
        Parse a timestamp string like "15 Mar, 01:27" to ISO format.
        
        Args:
            time_text: Timestamp string in format "DD MMM, HH:MM" or "DD MMM HH:MM"
            reference_match_date: Match datetime string used to infer the year.

        Returns:
            ISO format datetime string
        """
        if not time_text:
            return None
            
        try:
            # Handle both "15 Mar, 01:27" and "15 Mar 01:27" formats
            time_text = time_text.replace(',', ' ').strip()
            dt = datetime.strptime(time_text, "%d %b %H:%M")

            reference_dt = None
            if reference_match_date:
                try:
                    reference_dt = datetime.strptime(reference_match_date, "%Y-%m-%d %H:%M:%S %Z")
                except ValueError:
                    try:
                        reference_dt = datetime.fromisoformat(reference_match_date.replace("Z", "+00:00"))
                    except ValueError:
                        self.logger.debug(
                            f"Could not parse reference_match_date for odds history year inference: {reference_match_date}"
                        )

            base_year = reference_dt.year if reference_dt else datetime.now(timezone.utc).year
            dt = dt.replace(year=base_year, tzinfo=UTC)

            if reference_dt:
                # Odds history timestamps should be near or before the match date.
                # If month/day falls after the match date in the same inferred year,
                # treat it as belonging to the previous calendar year (season crossover).
                if dt > reference_dt.replace(tzinfo=UTC):
                    dt = dt.replace(year=dt.year - 1)

            return dt.isoformat()
        except ValueError:
            self.logger.warning(f"Failed to parse timestamp: {time_text}")
            return time_text

    def _extract_bookmaker_name(self, block: Tag) -> str | None:
        """Extract bookmaker name from a row using a fallback chain.

        Strategies tried in order:
        1. ``<img class="bookmaker-logo" title="...">``
        2. ``<a title="...">`` wrapping the logo / name
        3. ``<img>`` with an ``alt`` attribute containing the name
        """
        # 1. Primary: img.bookmaker-logo[title]
        img_tag = block.find("img", class_=OddsPortalSelectors.BOOKMAKER_LOGO_CLASS)
        if img_tag and img_tag.get("title"):
            return img_tag["title"]

        # 2. Fallback: <a> with a title attribute (logo links)
        a_tag = block.find("a", attrs={"title": True})
        if a_tag and a_tag["title"]:
            name = a_tag["title"]
            # Normalise CTA-style titles like "Go to Betfair Exchange website!"
            if name.lower().startswith("go to ") and name.endswith("!"):
                name = name[len("go to ") : -1].strip()
                # Strip trailing "website" if present
                if name.lower().endswith(" website"):
                    name = name[: -len(" website")].strip()
            self.logger.debug(f"Resolved bookmaker name via <a title>: {name}")
            return name

        # 3. Fallback: any <img> with a meaningful alt attribute
        for img in block.find_all("img"):
            alt = img.get("alt", "")
            if alt and alt.lower() not in ("", "logo"):
                self.logger.debug(f"Resolved bookmaker name via <img alt>: {alt}")
                return alt

        self.logger.debug("Could not resolve bookmaker name from block")
        return None
