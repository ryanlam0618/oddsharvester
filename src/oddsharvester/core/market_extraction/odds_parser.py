"""
Odds Parser with fixed modal parsing.

This module handles parsing of odds data from HTML content.
"""

from datetime import UTC, datetime
import logging
import re
from typing import Any

from bs4 import BeautifulSoup, Tag

from oddsharvester.core.odds_portal_selectors import OddsPortalSelectors

_FRACTIONAL_RE = re.compile(r"^(\d+)/(\d+)$")
# OddsPortal abbreviates September as "Sept", which %b does not accept.
_MONTH_ABBR_RE = re.compile(r"\bSept\b")
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

        # Odds are a real <table>, one leaf <tr> per bookmaker, identified by the
        # bookmaker links in its first cell: collapsed submarket line rows carry
        # the expand arrow instead, and the peripheral rows (My coupon, User
        # Predictions, OddsAlert) render outside the table. Non-leaf rows are
        # excluded: an expanded submarket row wraps a nested bookmaker table.
        root = OddsPortalSelectors.content_root(soup)
        bookmaker_rows = [
            tr for tr in root.select(OddsPortalSelectors.BOOKMAKER_ROW_WITH_NAME_CSS) if tr.find("tr") is None
        ]

        if not bookmaker_rows:
            self.logger.warning("No bookmaker rows found.")
            return []

        odds_data = []
        for row in bookmaker_rows:
            try:
                bookmaker_name = self._extract_bookmaker_name(row)

                if not bookmaker_name or (target_bookmaker and bookmaker_name.lower() != target_bookmaker.lower()):
                    continue

                odds_cells = row.select(OddsPortalSelectors.ODD_CELL_CSS)

                if len(odds_cells) < len(odds_labels):
                    self.logger.warning(f"Incomplete odds data for bookmaker: {bookmaker_name}. Skipping...")
                    continue

                extracted_odds = {label: odds_cells[i].get_text(strip=True) for i, label in enumerate(odds_labels)}

                for key, value in extracted_odds.items():
                    extracted_odds[key] = re.sub(r"(\d+\.\d+)\1", r"\1", value)

                blocked_outcomes = [
                    label
                    for i, label in enumerate(odds_labels)
                    if odds_cells[i].select_one(OddsPortalSelectors.ODDS_BLOCKED_SELECTOR)
                ]

                extracted_odds["bookmaker_name"] = bookmaker_name
                extracted_odds["period"] = period
                if blocked_outcomes:
                    extracted_odds["blocked_outcomes"] = blocked_outcomes
                odds_data.append(extracted_odds)

            except Exception as e:
                self.logger.error(f"Error parsing odds: {e}")
                continue

        self.logger.info(f"Successfully parsed odds for {len(odds_data)} bookmakers.")
        return odds_data

    def parse_odds_history_modal(self, modal_html: str, reference_match_date: str | None = None) -> dict[str, Any]:
        """
        Parses the HTML content of an odds history modal.

        Primary path matches upstream (current post-redesign modal layout). Fork
        extension: if the current layout yields nothing, retry with the legacy
        flat-text regex layout ("Odds movement | ... | Opening odds: | ..."),
        using `reference_match_date` to infer the missing calendar year.

        Args:
            modal_html (str): Raw HTML from the modal.
            reference_match_date (str | None): Match datetime string used to infer
                the correct calendar year for modal timestamps that omit a year.

        Returns:
            dict: Parsed odds history data, including historical odds and the opening odds.
        """
        self.logger.info("Parsing modal content for odds history.")
        soup = BeautifulSoup(modal_html, "html.parser")

        try:
            odds_history = []
            # Redesign: history columns are siblings inside a flex-row wrapper
            # (col 0 = timestamps, col 1 = values, col 2 = deltas).
            cols = soup.select("div.flex.flex-row.gap-3 > div.flex.flex-col.gap-1")
            timestamps = cols[0].select("div.font-normal") if cols else []
            odds_values = cols[1].select("div.font-bold") if len(cols) > 1 else []

            for ts, odd in zip(timestamps, odds_values, strict=False):
                time_text = ts.get_text(strip=True)
                try:
                    dt = datetime.strptime(_MONTH_ABBR_RE.sub("Sep", time_text), "%d %b, %H:%M")
                    formatted_time = dt.replace(year=datetime.now(UTC).year).isoformat()
                except ValueError:
                    self.logger.warning(f"Failed to parse datetime: {time_text}")
                    continue

                odds_history.append({"timestamp": formatted_time, "odds": parse_odds_value(odd.get_text(strip=True))})

            # Parse opening odds
            opening_odds_block = soup.select_one("div.mt-2.gap-1")
            opening_ts_div = opening_odds_block.select_one("div.flex.gap-1 div") if opening_odds_block else None
            opening_val_div = opening_odds_block.select_one("div.flex.gap-1 .font-bold") if opening_odds_block else None

            opening_odds = None
            if opening_ts_div and opening_val_div:
                try:
                    dt = datetime.strptime(
                        _MONTH_ABBR_RE.sub("Sep", opening_ts_div.get_text(strip=True)), "%d %b, %H:%M"
                    )
                    opening_odds = {
                        "timestamp": dt.replace(year=datetime.now(UTC).year).isoformat(),
                        "odds": parse_odds_value(opening_val_div.get_text(strip=True)),
                    }
                except ValueError:
                    self.logger.warning("Failed to parse opening odds timestamp.")

            if not odds_history and opening_odds is None:
                legacy = self._parse_legacy_modal(soup, reference_match_date)
                if legacy is not None:
                    return legacy
                if opening_odds_block is None:
                    return {}

            return {"odds_history": odds_history, "opening_odds": opening_odds}

        except Exception as e:
            self.logger.error(f"Failed to parse odds history modal: {e}")
            return {}

    def _parse_legacy_modal(self, soup: BeautifulSoup, reference_match_date: str | None = None) -> dict[str, Any] | None:
        """Fork fallback: parse the legacy flat-text modal layout via regex.

        Layout: "Odds movement | 15 Mar, 01:27 | 1.39 | +0.03 | Opening odds: | 08 Mar, 01:32 | 1.36".
        Uses `reference_match_date` to infer the missing calendar year. Returns None on no match.
        """
        text = soup.get_text(separator=" | ", strip=True)
        pattern = (
            r"(\d{1,2}\s+\w{3},?\s+\d{2}:\d{2})\s*\|\s*(\d+\.\d+)\s*\|\s*([+-]\d+\.\d+)"
            r"\s*\|\s*Opening odds:\s*\|\s*(\d{1,2}\s+\w{3},?\s+\d{2}:\d{2})\s*\|\s*(\d+\.\d+)"
        )
        match = re.search(pattern, text)
        if not match:
            return None

        current_ts, current_odds_str, change, opening_ts, opening_odds_str = match.groups()
        legacy = {
            "current_odds": parse_odds_value(current_odds_str),
            "current_timestamp": self._parse_timestamp(current_ts, reference_match_date),
            "change": change,
            "opening_odds": parse_odds_value(opening_odds_str),
            "opening_timestamp": self._parse_timestamp(opening_ts, reference_match_date),
        }
        legacy["closing_odds"] = legacy["current_odds"]
        legacy["closing_timestamp"] = legacy["current_timestamp"]
        self.logger.info(
            f"Parsed legacy odds history: current={legacy['current_odds']} "
            f"({legacy['current_timestamp']}), opening={legacy['opening_odds']}, change={change}"
        )
        return legacy

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
            time_text = time_text.replace(",", " ").strip()
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

            base_year = reference_dt.year if reference_dt else datetime.now(UTC).year
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
        1. the name paragraph inside the bookmaker link
        2. ``<a title="...">`` wrapping the logo / bonus link (the only source on
           rows whose name is rendered as a logo only)
        """
        # 1. Primary: the visible name next to the logo
        name_el = block.select_one(f"{OddsPortalSelectors.BOOKMAKER_LINK_CSS} p")
        if name_el:
            name = name_el.get_text(strip=True)
            if name:
                return name

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

        self.logger.debug("Could not resolve bookmaker name from block")
        return None
