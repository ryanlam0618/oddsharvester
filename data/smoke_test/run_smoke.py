"""Smoke test wrapper: disable the breaking STEALTH_SCRIPT, then run the CLI."""
import sys

import oddsharvester.core.playwright_manager as pwm

# ROOT CAUSE (bisect 2026-11-04): the STEALTH_SCRIPT fingerprint patches break
# the OddsPortal SPA render (page 68px, 0 event rows). Disabled for this smoke
# test; UA/locale/timezone anti-detection alone works (43 anchors rendered).
pwm.STEALTH_SCRIPT = "// stealth disabled: breaks OddsPortal SPA render (bisect 2026-11-04)"

sys.argv = [
    "oddsharvester", "historic",
    "-s", "football",
    "-l", "england-premier-league",
    "--season", "2024-2025",
    "-m", "1x2",
    "--headless",
    "--user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "--locale", "en-GB",
    "--timezone", "Europe/London",
    "-f", "json",
    "-o", "data/smoke_test/epl_1x2_2024_2025.json",
]
from oddsharvester.cli.cli import main
main()
