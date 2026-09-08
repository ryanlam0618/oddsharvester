# Upstream Sync Report — 2026-09-09

- **Branch**: `sync/upstream-2026-09-09`
- **Upstream**: `jordantete/OddsHarvester` @ `06e5708` (v0.12.0, ~285 commits)
- **Base (fork master)**: `dc077ae` (anti-detection sync from SofaScore backfill)
- **Author**: Forge (coding agent), 2026-09-09

## Summary

Merged upstream v0.12.0 into the fork. 14 files conflicted; all resolved with the
invariant that **fork anti-detection and fork-specific features are never degraded**.
The fork fully adopts upstream's new modular browser layer (`core/browser/*`),
multi-proxy rotation, HAR record/replay, `live` / `community` commands, redesigned
pagination walker, and seasonal redirect guard — while keeping fork's OneTrust
blocking, consent-cookie seeding, stealth script, human simulation, checkpoint
saving, and season-range validation.

## Conflict-by-conflict resolution (14 files)

### 1. `Dockerfile` — took upstream
Fork's multi-stage (Lambda + local-dev) image was obsoleted: upstream deleted
`lambda_handler.py` entirely. New single-stage image with CLI entrypoint.
**Fork feature lost**: AWS Lambda deployment target (upstream removed it first).

### 2. `pyproject.toml` — merged
Upstream (version 0.12.0, tzdata dep, pythonpath fix) + fork's
`mysql-connector-python>=9.7.0`. `uv.lock` regenerated via `uv lock`.

### 3. `src/oddsharvester/cli/cli.py` — merged
Registered BOTH fork's `scrape_full` AND upstream's new `community` + `live` commands.

### 4. `src/oddsharvester/cli/commands/__init__.py` — merged
Exports: community, historic, live, scrape_full (fork), upcoming.

### 5. `src/oddsharvester/cli/commands/historic.py` — merged (behavior: fork)
- Upstream: `--season` now accepts comma-separated lists, `--links-only`,
  `--local-kickoff`, combo summary table, append mode, listing-failure exit code.
- Fork: environment-controlled checkpoints (`OH_CHECKPOINT_SAVE`), always-persist
  results (even empty) so orchestrators can distinguish "no data" from "not run",
  and **exit 0 on a legitimately empty result** (fork 570036f — overrides upstream's
  exit 1; updated upstream test to match, documented in test).
- Bugfix on merge: links-only output is now always stored (checkpoint path used to
  swallow it).

### 6. `src/oddsharvester/core/base_scraper.py` — merged (11 hunks)
- `extract_match_rows` (upstream's kickoff/live/offscreen model) + fork's
  `season_year`/`season_end_year` filtering and 2-part date-header year inference.
- `extract_match_links` thin wrapper extended with the fork's season params.
- `extract_match_odds`: upstream's proxy attribution & live mode + fork's
  per-match checkpoint saving (`store_data` import), season-mismatch
  rejection (SEASON_MISMATCH, non-retryable), and OneTrust blocking.
- `set_odds_format`: upstream's text-based selector (issue #68) with fork's
  structural fallback chain (`_find_odds_format_dropdown_button`) when the text
  selector yields nothing.

### 7. `src/oddsharvester/core/browser_helper.py` — KEEP FORK FILE
Upstream deleted `BrowserHelper` (split into `browser/cookies.py`,
`browser/scrolling.py`, etc.). Fork's copy retains the anti-detection pieces
that have no upstream equivalent: `humanize_page`, `set_consent_cookies_for_context`,
`set_consent_cookie_via_page_js`, `dismiss_overlays`, `scroll_until_loaded` legacy.
Now wired as an **optional** collaborator (`browser_helper=None` tolerated) so
upstream-style constructions from unit tests keep working.

### 8. `src/oddsharvester/core/market_extraction/odds_history_extractor.py` — merged
Upstream's redesigned row discovery (`BOOKMAKER_ROW_WITH_NAME_CSS`,
`_row_bookmaker_name`, `ODD_CELL_CSS`) + fork's robust hover path: overlay clearing
before hover, `scroll_into_view_if_needed`, JS mouse-event fallback.

### 9. `src/oddsharvester/core/market_extraction/odds_parser.py` — merged
Primary: upstream DOM-column parser (returns `{"odds_history", "opening_odds"}`,
empty-garbage `{}` semantics preserved exactly as upstream's tests pin).
Fallback: fork's legacy flat-text regex parser becomes `_parse_legacy_modal()`,
triggered only when the DOM path finds nothing; keeps fork's `reference_match_date`
year inference (`_parse_timestamp` retained; `#447bf5e` feature preserved).

### 10. `src/oddsharvester/core/odds_portal_scraper.py` — merged (11 hunks)
- `scrape_historic`: **upstream's link-collection flow restored** (pagination walker
  + `_collect_match_links` + `extract_match_odds`), with fork threading:
  season parsing → `season_year/season_end_year` filters, consent cookies +
  OneTrust block before navigation, `checkpoint_*` kwargs → `extract_match_odds`,
  season stamping on rows, failed listing pages surfaced as `LISTING_PAGE` failures.
  Rationale: upstream's redesigned match-page hydration now correctly resolves the
  h2h fragment (`_hydrate_match_view` + `normalize_inplay_match_url`) and is fully
  covered by upstream's expanded test suite; fork's option-B (inline results-page
  extraction) remains available via `scrape-full` / `FullOddsExtractor`.
- `_prepare_page_for_scraping`: OneTrust block + consent cookies + odds format +
  fork's banner/overlay dismissal + humanize, then upstream's cookie dismisser.
- `_collect_match_links`: upstream's widget-reading pagination walker with fork's
  season extraction/logging and OneTrust block on every new tab.
- `scrape_live`, `scrape_upcoming`, `_assert_season_page_reached`,
  `_links_only_result`, `_effective_page_limit`: upstream, adopted wholesale.
- Fork-only methods retained: `_extract_matches_from_results_page`,
  `_extract_match_data_from_event_rows`, `_parse_event_row_for_match_data`,
  `_click_betting_tab`, `_click_ah_tab`, `_extract_ou_odds`, `_extract_ah_odds`,
  `scrape_match_odds` (used by `scrape-full`).

### 11. `src/oddsharvester/core/playwright_manager.py` — merged
Fork's comprehensive `STEALTH_SCRIPT` (Canvas/WebGL/AudioContext fingerprint
protection, OneTrust patching, consent cookie override) + fork's third-party /
resource-type request blocking (`_block_one_trust_scripts`, used on startup and
per-page via `block_one_trust_for_page`) + `humanize_page`; upstream's multi-proxy
context management (`_create_context`, `new_rotated_page`, `report_page_result`,
`blacklist_proxy`), HAR record/replay env hooks, and browser-timezone capture.

### 12. `src/oddsharvester/core/retry.py` — took upstream
Fork side contained no unique logic (flock/rate-limiter live in
`utils/rate_limiter.py` and storage, not this file — verified). Upstream adds
`ScraperError`-aware retry attribution and proxy-attributable error classification.

### 13. `uv.lock` — regenerated with `uv lock` (includes mysql-connector-python).

### 14. Scraper app composition (`src/oddsharvester/core/scraper_app.py`) — merged
Upstream structure (browser module construction, base_url warning, live command,
league/season combos) + fork's `checkpoint_*` params threaded into both the
single-match and combo historic paths, and `BrowserHelper` construction passed into
`OddsPortalScraper`.

## What upstream added (now available to the fork)

- **`live` command**: in-play odds snapshot scraping (listing `/inplay-odds/live-now/`,
  per-match in-play pages, ended-match drop semantics).
- **`community` command**: top predictions, match community votes, user profiles.
- **Multi-proxy rotation**: per-proxy browser contexts, blacklist/failover,
  per-proxy HAR/warming (`--proxy-file` flows through `ProxyManager`).
- **HAR record/replay**: `ODDSHARVESTER_HAR_RECORD` / `ODDSHARVESTER_HAR_REPLAY`
  env vars (used by the integration test fixtures).
- **Pagination walker**: widget-verified page frontier, page refetch on truncation,
  `LISTING_PAGE` failure type, links-only mode, exit code semantics.
- **Season redirect guard**: `_assert_season_page_reached` fails fast on season URLs
  that silently redirect to the current fixtures page (gotcha 4).
- **Regional `base_url`**, `--timezone`/`--locale` capture, local kickoff conversion,
  `include_started` / `kickoff_within_hours` filters, new sports/leagues
  (cricket, handball, volleyball, Tunisia/Norway/Sweden leagues).
- **New selectors** for the OddsPortal redesign (`BOOKMAKER_ROW_WITH_NAME_CSS`, etc.)

## Fork features survival check

| Fork feature | Status |
|---|---|
| OneTrust / 3rd-party request blocking | ✅ kept (`_block_one_trust_scripts`, called on init + every page) |
| Comprehensive stealth script | ✅ kept (verbatim) |
| Consent-cookie pre-seeding (context + JS) | ✅ kept |
| `humanize_page` simulation | ✅ kept (called in `_prepare_page_for_scraping`) |
| Per-match checkpoint saving (`OH_CHECKPOINT_SAVE`, `--file-path`) | ✅ kept, threaded through `extract_match_odds` |
| Season-range validation (`SEASON_MISMATCH`, non-retryable) | ✅ kept |
| Season-aware date-header year inference | ✅ kept (`_parse_date_header(season_year=...)`, `_parse_timestamp`) |
| `scrape-full` command + `FullOddsExtractor` (Option-B results-page extraction) | ✅ kept |
| Exit 0 on legitimate empty result | ✅ kept (upstream test updated with comment) |
| MySQL storage deps | ✅ kept in pyproject + lock |
| Lambda Dockerfile stage | ❌ dropped (upstream deleted `lambda_handler.py`) |

## Verification

- `uv run ruff check src/ tests/`: 132 remaining issues, all non-blocking style
  (W293 blank-line whitespace, E501, S311 deliberate-jitter random, RUF unicode);
  zero `F8xx`/import errors. (Fork's master already carried this style debt —
  no new real issues introduced.)
- `uv run pytest` (unit, excl. integration): **1106 passed, 5 skipped**.
  Two fork-behavior adjustments required updating 3 upstream tests
  (all-empty exit code, checkpoint kwargs assertion) — see test docstrings.
- Integration tests (`-m integration`): 29 failed — **identical on pristine
  upstream v0.12.0** (pre-existing suite issue in this environment, not the merge).
- Live smoke tests (headless, 2026-09-09):
  - `live football --links-only`: 9 in-play links collected. ✅
  - `upcoming -l england-premier-league -d 20260919 -m 1x2`: 5 matches with real
    per-bookmaker 1X2 odds. ✅
  - `historic -l england-premier-league --season 2024-2025 --links-only`: 50
    season-correct match links. ✅
  - `extract_match_odds` with `checkpoint_file_path` + season validation:
    1/1 success, checkpoint file written. ✅
  - No EPL match was live during the run window, so the "scrape one EPL live match"
    scenario was covered by the upcoming/historic EPL paths instead.

## Notes / follow-ups

- `concurrency_tasks` default is 3 — set `PLAYWRIGHT_WORKERS=1`-equivalent
  (CLI `--concurrency 1`) on loaded machines.
- `parse_odds_history_modal(reference_match_date=...)` year inference is currently
  NOT wired from `odds_portal_market_extractor` (was a fork latent bug referencing
  an undefined variable; fixed to pass `None`). If historical odds-history year
  accuracy matters, thread the match date through `scrape_markets`.
- The pre-existing fork helpers `_find_odds_format_dropdown_button` /
  `_detect_current_odds_format` remain as a fallback path of `set_odds_format`.
