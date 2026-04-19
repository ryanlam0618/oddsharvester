#!/usr/bin/env bash
set -euo pipefail

# Sequential historic scraper runner for multiple football leagues/seasons.
# Runs one job after another and continues through the queue.

ROOT_DIR="/app"
DATA_DIR="${OH_BATCH_DATA_DIR:-/app/data/batch}"
LOG_DIR="${OH_BATCH_LOG_DIR:-/app/data/batch-logs}"
MARKET="${OH_BATCH_MARKET:-1x2}"
HEADLESS_FLAG="${OH_BATCH_HEADLESS_FLAG:---headless}"
ODDS_HISTORY_FLAG="${OH_BATCH_ODDS_HISTORY_FLAG:---odds-history}"
SPORT="football"
START_SEASON="${OH_BATCH_START_SEASON:-2015-2016}"
END_SEASON="${OH_BATCH_END_SEASON:-2024-2025}"

mkdir -p "$DATA_DIR" "$LOG_DIR"

# label|slug
LEAGUES=(
  "Premier League|england-premier-league"
  "La Liga|spain-laliga"
  "Serie A|italy-serie-a"
  "Ligue 1|france-ligue-1"
  "Bundesliga|germany-bundesliga"
  "J1 League|japan-j1-league"
  "Chinese Super League|china-super-league"
  "K League 1|south-korea-k-league-1"
  "A-League Men|australia-a-league"
  "UEFA Champions League|champions-league"
  "UEFA Europa League|europa-league"
  "UEFA Europa Conference League|conference-league"
  "AFC Champions League|afc-champions-league"
  "FA Cup|england-fa-cup"
  "Coppa Italia|italy-coppa-italia"
  "Copa del Rey|spain-copa-del-rey"
  "Coupe de France|france-coupe-de-france"
  "DFB Pokal|germany-dfb-pokal"
)

slugify() {
  echo "$1" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9]+/-/g; s/^-+//; s/-+$//'
}

seasons=()
start_year="${START_SEASON%-*}"
end_year="${END_SEASON%-*}"
for ((y=start_year; y<=end_year; y++)); do
  seasons+=("$y-$((y+1))")
done

total_jobs=$((${#LEAGUES[@]} * ${#seasons[@]}))
job_num=0

echo "=== OddsHarvester batch start ==="
echo "Leagues: ${#LEAGUES[@]} | Seasons: ${#seasons[@]} | Total jobs: ${total_jobs}"
echo "Output dir: $DATA_DIR"
echo "Log dir: $LOG_DIR"

for league_entry in "${LEAGUES[@]}"; do
  IFS='|' read -r league_label league_slug <<< "$league_entry"
  league_key="$(slugify "$league_label")"
  
  for season in "${seasons[@]}"; do
    job_num=$((job_num + 1))
    output_path="$DATA_DIR/${league_key}/${season}.json"
    log_path="$LOG_DIR/${league_key}__${season}.log"
    mkdir -p "$(dirname "$output_path")"

    echo ""
    echo ">>> [${job_num}/${total_jobs}] ${league_label} | ${season}"
    echo ">>> slug=${league_slug}"
    echo ">>> output=${output_path}"
    echo ">>> log=${log_path}"

    if [[ -f "$output_path" && -s "$output_path" ]]; then
      echo ">>> Skip existing non-empty output"
      continue
    fi

    set +e
    PYTHONPATH="$ROOT_DIR/src" oddsharvester historic \
      -s "$SPORT" \
      -l "$league_slug" \
      --season "$season" \
      -m "$MARKET" \
      $ODDS_HISTORY_FLAG \
      $HEADLESS_FLAG \
      -f json \
      -o "$output_path" \
      2>&1 | tee "$log_path"
    exit_code=${PIPESTATUS[0]}
    set -e

    if [[ $exit_code -ne 0 ]]; then
      echo "!!! FAILED [${job_num}/${total_jobs}] ${league_label} | ${season} (exit=${exit_code})"
    else
      echo "+++ DONE [${job_num}/${total_jobs}] ${league_label} | ${season}"
    fi
  done
done

echo "=== OddsHarvester batch finished ==="
