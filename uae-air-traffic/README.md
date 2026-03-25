# UAE Air Traffic Tracker

Tracks daily flight volumes at Dubai (OMDB/DXB) and Abu Dhabi (OMAA/AUH) airports using the FlightAware AeroAPI.

## How it works

- `tracker.py` pulls daily arrival/departure counts from FlightAware
- Data accumulates in `daily_counts.csv` — one row per airport per day
- Chart shows 7-day rolling totals vs pre-conflict baseline
- Commits and pushes to this repo automatically

## Usage

```bash
# Set your API key
export FLIGHTAWARE_API_KEY="your_key_here"

# Pull yesterday's data (default)
python3 tracker.py

# Pull a specific date
python3 tracker.py 2026-03-22

# Backfill last N days (max 10 due to API limit)
python3 tracker.py --backfill 9

# Run on a daily schedule (06:00 UTC)
python3 tracker.py --schedule

# Regenerate chart from existing data
python3 tracker.py --chart-only
```

## Data

- **Source**: FlightAware AeroAPI (personal tier)
- **Limitation**: API only allows 10 days of historical lookback, so data accumulates over time
- **Baseline**: Pre-conflict 7-day totals — OMDB: ~850, OMAA: ~150
- **Chart**: 7-day moving total of arrivals + departures

## Requirements

```bash
pip install matplotlib
```
