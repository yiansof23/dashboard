#!/usr/bin/env python3
"""
UAE Air Traffic Tracker — Local Edition
Pulls daily flight counts from FlightAware AeroAPI,
appends to CSV, generates chart, commits to GitHub.

Usage:
  python3 tracker.py                    # pull yesterday's data
  python3 tracker.py 2026-03-22         # pull a specific date
  python3 tracker.py --backfill 5       # pull last N days
  python3 tracker.py --chart-only       # regenerate chart only
  python3 tracker.py --schedule         # run daily at 06:00 UTC

Requires:
  pip install matplotlib
  Environment: FLIGHTAWARE_API_KEY (or set in config below)
"""

import json, urllib.request, time, csv, os, sys, subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ── Config ───────────────────────────────────────────────────────────────────
API_KEY = os.environ.get("FLIGHTAWARE_API_KEY", "4DXRh6EeuDc7YlFQP1lhWPQmuczYwQI3")
BASE_URL = "https://aeroapi.flightaware.com/aeroapi"
AIRPORTS = ["OMDB", "OMAA"]
AIRPORT_LABELS = {"OMDB": "Dubai International (DXB)", "OMAA": "Abu Dhabi International (AUH)"}
DIRECTIONS = ["arrivals", "departures"]
BASELINES = {"OMDB": 850, "OMAA": 150}  # 7-day pre-conflict totals

# Git config
GIT_REMOTE = os.environ.get("GIT_REMOTE", "")  # set to push automatically
GIT_BRANCH = "main"

# Paths
SCRIPT_DIR = Path(__file__).parent
DATA_CSV = SCRIPT_DIR / "daily_counts.csv"
CHART_FILE = SCRIPT_DIR / "air_traffic_chart.png"

# Rate limit settings
API_DELAY = 3        # seconds between API calls
RATE_RETRY_WAIT = 90 # seconds to wait on 429
MAX_RETRIES = 5


def log(msg):
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}", flush=True)


# ── API ──────────────────────────────────────────────────────────────────────
def fetch_flight_count(airport, direction, date_str):
    """Count all flights for airport/direction on a given date, handling pagination."""
    start = f"{date_str}T00:00:00Z"
    end = (datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")
    url = f"{BASE_URL}/airports/{airport}/flights/{direction}?start={start}&end={end}&max_pages=10"
    
    total = 0
    retries = 0
    
    while url:
        if url.startswith("/"):
            url = BASE_URL + url
        req = urllib.request.Request(url, headers={"x-apikey": API_KEY})
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read())
                flights = data.get(direction, [])
                total += len(flights)
                nxt = (data.get("links") or {}).get("next")
                url = nxt if (nxt and flights) else None
                retries = 0
                if url:
                    time.sleep(API_DELAY)
        except urllib.error.HTTPError as e:
            if e.code == 429 and retries < MAX_RETRIES:
                retries += 1
                log(f"    Rate limited ({retries}/{MAX_RETRIES}), waiting {RATE_RETRY_WAIT}s...")
                time.sleep(RATE_RETRY_WAIT)
                continue
            elif e.code == 400:
                log(f"    Date too old for API (10-day limit)")
                return None
            else:
                log(f"    HTTP {e.code} (got {total} so far)")
                return total
        except Exception as e:
            log(f"    Error: {e}")
            return total
    
    return total


def pull_day(date_str):
    """Pull all counts for one day. Returns dict or None if date too old."""
    log(f"Pulling {date_str}...")
    results = {}
    
    for airport in AIRPORTS:
        results[airport] = {}
        for direction in DIRECTIONS:
            count = fetch_flight_count(airport, direction, date_str)
            if count is None:
                log(f"  {airport} {direction}: unavailable (too old)")
                return None
            results[airport][direction] = count
            log(f"  {airport} {direction}: {count}")
            time.sleep(API_DELAY)
    
    return results


# ── Data ─────────────────────────────────────────────────────────────────────
def load_daily_data():
    """Load existing CSV into dict keyed by (date, airport)."""
    data = {}
    if DATA_CSV.exists():
        with open(DATA_CSV) as f:
            for row in csv.DictReader(f):
                data[(row["date"], row["airport"])] = {
                    "arrivals": int(row["arrivals"]),
                    "departures": int(row["departures"]),
                    "source": row.get("source", "api"),
                }
    return data


def save_daily_data(data):
    """Write full dataset to CSV, sorted by date then airport."""
    with open(DATA_CSV, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "airport", "arrivals", "departures", "total", "source"])
        for (date_str, airport) in sorted(data.keys()):
            row = data[(date_str, airport)]
            total = row["arrivals"] + row["departures"]
            writer.writerow([date_str, airport, row["arrivals"], row["departures"], total, row["source"]])


def add_day_to_data(data, date_str, results):
    """Add or update a day's results in the data dict."""
    for airport in AIRPORTS:
        key = (date_str, airport)
        data[key] = {
            "arrivals": results[airport]["arrivals"],
            "departures": results[airport]["departures"],
            "source": "api",
        }


# ── Chart ────────────────────────────────────────────────────────────────────
def build_chart(data):
    """Generate two-panel stacked bar chart with 7-day rolling totals."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates

    # Compute 7-day rolling sums
    rolling = {}
    for airport in AIRPORTS:
        airport_dates = sorted(set(d for (d, a) in data if a == airport))
        for date_str in airport_dates:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            arr_7d = dep_7d = 0
            days_found = 0
            for i in range(7):
                lb = (dt - timedelta(days=i)).strftime("%Y-%m-%d")
                key = (lb, airport)
                if key in data:
                    arr_7d += data[key]["arrivals"]
                    dep_7d += data[key]["departures"]
                    days_found += 1
            if days_found >= 4:
                rolling[(dt, airport)] = {"arrivals": arr_7d, "departures": dep_7d}

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 9), facecolor="white")
    fig.suptitle("Air Traffic Is Rebounding", fontsize=16, fontweight="bold",
                 x=0.08, ha="left", y=0.97)

    for ax, airport, title_lines in [
        (ax1, "OMDB", "FLIGHTS THROUGH\nDUBAI INTERNATIONAL AIRPORT*"),
        (ax2, "OMAA", "FLIGHTS THROUGH\nABU DHABI INTERNATIONAL AIRPORT*"),
    ]:
        dates = sorted(set(dt for (dt, a) in rolling if a == airport))
        if not dates:
            ax.set_title(title_lines + "\n(No data yet)", fontsize=9, fontweight="bold", loc="left")
            continue

        arrivals = [rolling[(d, airport)]["arrivals"] for d in dates]
        departures = [rolling[(d, airport)]["departures"] for d in dates]
        totals = [a + dep for a, dep in zip(arrivals, departures)]

        ax.bar(dates, arrivals, width=0.8, color="#1a4d3e", label="ARRIVALS", zorder=2)
        ax.bar(dates, departures, width=0.8, bottom=arrivals, color="#4da688",
               label="DEPARTURES", zorder=2)
        ax.axhline(y=BASELINES[airport], color="#cc3333", linewidth=1.5,
                   linestyle="--", label="PRE-CONFLICT BASELINE", zorder=3)

        peak = max(max(totals), BASELINES[airport])
        ymax = int(peak * 1.2 / 100 + 1) * 100
        ax.set_title(title_lines, fontsize=9, fontweight="bold", loc="left", pad=8)
        ax.set_ylim(0, ymax)
        ax.set_xlim(min(dates) - timedelta(hours=12), max(dates) + timedelta(hours=12))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%d"))
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=2))
        ax.yaxis.set_major_locator(plt.MultipleLocator(200 if ymax <= 1200 else 400))
        ax.tick_params(axis="both", labelsize=8)
        ax.set_ylabel("#", fontsize=9)
        ax.grid(axis="y", alpha=0.3, zorder=0)
        ax.legend(fontsize=7, loc="upper right", framealpha=0.9)

    # Month labels on x-axis
    all_dates = sorted(set(dt for (dt, _) in rolling))
    if all_dates:
        months_seen = []
        for d in all_dates:
            label = d.strftime("%Y %b")
            if label not in months_seen:
                months_seen.append(label)
        ax2.set_xlabel("    ".join(months_seen), fontsize=8)

    fig.text(0.08, 0.01,
             "* SHOWN AS A 7-DAY MOVING TOTAL.  SOURCE: FLIGHTAWARE AEROAPI.",
             fontsize=6, style="italic", color="gray")

    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.savefig(CHART_FILE, dpi=150, bbox_inches="tight")
    plt.close()
    log(f"Chart saved to {CHART_FILE}")


# ── Git ──────────────────────────────────────────────────────────────────────
def git_commit_and_push():
    """Commit data and chart, push to GitHub."""
    try:
        os.chdir(SCRIPT_DIR)
        subprocess.run(["git", "add", "daily_counts.csv", "air_traffic_chart.png"],
                       check=True, capture_output=True)
        
        # Check if there are changes to commit
        result = subprocess.run(["git", "diff", "--staged", "--quiet"], capture_output=True)
        if result.returncode == 0:
            log("No changes to commit")
            return
        
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        subprocess.run(
            ["git", "commit", "-m", f"📊 Air traffic update {date_str}"],
            check=True, capture_output=True,
        )
        subprocess.run(["git", "push"], check=True, capture_output=True, timeout=30)
        log("Pushed to GitHub")
    except subprocess.CalledProcessError as e:
        log(f"Git error: {e.stderr.decode()[:200] if e.stderr else e}")
    except Exception as e:
        log(f"Git error: {e}")


# ── Schedule ─────────────────────────────────────────────────────────────────
def run_scheduled():
    """Run once daily at 06:00 UTC using a simple sleep loop."""
    import signal
    signal.signal(signal.SIGINT, lambda *_: sys.exit(0))
    
    log("Scheduler started. Will pull data daily at 06:00 UTC.")
    log("Press Ctrl+C to stop.\n")
    
    while True:
        now = datetime.now(timezone.utc)
        # Next 06:00 UTC
        target = now.replace(hour=6, minute=0, second=0, microsecond=0)
        if now >= target:
            target += timedelta(days=1)
        
        wait = (target - now).total_seconds()
        log(f"Next run at {target.strftime('%Y-%m-%d %H:%M UTC')} (in {wait/3600:.1f}h)")
        time.sleep(wait)
        
        # Pull yesterday
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
        run_pull([yesterday])


def run_pull(dates):
    """Pull data for given dates, update CSV, regenerate chart, push."""
    data = load_daily_data()
    
    for date_str in dates:
        if (date_str, "OMDB") in data and data[(date_str, "OMDB")]["source"] == "api":
            log(f"  {date_str} already has API data, skipping")
            continue
        results = pull_day(date_str)
        if results:
            add_day_to_data(data, date_str, results)
    
    save_daily_data(data)
    build_chart(data)
    git_commit_and_push()
    log("Done!")


# ── Main ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if "--schedule" in sys.argv:
        run_scheduled()
    elif "--chart-only" in sys.argv:
        data = load_daily_data()
        build_chart(data)
        log("Chart regenerated")
    elif "--backfill" in sys.argv:
        idx = sys.argv.index("--backfill")
        n = int(sys.argv[idx + 1]) if idx + 1 < len(sys.argv) else 5
        dates = []
        for i in range(n, 0, -1):
            d = (datetime.now(timezone.utc) - timedelta(days=i)).strftime("%Y-%m-%d")
            dates.append(d)
        log(f"Backfilling {len(dates)} days: {dates[0]} to {dates[-1]}")
        run_pull(dates)
    else:
        # Specific date or yesterday
        dates = [a for a in sys.argv[1:] if a.startswith("20") and len(a) == 10]
        if not dates:
            dates = [(datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")]
        run_pull(dates)
