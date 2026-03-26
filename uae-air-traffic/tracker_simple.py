#!/usr/bin/env python3
"""
UAE Air Traffic Tracker — Simple Edition
Uses the /counts endpoint: 2 API calls per day instead of hundreds.

Records scheduled_arrivals + scheduled_departures as the daily activity level.
Best called once daily at ~20:00 UTC (midnight Dubai time) for most complete numbers.

Usage:
  python3 tracker_simple.py              # record today's counts
  python3 tracker_simple.py --schedule   # run daily at 20:00 UTC
  python3 tracker_simple.py --chart-only # regenerate chart
"""

import json, urllib.request, csv, os, sys, subprocess, time
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ── Config ───────────────────────────────────────────────────────────────────
API_KEY = os.environ.get("FLIGHTAWARE_API_KEY", "4DXRh6EeuDc7YlFQP1lhWPQmuczYwQI3")
BASE_URL = "https://aeroapi.flightaware.com/aeroapi"
AIRPORTS = {
    "OMDB": "Dubai International (DXB)",
    "OMAA": "Abu Dhabi International (AUH)",
}
BASELINES = {"OMDB": 1200, "OMAA": 500}  # daily scheduled flights, pre-conflict

SCRIPT_DIR = Path(__file__).parent
DATA_CSV = SCRIPT_DIR / "daily_counts.csv"
CHART_FILE = SCRIPT_DIR / "air_traffic_chart.png"

DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK_URL", "")


def log(msg):
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}", flush=True)


# ── API (2 calls total!) ────────────────────────────────────────────────────
def fetch_counts(airport):
    """Single API call → scheduled arrivals + departures for the day."""
    url = f"{BASE_URL}/airports/{airport}/flights/counts"
    req = urllib.request.Request(url, headers={"x-apikey": API_KEY})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
            return {
                "scheduled_arrivals": data.get("scheduled_arrivals", 0),
                "scheduled_departures": data.get("scheduled_departures", 0),
                "departed": data.get("departed", 0),
                "enroute": data.get("enroute", 0),
            }
    except Exception as e:
        log(f"  Error fetching {airport}: {e}")
        return None


def pull_today():
    """Pull counts for both airports. Total: 2 API calls."""
    today = datetime.now(timezone(timedelta(hours=4))).strftime("%Y-%m-%d")  # Dubai time
    log(f"Pulling counts for {today} (Dubai date)...")
    
    results = {}
    for airport in AIRPORTS:
        counts = fetch_counts(airport)
        if counts:
            results[airport] = counts
            log(f"  {AIRPORTS[airport]}: {counts['scheduled_arrivals']} arr / {counts['scheduled_departures']} dep scheduled")
        time.sleep(2)
    
    return today, results


# ── Data ─────────────────────────────────────────────────────────────────────
def load_data():
    data = {}
    if DATA_CSV.exists():
        with open(DATA_CSV) as f:
            for row in csv.DictReader(f):
                data[(row["date"], row["airport"])] = row
    return data


def save_data(data):
    with open(DATA_CSV, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "airport", "scheduled_arrivals", "scheduled_departures",
                         "total_scheduled", "departed", "enroute", "timestamp_utc"])
        for key in sorted(data.keys()):
            row = data[key]
            writer.writerow([
                row["date"], row["airport"],
                row["scheduled_arrivals"], row["scheduled_departures"],
                row["total_scheduled"], row.get("departed", ""),
                row.get("enroute", ""), row.get("timestamp_utc", ""),
            ])


def add_counts(data, date_str, results):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    for airport, counts in results.items():
        total = counts["scheduled_arrivals"] + counts["scheduled_departures"]
        data[(date_str, airport)] = {
            "date": date_str,
            "airport": airport,
            "scheduled_arrivals": counts["scheduled_arrivals"],
            "scheduled_departures": counts["scheduled_departures"],
            "total_scheduled": total,
            "departed": counts["departed"],
            "enroute": counts["enroute"],
            "timestamp_utc": ts,
        }


# ── Chart ────────────────────────────────────────────────────────────────────
def build_chart(data):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 9), facecolor="white")
    fig.suptitle("UAE Airport Activity — Daily Scheduled Flights",
                 fontsize=14, fontweight="bold", x=0.08, ha="left", y=0.97)

    for ax, airport, title in [
        (ax1, "OMDB", "DUBAI INTERNATIONAL AIRPORT (DXB)"),
        (ax2, "OMAA", "ABU DHABI INTERNATIONAL AIRPORT (AUH)"),
    ]:
        # Get data for this airport
        airport_data = {k: v for k, v in data.items() if k[1] == airport}
        if not airport_data:
            ax.set_title(f"{title}\n(No data yet)", fontsize=9, fontweight="bold", loc="left")
            continue

        dates = sorted(set(k[0] for k in airport_data))
        dt_dates = [datetime.strptime(d, "%Y-%m-%d") for d in dates]
        arr = [int(airport_data[(d, airport)]["scheduled_arrivals"]) for d in dates]
        dep = [int(airport_data[(d, airport)]["scheduled_departures"]) for d in dates]

        ax.bar(dt_dates, arr, width=0.8, color="#1a4d3e", label="Scheduled Arrivals", zorder=2)
        ax.bar(dt_dates, dep, width=0.8, bottom=arr, color="#4da688",
               label="Scheduled Departures", zorder=2)
        ax.axhline(y=BASELINES[airport], color="#cc3333", linewidth=1.5,
                   linestyle="--", label="Pre-conflict baseline", zorder=3)

        totals = [a + d for a, d in zip(arr, dep)]
        peak = max(max(totals), BASELINES[airport])
        ymax = int(peak * 1.2 / 100 + 1) * 100

        ax.set_title(title, fontsize=10, fontweight="bold", loc="left", pad=8)
        ax.set_ylim(0, ymax)
        if len(dt_dates) > 1:
            ax.set_xlim(min(dt_dates) - timedelta(hours=12), max(dt_dates) + timedelta(hours=12))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=max(1, len(dates) // 15)))
        ax.tick_params(axis="x", rotation=45, labelsize=8)
        ax.tick_params(axis="y", labelsize=8)
        ax.set_ylabel("Flights", fontsize=9)
        ax.grid(axis="y", alpha=0.3, zorder=0)
        ax.legend(fontsize=7, loc="upper right", framealpha=0.9)

    fig.text(0.08, 0.01,
             "SOURCE: FLIGHTAWARE AEROAPI /counts ENDPOINT. DAILY SCHEDULED FLIGHTS.",
             fontsize=6, style="italic", color="gray")

    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.savefig(CHART_FILE, dpi=150, bbox_inches="tight")
    plt.close()
    log(f"Chart saved")


# ── Discord ──────────────────────────────────────────────────────────────────
def post_to_discord(date_str, results):
    if not DISCORD_WEBHOOK:
        return
    
    lines = [f"**UAE Airport Activity — {date_str}**"]
    for airport, label in AIRPORTS.items():
        if airport in results:
            c = results[airport]
            total = c["scheduled_arrivals"] + c["scheduled_departures"]
            bl = BASELINES[airport]
            pct = total / bl * 100
            emoji = "🟢" if pct > 80 else "🟡" if pct > 50 else "🔴"
            lines.append(f"{emoji} **{label}**: {total:,} scheduled ({pct:.0f}% of baseline)")
    
    summary = "\n".join(lines)
    payload = json.dumps({"content": summary})
    
    if CHART_FILE.exists():
        subprocess.run([
            "curl", "-s", "-X", "POST",
            "-F", f"payload_json={payload}",
            "-F", f"file=@{CHART_FILE}",
            DISCORD_WEBHOOK,
        ], capture_output=True, timeout=30)
    log("Discord posted")


# ── Git ──────────────────────────────────────────────────────────────────────
def git_push():
    try:
        os.chdir(SCRIPT_DIR)
        subprocess.run(["git", "add", "daily_counts.csv", "air_traffic_chart.png"],
                       check=True, capture_output=True)
        result = subprocess.run(["git", "diff", "--staged", "--quiet"], capture_output=True)
        if result.returncode == 0:
            log("No changes to commit")
            return
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        subprocess.run(["git", "commit", "-m", f"📊 Daily counts {date_str}"],
                       check=True, capture_output=True)
        subprocess.run(["git", "push"], check=True, capture_output=True, timeout=30)
        log("Pushed to GitHub")
    except Exception as e:
        log(f"Git: {e}")


# ── Main ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if "--chart-only" in sys.argv:
        data = load_data()
        build_chart(data)
    elif "--schedule" in sys.argv:
        import signal
        signal.signal(signal.SIGINT, lambda *_: sys.exit(0))
        log("Scheduler started — runs daily at 20:00 UTC (midnight Dubai)")
        while True:
            now = datetime.now(timezone.utc)
            target = now.replace(hour=20, minute=0, second=0, microsecond=0)
            if now >= target:
                target += timedelta(days=1)
            wait = (target - now).total_seconds()
            log(f"Next run: {target.strftime('%Y-%m-%d %H:%M UTC')} ({wait/3600:.1f}h)")
            time.sleep(wait)
            
            date_str, results = pull_today()
            if results:
                data = load_data()
                add_counts(data, date_str, results)
                save_data(data)
                build_chart(data)
                post_to_discord(date_str, results)
                git_push()
            log("Done\n")
    else:
        date_str, results = pull_today()
        if results:
            data = load_data()
            add_counts(data, date_str, results)
            save_data(data)
            build_chart(data)
            post_to_discord(date_str, results)
            git_push()
        log("Done")
