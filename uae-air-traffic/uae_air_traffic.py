#!/usr/bin/env python3
"""
UAE Air Traffic Tracker
Pulls daily flight counts for Dubai (OMDB) & Abu Dhabi (OMAA),
computes 7-day moving totals, generates chart, posts to Discord.

Reads secrets from environment variables:
  FLIGHTAWARE_API_KEY
  DISCORD_WEBHOOK_URL

Usage:
  python3 uae_air_traffic.py                  # pull yesterday + chart + Discord
  python3 uae_air_traffic.py 2026-03-19       # pull a specific date
  python3 uae_air_traffic.py --chart-only     # regenerate chart + Discord (no API call)
"""

import json, urllib.request, time, csv, os, sys, subprocess
from datetime import datetime, timedelta, timezone

# ── Config (secrets from env vars) ──────────────────────────────────────────
API_KEY         = os.environ.get("FLIGHTAWARE_API_KEY", "")
DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK_URL", "")
BASE_URL        = "https://aeroapi.flightaware.com/aeroapi"
AIRPORTS        = ["OMDB", "OMAA"]
AIRPORT_NAMES   = {"OMDB": "Dubai (DXB)", "OMAA": "Abu Dhabi (AUH)"}
DIRECTIONS      = ["arrivals", "departures"]
API_DELAY       = 10  # generous spacing — this only runs once/day
BASELINES       = {"OMDB": 850, "OMAA": 150}

# Paths (relative to script location)
SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
BACKFILL_CSV = os.path.join(SCRIPT_DIR, "backfill_data.csv")
DAILY_CSV    = os.path.join(SCRIPT_DIR, "daily_counts.csv")
MERGED_CSV   = os.path.join(SCRIPT_DIR, "merged_7d.csv")
CHART_FILE   = os.path.join(SCRIPT_DIR, "air_traffic_chart.png")


def log(msg, **kwargs):
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}", **kwargs, flush=True)


# ── API Layer ───────────────────────────────────────────────────────────────
def fetch_count(airport, direction, start_iso, end_iso):
    """Fetch total flight count for one airport/direction/day."""
    MAX_RETRIES = 3
    all_flights = []
    url = f"{BASE_URL}/airports/{airport}/flights/{direction}?start={start_iso}&end={end_iso}&max_pages=20"
    pages_used = 0
    retries = 0

    while url:
        if url.startswith("/"):
            url = BASE_URL + url
        req = urllib.request.Request(url, headers={"x-apikey": API_KEY})
        try:
            with urllib.request.urlopen(req) as resp:
                data = json.loads(resp.read())
                flights = data.get(direction, [])
                all_flights.extend(flights)
                pages_used += max(1, len(flights) // 15 + (1 if len(flights) % 15 else 0))
                next_link = (data.get("links") or {}).get("next")
                if next_link:
                    url = next_link
                    time.sleep(API_DELAY)
                else:
                    url = None
                retries = 0  # reset on success
        except urllib.error.HTTPError as e:
            if e.code == 429:
                retries += 1
                body = e.read().decode()[:200]
                if retries >= MAX_RETRIES:
                    log(f"    Rate limited {MAX_RETRIES}x, giving up ({body})")
                    return len(all_flights), pages_used
                log(f"    Rate limited (attempt {retries}/{MAX_RETRIES}), waiting 60s...")
                time.sleep(60)
                continue
            else:
                log(f"    HTTP Error {e.code}")
                return len(all_flights), pages_used
    return len(all_flights), pages_used


def pull_day(date_str):
    """Pull arrivals + departures for both airports."""
    start = f"{date_str}T00:00:00Z"
    end_dt = datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=1)
    end = end_dt.strftime("%Y-%m-%dT00:00:00Z")

    results = {}
    total_pages = 0
    for airport in AIRPORTS:
        results[airport] = {}
        for direction in DIRECTIONS:
            log(f"  {airport} {direction}...", end=" ")
            count, pages = fetch_count(airport, direction, start, end)
            results[airport][direction] = count
            total_pages += pages
            print(f"{count} ({pages} pg)")
            time.sleep(API_DELAY)
    return results, total_pages


# ── Data Layer ──────────────────────────────────────────────────────────────
def append_to_daily_csv(date_str, results):
    """Append raw daily counts. Skips if date+airport already present."""
    existing = set()
    if os.path.exists(DAILY_CSV):
        with open(DAILY_CSV) as f:
            for row in csv.DictReader(f):
                existing.add((row["date"], row["airport"]))

    write_header = not os.path.exists(DAILY_CSV)
    with open(DAILY_CSV, "a", newline="") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(["date", "airport", "arrivals", "departures", "total", "source"])
        for airport in AIRPORTS:
            if (date_str, airport) in existing:
                log(f"  {airport} {date_str} already exists, skipping")
                continue
            arr = results[airport]["arrivals"]
            dep = results[airport]["departures"]
            writer.writerow([date_str, airport, arr, dep, arr + dep, "api"])


def compute_7d_totals():
    """Merge backfill (already 7d totals) with daily API data (compute rolling 7d sum)."""
    backfill = {}
    if os.path.exists(BACKFILL_CSV):
        with open(BACKFILL_CSV) as f:
            for row in csv.DictReader(f):
                backfill[(row["date"], row["airport"])] = {
                    "arrivals_7d": int(row["arrivals_7d"]),
                    "departures_7d": int(row["departures_7d"]),
                }

    daily = {}
    if os.path.exists(DAILY_CSV):
        with open(DAILY_CSV) as f:
            for row in csv.DictReader(f):
                daily[(row["date"], row["airport"])] = {
                    "arrivals": int(row["arrivals"]),
                    "departures": int(row["departures"]),
                }

    all_dates = sorted(set(d for d, _ in list(backfill.keys()) + list(daily.keys())))

    with open(MERGED_CSV, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "airport", "arrivals_7d", "departures_7d", "total_7d", "source"])
        for airport in AIRPORTS:
            for date_str in all_dates:
                key = (date_str, airport)
                if key in backfill:
                    d = backfill[key]
                    writer.writerow([date_str, airport, d["arrivals_7d"], d["departures_7d"],
                                     d["arrivals_7d"] + d["departures_7d"], "chart_extract"])
                elif key in daily:
                    dt = datetime.strptime(date_str, "%Y-%m-%d")
                    arr_7d, dep_7d = 0, 0
                    for i in range(7):
                        lb = (dt - timedelta(days=i)).strftime("%Y-%m-%d")
                        lb_key = (lb, airport)
                        if lb_key in daily:
                            arr_7d += daily[lb_key]["arrivals"]
                            dep_7d += daily[lb_key]["departures"]
                    writer.writerow([date_str, airport, arr_7d, dep_7d, arr_7d + dep_7d, "api_7d"])
    log(f"Merged 7d totals written")


# ── Chart ───────────────────────────────────────────────────────────────────
def build_chart():
    """Generate the two-panel stacked bar chart."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates

    data = {"OMDB": {}, "OMAA": {}}
    with open(MERGED_CSV) as f:
        for row in csv.DictReader(f):
            dt = datetime.strptime(row["date"], "%Y-%m-%d")
            data[row["airport"]][dt] = {
                "arrivals": int(row["arrivals_7d"]),
                "departures": int(row["departures_7d"]),
            }

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 10), facecolor="white")
    fig.suptitle("Air Traffic Is Rebounding", fontsize=16, fontweight="bold",
                 x=0.12, ha="left", y=0.97)

    for ax, airport, title, ymax in [
        (ax1, "OMDB", "FLIGHTS THROUGH\nDUBAI INTERNATIONAL AIRPORT*", 1600),
        (ax2, "OMAA", "FLIGHTS THROUGH\nABU DHABI INTERNATIONAL AIRPORT*", 700),
    ]:
        dates = sorted(data[airport].keys())
        arrivals = [data[airport][d]["arrivals"] for d in dates]
        departures = [data[airport][d]["departures"] for d in dates]

        ax.bar(dates, arrivals, width=0.8, color="#1a4d3e", label="ARRIVALS", zorder=2)
        ax.bar(dates, departures, width=0.8, bottom=arrivals, color="#4da688",
               label="DEPARTURES", zorder=2)
        ax.axhline(y=BASELINES[airport], color="#cc3333", linewidth=1.5,
                   linestyle="--", label="PRE-CONFLICT BASELINE", zorder=3)

        ax.set_title(title, fontsize=9, fontweight="bold", loc="left", pad=8)
        ax.set_ylim(0, ymax)
        ax.set_xlim(min(dates) - timedelta(hours=12), max(dates) + timedelta(hours=12))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%d"))
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=2))
        ax.yaxis.set_major_locator(plt.MultipleLocator(400 if ymax > 1000 else 200))
        ax.tick_params(axis="both", labelsize=8)
        ax.set_ylabel("#", fontsize=9)
        ax.grid(axis="y", alpha=0.3, zorder=0)
        ax.legend(fontsize=7, loc="upper right", framealpha=0.9)

    # Dynamic x-axis label
    all_dates = sorted(set(
        list(data["OMDB"].keys()) + list(data["OMAA"].keys())
    ))
    if all_dates:
        first_month = all_dates[0].strftime("%Y %b")
        last_month = all_dates[-1].strftime("%Y %b")
        ax2.set_xlabel(
            f"{first_month}                                                    {last_month}",
            fontsize=8,
        )

    fig.text(0.12, 0.01,
             "* SHOWN AS A 7-DAY MOVING TOTAL.  SOURCE: FLIGHTAWARE.",
             fontsize=6, style="italic", color="gray")

    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.savefig(CHART_FILE, dpi=150, bbox_inches="tight")
    plt.close()
    log(f"Chart saved")


# ── Discord ─────────────────────────────────────────────────────────────────
def post_to_discord(date_str=None):
    """Send chart image + summary to Discord via curl."""
    if not DISCORD_WEBHOOK:
        log("No DISCORD_WEBHOOK_URL set, skipping post")
        return False

    summary_lines = []
    if date_str:
        summary_lines.append(f"**UAE Air Traffic Update — {date_str}**")
    else:
        summary_lines.append("**UAE Air Traffic Update**")

    latest = {}
    if os.path.exists(MERGED_CSV):
        with open(MERGED_CSV) as f:
            for row in csv.DictReader(f):
                ap = row["airport"]
                if ap not in latest or row["date"] > latest[ap]["date"]:
                    latest[ap] = row

    for ap in AIRPORTS:
        if ap in latest:
            r = latest[ap]
            total = int(r["total_7d"])
            bl = BASELINES[ap]
            pct = (total / bl * 100) if bl else 0
            arrow = "📈" if total > bl * 0.5 else "📉"
            summary_lines.append(
                f"{arrow} **{AIRPORT_NAMES[ap]}**: {total:,} flights (7d) — "
                f"{pct:.0f}% of pre-conflict baseline ({bl:,})"
            )

    summary = "\n".join(summary_lines)
    payload = json.dumps({"content": summary})

    result = subprocess.run(
        [
            "curl", "-s", "-w", "%{http_code}", "-X", "POST",
            "-F", f"payload_json={payload}",
            "-F", f"file=@{CHART_FILE}",
            DISCORD_WEBHOOK,
        ],
        capture_output=True, text=True, timeout=30,
    )

    http_code = result.stdout.strip()[-3:]
    if http_code in ("200", "204"):
        log(f"Discord: posted (HTTP {http_code})")
        return True
    else:
        log(f"Discord error: {result.stdout[:200]}")
        return False


# ── Main ────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    chart_only = "--chart-only" in sys.argv
    target_date = None

    for arg in sys.argv[1:]:
        if arg.startswith("20") and len(arg) == 10:
            target_date = arg

    if not target_date:
        target_date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

    log(f"=== UAE Air Traffic Tracker ===")

    if not chart_only:
        if not API_KEY:
            log("ERROR: FLIGHTAWARE_API_KEY not set")
            sys.exit(1)

        # Health check — verify API is responsive before starting
        log("API health check...", end=" ")
        try:
            hc_req = urllib.request.Request(
                f"{BASE_URL}/airports/OMDB/flights/counts",
                headers={"x-apikey": API_KEY},
            )
            with urllib.request.urlopen(hc_req, timeout=15) as resp:
                print("OK")
        except Exception as e:
            log(f"FAILED ({e}). Will still generate chart from existing data.")
            chart_only = True

    if not chart_only:
        log(f"Pulling data for {target_date}")
        results, pages = pull_day(target_date)
        # Only save if we got at least some data
        got_data = any(
            results[ap].get("arrivals", 0) + results[ap].get("departures", 0) > 0
            for ap in AIRPORTS
        )
        if got_data:
            append_to_daily_csv(target_date, results)
            log(f"API pages used: {pages}")
            for ap in AIRPORTS:
                arr = results[ap]["arrivals"]
                dep = results[ap]["departures"]
                log(f"  {AIRPORT_NAMES[ap]}: {arr} arr + {dep} dep = {arr + dep}")
        else:
            log("No data returned (API may be rate limited). Chart will use existing data.")
    else:
        log("Chart-only mode (no API pull)")

    compute_7d_totals()
    build_chart()
    post_to_discord(target_date)
    log("=== Done ===")
