#!/usr/bin/env python3
"""
UAE-Iran Strike Tracker — Daily Update Script
===============================================
Reads data.json, appends a new day's figures from CLI args,
recalculates cumulative totals, and writes back.

Usage:
  python3 update.py \
    --date "Mar 9" \
    --bm-detected 10 \
    --bm-intercepted 9 \
    --bm-sea 1 \
    --bm-landed 0 \
    --drones-detected 105 \
    --drones-intercepted 100 \
    --drones-landed 5 \
    --killed 4 \
    --injured 120 \
    --salvo 10

Or shorthand (if all BMs intercepted, none to sea/land):
  python3 update.py --date "Mar 9" --bm 10 --drones 105 --drones-int 100 --killed 4 --injured 120

Notes:
  - --killed and --injured are CUMULATIVE totals (not daily increments)
  - --salvo is the estimated max salvo size (optional, defaults to daily BM count)
  - Cruise missile count only updates if --cruise is provided
  - The script updates both data.json AND the embedded data in index.html
"""

import argparse
import json
import re
import sys
from datetime import datetime


def load_data(path="uae-data.json"):
    with open(path, "r") as f:
        return json.load(f)


def save_data(data, path="uae-data.json"):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"  ✓ Saved {path}")


def update_html_embedded(data, path="uae-missile-monitor.html"):
    """Update the EMBEDDED_DATA block inside index.html."""
    try:
        with open(path, "r") as f:
            html = f.read()
    except FileNotFoundError:
        print(f"  ⚠ {path} not found — skipping HTML update")
        return

    # Find and replace the EMBEDDED_DATA JSON block
    pattern = r"(var EMBEDDED_DATA\s*=\s*)\{.*?\};"
    replacement = f"\\1{json.dumps(data, indent=2)};"

    new_html, count = re.subn(pattern, replacement, html, count=1, flags=re.DOTALL)
    if count == 0:
        print(f"  ⚠ Could not find EMBEDDED_DATA in {path}")
        return

    with open(path, "w") as f:
        f.write(new_html)
    print(f"  ✓ Updated embedded data in {path}")


def main():
    parser = argparse.ArgumentParser(description="Add a day to the UAE strike tracker")

    parser.add_argument("--date", required=True, help="Date label, e.g. 'Mar 9'")

    # Ballistic missiles
    parser.add_argument("--bm", type=int, help="Daily BMs detected (shorthand: assumes all intercepted)")
    parser.add_argument("--bm-detected", type=int, help="Daily BMs detected")
    parser.add_argument("--bm-intercepted", type=int, help="Daily BMs intercepted/destroyed")
    parser.add_argument("--bm-sea", type=int, default=0, help="Daily BMs fell to sea")
    parser.add_argument("--bm-landed", type=int, default=0, help="Daily BMs landed in country")

    # Cumulative BM totals (alternative to daily deltas)
    parser.add_argument("--bm-total", type=int, help="Cumulative BMs detected (overrides calculation)")
    parser.add_argument("--bm-int-total", type=int, help="Cumulative BMs intercepted (overrides)")
    parser.add_argument("--bm-sea-total", type=int, help="Cumulative BMs to sea (overrides)")
    parser.add_argument("--bm-landed-total", type=int, help="Cumulative BMs landed (overrides)")

    # Drones
    parser.add_argument("--drones", type=int, help="Daily drones detected (shorthand)")
    parser.add_argument("--drones-detected", type=int, help="Daily drones detected")
    parser.add_argument("--drones-int", type=int, help="Daily drones intercepted")
    parser.add_argument("--drones-landed", type=int, default=0, help="Daily drones landed")

    # Cumulative drone totals
    parser.add_argument("--drone-total", type=int, help="Cumulative drones detected (overrides)")
    parser.add_argument("--drone-int-total", type=int, help="Cumulative drones intercepted (overrides)")
    parser.add_argument("--drone-landed-total", type=int, help="Cumulative drones landed (overrides)")

    # Other
    parser.add_argument("--cruise", type=int, help="Cumulative cruise missiles (only if changed)")
    parser.add_argument("--killed", type=int, help="Cumulative killed")
    parser.add_argument("--injured", type=int, help="Cumulative injured")
    parser.add_argument("--salvo", type=int, help="Est. max salvo size (defaults to daily BM count)")

    parser.add_argument("--est", action="store_true", help="Mark this day's figures as estimates")
    parser.add_argument("--data", default="uae-data.json", help="Path to data.json")
    parser.add_argument("--html", default="uae-missile-monitor.html", help="Path to index.html")

    args = parser.parse_args()

    # --- Load existing data ---
    data = load_data(args.data)
    bc = data["ballisticCumulative"]
    dc = data["droneCumulative"]

    # --- Resolve daily BM figures ---
    bm_detected = args.bm_detected or args.bm
    if bm_detected is None:
        print("ERROR: Must provide --bm or --bm-detected")
        sys.exit(1)

    bm_intercepted = args.bm_intercepted
    if bm_intercepted is None:
        # If shorthand --bm used, assume all intercepted minus sea/landed
        bm_intercepted = bm_detected - args.bm_sea - args.bm_landed

    # --- Resolve daily drone figures ---
    dr_detected = args.drones_detected or args.drones
    if dr_detected is None:
        print("ERROR: Must provide --drones or --drones-detected")
        sys.exit(1)

    dr_intercepted = args.drones_int
    if dr_intercepted is None:
        dr_intercepted = dr_detected - args.drones_landed

    # --- Date label ---
    date_label = args.date + (" (est)" if args.est else "")
    salvo_label = args.date + " (est)"

    # --- Append daily entries ---
    data["ballistic"].append({"date": date_label, "value": bm_detected})
    data["drones"].append({"date": date_label, "value": dr_detected})

    salvo_val = args.salvo if args.salvo is not None else bm_detected
    data["salvo"].append({
        "date": salvo_label,
        "value": salvo_val,
        "label": f"~{salvo_val} msls/barrage"
    })

    # --- Update cumulative BM ---
    if args.bm_total is not None:
        bc["total"] = args.bm_total
    else:
        bc["total"] += bm_detected

    if args.bm_int_total is not None:
        bc["intercepted"] = args.bm_int_total
    else:
        bc["intercepted"] += bm_intercepted

    if args.bm_sea_total is not None:
        bc["seaFall"] = args.bm_sea_total
    else:
        bc["seaFall"] += args.bm_sea

    if args.bm_landed_total is not None:
        bc["landed"] = args.bm_landed_total
    else:
        bc["landed"] += args.bm_landed

    # --- Update cumulative drones ---
    if args.drone_total is not None:
        dc["total"] = args.drone_total
    else:
        dc["total"] += dr_detected

    if args.drone_int_total is not None:
        dc["intercepted"] = args.drone_int_total
    else:
        dc["intercepted"] += dr_intercepted

    if args.drone_landed_total is not None:
        dc["landed"] = args.drone_landed_total
    else:
        dc["landed"] += args.drones_landed

    # --- Cruise ---
    if args.cruise is not None:
        data["cruise"]["total"] = args.cruise
        data["cruise"]["destroyed"] = args.cruise

    # --- Casualties ---
    if args.killed is not None:
        data["casualties"]["killed"] = args.killed
    if args.injured is not None:
        data["casualties"]["injured"] = args.injured

    # --- Timestamp ---
    data["lastUpdated"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    # --- Save ---
    print(f"\n📊 Adding {date_label}:")
    print(f"   BMs: {bm_detected} detected, {bm_intercepted} intercepted, {args.bm_sea} sea, {args.bm_landed} landed")
    print(f"   Drones: {dr_detected} detected, {dr_intercepted} intercepted, {args.drones_landed} landed")
    print(f"   Salvo est: ~{salvo_val}")
    print(f"   Cumulative BMs: {bc['total']} | Drones: {dc['total']}")
    print(f"   Casualties: {data['casualties']['killed']} killed, {data['casualties']['injured']} injured")
    print()

    save_data(data, args.data)
    update_html_embedded(data, args.html)

    print(f"\n✅ Done. Refresh browser to see changes.\n")


if __name__ == "__main__":
    main()
