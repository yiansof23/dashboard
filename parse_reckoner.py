#!/usr/bin/env python3
"""
parse_reckoner.py
─────────────────
Extracts monthly metrics from CareEdge Rating Reckoner PDFs and produces
a JavaScript object ready to paste into careratings-dashboard.jsx.

Usage:
    python parse_reckoner.py --pdf current.pdf --prev previous.pdf --month "Apr-26"

Requirements:
    pip install pdfplumber pandas

Source PDFs:
    https://www.careratings.com/public/admin/pdf/{Month}_{Year}_{timestamp}.pdf

Known URLs:
    Oct 2024: https://www.careratings.com/public/admin/pdf/October_2024_1732280549.pdf
    Jan 2025: https://www.careratings.com/public/admin/pdf/January_2025_1739592582.pdf
"""

import argparse
import json
import re
import sys
from pathlib import Path

try:
    import pdfplumber
    import pandas as pd
except ImportError:
    print("Missing dependencies. Run:  pip install pdfplumber pandas")
    sys.exit(1)


# ── Rating category normalisation ──────────────────────────────────────────

RATING_CATEGORY_MAP = {
    "AAA": "AAA",
    "AA+": "AA", "AA": "AA", "AA-": "AA",
    "A+":  "A",  "A":  "A",  "A-":  "A",
    "BBB+":"BBB","BBB":"BBB","BBB-":"BBB",
    "BB+": "BB", "BB": "BB", "BB-": "BB",
    "B+":  "B",  "B":  "B",  "B-":  "B",
    "C+":  "C",  "C":  "C",  "C-":  "C",
    "D":   "D",
}

RATING_RE = re.compile(
    r"CARE\s*(PP-MLD\s*)?(A1\+?|A2\+?|A3|A4|"
    r"AAA|AA[\+\-]?|A[\+\-]?|BBB[\+\-]?|BB[\+\-]?|B[\+\-]?|C[\+\-]?|D)"
    r"(?:\s*[\;\(].*)?",
    re.IGNORECASE,
)


def parse_rating(raw: str) -> str | None:
    """Extract the base rating grade from a raw rating string."""
    if not raw or not isinstance(raw, str):
        return None
    m = RATING_RE.search(raw.strip())
    if not m:
        return None
    grade = m.group(2).upper().replace(" ", "")
    # Short-term ratings (A1+, A2+ etc.) → skip for category distribution
    if re.match(r"A[1-4]", grade):
        return None
    return RATING_CATEGORY_MAP.get(grade)


# ── PDF extraction ──────────────────────────────────────────────────────────

def extract_rows(pdf_path: str) -> pd.DataFrame:
    """
    Extract all instrument rows from a Rating Reckoner PDF.
    Returns a DataFrame with columns:
        issuer, sub_type, instrument, amount_mn, rating, industry, basic_industry
    """
    rows = []
    header_pattern = re.compile(
        r"name\s+of\s+issuer|issuer\s+name", re.IGNORECASE
    )

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            if not table:
                continue

            # Find header row
            header_idx = None
            for i, row in enumerate(table):
                if row and any(
                    header_pattern.search(str(c)) for c in row if c
                ):
                    header_idx = i
                    break

            if header_idx is None:
                continue

            for row in table[header_idx + 1:]:
                if not row or len(row) < 5:
                    continue
                # Clean cells
                cells = [str(c).strip().replace("\n", " ") if c else "" for c in row]
                # Skip blank or header-repeat rows
                if not cells[0] or header_pattern.search(cells[0]):
                    continue
                try:
                    amount = float(re.sub(r"[^\d\.]", "", cells[3])) if cells[3] else 0.0
                except ValueError:
                    amount = 0.0

                rows.append({
                    "issuer":         cells[0],
                    "sub_type":       cells[1],
                    "instrument":     cells[2],
                    "amount_mn":      amount,
                    "rating":         cells[4] if len(cells) > 4 else "",
                    "industry":       cells[5] if len(cells) > 5 else "",
                    "basic_industry": cells[6] if len(cells) > 6 else "",
                })

    df = pd.DataFrame(rows)
    df["rating_category"] = df["rating"].apply(parse_rating)
    return df


# ── Comparison helpers ──────────────────────────────────────────────────────

def make_key(row) -> str:
    """Unique key for an instrument: issuer + instrument type."""
    return f"{row['issuer'].lower().strip()}|{row['instrument'].lower().strip()}"


def compute_transitions(
    curr: pd.DataFrame, prev: pd.DataFrame
) -> tuple[int, int]:
    """
    Count upgrades and downgrades by comparing rating categories
    between current and previous month for matched instruments.
    """
    GRADE_ORDER = ["AAA", "AA", "A", "BBB", "BB", "B", "C", "D"]
    grade_rank = {g: i for i, g in enumerate(GRADE_ORDER)}

    curr_map = {make_key(r): r["rating_category"] for _, r in curr.iterrows()}
    prev_map = {make_key(r): r["rating_category"] for _, r in prev.iterrows()}

    upgrades = 0
    downgrades = 0

    for key, curr_grade in curr_map.items():
        prev_grade = prev_map.get(key)
        if not prev_grade or not curr_grade:
            continue
        cr = grade_rank.get(curr_grade)
        pr = grade_rank.get(prev_grade)
        if cr is None or pr is None:
            continue
        if cr < pr:     # lower index = better grade
            upgrades += 1
        elif cr > pr:
            downgrades += 1

    return upgrades, downgrades


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Parse CareEdge Rating Reckoner PDFs into dashboard-ready JSON."
    )
    parser.add_argument("--pdf",   required=True, help="Path to current month's PDF")
    parser.add_argument("--prev",  required=False, help="Path to previous month's PDF (for transitions)")
    parser.add_argument("--month", required=True, help='Month label, e.g. "Apr-26"')
    args = parser.parse_args()

    if not Path(args.pdf).exists():
        print(f"Error: PDF not found: {args.pdf}")
        sys.exit(1)

    print(f"Parsing {args.pdf} …")
    curr = extract_rows(args.pdf)
    print(f"  → {len(curr)} instrument rows extracted")

    # Outstanding = total row count
    outstanding = len(curr)

    # Total debt rated (₹ Mn → keep as ₹ Mn, same units as dashboard)
    debt_rated = int(curr["amount_mn"].sum())

    # Rating distribution (% of rows with a classifiable LT category)
    lt = curr[curr["rating_category"].notna()]
    dist_counts = lt["rating_category"].value_counts()
    total_lt = len(lt)
    categories = ["AAA", "AA", "A", "BBB", "BB", "B", "C", "D"]
    dist = {
        c: round(dist_counts.get(c, 0) / total_lt * 100) if total_lt > 0 else 0
        for c in categories
    }
    # Ensure sum = 100 (adjust largest bucket for rounding)
    diff = 100 - sum(dist.values())
    if diff != 0:
        largest = max(dist, key=dist.get)
        dist[largest] += diff

    # Transitions (requires previous month)
    new_ratings = 0
    withdrawn   = 0
    upgrades    = 0
    downgrades  = 0

    if args.prev:
        if not Path(args.prev).exists():
            print(f"Warning: previous PDF not found: {args.prev} — transitions will be 0")
        else:
            print(f"Parsing {args.prev} for comparison …")
            prev = extract_rows(args.prev)
            curr_keys = set(make_key(r) for _, r in curr.iterrows())
            prev_keys = set(make_key(r) for _, r in prev.iterrows())
            new_ratings = len(curr_keys - prev_keys)
            withdrawn   = len(prev_keys - curr_keys)
            upgrades, downgrades = compute_transitions(curr, prev)
            print(f"  New: {new_ratings}  Withdrawn: {withdrawn}  Upgrades: {upgrades}  Downgrades: {downgrades}")
    else:
        print("No --prev file supplied; new/withdrawn/upgrades/downgrades will be 0.")

    # Build output object
    result = {
        "month":       args.month,
        "outstanding": outstanding,
        "newRatings":  new_ratings,
        "withdrawn":   withdrawn,
        "debtRated":   debt_rated,
        "upgrades":    upgrades,
        "downgrades":  downgrades,
        **dist,
    }

    # Pretty-print as JS object literal (for direct paste)
    fields = (
        f'month: "{result["month"]}", '
        f'outstanding: {result["outstanding"]}, '
        f'newRatings: {result["newRatings"]}, '
        f'withdrawn: {result["withdrawn"]}, '
        f'debtRated: {result["debtRated"]}, '
        f'upgrades: {result["upgrades"]}, '
        f'downgrades: {result["downgrades"]}, '
        + ", ".join(f'{c}: {result[c]}' for c in categories)
    )

    print("\n─── PASTE THIS LINE INTO monthlyData in careratings-dashboard.jsx ───\n")
    print(f"  {{ {fields} }},")
    print("\n─── JSON (for programmatic use) ───\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
