"""
scripts/verify_output.py — R2 output integrity checker

Reads data/output/deactivated_members.xlsx and validates:
  1. No duplicate (carrier, policy_number, coverage_end_date) — EXIT 1 if found
  2. No null policy_number   — WARN, count
  3. No null last_status     — WARN, count
  4. No null detection_method — WARN, count
  5. No null member_name     — WARN, count
  6. Per-carrier summary: carrier | total_rows | runs | earliest_run_date | latest_run_date
  7. Print overall row count. Exit 0 = clean, 1 = duplicate-key integrity failure.
     Warnings do not affect exit code.

Usage:
  python scripts/verify_output.py
  python scripts/verify_output.py --carrier Oscar
  python scripts/verify_output.py --fix-nulls
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
OUTPUT_PATH = ROOT / "data" / "output" / "deactivated_members.xlsx"

# Hardcoded per-carrier defaults for --fix-nulls backfill
CARRIER_DEFAULTS: dict[str, dict[str, str]] = {
    "Oscar":    {"last_status": "Inactive",    "detection_method": "file_extract"},
    "Molina":   {"last_status": "Terminated",  "detection_method": "file_extract"},
    "Ambetter": {"last_status": "Cancelled",   "detection_method": "download_filter"},
    "Cigna":    {"last_status": "Terminated",  "detection_method": "portal_export"},
    "United":   {"last_status": "Terminated",  "detection_method": "file_extract"},
}


def _load(carrier_filter: str | None) -> pd.DataFrame:
    if not OUTPUT_PATH.exists():
        print(f"ERROR: {OUTPUT_PATH} does not exist.", file=sys.stderr)
        sys.exit(1)

    df = pd.read_excel(OUTPUT_PATH, engine="openpyxl", dtype=str)

    if carrier_filter:
        df = df[df["carrier"].str.strip().str.lower() == carrier_filter.lower()]
        if df.empty:
            print(f"No rows found for carrier '{carrier_filter}'.")
            sys.exit(0)

    return df


def _is_null(series: pd.Series) -> pd.Series:
    """True where value is NaN, 'nan', 'None', or empty string after strip."""
    return series.isna() | series.astype(str).str.strip().isin(["", "nan", "None"])


def run_checks(df: pd.DataFrame) -> int:
    """Run all assertions. Returns exit code (0 = clean, 1 = integrity failure)."""
    exit_code = 0
    print(f"\nTotal rows: {len(df)}")

    # ── Check 1 — Duplicate dedup key ────────────────────────────────────────
    dup_mask = df.duplicated(subset=["carrier", "policy_number", "coverage_end_date"], keep=False)
    dup_count = dup_mask.sum()
    if dup_count > 0:
        exit_code = 1
        print(f"\n[FAIL] Duplicate (carrier, policy_number, coverage_end_date): {dup_count} rows affected")
        dup_sample = (
            df[dup_mask][["carrier", "policy_number", "coverage_end_date", "agent_name"]]
            .drop_duplicates()
            .head(10)
        )
        print(dup_sample.to_string(index=False))
    else:
        print("[OK]   No duplicate dedup keys")

    # ── Check 2–5 — Null field warnings ──────────────────────────────────────
    null_checks = [
        ("policy_number",    "policy_number"),
        ("last_status",      "last_status"),
        ("detection_method", "detection_method"),
        ("member_name",      "member_name"),
    ]

    for col, label in null_checks:
        if col not in df.columns:
            print(f"[WARN] Column '{col}' not present in file")
            continue
        null_count = _is_null(df[col]).sum()
        if null_count > 0:
            print(f"[WARN] Null {label}: {null_count} rows")
        else:
            print(f"[OK]   No null {label}")

    # ── Check 6 — Per-carrier summary ─────────────────────────────────────────
    print("\nPer-carrier summary:")
    print(f"  {'carrier':<12} {'total_rows':>10} {'runs':>6} {'earliest_run_date':>18} {'latest_run_date':>16}")
    print("  " + "-" * 64)

    for carrier, grp in df.groupby("carrier", sort=True):
        total = len(grp)
        run_dates = grp["run_date"].dropna() if "run_date" in grp.columns else pd.Series(dtype=str)
        runs = run_dates.nunique()
        earliest = run_dates.min() if not run_dates.empty else "—"
        latest   = run_dates.max() if not run_dates.empty else "—"
        print(f"  {carrier:<12} {total:>10} {runs:>6} {str(earliest):>18} {str(latest):>16}")

    return exit_code


def fix_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """
    Backfill last_status and detection_method for rows where they are null,
    using the hardcoded per-carrier constants.
    Safe to run once on the production file to clean up legacy null rows.
    """
    if "last_status" not in df.columns:
        df["last_status"] = None
    if "detection_method" not in df.columns:
        df["detection_method"] = None

    patched = 0
    for carrier, defaults in CARRIER_DEFAULTS.items():
        mask_carrier = df["carrier"].str.strip() == carrier
        for field, value in defaults.items():
            null_mask = mask_carrier & _is_null(df[field])
            count = null_mask.sum()
            if count > 0:
                df.loc[null_mask, field] = value
                patched += count
                print(f"  patched {count} '{field}' rows for {carrier} → '{value}'")

    if patched == 0:
        print("  No null rows to patch — file already clean.")

    return df


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Verify data/output/deactivated_members.xlsx integrity",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  python scripts/verify_output.py\n"
            "  python scripts/verify_output.py --carrier Oscar\n"
            "  python scripts/verify_output.py --fix-nulls\n"
        ),
    )
    parser.add_argument(
        "--carrier", default=None,
        help="Filter to a single carrier (e.g. Oscar, Molina, Ambetter, Cigna, United)",
    )
    parser.add_argument(
        "--fix-nulls", action="store_true",
        help=(
            "Backfill last_status and detection_method for existing rows using "
            "hardcoded per-carrier constants. Writes the patched file in-place."
        ),
    )
    args = parser.parse_args()

    df = _load(args.carrier)

    if args.fix_nulls:
        print(f"\n[--fix-nulls] Backfilling null last_status / detection_method...")
        df = fix_nulls(df)
        with pd.ExcelWriter(OUTPUT_PATH, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Deactivated Members")
        print(f"  File saved → {OUTPUT_PATH}")

    exit_code = run_checks(df)

    if exit_code == 0:
        print("\nResult: CLEAN (exit 0)")
    else:
        print("\nResult: INTEGRITY FAILURE — duplicate dedup key found (exit 1)")

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
