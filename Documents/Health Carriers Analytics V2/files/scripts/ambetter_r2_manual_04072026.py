"""
ambetter_r2_manual_04072026.py — One-off R2 recovery script for Ambetter.

Context:
  - Monday 2026-04-06 run was missed.
  - Today is Tuesday 2026-04-07.
  - Window: Friday 2026-04-04 -> Tuesday 2026-04-07.
  - period_start is hard-coded to 2026-04-04 (NOT derived from calculate_period_start
    because today is Tuesday, not a scheduled run day).

What this script does:
  1. Reads cancelled-policy CSVs from data/raw/ambetter/2026-04/2026-04-07/<agent>/
  2. Filters rows where Policy Term Date >= 2026-04-04.
  3. Maps to R2 schema. agent_name always from agents.yaml, never from CSV.
  4. Prints a summary table to stdout.
  5. Appends new rows to data/output/deactivated_members.xlsx (existing rows untouched).

Does NOT:
  - Update any state files.
  - Write to Google Sheets.
  - Touch any browser or portal.
  - Modify any other script.
"""

import logging
import re
import sys
from pathlib import Path

import pandas as pd
import yaml

# ---------------------------------------------------------------------------
# Paths (relative to repo root, resolved from this script's location)
# ---------------------------------------------------------------------------
SCRIPT_DIR  = Path(__file__).parent
REPO_ROOT   = SCRIPT_DIR.parent
RAW_DIR     = REPO_ROOT / "data" / "raw" / "ambetter" / "2026-04" / "2026-04-07"
OUTPUT_FILE = REPO_ROOT / "data" / "output" / "deactivated_members.xlsx"
AGENTS_YAML = REPO_ROOT / "config" / "agents.yaml"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
PERIOD_START = pd.Timestamp("2026-04-04")
RUN_DATE     = "2026-04-07"
CARRIER      = "Ambetter"

# Ambetter CSV column names (confirmed in CLAUDE.md §9)
COL_TERM_DATE  = "Policy Term Date"
COL_FIRST_NAME = "Insured First Name"
COL_LAST_NAME  = "Insured Last Name"
COL_POLICY_NUM = "Policy Number"
COL_STATE      = "State"
COL_DOB        = "Member Date Of Birth"
# Dedup columns (Molina-origin rule — may not exist in Ambetter CSV)
COL_MEMBER_COUNT = "Member_Count"   # Ambetter equivalent: "Number of Members"
COL_MEMBER_COUNT_ALT = "Number of Members"
COL_ADDRESS      = "Address1"       # Not present in Ambetter CSV

# R2 output columns (full schema)
R2_COLUMNS = [
    "run_date", "carrier", "agent_name", "member_name",
    "member_dob", "state", "coverage_end_date", "policy_number",
    "last_status", "detection_method",
]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | ambetter_r2_manual | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalize(name: str) -> str:
    """'Kerry St Germain' -> 'kerry_st_germain'"""
    return re.sub(r"\s+", "_", name.strip().lower())


def load_agent_map(yaml_path: Path) -> dict[str, str]:
    """
    Returns {normalized_folder_name: display_name} for all Ambetter agents.
    e.g. {'brandon_kaplan': 'Brandon Kaplan', 'yusbell': 'Yusbell', ...}
    """
    with open(yaml_path, encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)
    agents = cfg.get("ambetter", [])
    return {_normalize(a["name"]): a["name"] for a in agents}


def read_agent_csvs(agent_dir: Path) -> pd.DataFrame | None:
    """
    Reads all CSVs in agent_dir, concatenates them, and deduplicates by
    Policy Number (handles identical duplicate exports for the same agent).
    Returns None if no CSV found.
    """
    csv_files = list(agent_dir.glob("*.csv")) + list(agent_dir.glob("*.CSV"))
    if not csv_files:
        return None

    frames = []
    for f in csv_files:
        try:
            frames.append(pd.read_csv(f, dtype=str))
        except Exception as exc:
            log.warning("Could not read %s: %s", f, exc)

    if not frames:
        return None

    df = pd.concat(frames, ignore_index=True)
    before = len(df)
    df = df.drop_duplicates(subset=[COL_POLICY_NUM])
    after = len(df)
    if after < before:
        log.info("  Deduplicated %d duplicate rows by Policy Number", before - after)
    return df


def parse_term_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts Policy Term Date to datetime. Rows that cannot be parsed are
    logged as warnings and dropped.
    """
    parsed = pd.to_datetime(df[COL_TERM_DATE], errors="coerce")
    bad_mask = parsed.isna()
    if bad_mask.any():
        for idx in df[bad_mask].index:
            raw_val = df.at[idx, COL_TERM_DATE]
            log.warning(
                "  Row %d: cannot parse Policy Term Date %r — skipping", idx, raw_val
            )
    df = df[~bad_mask].copy()
    df[COL_TERM_DATE] = parsed[~bad_mask]
    return df


def apply_dedup_rule(df: pd.DataFrame, agent_name: str) -> pd.DataFrame:
    """
    Molina-derived dedup: if Member_Count > 1, keep one row per address.
    For Ambetter CSVs: Member_Count column does not exist (Ambetter has
    'Number of Members' and no Address1). Log a warning and skip dedup.
    Dedup by Policy Number is already applied at read time.
    """
    # Check for canonical column name first, then Ambetter equivalent
    if COL_MEMBER_COUNT in df.columns and COL_ADDRESS in df.columns:
        before = len(df)
        multi = df[pd.to_numeric(df[COL_MEMBER_COUNT], errors="coerce").fillna(0) > 1]
        single = df[pd.to_numeric(df[COL_MEMBER_COUNT], errors="coerce").fillna(0) <= 1]
        multi_deduped = multi.drop_duplicates(subset=[COL_ADDRESS])
        df = pd.concat([single, multi_deduped], ignore_index=True)
        log.info("  [%s] Household dedup: %d -> %d rows", agent_name, before, len(df))
    elif COL_MEMBER_COUNT_ALT in df.columns and COL_ADDRESS in df.columns:
        before = len(df)
        multi = df[pd.to_numeric(df[COL_MEMBER_COUNT_ALT], errors="coerce").fillna(0) > 1]
        single = df[pd.to_numeric(df[COL_MEMBER_COUNT_ALT], errors="coerce").fillna(0) <= 1]
        multi_deduped = multi.drop_duplicates(subset=[COL_ADDRESS])
        df = pd.concat([single, multi_deduped], ignore_index=True)
        log.info("  [%s] Household dedup: %d -> %d rows", agent_name, before, len(df))
    else:
        log.info(
            "  [%s] No Member_Count/Address1 columns in Ambetter CSV — "
            "household dedup skipped (Policy Number dedup already applied)",
            agent_name,
        )
    return df


def build_r2_rows(df: pd.DataFrame, agent_name: str) -> list[dict]:
    """Map filtered DataFrame rows to R2 schema dicts."""
    rows = []
    for _, row in df.iterrows():
        term_date = row[COL_TERM_DATE]
        rows.append({
            "run_date":          RUN_DATE,
            "carrier":           CARRIER,
            "agent_name":        agent_name,
            "member_name":       f"{row[COL_FIRST_NAME]} {row[COL_LAST_NAME]}".strip(),
            "member_dob":        row.get(COL_DOB, ""),
            "state":             row.get(COL_STATE, ""),
            "coverage_end_date": term_date.strftime("%Y-%m-%d") if pd.notna(term_date) else "",
            "policy_number":     row.get(COL_POLICY_NUM, ""),
            "last_status":       "Cancelled",
            "detection_method":  "download_filter",
        })
    return rows


def append_to_excel(new_rows: list[dict], output_path: Path) -> None:
    """
    Append-only write to deactivated_members.xlsx.
    Loads existing rows first, concats, saves. Existing rows are never modified.
    New R2 columns (last_status, detection_method) will be NaN for existing rows.
    """
    new_df = pd.DataFrame(new_rows, columns=R2_COLUMNS)

    if output_path.exists():
        existing = pd.read_excel(output_path, dtype=str)
        log.info("Loaded %d existing rows from %s", len(existing), output_path.name)
        combined = pd.concat([existing, new_df], ignore_index=True)
    else:
        log.info("%s does not exist — creating new file", output_path.name)
        combined = new_df

    output_path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_excel(output_path, index=False)
    log.info(
        "Saved %d total rows (%d new) to %s",
        len(combined), len(new_rows), output_path,
    )


def print_summary(all_rows: list[dict]) -> None:
    if not all_rows:
        print("\nNo deactivated members found in window 2026-04-04 -> 2026-04-07.")
        return

    col_widths = {
        "agent":        max(len(r["agent_name"])       for r in all_rows),
        "member":       max(len(r["member_name"])       for r in all_rows),
        "end_date":     max(len(r["coverage_end_date"]) for r in all_rows),
        "state":        max(len(r["state"])              for r in all_rows),
    }
    col_widths = {k: max(v, len(k)) for k, v in col_widths.items()}

    header = (
        f"{'Agent':<{col_widths['agent']}}  "
        f"{'Member Name':<{col_widths['member']}}  "
        f"{'Coverage End':<{col_widths['end_date']}}  "
        f"{'State':<{col_widths['state']}}"
    )
    sep = "-" * len(header)

    print(f"\n{'='*len(header)}")
    print(f"Ambetter R2 — Deactivated Members  |  Window: 2026-04-04 -> 2026-04-07")
    print(f"{'='*len(header)}")
    print(header)
    print(sep)
    for r in all_rows:
        print(
            f"{r['agent_name']:<{col_widths['agent']}}  "
            f"{r['member_name']:<{col_widths['member']}}  "
            f"{r['coverage_end_date']:<{col_widths['end_date']}}  "
            f"{r['state']:<{col_widths['state']}}"
        )
    print(sep)
    print(f"Total: {len(all_rows)} member(s)\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    log.info("=== Ambetter R2 manual recovery — run_date=%s, period_start=2026-04-04 ===",
             RUN_DATE)

    if not RAW_DIR.exists():
        log.error("Raw data directory not found: %s", RAW_DIR)
        sys.exit(1)

    agent_map = load_agent_map(AGENTS_YAML)
    log.info("Loaded %d Ambetter agents from agents.yaml", len(agent_map))

    all_rows: list[dict] = []

    agent_dirs = sorted(d for d in RAW_DIR.iterdir() if d.is_dir())
    if not agent_dirs:
        log.error("No agent subdirectories found in %s", RAW_DIR)
        sys.exit(1)

    for agent_dir in agent_dirs:
        folder_key = agent_dir.name  # e.g. "felipe_ramirez"
        agent_name = agent_map.get(folder_key)

        if agent_name is None:
            log.warning("Folder '%s' has no matching agent in agents.yaml — skipping",
                        folder_key)
            continue

        log.info("[%s] Processing folder: %s", agent_name, agent_dir.name)

        df = read_agent_csvs(agent_dir)
        if df is None:
            log.warning("[%s] No CSV file found in %s — skipping", agent_name, agent_dir)
            continue

        log.info("  Loaded %d rows total", len(df))

        if COL_TERM_DATE not in df.columns:
            log.warning("  '%s' column missing — skipping agent %s", COL_TERM_DATE, agent_name)
            continue

        df = parse_term_dates(df)
        if df.empty:
            log.info("  No rows with parseable dates — skipping")
            continue

        df = apply_dedup_rule(df, agent_name)

        filtered = df[df[COL_TERM_DATE] >= PERIOD_START].copy()
        log.info("  After date filter (>= 2026-04-04): %d row(s)", len(filtered))

        if filtered.empty:
            continue

        rows = build_r2_rows(filtered, agent_name)
        all_rows.extend(rows)

    log.info("--- Total new R2 rows: %d ---", len(all_rows))

    print_summary(all_rows)

    if all_rows:
        append_to_excel(all_rows, OUTPUT_FILE)
    else:
        log.info("No rows to append — deactivated_members.xlsx unchanged.")


if __name__ == "__main__":
    main()
