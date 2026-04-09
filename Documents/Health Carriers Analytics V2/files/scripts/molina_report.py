"""
molina_report.py — v2.1

Changes from v2.0:
  - R2 now returns member_first_name, member_last_name, dob, state
    instead of a combined member_name string.
  - calculate_period_start() replaces the "dump all history on first run"
    fallback. If no state file exists, it calculates the previous scheduled
    run date from the calendar (Mon run → previous Friday; Fri run → previous
    Monday). This means R2 is always scoped to "this period" from day one.

Column map (all confirmed from actual CSV):
  Status / End_Date / Subscriber_ID / dob / State
"""

import sys
import logging
from pathlib import Path
from datetime import date, timedelta

import pandas as pd
import yaml

# ─── Config ───────────────────────────────────────────────────────────────────
_CONFIG_PATH = Path(__file__).parent.parent / "config" / "config.yaml"
with open(_CONFIG_PATH) as _f:
    _CFG = yaml.safe_load(_f)

_MCFG = _CFG["carriers"]["molina"]
COL = _MCFG["columns"]
ACTIVE_STATUSES = _MCFG["active_statuses"]
TERMINATED_STATUS = _MCFG["terminated_status"]

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Period start calculation
# ─────────────────────────────────────────────────────────────────────────────

def calculate_period_start(today: date = None) -> date:
    if today is None:
        today = date.today()
    return today.replace(day=1) - timedelta(days=1)


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _load_csv(csv_path: Path) -> pd.DataFrame:
    """
    Load CSV and validate required columns are present.
    Raises ValueError immediately if anything critical is missing.
    """
    df = pd.read_csv(csv_path, dtype=str, encoding="windows-1252")
    df.columns = df.columns.str.strip()
    logger.info(f"Molina CSV columns: {list(df.columns)}")

    required = [
        COL["status"], COL["member_count"], COL["address1"],
        COL["broker_first"], COL["broker_last"],
        COL["member_first"], COL["member_last"],
        COL["end_date"], COL["subscriber_id"],
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"Molina CSV is missing expected columns: {missing}\n"
            f"Actual columns found: {list(df.columns)}\n"
            "Check config/config.yaml → carriers.molina.columns"
        )

    df[COL["member_count"]] = (
        pd.to_numeric(df[COL["member_count"]], errors="coerce").fillna(1).astype(int)
    )
    return df


def _dedup_address(df: pd.DataFrame) -> pd.DataFrame:
    """
    Household dedup: when Member_Count > 1, rows sharing Address1 per agent
    represent the same household. Keep first row only to avoid overcounting.
    Rows with Member_Count == 1 are untouched.
    """
    df["_agent_key"] = (
        df[COL["broker_first"]].str.strip() + "|" + df[COL["broker_last"]].str.strip()
    )
    single = df[df[COL["member_count"]] == 1].copy()
    multi  = df[df[COL["member_count"]] > 1].copy()
    multi_deduped = multi.drop_duplicates(
        subset=["_agent_key", COL["address1"]], keep="first"
    )
    result = pd.concat([single, multi_deduped], ignore_index=True)
    return result.drop(columns=["_agent_key"])


def _agent_name(row: pd.Series) -> str:
    return f"{str(row[COL['broker_first']]).strip()} {str(row[COL['broker_last']]).strip()}"


def _safe_date(raw) -> str | None:
    """Parse a date value to YYYY-MM-DD string, or None if unparseable."""
    try:
        return pd.to_datetime(raw).strftime("%Y-%m-%d") if pd.notna(raw) else None
    except Exception:
        return str(raw).strip() if pd.notna(raw) else None


# ─────────────────────────────────────────────────────────────────────────────
# R1 — Active Members
# ─────────────────────────────────────────────────────────────────────────────

def _build_r1_records(df: pd.DataFrame, run_date: str, run_type: str) -> list[dict]:
    """Returns one R1 dict per agent with their total active member count."""
    active_df = df[df[COL["status"]].isin(ACTIVE_STATUSES)].copy()
    active_df = _dedup_address(active_df)
    active_df["_agent"] = (
        active_df[COL["broker_first"]].str.strip()
        + " "
        + active_df[COL["broker_last"]].str.strip()
    )
    summary = (
        active_df.groupby("_agent")[COL["member_count"]]
        .sum()
        .reset_index()
        .sort_values(COL["member_count"], ascending=False)
    )
    records = []
    for _, row in summary.iterrows():
        records.append({
            "run_date":         run_date,
            "run_type":         run_type,
            "carrier":          "Molina",
            "agent_name":       row["_agent"],
            "active_members":   int(row[COL["member_count"]]),
            "status":           "success",
            "error_message":    None,
            "duration_seconds": None,   # stamped by molina_downloader.py
        })
    logger.info(
        f"R1 built: {len(records)} agents, "
        f"{sum(r['active_members'] for r in records)} total active members"
    )
    return records


# ─────────────────────────────────────────────────────────────────────────────
# R2 — Deactivated This Period
# ─────────────────────────────────────────────────────────────────────────────

def _build_r2_records(
    df: pd.DataFrame, run_date: str, period_start: str
) -> list[dict]:
    """
    Returns one R2 dict per member terminated since period_start.

    Fields returned (updated from v2.0):
      member_first_name, member_last_name, agent_name, dob, state,
      coverage_end_date, member_id, last_status, detection_method,
      run_date, carrier

    period_start is always a real date — either from the state file
    (subsequent runs) or from calculate_period_start() (first run).
    There is no "dump all history" fallback anymore.
    """
    term_df = df[df[COL["status"]] == TERMINATED_STATUS].copy()

    # Parse end dates for filtering
    term_df["_end_dt"] = pd.to_datetime(term_df[COL["end_date"]], errors="coerce")
    cutoff = pd.to_datetime(period_start)
    original_count = len(term_df)
    term_df = term_df[term_df["_end_dt"] >= cutoff]

    logger.info(
        f"R2 date filter: {original_count} Terminated rows → "
        f"{len(term_df)} with End_Date >= {period_start}"
    )

    records = []
    for _, row in term_df.iterrows():
        sub_id = row.get(COL["subscriber_id"])
        member_id = str(sub_id).strip() if pd.notna(sub_id) and str(sub_id).strip() else None

        dob_raw = row.get(COL["dob"])
        dob_str = _safe_date(dob_raw)

        state_raw = row.get(COL["state"])
        state_str = str(state_raw).strip() if pd.notna(state_raw) else None

        records.append({
            "run_date":          run_date,
            "carrier":           "Molina",
            "agent_name":        _agent_name(row),
            "member_name":       f"{str(row[COL['member_first']]).strip()} {str(row[COL['member_last']]).strip()}",
            "member_dob":        dob_str,
            "state":             state_str,
            "coverage_end_date": _safe_date(row[COL["end_date"]]),
            "policy_number":     member_id,
            "last_status":       TERMINATED_STATUS,
            "detection_method":  "file_extract",
        })

    logger.info(f"R2 built: {len(records)} deactivated members this period")
    return records


# ─────────────────────────────────────────────────────────────────────────────
# XLSX output (preserved from v1.0)
# ─────────────────────────────────────────────────────────────────────────────

def _write_xlsx(active_df: pd.DataFrame, output_path: Path) -> None:
    active_df = _dedup_address(active_df.copy())
    active_df["_agent"] = (
        active_df[COL["broker_first"]].str.strip()
        + " "
        + active_df[COL["broker_last"]].str.strip()
    )
    summary = (
        active_df.groupby("_agent")[COL["member_count"]]
        .sum()
        .reset_index()
        .sort_values(COL["member_count"], ascending=False)
    )
    summary.columns = ["Agent", "Active Members"]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        summary.to_excel(writer, index=False, sheet_name="Active Members")
    logger.info(f"XLSX written → {output_path} ({len(summary)} agents)")


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def process_csv(
    csv_path: Path,
    run_date: str,
    run_type: str,
    last_run_date: str | None = None,
    write_xlsx: bool = True,
) -> tuple[list[dict], list[dict]]:
    """
    Main entry point. Called by molina_downloader.py.

    Args:
        csv_path:       Path to the downloaded Molina CSV.
        run_date:       ISO date of this run (YYYY-MM-DD).
        run_type:       "Monday" or "Friday".
        last_run_date:  ISO date from state file. None on first run.
                        If None, calculate_period_start() derives the cutoff
                        from the calendar — R2 is always period-scoped.
        write_xlsx:     Write agent XLSX. False in --dry-run mode.

    Returns:
        (r1_records, r2_records)
    """
    logger.info(f"Processing Molina CSV: {csv_path}")
    df = _load_csv(csv_path)

    status_dist = df[COL["status"]].value_counts().to_dict()
    logger.info(f"Loaded {len(df):,} rows. Status distribution: {status_dist}")

    # Resolve period start — state file wins, calendar fallback on first run
    if last_run_date is None:
        logger.warning(
            "First run — no state file. "
            "R2 skipped. Baseline established for next run."
        )
        r1 = _build_r1_records(df, run_date, run_type)
        return r1, []

    period_start = calculate_period_start()
    logger.info(f"Period start from calendar: {period_start}")

    r1 = _build_r1_records(df, run_date, run_type)
    r2 = _build_r2_records(df, run_date, period_start)

    if write_xlsx:
        active_df = df[df[COL["status"]].isin(ACTIVE_STATUSES)].copy()
        _write_xlsx(active_df, Path("data/output/molina_all_agents.xlsx"))

    return r1, r2


# ─────────────────────────────────────────────────────────────────────────────
# Standalone test
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/molina_report.py path/to/molina.csv [last_run_date]")
        print("       last_run_date: optional YYYY-MM-DD (omit to use calendar fallback)")
        sys.exit(1)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    csv_path = Path(sys.argv[1])
    last_run = sys.argv[2] if len(sys.argv) > 2 else None
    today    = date.today().isoformat()
    run_type = "Monday" if date.today().weekday() == 0 else "Friday"

    r1_records, r2_records = process_csv(
        csv_path, today, run_type, last_run_date=last_run
    )

    print("\n" + "=" * 65)
    print(f"R1 — Active Members  ({len(r1_records)} agents)")
    print("=" * 65)
    for r in r1_records:
        print(f"  {r['agent_name']:<34} {r['active_members']:>4} members")

    total_active = sum(r["active_members"] for r in r1_records)
    print(f"  {'TOTAL':<34} {total_active:>4}")

    print(f"\n{'=' * 65}")
    print(f"R2 — Deactivated This Period  ({len(r2_records)} members)")
    print("=" * 65)
    if r2_records:
        header = f"  {'Member':<38} {'Agent':<26} {'DOB':<12} {'ST':<4} {'End Date'}"
        print(header)
        print("  " + "-" * 95)
        for r in r2_records[:30]:
            print(
                f"  {r['member_name']:<38} "
                f"{r['agent_name']:<26} "
                f"{str(r['member_dob']):<12} "
                f"{str(r['state']):<4} "
                f"{r['coverage_end_date']}"
            )
        if len(r2_records) > 30:
            print(f"  … and {len(r2_records) - 30} more")
    else:
        print("  No deactivations found in this period")
