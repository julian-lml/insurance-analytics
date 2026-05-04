"""
scripts/utils.py — Shared infrastructure for all carrier bots.

Single source of truth for:
  - get_r2_start_date()      : fixed historical R2 cutoff from config.yaml
  - run_type()               : "Monday" / "Friday" / "Manual"
  - setup_logging()          : logs/run_YYYYMMDD_HHMMSS_{carrier}.log + stdout
  - with_retry()             : 3-attempt retry, 5/15/45s backoff. Never use for auth.
  - write_r1_xlsx()          : merge-on-write per §8.22. Safe for single-agent reruns.
  - append_deactivated_xlsx(): append + dedup by (carrier, policy_number, coverage_end_date).

See CLAUDE.md §15 for full rationale.
"""

from __future__ import annotations

import logging
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Callable

import pandas as pd
import yaml

ROOT = Path(__file__).parent.parent
_CONFIG_PATH = ROOT / "config" / "config.yaml"

_BACKOFF = [5, 15, 45]


# ─────────────────────────────────────────────────────────────────────────────
# R2 start date
# ─────────────────────────────────────────────────────────────────────────────

def get_r2_start_date() -> date:
    """
    Fixed historical cutoff for R2. Read from config/config.yaml under r2.start_date.

    We use a fixed date (not a rolling monthly window) because carrier portals
    publish cancellations with days-to-weeks of lag. A rolling window silently
    drops records that arrive late. The dedup key
    (carrier, policy_number, coverage_end_date) keep="first" makes re-capture safe,
    so we keep the window wide.
    """
    config = yaml.safe_load(_CONFIG_PATH.read_text())
    return date.fromisoformat(config["r2"]["start_date"])


# ─────────────────────────────────────────────────────────────────────────────
# Run type
# ─────────────────────────────────────────────────────────────────────────────

def run_type() -> str:
    """Monday (weekday 0), Friday (weekday 4), else Manual."""
    wd = date.today().weekday()
    if wd == 0:
        return "Monday"
    if wd == 4:
        return "Friday"
    return "Manual"


# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────

def setup_logging(carrier: str) -> logging.Logger:
    """
    Configure root logger to write to logs/run_YYYYMMDD_HHMMSS_{carrier}.log + stdout.
    Seconds in the filename prevent collisions when bots are launched back-to-back.
    """
    logs_dir = ROOT / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = logs_dir / f"run_{stamp}_{carrier.lower()}.log"

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    for h in list(logger.handlers):
        logger.removeHandler(h)

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(sh)

    logger.info("Log file: %s", log_path)
    return logger


# ─────────────────────────────────────────────────────────────────────────────
# Retry
# ─────────────────────────────────────────────────────────────────────────────

def with_retry(
    func: Callable,
    operation_name: str,
    max_attempts: int = 3,
    log: logging.Logger | None = None,
):
    """
    Run `func` up to max_attempts with 5/15/45s backoff.
    Never use for auth — account lockout risk (CLAUDE.md §7).
    """
    log = log or logging.getLogger(__name__)
    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return func()
        except Exception as exc:
            last_exc = exc
            if attempt >= max_attempts:
                log.error("%s | attempt %d/%d failed: %s", operation_name, attempt, max_attempts, exc)
                raise
            delay = _BACKOFF[min(attempt - 1, len(_BACKOFF) - 1)]
            log.warning(
                "%s | attempt %d/%d failed: %s — retrying in %ds",
                operation_name, attempt, max_attempts, exc, delay,
            )
            time.sleep(delay)
    if last_exc:
        raise last_exc


# ─────────────────────────────────────────────────────────────────────────────
# R1 XLSX — merge-on-write (CLAUDE.md §8.22)
# ─────────────────────────────────────────────────────────────────────────────

def write_r1_xlsx(r1_records: list[dict], carrier: str, log: logging.Logger | None = None) -> None:
    """
    Merge-on-write. Load existing file → drop rows for agents in current run
    → append new rows → save. Safe for single-agent reruns and full runs.
    """
    log = log or logging.getLogger(__name__)
    if not r1_records:
        log.info("%s | R1 XLSX: no records to write", carrier)
        return

    output_path = ROOT / "data" / "output" / f"{carrier.lower()}_all_agents.xlsx"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    new_df = pd.DataFrame(r1_records)
    agent_names = new_df["agent_name"].unique().tolist()

    if output_path.exists():
        existing_df = pd.read_excel(output_path, engine="openpyxl")
        existing_df = existing_df[~existing_df["agent_name"].isin(agent_names)]
        combined = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined = new_df

    combined = combined.sort_values("active_members", ascending=False)

    try:
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            combined.to_excel(writer, index=False, sheet_name="Active Members")
        log.info("%s | R1 XLSX written → %s (%d rows)", carrier, output_path, len(combined))
    except Exception as exc:
        log.warning("%s | R1 XLSX write failed (non-fatal): %s", carrier, exc)


# ─────────────────────────────────────────────────────────────────────────────
# R2 XLSX — append + dedup
# ─────────────────────────────────────────────────────────────────────────────

_R2_OUTPUT = ROOT / "data" / "output" / "deactivated_members.xlsx"
_R2_DEDUP_KEY = ["carrier", "policy_number", "coverage_end_date"]


def append_deactivated_xlsx(
    r2_records: list[dict],
    carrier: str,
    log: logging.Logger | None = None,
) -> None:
    """
    Append R2 records to deactivated_members.xlsx with dedup.
    Dedup key: (carrier, policy_number, coverage_end_date). Existing rows win.
    Rows with null policy_number are dropped (cannot dedup safely).
    """
    log = log or logging.getLogger(__name__)
    if not r2_records:
        log.info("%s | R2 XLSX: no records to append", carrier)
        return

    new_df = pd.DataFrame(r2_records)

    before = len(new_df)
    new_df = new_df[new_df["policy_number"].notna() & (new_df["policy_number"].astype(str).str.strip() != "")]
    dropped_null = before - len(new_df)
    if dropped_null:
        log.warning("%s | R2: dropped %d rows with null policy_number", carrier, dropped_null)

    if new_df.empty:
        log.info("%s | R2 XLSX: nothing to append after null-policy filter", carrier)
        return

    _R2_OUTPUT.parent.mkdir(parents=True, exist_ok=True)

    if _R2_OUTPUT.exists():
        existing_df = pd.read_excel(_R2_OUTPUT, engine="openpyxl")
        combined = pd.concat([existing_df, new_df], ignore_index=True)
        before_dedup = len(combined)
        combined = combined.drop_duplicates(subset=_R2_DEDUP_KEY, keep="first")
        deduped = before_dedup - len(combined)
        net_new = len(combined) - len(existing_df)
        log.info(
            "%s | R2: existing=%d, candidates=%d, deduped=%d, net_new=%d",
            carrier, len(existing_df), len(new_df), deduped, net_new,
        )
    else:
        combined = new_df.drop_duplicates(subset=_R2_DEDUP_KEY, keep="first")
        log.info("%s | R2: first write, %d rows", carrier, len(combined))

    try:
        with pd.ExcelWriter(_R2_OUTPUT, engine="openpyxl") as writer:
            combined.to_excel(writer, index=False, sheet_name="Deactivated")
        log.info("%s | R2 XLSX written → %s (%d rows)", carrier, _R2_OUTPUT, len(combined))
    except Exception as exc:
        log.warning("%s | R2 XLSX write failed (non-fatal): %s", carrier, exc)


# ─────────────────────────────────────────────────────────────────────────────
# R3 XLSX — append + dedup (roster mode only)
# ─────────────────────────────────────────────────────────────────────────────

_R3_OUTPUT = ROOT / "data" / "output" / "active_members_all.xlsx"
_R3_DEDUP_KEY = ["carrier", "policy_number", "run_date"]


def write_active_members_xlsx(
    r3_records: list[dict],
    carrier: str,
    log: logging.Logger | None = None,
) -> None:
    """
    Append R3 records to active_members_all.xlsx with dedup.
    Dedup key: (carrier, policy_number, run_date). Existing rows win.
    Rows with null policy_number are dropped (cannot dedup safely).
    Called ONLY when mode='roster'. Never called in regular mode.
    """
    log = log or logging.getLogger(__name__)
    if not r3_records:
        log.info("%s | R3 XLSX: no records to write", carrier)
        return

    new_df = pd.DataFrame(r3_records)

    before = len(new_df)
    new_df = new_df[
        new_df["policy_number"].notna()
        & (new_df["policy_number"].astype(str).str.strip() != "")
    ]
    dropped_null = before - len(new_df)
    if dropped_null:
        log.warning("%s | R3: dropped %d rows with null policy_number", carrier, dropped_null)

    if new_df.empty:
        log.info("%s | R3 XLSX: nothing to append after null-policy filter", carrier)
        return

    _R3_OUTPUT.parent.mkdir(parents=True, exist_ok=True)

    if _R3_OUTPUT.exists():
        existing_df = pd.read_excel(_R3_OUTPUT, engine="openpyxl")
        combined = pd.concat([existing_df, new_df], ignore_index=True)
        before_dedup = len(combined)
        combined = combined.drop_duplicates(subset=_R3_DEDUP_KEY, keep="first")
        deduped = before_dedup - len(combined)
        net_new = len(combined) - len(existing_df)
        log.info(
            "%s | R3: existing=%d, candidates=%d, deduped=%d, net_new=%d",
            carrier, len(existing_df), len(new_df), deduped, net_new,
        )
    else:
        combined = new_df.drop_duplicates(subset=_R3_DEDUP_KEY, keep="first")
        log.info("%s | R3: first write, %d rows", carrier, len(combined))

    try:
        with pd.ExcelWriter(_R3_OUTPUT, engine="openpyxl") as writer:
            combined.to_excel(writer, index=False, sheet_name="Active Roster")
        log.info("%s | R3 XLSX written → %s (%d rows)", carrier, _R3_OUTPUT, len(combined))
    except Exception as exc:
        log.warning("%s | R3 XLSX write failed (non-fatal): %s", carrier, exc)
