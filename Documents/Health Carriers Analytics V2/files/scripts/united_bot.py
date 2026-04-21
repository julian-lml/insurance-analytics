"""
scripts/united_bot.py — Phase 6
UHC Jarvis: Playwright browser automation — R1 (active members) + R2 (deactivated).

Public API (called by launcher):
    run_united(dry_run=False, agent_filter=None, headless=False)
        → (list[dict], list[dict])   # (r1_records, r2_records)

Standalone usage:
    python scripts/united_bot.py                    # all agents, writes XLSX
    python scripts/united_bot.py --dry-run          # all agents, no writes
    python scripts/united_bot.py --agent 0          # single agent, writes XLSX
    python scripts/united_bot.py --agent 0 --dry-run
    python scripts/united_bot.py --headless --dry-run

Auth: fully manual login — bot opens the sign-in page, human logs in + approves MFA.
      Auth failures do NOT retry (CLAUDE.md §7).

R1: Dashboard count — semi-auto: human navigates to Book of Business,
    bot reads active count label (selector from config/config.yaml).

R2: file_extract — human triggers Export in browser, bot captures download, filters:
      planStatus == "I" AND policyTermDate >= get_r2_start_date()
    member_dob from dateOfBirth column.
    detection_method = "file_extract"

No state file for United (CLAUDE.md §10 / §8.17).
agent_name: ALWAYS from agents.yaml — never from "Writing Agent" column (CLAUDE.md §7).
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import re
import sys
import time
from datetime import date
from pathlib import Path

import pandas as pd
import yaml
from dotenv import load_dotenv
from playwright.async_api import TimeoutError as PWTimeout

load_dotenv()

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scripts.utils import (
    setup_logging,
    run_type,
    get_r2_start_date,
    write_r1_xlsx,
    append_deactivated_xlsx,
)

log = logging.getLogger(__name__)

# ─── Column names — pulled from config/config.yaml carriers.united.columns ────

_CFG_PATH = ROOT / "config" / "config.yaml"
with open(_CFG_PATH, encoding="utf-8") as _f:
    _CFG = yaml.safe_load(_f)

_UNITED           = _CFG["carriers"]["united"]
_COLS             = _UNITED["columns"]
COL_TERM_DATE     = _COLS["termination_date"]
COL_FIRST_NAME    = _COLS["first_name"]
COL_LAST_NAME     = _COLS["last_name"]
COL_STATE         = _COLS["state"]
COL_DOB           = _COLS["member_dob"]
COL_POLICY_STATUS = _COLS["policy_status"]

# ─── Selectors ────────────────────────────────────────────────────────────────

SIGN_IN_URL      = _UNITED["portal_url"]
SEL_ACTIVE_COUNT = _UNITED["selectors"]["active_count"]


# ─── Config / agents ──────────────────────────────────────────────────────────

def _load_agents() -> list[dict]:
    cfg_path = ROOT / "config" / "agents.yaml"
    with open(cfg_path, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    agents = cfg.get("united")
    if not agents:
        raise ValueError(
            "No united: key found in config/agents.yaml.\n"
            "Add a united: section with name/user/pass for each agent."
        )
    return agents


# ─── R1 / R2 builders ─────────────────────────────────────────────────────────

def _build_r1_record(
    agent_name: str,
    active_members: int,
    run_date: str,
    run_type: str,
    duration: float,
) -> dict:
    """R1 schema — CLAUDE.md §5."""
    return {
        "run_date":         run_date,
        "run_type":         run_type,
        "carrier":          "United",
        "agent_name":       agent_name,
        "active_members":   active_members,
        "status":           "success",
        "error_message":    None,
        "duration_seconds": round(duration, 1),
    }


def _failed_r1(
    agent_name: str,
    run_date: str,
    run_type: str,
    error_msg: str,
) -> dict:
    return {
        "run_date":         run_date,
        "run_type":         run_type,
        "carrier":          "United",
        "agent_name":       agent_name,
        "active_members":   0,
        "status":           "failed",
        "error_message":    error_msg,
        "duration_seconds": 0.0,
    }


def _build_r2_records(
    df: pd.DataFrame,
    agent_name: str,
    run_date: str,
) -> list[dict]:
    """
    R2 schema — CLAUDE.md §5.
    Filter: planStatus == "I" AND policyTermDate >= get_r2_start_date().

    policy_number: no subscriber ID column confirmed in this download.
    Temporary composite key: memberFirstName_memberLastName (§8.20).
    """
    period_start = get_r2_start_date()

    df = df.copy()

    if COL_POLICY_STATUS in df.columns:
        df = df[df[COL_POLICY_STATUS].str.strip() == "I"]

    df[COL_TERM_DATE] = pd.to_datetime(df[COL_TERM_DATE], errors="coerce")
    df = df.dropna(subset=[COL_TERM_DATE])
    df = df[df[COL_TERM_DATE].dt.date >= period_start]

    log.info(
        "[United] %s: R2 date filter -> %d non-Active rows with policyTermDate >= %s",
        agent_name, len(df), period_start,
    )

    log.warning(
        "[United] %s: No subscriber ID column confirmed in download — "
        "using name-based composite key for dedup (temporary). "
        "Confirm policy ID column name after first successful run.",
        agent_name,
    )

    records = []
    for _, row in df.iterrows():
        first = str(row.get(COL_FIRST_NAME, "") or "").strip()
        last  = str(row.get(COL_LAST_NAME,  "") or "").strip()
        member_name = f"{first} {last}".strip()
        dob_val = row.get(COL_DOB)
        records.append({
            "run_date":          run_date,
            "carrier":           "United",
            "agent_name":        agent_name,
            "member_name":       member_name,
            "member_dob":        str(dob_val) if pd.notna(dob_val) else None,
            "state":             str(row.get(COL_STATE, "") or "").strip(),
            "coverage_end_date": row[COL_TERM_DATE].strftime("%Y-%m-%d"),
            "policy_number":     f"{first}_{last}",   # temporary — no ID col confirmed
            "last_status":       "Inactive",
            "detection_method":  "file_extract",
        })
    return records


def _parse_export_file(file_path: Path) -> pd.DataFrame:
    """Read XLSX or CSV by extension."""
    if file_path.suffix.lower() == ".csv":
        return pd.read_csv(file_path, dtype=str)
    return pd.read_excel(file_path, engine="openpyxl", dtype=str)


def _read_export(path: Path) -> pd.DataFrame:
    """
    Read UHC export XLSX, finding the real header row by scanning for
    'memberFirstName' which is always the first column header.
    Tries rows 0-9. Raises ValueError if not found.
    """
    for header_row in range(10):
        df = pd.read_excel(path, header=header_row, engine="openpyxl", dtype=str)
        if COL_FIRST_NAME in df.columns:
            return df
    raise ValueError(
        f"Could not find expected headers in {path.name}. "
        f"Ensure '{COL_FIRST_NAME}' column is present in the download."
    )


# ─── Single-agent Playwright flow ─────────────────────────────────────────────

async def _run_single_agent(
    agent: dict,
    context,            # playwright BrowserContext — owned and closed by caller
    dry_run: bool,
    run_date: str,
    run_type: str,
) -> tuple[dict, list[dict]]:
    """
    Full browser flow for one United agent.
    Receives a fresh BrowserContext per call — browser lifecycle is managed
    by _run_all_agents_async (one playwright + browser per agent).
    Returns (r1_record, r2_records).
    Raises on unrecoverable error — caller wraps in try/except.
    """
    agent_name = agent["name"]
    t_start = time.monotonic()

    dl_dir = (
        ROOT / "data" / "raw" / "united"
        / run_date[:7]
        / run_date
        / agent_name.replace(" ", "_")
    )
    dl_dir.mkdir(parents=True, exist_ok=True)

    period_start = get_r2_start_date()
    export_path: Path | None = None

    page = await context.new_page()

    # ── Login — fully manual ──────────────────────────────────────────────
    # Portal detects Playwright at browser level for some accounts.
    # Bot opens the sign-in page; human completes the full login flow.
    await page.goto(SIGN_IN_URL, wait_until="domcontentloaded")
    print(f"\n[United] [{agent['name']}]")
    print(f"  1. Log in completely manually in the browser")
    print(f"     Account: {agent['user']}")
    print(f"     (Use your password manager for the password)")
    print(f"  2. Approve Microsoft Authenticator when prompted")
    print(f"  3. When you reach the Jarvis dashboard, press ENTER here...")
    input()

    # Auth guard — confirm dashboard loaded before proceeding
    try:
        await page.locator(SEL_ACTIVE_COUNT).wait_for(state="visible", timeout=20_000)
    except PWTimeout:
        raise RuntimeError(
            f"Dashboard not loaded after manual login for {agent_name}. "
            "Ensure you are on the Jarvis home page before pressing ENTER."
        )
    log.info("[United] %s: dashboard loaded", agent_name)

    # ── R1: read active count from dashboard ─────────────────────────────
    print(f"\n[United -- {agent_name}] R1 COUNT -- do this in the browser:")
    print("  1. Navigate to Book of Business so the active member count is visible")
    print("  2. Press ENTER — bot will read the count automatically")
    input()

    count_text = await page.locator(SEL_ACTIVE_COUNT).first.inner_text(timeout=8000)
    match = re.search(r"([\d,]+)", count_text)
    if not match:
        raise ValueError(
            f"[United] {agent_name}: could not parse R1 count from '{count_text}'"
        )
    active_members = int(match.group(1).replace(",", ""))
    log.info(
        "[United] %s: R1 active_members=%d (raw: '%s')",
        agent_name, active_members, count_text.strip(),
    )

    duration_r1 = time.monotonic() - t_start
    r1 = _build_r1_record(agent_name, active_members, run_date, run_type, duration_r1)

    # ── R2: wait for human-triggered download ────────────────────────────
    print(f"\n[United -- {agent_name}] R2 EXPORT -- do this in the browser:")
    print("  1. Navigate to the Book of Business export section")
    print(f"  2. Apply filter: Terminated, Termination Date on/after {period_start.strftime('%m/%d/%Y')}")
    print("  3. Click Download, select columns, click Download in the modal")
    print("  (bot is listening — do NOT press ENTER, just complete the download)")

    download = await page.wait_for_event("download", timeout=180_000)
    export_path = dl_dir / download.suggested_filename
    await download.save_as(export_path)
    log.info("[United] %s: downloaded %s -> %s", agent_name, download.suggested_filename, export_path)

    if export_path is None:
        log.info("[United] %s: R1 active=%d  R2 records=0", agent_name, r1["active_members"])
        return r1, []

    # ── Parse export file -> R2 records ───────────────────────────────────────
    df = _read_export(export_path)

    log.info("[United] %s: export rows=%d  columns=%s", agent_name, len(df), list(df.columns))

    if COL_POLICY_STATUS in df.columns:
        log.info(
            "[United] %s: Policy Status distribution: %s",
            agent_name, df[COL_POLICY_STATUS].value_counts().to_dict(),
        )

    required = {COL_TERM_DATE, COL_FIRST_NAME, COL_LAST_NAME}
    missing = required - set(df.columns)
    if missing:
        log.warning(
            "[United] %s: export missing expected columns: %s. Columns present: %s",
            agent_name, missing, list(df.columns),
        )
        if missing:
            raise ValueError(f"Cannot process export — missing critical columns: {missing}")

    r2 = _build_r2_records(df, agent_name, run_date)

    log.info(
        "[United] %s: R1 active=%d  R2 records=%d  period_start=%s",
        agent_name, r1["active_members"], len(r2), period_start,
    )
    return r1, r2


# ─── All-agents loop ──────────────────────────────────────────────────────────

async def _run_all_agents_async(
    agents: list[dict],
    dry_run: bool,
    run_date: str,
    run_type: str,
    headless: bool,
) -> tuple[list[dict], list[dict]]:
    from playwright.async_api import async_playwright

    all_r1: list[dict] = []
    all_r2: list[dict] = []

    for agent in agents:
        # Per-agent persistent Chrome profile — accumulates real session data
        # so the portal sees a legitimate returning browser, not a sterile context.
        agent_profile = str(
            ROOT / "data" / "chrome_profiles" / agent["name"].replace(" ", "_")
        )

        async with async_playwright() as p:
            context = await p.chromium.launch_persistent_context(
                user_data_dir=agent_profile,
                channel="chrome",
                headless=headless,
                args=["--disable-blink-features=AutomationControlled"],
                accept_downloads=True,
            )
            await context.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )
            try:
                r1_record, r2_records = await _run_single_agent(
                    agent, context, dry_run, run_date, run_type
                )
            except Exception as exc:
                log.error(f"[United] {agent['name']}: unhandled error — {exc}", exc_info=True)
                r1_record = _failed_r1(agent["name"], run_date, run_type, str(exc))
                r2_records = []
            finally:
                await context.close()

        if r1_record:
            all_r1.append(r1_record)
        all_r2.extend(r2_records)

    success_count = sum(1 for r in all_r1 if r["status"] == "success")

    if not dry_run:
        success_r1 = [r for r in all_r1 if r["status"] == "success"]
        write_r1_xlsx(success_r1, "United", log)
        append_deactivated_xlsx(all_r2, "United", log)
        # No state file for United (CLAUDE.md §10 / §8.17)
        log.info(
            "[United] Run complete -- %d/%d agents succeeded, %d R2 records written",
            success_count, len(agents), len(all_r2),
        )
    else:
        log.info("[United] DRY RUN -- XLSX write skipped")
        _print_dry_run_summary(all_r1, all_r2)

    return all_r1, all_r2


def _print_dry_run_summary(r1_records: list[dict], r2_records: list[dict]) -> None:
    print("\n-- United DRY RUN summary ------------------------------")
    for r in r1_records:
        status = "OK" if r["status"] == "success" else "!!"
        print(f"  {status} {r['agent_name']:25s}  active={r['active_members']}")
    print(f"\n  R2 records this period: {len(r2_records)}")
    if r2_records:
        period_start = get_r2_start_date()
        print(f"  Period start: {period_start}")
        for rec in r2_records:
            print(
                f"    {rec['agent_name']:20s}  {rec['member_name']:25s}  "
                f"end={rec['coverage_end_date']}  policy={rec['policy_number']}"
            )
    else:
        print(f"  (0 R2 records for period starting {get_r2_start_date()})")
    print("--------------------------------------------------------\n")


# ─── Public sync wrapper ──────────────────────────────────────────────────────

def run_united(
    dry_run: bool = False,
    agent_filter: int | None = None,
    headless: bool = False,
) -> tuple[list[dict], list[dict]]:
    """
    Public API. Called by launcher or standalone.
    Returns (r1_records, r2_records).
    """
    global log
    log = setup_logging("UNITED")

    agents   = _load_agents()
    run_date = date.today().isoformat()
    rt       = run_type()

    if agent_filter is not None:
        if agent_filter >= len(agents):
            raise IndexError(
                f"--agent {agent_filter} out of range "
                f"(0-{len(agents) - 1} available)"
            )
        agents = [agents[agent_filter]]
        log.info(f"[United] Single-agent mode: {agents[0]['name']}")

    return asyncio.run(
        _run_all_agents_async(agents, dry_run, run_date, rt, headless)
    )


# ─── Standalone CLI ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="United Healthcare Playwright bot",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  python scripts/united_bot.py\n"
            "  python scripts/united_bot.py --dry-run\n"
            "  python scripts/united_bot.py --agent 0\n"
            "  python scripts/united_bot.py --agent 0 --dry-run\n"
            "  python scripts/united_bot.py --headless --dry-run\n"
        ),
    )
    parser.add_argument("--agent", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--headless", action="store_true")
    args = parser.parse_args()

    run_united(dry_run=args.dry_run, agent_filter=args.agent, headless=args.headless)
