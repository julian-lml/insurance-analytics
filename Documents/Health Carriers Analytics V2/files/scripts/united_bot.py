"""
scripts/united_bot.py — Phase 6
UHC Jarvis: Playwright browser automation — R1 (active members) + R2 (deactivated).

Public API (called by main.py in Phase 8):
    run_united(dry_run=False) -> (list[dict], list[dict])   # (r1_records, r2_records)

Standalone usage:
    python scripts/united_bot.py                    # all agents, writes XLSX
    python scripts/united_bot.py --dry-run          # all agents, no writes
    python scripts/united_bot.py --agent 0          # single agent, writes XLSX
    python scripts/united_bot.py --agent 0 --dry-run
    python scripts/united_bot.py --headless --dry-run

Auth: user+pass then MS Authenticator MFA on boss's phone — SEMI-AUTO.
      Human must press ENTER after each agent's MFA approval.
      Auth failures do NOT retry (CLAUDE.md §7).

R1: Dashboard count — semi-auto: human navigates to Book of Business,
    bot reads active count label. Falls back to manual entry if selector fails.

R2: file_extract — bot clicks Export, downloads XLSX, filters in pandas:
      Policy Status == "Terminated" AND Termination Date >= calculate_period_start()
    Termination Date stored as M/D/YYYY strings (CLAUDE.md §9).
    member_dob = null always — not available in United export (CLAUDE.md §8.17).
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
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import yaml
from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).resolve().parent.parent

# ─── Logging ──────────────────────────────────────────────────────────────────

def _setup_logging() -> None:
    log_dir = ROOT / "logs"
    log_dir.mkdir(exist_ok=True)
    from datetime import datetime
    log_file = log_dir / f"run_{datetime.now().strftime('%Y%m%d_%H%M')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | united | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )

log = logging.getLogger(__name__)

# ─── Column constants — confirmed from live Jarvis BookOfBusiness DownloadResults.xlsx ──
# NOTE: these are camelCase, different from the original sample file (CLAUDE.md §8.17).
# No subscriber/policy ID column confirmed in this download — policy_number is a
# temporary name-based composite key until confirmed (see _build_r2_records).

COL_TERM_DATE     = "policyTermDate"   # M/D/YYYY or YYYY-MM-DD — parse with errors="coerce"
COL_FIRST_NAME    = "memberFirstName"
COL_LAST_NAME     = "memberLastName"
COL_STATE         = "memberState"
COL_DOB           = "dateOfBirth"      # IS available in this export (not null)
COL_POLICY_STATUS = "planStatus"       # "Active" confirmed; terminated value TBD after first run

# ─── Selectors — verify against live portal before first run ──────────────────
# TODO: Inspect uhcjarvis.com login page and update these selectors as needed.

SIGN_IN_URL      = "https://www.uhcjarvis.com/content/jarvis/en/sign_in.html#/sign_in"
SEL_SSO_BTN      = "button:has-text('Sign in with One Healthcare ID')"
SEL_USERNAME     = "input#username"
SEL_PASSWORD     = "input#login-pwd"
SEL_LOGIN_BTN    = "button#btnLogin"

# R1 — active count label shown after navigating to Book of Business.
# Confirmed from live portal HTML inspection.
SEL_ACTIVE_COUNT = "p#activemembercount"

# ─── Date scoping — verbatim from CLAUDE.md §6 ───────────────────────────────

def calculate_period_start(today: date = None) -> date:
    """
    Returns the last day of the previous month.

    Rationale: Molina and Oscar stamp all terminations at end-of-month.
    Anchoring to last day of previous month captures everything correctly.
    United uses real termination dates (M/D/YYYY) — the wider window does not
    cause duplicates because dedup key (carrier, policy_number, coverage_end_date)
    handles re-capture across consecutive runs. See CLAUDE.md §6.
    """
    if today is None:
        today = date.today()
    first_of_this_month = today.replace(day=1)
    last_of_prev_month = first_of_this_month - timedelta(days=1)
    return last_of_prev_month


def _run_type() -> str:
    weekday = date.today().weekday()
    if weekday == 0:
        return "Monday"
    elif weekday == 4:
        return "Friday"
    return "Manual"


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
    Filter: planStatus != "Active" AND policyTermDate >= period_start.

    policyTermDate may be M/D/YYYY or YYYY-MM-DD — parsed with errors="coerce".
    member_dob available via dateOfBirth column (confirmed in live export).
    detection_method = "file_extract".

    policy_number: no subscriber ID column confirmed in this download.
    Temporary composite key: memberFirstName_memberLastName.
    WARNING is logged. Replace once policy ID column is confirmed.
    """
    period_start = calculate_period_start()

    df = df.copy()

    # Filter to inactive rows only — planStatus == "I" confirmed from live file
    if COL_POLICY_STATUS in df.columns:
        df = df[df[COL_POLICY_STATUS].str.strip() == "I"]

    # Parse policyTermDate — YYYY-MM-DD confirmed from live file; errors="coerce"
    # handles any format variation without crashing
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
            "last_status":       "Terminated",
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
        if "memberFirstName" in df.columns:
            return df
    raise ValueError(
        f"Could not find expected headers in {path.name}. "
        "Ensure 'memberFirstName' column is present in the download."
    )


# ─── XLSX append with dedup ───────────────────────────────────────────────────

def _append_deactivated_xlsx(r2_records: list[dict]) -> None:
    """
    Append R2 records to shared deactivated_members.xlsx.
    Dedup key: (carrier, policy_number, coverage_end_date).
    Existing rows always win (keep="first"). CLAUDE.md §6.
    """
    if not r2_records:
        return

    output_path = ROOT / "data" / "output" / "deactivated_members.xlsx"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    new_df = pd.DataFrame(r2_records)
    cols = [
        "run_date", "carrier", "agent_name", "member_name",
        "member_dob", "state", "coverage_end_date", "policy_number",
        "last_status", "detection_method",
    ]
    new_df = new_df[[c for c in cols if c in new_df.columns]]

    if output_path.exists():
        existing_df = pd.read_excel(output_path, engine="openpyxl")
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        rows_before = len(existing_df)
    else:
        combined_df = new_df
        rows_before = 0

    combined_df = combined_df.drop_duplicates(
        subset=["carrier", "policy_number", "coverage_end_date"],
        keep="first",
    )
    net_new = len(combined_df) - rows_before

    try:
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            combined_df.to_excel(writer, index=False, sheet_name="Deactivated Members")
        log.info("[United] appended %d net-new R2 records to deactivated_members.xlsx", net_new)
    except Exception as exc:
        log.warning(f"[United] XLSX write failed — {exc}. Continuing.")


# ─── R1 XLSX writer ───────────────────────────────────────────────────────────

def _write_united_xlsx(r1_records: list[dict]) -> None:
    """Write R1 records to data/output/united_all_agents.xlsx (overwrite each run)."""
    if not r1_records:
        return

    output_path = ROOT / "data" / "output" / "united_all_agents.xlsx"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    cols = [
        "run_date", "run_type", "carrier", "agent_name", "active_members",
        "status", "error_message", "duration_seconds",
    ]
    df = pd.DataFrame(r1_records)
    df = df[[c for c in cols if c in df.columns]]
    df = df.sort_values("active_members", ascending=False)

    try:
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Active Members")
        log.info("[United] wrote united_all_agents.xlsx — %d agents", len(df))
    except Exception as exc:
        log.warning(f"[United] united_all_agents.xlsx write failed — {exc}. Continuing.")


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

    period_start = calculate_period_start()
    export_path: Path | None = None

    page = await context.new_page()

    # ── Login (retry on page-load failure, NOT on auth failure) ───────────
    # Sequence (CLAUDE.md §8.18):
    #   1. Load sign-in page
    #   2. Click SSO button → redirects to One Healthcare ID (Optum domain)
    #   3. Wait for One Healthcare ID redirect (identity.onehealthcareid.com)
    #   4. Fill username + click Login
    #   5. Fill password + click Login
    backoffs = [5, 15, 45]
    last_exc: Exception | None = None

    for attempt, backoff in enumerate(backoffs, start=1):
        try:
            await page.goto(SIGN_IN_URL, wait_until="domcontentloaded")
            await page.click(SEL_SSO_BTN)
            await page.wait_for_url("**onehealthcareid.com**", timeout=15_000)
            await page.fill(SEL_USERNAME, agent["user"])
            await page.click(SEL_LOGIN_BTN)
            await page.locator(SEL_PASSWORD).wait_for(state="visible", timeout=15_000)
            await page.locator(SEL_PASSWORD).click()
            await page.locator(SEL_PASSWORD).type(agent["pass"], delay=50)

            # Verify the field was actually filled — fallback to manual if empty
            filled_value = await page.locator(SEL_PASSWORD).input_value()
            if not filled_value:
                print(
                    f"\n[United] [{agent['name']}] Password field empty after type() — "
                    f"fill it manually in the browser, then press ENTER..."
                )
                input()

            await page.click(SEL_LOGIN_BTN)
            last_exc = None
            break
        except Exception as exc:
            last_exc = exc
            log.warning(
                f"[United] {agent_name}: login attempt {attempt}/3 failed — {exc}"
            )
            if attempt < len(backoffs):
                await asyncio.sleep(backoff)

    if last_exc:
        raise last_exc

    # ── MFA pause — human approves MS Authenticator ───────────────────────
    print(f"\n[United] Agent: {agent_name}")
    print("[United] Approve Microsoft Authenticator on the boss's phone, then press ENTER...")
    input()

    # Wait for dashboard to fully load before any further interaction
    await page.locator(SEL_ACTIVE_COUNT).wait_for(timeout=20_000)
    log.info("[United] %s: dashboard loaded", agent_name)

    # ── R1: read active count from dashboard ─────────────────────────────
    # UHC Jarvis shows active member count in p#activemembercount.
    # Human navigates to the Book of Business dashboard; bot reads the label.

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
    # Bot registers the download listener first, then prints instructions.
    # Human applies Terminated filter, clicks Download, selects columns,
    # and clicks Download in the modal. The download event fires automatically.

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

    # Log Policy Status distribution to confirm filter worked
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
        # Fresh playwright + browser + context per agent — prevents fingerprint
        # accumulation that causes identity.onehealthcareid.com to reject input
        # after 5-6 sequential logins from the same browser instance.
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=headless,
                args=["--disable-blink-features=AutomationControlled"],
            )
            context = await browser.new_context(
                accept_downloads=True,
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                ),
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
                await browser.close()

        if r1_record:
            all_r1.append(r1_record)
        all_r2.extend(r2_records)

    success_count = sum(1 for r in all_r1 if r["status"] == "success")

    if not dry_run:
        _append_deactivated_xlsx(all_r2)
        _write_united_xlsx(all_r1)
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
        period_start = calculate_period_start()
        print(f"  Period start: {period_start}  (member_dob always null for United)")
        for rec in r2_records:
            print(
                f"    {rec['agent_name']:20s}  {rec['member_name']:25s}  "
                f"end={rec['coverage_end_date']}  policy={rec['policy_number']}"
            )
    else:
        print(f"  (0 R2 records for period starting {calculate_period_start()})")
    print("--------------------------------------------------------\n")


# ─── Public sync wrapper ──────────────────────────────────────────────────────

def run_united(dry_run: bool = False) -> tuple[list[dict], list[dict]]:
    """
    Sync wrapper — called by main.py in Phase 8.
    Returns (r1_records, r2_records).
    """
    agents   = _load_agents()
    run_date = date.today().isoformat()
    run_type = _run_type()
    return asyncio.run(
        _run_all_agents_async(agents, dry_run, run_date, run_type, headless=False)
    )


# ─── Standalone CLI ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Phase 6 -- United Healthcare Playwright bot",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  python scripts/united_bot.py\n"
            "    -> all agents, full browser flow, writes XLSX\n\n"
            "  python scripts/united_bot.py --dry-run\n"
            "    -> all agents, full browser flow, no XLSX write\n\n"
            "  python scripts/united_bot.py --agent 0\n"
            "    -> single agent, writes XLSX\n\n"
            "  python scripts/united_bot.py --agent 0 --dry-run\n"
            "    -> single agent, no writes\n\n"
            "  python scripts/united_bot.py --headless --dry-run\n"
            "    -> headless browser (MFA prompt still appears in terminal)\n\n"
            "FIRST RUN CHECKLIST:\n"
            "  1. Add a 'united:' section to config/agents.yaml (name/user/pass per agent)\n"
            "  2. Run --agent 0 --dry-run to test login + export flow\n"
            "  3. Inspect the R2 export columns and confirm against CLAUDE.md §9\n"
            "  4. Update SEL_ACTIVE_COUNT once you identify the correct CSS selector\n"
            "  5. Update SEL_EXPORT_BTN if the portal's export button selector differs"
        ),
    )
    parser.add_argument(
        "--agent", type=int, default=None,
        help="Run only agent at index N (0-based) from agents.yaml united: key",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Full browser flow; no XLSX write",
    )
    parser.add_argument(
        "--headless", action="store_true",
        help="Run browser headless (default: headed so you can see the portal)",
    )
    args = parser.parse_args()

    _setup_logging()

    agents   = _load_agents()
    run_date = date.today().isoformat()
    run_type = _run_type()

    if args.agent is not None:
        if args.agent >= len(agents):
            print(
                f"Error: --agent {args.agent} out of range "
                f"(0-{len(agents) - 1} available)"
            )
            sys.exit(1)
        agents = [agents[args.agent]]
        log.info(f"[United] Single-agent mode: {agents[0]['name']}")

    asyncio.run(
        _run_all_agents_async(agents, args.dry_run, run_date, run_type, args.headless)
    )
