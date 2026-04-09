"""
scripts/cigna_bot.py — Phase 5
Cigna for Brokers: Playwright browser automation — R1 (active members) + R2 (deactivated).

Public API (called by main.py in Phase 8):
    run_cigna(dry_run=False) → (list[dict], list[dict])   # (r1_records, r2_records)

Standalone usage:
    python scripts/cigna_bot.py                    # all agents, writes XLSX
    python scripts/cigna_bot.py --dry-run          # all agents, no writes
    python scripts/cigna_bot.py --agent 0          # single agent, writes XLSX
    python scripts/cigna_bot.py --agent 0 --dry-run
    python scripts/cigna_bot.py --headless --dry-run

Auth: user+pass then email 2FA (webmail.ligagent.com) — SEMI-AUTO.
      VPN must be active BEFORE running this script. Bot does not touch VPN.
      Human must enter 2FA code and press ENTER after each agent's login.
      Auth failures do NOT retry (CLAUDE.md §7).

R1: Parse "Total results: N" from label.pr-5 after applying Medical filter.
R2: Select Terminated + date filter (>= period_start) → Export Filtered → parse file.
    member_dob = null always — not available in Cigna export (CLAUDE.md §8.12).
    detection_method = "portal_export"

agent_name: ALWAYS from agents.yaml — never from portal column (CLAUDE.md §7).
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import re
import sys
import time
from calendar import monthrange
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
        format="%(asctime)s | cigna | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )

log = logging.getLogger(__name__)

# ─── Column constants — must match config/config.yaml carriers.cigna.columns ──

COL_TERM_DATE   = "Termination Date"
COL_FIRST_NAME  = "Primary First Name"
COL_LAST_NAME   = "Primary Last Name"
COL_POLICY_NUM  = "Subscriber ID (Detail Case #)"
COL_STATE       = "State"

# ─── Selectors — must match config/config.yaml carriers.cigna.selectors ───────

PORTAL_URL              = "https://cignaforbrokers.com/public/login"
SEL_USERNAME            = "[data-test-id='username']"
SEL_PASSWORD            = "[data-test-id='password']"
SEL_LOGIN_BTN           = "button[type='submit']"
# 2FA selectors — tried in order until one resolves (portal changes between sessions)
_PASSCODE_CANDIDATES = [
    "input[formcontrolname='passcode']",
    "input[data-test-id='passcode']",
    "input[type='text'][name='passcode']",
    "input[placeholder*='code' i]",
]
SEL_PASSCODE_SUBMIT     = "button[type='submit']"
SEL_PRIVATE_NET_CONT    = "text=Continue"
SEL_INDIVIDUAL_FAMILY   = "[data-test-id='left-nav-menu'] >> text=Individual and Family"
SEL_BOOK_OF_BUSINESS    = "text=Book of Business"
SEL_TOTAL_RESULTS       = "label.pr-5"
SEL_EXPORT_FILTERED     = "button >> text='Export Filtered'"

# ─── Date scoping — CLAUDE.md §6 ─────────────────────────────────────────────

def calculate_period_start(today: date = None) -> date:
    """
    Returns the last day of the previous month.

    Rationale: Molina and Oscar stamp all terminations at end-of-month.
    Anchoring to last day of previous month captures everything correctly.
    Dedup key (carrier, policy_number, coverage_end_date) prevents
    double-counting across runs. See CLAUDE.md §6.
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
    agents = cfg.get("cigna")
    if not agents:
        raise ValueError(
            "No cigna: key found in config/agents.yaml.\n"
            "Add a cigna: section with name/user/pass for each agent."
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
        "carrier":          "Cigna",
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
        "carrier":          "Cigna",
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
    Terminated rows where Termination Date >= period_start.
    member_dob is always None — not available in Cigna export (CLAUDE.md §8.12).
    """
    period_start = calculate_period_start()

    df = df.copy()
    df[COL_TERM_DATE] = pd.to_datetime(df[COL_TERM_DATE], errors="coerce")
    df = df.dropna(subset=[COL_TERM_DATE])
    df = df[df[COL_TERM_DATE].dt.date >= period_start]

    records = []
    for _, row in df.iterrows():
        member_name = " ".join(
            filter(None, [
                str(row.get(COL_FIRST_NAME, "") or "").strip(),
                str(row.get(COL_LAST_NAME, "") or "").strip(),
            ])
        )
        records.append({
            "run_date":          run_date,
            "carrier":           "Cigna",
            "agent_name":        agent_name,
            "member_name":       member_name,
            "member_dob":        None,          # not in Cigna export — permanent (CLAUDE.md §8.12)
            "state":             str(row.get(COL_STATE, "") or "").strip(),
            "coverage_end_date": row[COL_TERM_DATE].strftime("%Y-%m-%d"),
            "policy_number":     str(row.get(COL_POLICY_NUM, "") or "").strip(),
            "last_status":       "Terminated",
            "detection_method":  "portal_export",
        })
    return records


def _parse_export_file(file_path: Path) -> pd.DataFrame:
    """Read XLSX or CSV by extension."""
    if file_path.suffix.lower() == ".csv":
        return pd.read_csv(file_path)
    return pd.read_excel(file_path, engine="openpyxl")


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
    ]
    new_df = new_df[[c for c in cols if c in new_df.columns]]

    if output_path.exists():
        existing_df = pd.read_excel(output_path, engine="openpyxl")
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined_df = new_df

    combined_df = combined_df.drop_duplicates(
        subset=["carrier", "policy_number", "coverage_end_date"],
        keep="first",
    )

    try:
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            combined_df.to_excel(writer, index=False, sheet_name="Deactivated Members")
        log.info(f"[Cigna] deactivated_members.xlsx updated — {len(combined_df)} total rows")
    except Exception as exc:
        log.warning(f"[Cigna] XLSX write failed — {exc}. Continuing.")


# ─── R1 XLSX writer ───────────────────────────────────────────────────────────

def _write_cigna_xlsx(r1_records: list[dict]) -> None:
    """Write R1 records to data/output/cigna_all_agents.xlsx (overwrite each run)."""
    if not r1_records:
        return

    output_path = ROOT / "data" / "output" / "cigna_all_agents.xlsx"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    cols = ["agent_name", "active_members", "run_date", "run_type", "status"]
    df = pd.DataFrame(r1_records)
    df = df[[c for c in cols if c in df.columns]]
    df = df.sort_values("active_members", ascending=False)

    try:
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Active Members")
        log.info(f"[Cigna] XLSX written → {output_path} ({len(df)} agents)")
    except Exception as exc:
        log.warning(f"[Cigna] cigna_all_agents.xlsx write failed — {exc}. Continuing.")


# ─── State file ───────────────────────────────────────────────────────────────

def _update_state_file(run_date: str) -> None:
    """Write run date to state file. Called ONLY on full success (CLAUDE.md §10)."""
    state_path = ROOT / "data" / "state" / "cigna_last_run_date.txt"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(run_date, encoding="utf-8")
    log.info(f"[Cigna] State file updated → {run_date}")


# ─── Single-agent Playwright flow ─────────────────────────────────────────────

async def _run_single_agent(
    agent: dict,
    dry_run: bool,
    run_date: str,
    run_type: str,
    headless: bool,
) -> tuple[dict, list[dict]]:
    """
    Full browser flow for one Cigna agent.
    Returns (r1_record, r2_records).
    Raises on unrecoverable error — caller wraps in try/except.
    """
    from playwright.async_api import async_playwright

    agent_name = agent["name"]
    t_start = time.monotonic()

    dl_dir = (
        ROOT / "data" / "raw" / "cigna"
        / run_date[:7]
        / run_date
        / agent_name.replace(" ", "_")
    )
    dl_dir.mkdir(parents=True, exist_ok=True)

    period_start = calculate_period_start()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()

        # ── Login (retry on page-load failure, NOT on auth failure) ───────────
        backoffs = [5, 15, 45]
        last_exc: Exception | None = None

        for attempt, backoff in enumerate(backoffs, start=1):
            try:
                await page.goto(PORTAL_URL)
                await page.fill(SEL_USERNAME, agent["user"])
                await page.fill(SEL_PASSWORD, agent["pass"])
                await page.click(SEL_LOGIN_BTN)
                last_exc = None
                break
            except Exception as exc:
                last_exc = exc
                log.warning(
                    f"[Cigna] {agent_name}: login attempt {attempt}/3 failed — {exc}"
                )
                if attempt < len(backoffs):
                    await asyncio.sleep(backoff)

        if last_exc:
            await browser.close()
            raise last_exc

        # ── 2FA — human enters code from email/phone ───────────────────────────
        print(f"\n[Cigna] Agent: {agent_name}")
        print(f"[Cigna] Check email or phone for the 2FA code.")
        code = input(f"[Cigna] {agent_name} — Enter the 2FA code and press ENTER: ").strip()

        # Fill 2FA code — try each candidate selector until one resolves
        _filled = False
        for _sel in _PASSCODE_CANDIDATES:
            try:
                _loc = page.locator(_sel).first
                await _loc.wait_for(timeout=3000)
                await _loc.fill(code)
                await page.click(SEL_PASSCODE_SUBMIT)
                log.info(f"[Cigna] {agent_name}: 2FA filled via selector '{_sel}'")
                _filled = True
                break
            except Exception:
                continue
        if not _filled:
            log.warning(
                f"[Cigna] {agent_name}: 2FA input not found with any candidate selector. "
                "Session may already be authenticated — continuing."
            )

        # ── Private network popup — auto-dismiss ───────────────────────────────
        try:
            await page.locator(SEL_PRIVATE_NET_CONT).click(timeout=5000)
            log.info(f"[Cigna] {agent_name}: private network popup dismissed")
        except Exception:
            pass  # popup not present — normal flow

        # ── Navigate to Individual and Family ─────────────────────────────────
        await page.click(SEL_INDIVIDUAL_FAMILY)

        # ── Navigate to Book of Business ──────────────────────────────────────
        await page.click(SEL_BOOK_OF_BUSINESS)

        # Wait for BOB dashboard — total results label signals the page is ready
        await page.locator(SEL_TOTAL_RESULTS).first.wait_for()

        # ── R1: semi-manual filter → read count ──────────────────────────────────
        # Angular SPA does not register programmatic filter changes (CLAUDE.md §8.15).
        # Human applies Medical filter; bot reads the count label.

        print(f"\n[Cigna — {agent_name}] R1 FILTER — do this manually in the browser:")
        print("  1. Click the 'Filter' button")
        print("  2. Ensure only 'Medical' is checked under Product Type")
        print("  3. Ensure 'Active' is checked under Policy Status")
        print("  4. Click Apply and wait for results to load")
        print("  5. Press ENTER when ready to capture the count")
        input()

        total_text = await page.locator(SEL_TOTAL_RESULTS).first.inner_text(timeout=10000)
        match = re.search(r"([\d,]+)", total_text)
        if not match:
            raise ValueError(
                f"[Cigna] {agent_name}: could not parse R1 count from '{total_text}'"
            )
        active_members = int(match.group(1).replace(",", ""))
        log.info(f"[Cigna] {agent_name}: R1 active_members={active_members} (raw: '{total_text}')")

        duration_r1 = time.monotonic() - t_start
        r1 = _build_r1_record(agent_name, active_members, run_date, run_type, duration_r1)

        # ── R2: semi-manual filter → Export ──────────────────────────────────────
        # Angular SPA does not register programmatic checkbox changes — automated
        # filter attempts all produced Active rows in the export (CLAUDE.md §8.15).
        # Human applies the filter; bot reads the count label and exports.

        period_start_str = period_start.strftime("%m/%d/%Y")
        print(f"\n[Cigna — {agent_name}] R2 FILTER — do this manually in the browser:")
        print("  1. Click the 'Filter' button")
        print("  2. Uncheck 'Active', check 'Terminated'")
        print(f"  3. Set Termination Date: 'on and after' → {period_start_str}")
        print("  4. Click Apply and wait for results to load")
        print("  5. Press ENTER when ready")
        input()

        # Read R2 count — if 0, skip export entirely and return early.
        try:
            r2_count_text = await page.locator(SEL_TOTAL_RESULTS).first.inner_text(timeout=10000)
            r2_count = int("".join(filter(str.isdigit, r2_count_text))) if any(c.isdigit() for c in r2_count_text) else -1
            log.info(f"[Cigna] {agent_name}: R2 count label (after manual Apply): '{r2_count_text.strip()}'")
        except Exception:
            r2_count = -1  # unknown — attempt export anyway
            log.warning(f"[Cigna] {agent_name}: R2 count label not readable — attempting export")

        if r2_count == 0:
            log.info(f"[Cigna] {agent_name}: R2 count = 0 — no terminated members this period, skipping export")
            await browser.close()
            log.info(
                f"[Cigna] {agent_name}: R1 active={r1['active_members']} "
                f"R2 records=0 period_start={period_start}"
            )
            return r1, []

        # r2_count > 0 or unknown — check button state before attempting download.
        # Portal disables Export Filtered when results = 0, causing download timeout.
        try:
            export_btn = page.locator(SEL_EXPORT_FILTERED)
            await export_btn.wait_for(state="visible", timeout=5000)
            is_disabled = await export_btn.is_disabled()
            if is_disabled:
                log.info(f"[Cigna] {agent_name}: Export button disabled — 0 results, skipping export")
                await browser.close()
                log.info(
                    f"[Cigna] {agent_name}: R1 active={r1['active_members']} "
                    f"R2 records=0 period_start={period_start}"
                )
                return r1, []
        except Exception:
            log.info(f"[Cigna] {agent_name}: Export button not found — 0 results, skipping export")
            await browser.close()
            log.info(
                f"[Cigna] {agent_name}: R1 active={r1['active_members']} "
                f"R2 records=0 period_start={period_start}"
            )
            return r1, []

        # Button is visible and enabled — proceed with download
        async with page.expect_download() as dl_info:
            await export_btn.click()
        download = await dl_info.value
        export_path = dl_dir / download.suggested_filename
        await download.save_as(export_path)
        log.info(f"[Cigna] {agent_name}: downloaded {download.suggested_filename} → {export_path}")

        await browser.close()

    # ── Parse export file → R2 records ────────────────────────────────────────
    df = _parse_export_file(export_path)

    # Log export content — confirms Terminated filter was applied correctly
    _status_col = next((c for c in df.columns if "status" in c.lower()), None)
    log.info(
        f"[Cigna] {agent_name}: export rows={len(df)}  "
        f"Policy Status values={df[_status_col].value_counts().to_dict() if _status_col else 'column not found'}"
    )
    log.info(f"[Cigna] {agent_name}: export columns={list(df.columns)}")

    required = {COL_TERM_DATE, COL_POLICY_NUM}
    missing = required - set(df.columns)
    if missing:
        log.warning(
            f"[Cigna] {agent_name}: export missing expected columns: {missing}. "
            f"Columns present: {list(df.columns)}"
        )
        if required - set(df.columns):
            raise ValueError(f"Cannot process export — missing critical columns: {missing}")

    r2 = _build_r2_records(df, agent_name, run_date)

    log.info(
        f"[Cigna] {agent_name}: R1 active={r1['active_members']} "
        f"R2 records={len(r2)} period_start={period_start}"
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
    all_r1: list[dict] = []
    all_r2: list[dict] = []

    for agent in agents:
        r1_record: dict | None = None
        try:
            r1_record, r2 = await _run_single_agent(
                agent, dry_run, run_date, run_type, headless
            )
            all_r1.append(r1_record)
            all_r2.extend(r2)
        except Exception as exc:
            log.error(f"[Cigna] {agent['name']}: unhandled error — {exc}", exc_info=True)
            # Preserve R1 if it was captured before the crash; fall back to failed record
            if r1_record is not None:
                all_r1.append(r1_record)
            else:
                all_r1.append(_failed_r1(agent["name"], run_date, run_type, str(exc)))

    success_count = sum(1 for r in all_r1 if r["status"] == "success")

    if not dry_run:
        _append_deactivated_xlsx(all_r2)
        _write_cigna_xlsx(all_r1)
        # State file — only on full success (CLAUDE.md §10)
        if success_count == len(agents):
            _update_state_file(run_date)
        else:
            log.warning(
                f"[Cigna] {success_count}/{len(agents)} agents succeeded — "
                "state file NOT updated (partial run)"
            )
        log.info(
            f"[Cigna] Run complete — {success_count}/{len(agents)} agents succeeded, "
            f"{len(all_r2)} R2 records written"
        )
    else:
        log.info("[Cigna] DRY RUN — XLSX write and state update skipped")
        _print_dry_run_summary(all_r1, all_r2)

    return all_r1, all_r2


def _print_dry_run_summary(r1_records: list[dict], r2_records: list[dict]) -> None:
    print("\n── Cigna DRY RUN summary ────────────────────────────────")
    for r in r1_records:
        status = "✓" if r["status"] == "success" else "✗"
        print(f"  {status} {r['agent_name']:25s}  active={r['active_members']}")
    print(f"\n  R2 records this period: {len(r2_records)}")
    if r2_records:
        period_start = calculate_period_start()
        print(f"  Period start: {period_start}  (member_dob always null for Cigna)")
        for rec in r2_records:
            print(
                f"    {rec['agent_name']:20s}  {rec['member_name']:25s}  "
                f"end={rec['coverage_end_date']}  policy={rec['policy_number']}"
            )
    else:
        print(f"  (0 R2 records for period starting {calculate_period_start()})")
    print("─────────────────────────────────────────────────────────\n")


# ─── Public sync wrapper ──────────────────────────────────────────────────────

def run_cigna(dry_run: bool = False) -> tuple[list[dict], list[dict]]:
    """
    Sync wrapper — called by main.py in Phase 8.
    Returns (r1_records, r2_records).
    VPN must be active before calling.
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
        description="Phase 5 — Cigna Playwright bot",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  python scripts/cigna_bot.py\n"
            "    → all agents, full browser flow, writes XLSX\n\n"
            "  python scripts/cigna_bot.py --dry-run\n"
            "    → all agents, full browser flow, no XLSX write, no state update\n\n"
            "  python scripts/cigna_bot.py --agent 0\n"
            "    → single agent (Anthony Montenegro), writes XLSX\n\n"
            "  python scripts/cigna_bot.py --agent 0 --dry-run\n"
            "    → single agent, no writes\n\n"
            "  python scripts/cigna_bot.py --headless --dry-run\n"
            "    → headless browser (2FA prompt still appears in terminal)\n\n"
            "NOTE: Activate VPN BEFORE running this script."
        ),
    )
    parser.add_argument(
        "--agent", type=int, default=None,
        help="Run only agent at index N (0-based) from agents.yaml cigna: key",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Full browser flow; no XLSX write, no state update",
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
                f"(0–{len(agents) - 1} available)"
            )
            sys.exit(1)
        agents = [agents[args.agent]]
        log.info(f"[Cigna] Single-agent mode: {agents[0]['name']}")

    asyncio.run(
        _run_all_agents_async(agents, args.dry_run, run_date, run_type, args.headless)
    )
