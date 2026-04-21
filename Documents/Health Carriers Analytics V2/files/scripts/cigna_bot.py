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
from datetime import date
from pathlib import Path

import pandas as pd
import yaml
from dotenv import load_dotenv

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

# ─── Column constants — pulled from config.yaml carriers.cigna.columns ────────

_CFG_PATH = ROOT / "config" / "config.yaml"
with open(_CFG_PATH, encoding="utf-8") as _f:
    _CFG = yaml.safe_load(_f)
_COLS = _CFG["carriers"]["cigna"]["columns"]
COL_TERM_DATE   = _COLS["termination_date"]
COL_FIRST_NAME  = _COLS["first_name"]
COL_LAST_NAME   = _COLS["last_name"]
COL_POLICY_NUM  = _COLS["policy_number"]
COL_STATE       = _COLS["state"]

# ─── Selectors — pulled from config.yaml carriers.cigna.selectors ─────────────

_CIG  = _CFG["carriers"]["cigna"]
_CSEL = _CIG["selectors"]
PORTAL_URL              = _CIG["portal_url"]
SEL_USERNAME            = _CSEL["username"]
SEL_PASSWORD            = _CSEL["password"]
SEL_LOGIN_BTN           = _CSEL["login_button"]
# 2FA selectors — tried in order until one resolves (portal changes between sessions)
_PASSCODE_CANDIDATES = [
    "input[formcontrolname='passcode']",
    "input[data-test-id='passcode']",
    "input[type='text'][name='passcode']",
    "input[placeholder*='code' i]",
]
SEL_PASSCODE_SUBMIT     = _CSEL["passcode_submit"]
SEL_PRIVATE_NET_CONT    = _CSEL["private_network_continue"]
SEL_INDIVIDUAL_FAMILY   = _CSEL["individual_family"]
SEL_BOOK_OF_BUSINESS    = _CSEL["book_of_business"]
SEL_TOTAL_RESULTS       = _CSEL["total_results_label"]
SEL_EXPORT              = _CSEL["export_filtered"]


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
    period_start = get_r2_start_date()

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

    period_start = get_r2_start_date()

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

        try:
            async with page.expect_download(timeout=8000) as dl_info:
                await page.locator(SEL_EXPORT).click()
                # Dismiss error modal if it appears
                try:
                    await page.locator("button:has-text('OK')").click(timeout=3000)
                except:
                    pass
            download = await dl_info.value
            export_path = dl_dir / download.suggested_filename
            await download.save_as(export_path)
            log.info(f"[Cigna] {agent_name}: downloaded {download.suggested_filename} → {export_path}")
        except Exception:
            log.info("[Cigna] %s: export timed out or portal error — R2 = 0", agent_name)
            return r1, []

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
        try:
            r1_record, r2_records = await _run_single_agent(
                agent, dry_run, run_date, run_type, headless
            )
        except Exception as exc:
            log.error(f"[Cigna] {agent['name']}: unhandled error — {exc}", exc_info=True)
            r1_record = _failed_r1(agent["name"], run_date, run_type, str(exc))
            r2_records = []

        if r1_record:
            all_r1.append(r1_record)
        all_r2.extend(r2_records)

    success_count = sum(1 for r in all_r1 if r["status"] == "success")

    if not dry_run:
        success_r1 = [r for r in all_r1 if r["status"] == "success"]
        write_r1_xlsx(success_r1, "Cigna", log)
        append_deactivated_xlsx(all_r2, "Cigna", log)
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
        period_start = get_r2_start_date()
        print(f"  Period start: {period_start}  (member_dob always null for Cigna)")
        for rec in r2_records:
            print(
                f"    {rec['agent_name']:20s}  {rec['member_name']:25s}  "
                f"end={rec['coverage_end_date']}  policy={rec['policy_number']}"
            )
    else:
        print(f"  (0 R2 records for period starting {get_r2_start_date()})")
    print("─────────────────────────────────────────────────────────\n")


# ─── Public sync wrapper ──────────────────────────────────────────────────────

def run_cigna(
    dry_run: bool = False,
    agent_filter: int | None = None,
    headless: bool = False,
) -> tuple[list[dict], list[dict]]:
    """
    Public API. Called by launcher or standalone.
    Returns (r1_records, r2_records).
    VPN must be active before calling.
    """
    global log
    log = setup_logging("CIGNA")

    agents   = _load_agents()
    run_date = date.today().isoformat()
    rt       = run_type()

    if agent_filter is not None:
        if agent_filter >= len(agents):
            raise IndexError(
                f"--agent {agent_filter} out of range "
                f"(0–{len(agents) - 1} available)"
            )
        agents = [agents[agent_filter]]
        log.info(f"[Cigna] Single-agent mode: {agents[0]['name']}")

    return asyncio.run(
        _run_all_agents_async(agents, dry_run, run_date, rt, headless)
    )


# ─── Standalone CLI ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Cigna Playwright bot",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="NOTE: Activate VPN BEFORE running this script.",
    )
    parser.add_argument("--agent", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--headless", action="store_true")
    args = parser.parse_args()

    run_cigna(dry_run=args.dry_run, agent_filter=args.agent, headless=args.headless)
