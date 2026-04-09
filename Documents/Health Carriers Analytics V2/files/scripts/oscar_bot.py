"""
scripts/oscar_bot.py — Phase 4
Oscar For Business: Playwright browser automation — R1 (active members) + R2 (deactivated).

Public API (called by main.py in Phase 8):
    run_oscar(dry_run=False) → (list[dict], list[dict])   # (r1_records, r2_records)

Standalone usage:
    python scripts/oscar_bot.py                    # all agents, writes XLSX
    python scripts/oscar_bot.py --dry-run          # all agents, no writes
    python scripts/oscar_bot.py --agent 0          # single agent, writes XLSX
    python scripts/oscar_bot.py --agent 0 --dry-run
    python scripts/oscar_bot.py --headless --dry-run

Auth: user+pass then MS Authenticator MFA on boss's phone — SEMI-AUTO.
      Human must press ENTER after each agent's MFA approval.
      Auth failures do NOT retry (CLAUDE.md §7).

R1: sum of Lives column where Policy status != "inactive"
R2: Inactive rows where Coverage end date >= calculate_period_start()
    detection_method = "file_extract"
    policy_number field = Member ID (Oscar has no policy number)

agent_name: ALWAYS from agents.yaml — never from CSV (CLAUDE.md §7).
"""

from __future__ import annotations

import argparse
import asyncio
import logging
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
        format="%(asctime)s | oscar | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )

log = logging.getLogger(__name__)

# ─── Column names — must match config/config.yaml carriers.oscar.columns ──────

COL_LIVES  = "Lives"
COL_STATUS = "Policy status"
COL_NAME   = "Member name"
COL_ID     = "Member ID"
COL_DOB    = "Date of birth"
COL_STATE  = "State"
COL_END    = "Coverage end date"

# ─── Selectors ────────────────────────────────────────────────────────────────

PORTAL_URL       = "https://accounts.hioscar.com/account/login/"
SEL_EMAIL        = "input[name='email']"
SEL_PASSWORD     = "input[name='password']"
SEL_SUBMIT       = "button[type='submit'].o-loginButton"
SEL_APP_LINK = "text=Oscar For Business"
SEL_INDIV_BOOK   = "text=Individual book"
SEL_EXPORT_CSV   = "button:has-text('Export CSV')"

# ─── Date scoping — verbatim from CLAUDE.md §6 ───────────────────────────────

def calculate_period_start(today: date = None) -> date:
    if today is None:
        today = date.today()
    return today.replace(day=1) - timedelta(days=1)


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
    agents = cfg.get("oscar")
    if not agents:
        raise ValueError(
            "No oscar: key found in config/agents.yaml.\n"
            "Add an oscar: section with name/user/pass for each agent."
        )
    return agents


# ─── R1 / R2 builders ─────────────────────────────────────────────────────────

def _build_r1_record(
    df: pd.DataFrame,
    agent_name: str,
    run_date: str,
    run_type: str,
    duration: float,
) -> dict:
    """R1 schema — CLAUDE.md §5. Counts Lives where Policy status != 'inactive'."""
    active_df = df[df[COL_STATUS].str.strip().str.lower() != "inactive"]
    return {
        "run_date":         run_date,
        "run_type":         run_type,
        "carrier":          "Oscar",
        "agent_name":       agent_name,
        "active_members":   int(active_df[COL_LIVES].sum()),
        "status":           "success",
        "error_message":    None,
        "duration_seconds": round(duration, 1),
    }


def _build_r2_records(
    df: pd.DataFrame,
    agent_name: str,
    run_date: str,
) -> list[dict]:
    """R2 schema — CLAUDE.md §5. Inactive rows within current period window."""
    period_start = calculate_period_start()

    inactive_df = df[df[COL_STATUS].str.strip().str.lower() == "inactive"].copy()

    inactive_df[COL_END] = pd.to_datetime(inactive_df[COL_END], errors="coerce")
    inactive_df = inactive_df.dropna(subset=[COL_END])
    inactive_df = inactive_df[inactive_df[COL_END].dt.date >= period_start]

    records = []
    for _, row in inactive_df.iterrows():
        records.append({
            "run_date":          run_date,
            "carrier":           "Oscar",
            "agent_name":        agent_name,
            "member_name":       str(row.get(COL_NAME, "")),
            "member_dob":        str(row.get(COL_DOB, "")),
            "state":             str(row.get(COL_STATE, "")),
            "coverage_end_date": row[COL_END].strftime("%Y-%m-%d"),
            "policy_number":     str(row.get(COL_ID, "")),
            "last_status":       "Inactive",
            "detection_method":  "file_extract",
        })
    return records


def _failed_r1(
    agent_name: str,
    run_date: str,
    run_type: str,
    error_msg: str,
) -> dict:
    return {
        "run_date":         run_date,
        "run_type":         run_type,
        "carrier":          "Oscar",
        "agent_name":       agent_name,
        "active_members":   0,
        "status":           "failed",
        "error_message":    error_msg,
        "duration_seconds": 0.0,
    }


# ─── CSV processing ───────────────────────────────────────────────────────────

def _process_csv(
    csv_path: Path,
    agent_name: str,
    run_date: str,
    run_type: str,
    duration: float,
) -> tuple[dict, list[dict]]:
    """Parse downloaded CSV → (r1_record, r2_records)."""
    df = pd.read_csv(csv_path)

    required = {COL_STATUS, COL_LIVES}
    missing = required - set(df.columns)
    if missing:
        log.warning(
            f"[Oscar] {agent_name}: CSV missing expected columns: {missing}. "
            f"Columns present: {list(df.columns)}"
        )
        if COL_STATUS not in df.columns or COL_LIVES not in df.columns:
            raise ValueError(f"Cannot process CSV — missing critical columns: {missing}")

    r1 = _build_r1_record(df, agent_name, run_date, run_type, duration)
    r2 = _build_r2_records(df, agent_name, run_date)
    return r1, r2


# ─── XLSX append ──────────────────────────────────────────────────────────────

def _append_deactivated_xlsx(r2_records: list[dict]) -> None:
    """
    Append R2 records to shared deactivated_members.xlsx.
    Creates file if absent. Never overwrites existing rows.
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
    cols = [c for c in cols if c in new_df.columns]
    new_df = new_df[cols]

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
    except Exception as exc:
        log.warning(f"[Oscar] XLSX write failed — {exc}. Continuing.")


# ─── R1 XLSX writer ───────────────────────────────────────────────────────────

def _write_oscar_xlsx(r1_records: list[dict]) -> None:
    """Write R1 records to data/output/oscar_all_agents.xlsx (overwrite each run)."""
    if not r1_records:
        return

    output_path = ROOT / "data" / "output" / "oscar_all_agents.xlsx"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    cols = ["agent_name", "active_members", "run_date", "run_type", "status"]
    df = pd.DataFrame(r1_records)
    df = df[[c for c in cols if c in df.columns]]
    df = df.sort_values("active_members", ascending=False)

    try:
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Active Members")
        log.info(f"[Oscar] XLSX written → {output_path} ({len(df)} agents)")
    except Exception as exc:
        log.warning(f"[Oscar] oscar_all_agents.xlsx write failed — {exc}. Continuing.")


# ─── Modal dismissal ──────────────────────────────────────────────────────────

async def _dismiss_modals(page) -> None:
    """
    Dismiss any Oscar UI modals using a probe loop.
    Handles: Welcome ("Done"), Broker Book Score ("Next" → "Continue to Book of Business"),
    and any Close button. Loop cap = 10 to prevent infinite spin.

    wait_for_timeout(500) here is the ONE approved exception to the no-timeout rule
    (CLAUDE.md §7) — waits for CSS modal animation after close, no DOM event to await.
    """
    dismiss_selectors = [
        "button:has-text('Done')",
        "button:has-text('Continue to Book of Business')",
        "button:has-text('Next')",
        "button[aria-label='Close']",
    ]
    for _ in range(10):
        dismissed = False
        for sel in dismiss_selectors:
            try:
                btn = page.locator(sel).first
                if await btn.is_visible():
                    await btn.click()
                    await page.wait_for_timeout(500)  # CSS animation settle — approved exception
                    dismissed = True
                    break
            except Exception:
                continue
        if not dismissed:
            break


# ─── Single-agent Playwright flow ─────────────────────────────────────────────

async def _run_single_agent(
    agent: dict,
    dry_run: bool,
    run_date: str,
    run_type: str,
    headless: bool,
) -> tuple[dict, list[dict]]:
    """
    Full browser + CSV flow for one Oscar agent.
    Returns (r1_record, r2_records).
    Raises on unrecoverable error — caller wraps in try/except.
    """
    from playwright.async_api import async_playwright

    agent_name = agent["name"]
    t_start = time.monotonic()

    dl_dir = (
        ROOT / "data" / "raw" / "oscar"
        / run_date[:7]
        / run_date
        / agent_name.replace(" ", "_")
    )
    dl_dir.mkdir(parents=True, exist_ok=True)

    # ── Login with retry (max 3 attempts, backoff 5s/15s/45s) ─────────────────
    # Auth failures do NOT retry — only page-load / navigation failures do.
    backoffs = [5, 15, 45]
    last_exc: Exception | None = None

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(
            accept_downloads=True,
        )

        page = await context.new_page()

        # Retry loop for login page load
        for attempt, backoff in enumerate(backoffs, start=1):
            try:
                await page.goto(PORTAL_URL)
                await page.fill(SEL_EMAIL, agent["user"])
                await page.fill(SEL_PASSWORD, agent["pass"])
                await page.click(SEL_SUBMIT)
                last_exc = None
                break
            except Exception as exc:
                last_exc = exc
                log.warning(
                    f"[Oscar] {agent_name}: login attempt {attempt}/3 failed — {exc}"
                )
                if attempt < len(backoffs):
                    await asyncio.sleep(backoff)

        if last_exc:
            await browser.close()
            raise last_exc

        # ── MFA pause — human approves MS Authenticator ───────────────────────
        print(f"\n[Oscar] Agent: {agent_name}")
        print("[Oscar] Approve Microsoft Authenticator on the boss's phone, then press ENTER...")
        input()

        if "accounts.hioscar.com" in page.url:
            log.warning(
                f"[Oscar] {agent_name}: still on login page after MFA — "
                "MFA may not have been approved. Continuing anyway."
            )

        # ── Dismiss post-login modals ─────────────────────────────────────────
        await _dismiss_modals(page)

        # ── Navigate to Oscar For Business app ────────────────────────────────
        await page.click(SEL_APP_LINK)
        await page.wait_for_url("**/business.hioscar.com/**")

        # ── Dismiss post-app-select modals ────────────────────────────────────
        await _dismiss_modals(page)

        # ── Individual Book ───────────────────────────────────────────────────
        await page.click(SEL_INDIV_BOOK)
        await page.wait_for_url("**/book/ivl**", timeout=15000)

        # ── Dismiss post-navigation modals ────────────────────────────────────
        await _dismiss_modals(page)

        # ── Export CSV ────────────────────────────────────────────────────────
        async with page.expect_download() as dl_info:
            await page.click(SEL_EXPORT_CSV)
        download = await dl_info.value
        csv_path = dl_dir / download.suggested_filename
        await download.save_as(csv_path)
        log.info(f"[Oscar] {agent_name}: downloaded {download.suggested_filename} → {csv_path}")

        await browser.close()

    duration = time.monotonic() - t_start

    # ── Process CSV ───────────────────────────────────────────────────────────
    r1, r2 = _process_csv(csv_path, agent_name, run_date, run_type, duration)

    log.info(
        f"[Oscar] {agent_name}: R1 active_members={r1['active_members']} "
        f"R2 records={len(r2)} period_start={calculate_period_start()}"
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
            r1, r2 = await _run_single_agent(
                agent, dry_run, run_date, run_type, headless
            )
            all_r1.append(r1)
            all_r2.extend(r2)
        except Exception as exc:
            log.error(f"[Oscar] {agent['name']}: unhandled error — {exc}", exc_info=True)
            all_r1.append(
                _failed_r1(agent["name"], run_date, run_type, str(exc))
            )

    if not dry_run:
        _append_deactivated_xlsx(all_r2)
        _write_oscar_xlsx(all_r1)
        log.info(
            f"[Oscar] Run complete — "
            f"{sum(1 for r in all_r1 if r['status'] == 'success')}/{len(all_r1)} agents succeeded, "
            f"{len(all_r2)} R2 records written"
        )
    else:
        log.info("[Oscar] DRY RUN — XLSX write skipped")
        _print_dry_run_summary(all_r1, all_r2)

    return all_r1, all_r2


def _print_dry_run_summary(r1_records: list[dict], r2_records: list[dict]) -> None:
    print("\n── Oscar DRY RUN summary ────────────────────────────────")
    for r in r1_records:
        status = "✓" if r["status"] == "success" else "✗"
        print(f"  {status} {r['agent_name']:25s}  active={r['active_members']}")
    print(f"\n  R2 records this period: {len(r2_records)}")
    if r2_records:
        period_start = calculate_period_start()
        print(f"  Period start: {period_start}")
        for rec in r2_records:
            print(
                f"    {rec['agent_name']:20s}  {rec['member_name']:25s}  "
                f"end={rec['coverage_end_date']}"
            )
    else:
        print(f"  (0 R2 records for period starting {calculate_period_start()})")
    print("─────────────────────────────────────────────────────────\n")


# ─── Public sync wrapper ──────────────────────────────────────────────────────

def run_oscar(dry_run: bool = False) -> tuple[list[dict], list[dict]]:
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
        description="Phase 4 — Oscar Playwright bot",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  python scripts/oscar_bot.py\n"
            "    → all agents, full browser + CSV, writes XLSX\n\n"
            "  python scripts/oscar_bot.py --dry-run\n"
            "    → all agents, full browser + CSV, no XLSX write\n\n"
            "  python scripts/oscar_bot.py --agent 0\n"
            "    → single agent, writes XLSX\n\n"
            "  python scripts/oscar_bot.py --agent 0 --dry-run\n"
            "    → single agent, no XLSX write\n\n"
            "  python scripts/oscar_bot.py --headless --dry-run\n"
            "    → all agents headless (MFA prompt still appears in terminal)\n"
        ),
    )
    parser.add_argument(
        "--agent", type=int, default=None,
        help="Run only agent at index N (0-based) from agents.yaml oscar: key",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Full browser + CSV flow; no XLSX write, no state update",
    )
    parser.add_argument(
        "--headless", action="store_true",
        help="Run browser headless (default: headed so you can see MFA prompt)",
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
        log.info(f"[Oscar] Single-agent mode: {agents[0]['name']}")

    asyncio.run(
        _run_all_agents_async(agents, args.dry_run, run_date, run_type, args.headless)
    )
