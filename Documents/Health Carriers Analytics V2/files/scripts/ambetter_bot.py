"""
scripts/ambetter_bot.py
Ambetter carrier bot — R1 (Active Members) + R2 (Deactivated This Period)

Config sources (single source of truth, same as every other carrier):
  config/agents.yaml      → agents list under key 'ambetter:'
                            format: {name, user, pass}   ← same as molina
  config/config.yaml      → portal URL, selectors, column names
                            under carriers.ambetter

Architecture:
  Agent-level login — one browser session per agent (same pattern as Molina).
  16 agents defined in agents.yaml under ambetter:.

  R1 — reads 'Total Active Members' odometer on that agent's dashboard.
  
       One R1 record per agent.

  R2 — Policies → Cancelled → Export → ZIP extracted to CSV → filtered by
       Policy Term Date >= last_run_date  (current period only).
       One R2 record per cancelled member for that agent.

State file: data/state/ambetter_last_run_date.txt
  Written ONLY after all agents succeed (same rule as Molina).

Usage:
  python scripts/ambetter_bot.py                 # all agents
  python scripts/ambetter_bot.py --agent 3       # single agent, 0-based index
  python scripts/ambetter_bot.py --dry-run       # skip state file update
  python scripts/ambetter_bot.py --agent 3 --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
import zipfile
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import yaml
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from datetime import date, timedelta

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

log = logging.getLogger(__name__)

# ── Load config ───────────────────────────────────────────────────────────────
with open(ROOT / "config" / "config.yaml") as _f:
    _CFG = yaml.safe_load(_f)

with open(ROOT / "config" / "agents.yaml") as _f:
    _AGENTS_CFG = yaml.safe_load(_f)

# Carrier-level config — all under carriers.ambetter in config.yaml
_AMB          = _CFG["carriers"]["ambetter"]
PORTAL_URL    = _AMB["portal_url"]
RETRY_DELAYS  = _CFG.get("retry_delays", [5, 15, 45])
POLICIES_URL = PORTAL_URL.rstrip("/").rsplit("/s/", 1)[0] + "/s/policies"

# Selenium timeouts — from top-level selenium: block, same as Molina uses
_SEL_CFG      = _CFG.get("selenium", {})
TIMEOUT       = _SEL_CFG.get("element_timeout", 15)
DL_TIMEOUT    = _SEL_CFG.get("download_timeout", 120)

# Column names
_COLS          = _AMB["columns"]
COL_TERM_DATE  = _COLS["policy_term_date"]
COL_FIRST_NAME = _COLS["first_name"]
COL_LAST_NAME  = _COLS["last_name"]
COL_POLICY_NUM = _COLS["policy_number"]

# Selectors — read once, used throughout
_SELS            = _AMB["selectors"]
SEL_EMAIL        = _SELS["email_field"]
SEL_PASSWORD     = _SELS["password_field"]
SEL_LOGIN_BTN    = _SELS["login_button"]          # XPath — prefixed with "xpath:"
SEL_ACTIVE_DIV   = _SELS["active_members"]
SEL_POLICIES_NAV = _SELS["policies_nav"]
SEL_CANCELLED    = _SELS["cancelled_button"]
SEL_EXPORT_BTN   = _SELS["export_button"]

def calculate_period_start(today: date = None) -> date:
    if today is None:
        today = date.today()
    return today.replace(day=1) - timedelta(days=1)

def _append_deactivated_xlsx(r2_records: list[dict]) -> None:
    """
    Appends R2 records to the shared deactivated members log.
    Creates the file if it doesn't exist. Never overwrites existing rows.
    """
    if not r2_records:
        return

    output_path = ROOT / "data" / "output" / "deactivated_members.xlsx"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    new_df = pd.DataFrame(r2_records)

    # Columns to keep, in order
    cols = [
        "run_date", "carrier", "agent_name", "member_name",
        "member_dob", "state", "coverage_end_date", "policy_number",
    ]
    # Only keep columns that exist (graceful if member_dob/state missing)
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

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        combined_df.to_excel(writer, index=False, sheet_name="Deactivated Members")
        ws = writer.sheets["Deactivated Members"]
        ws.column_dimensions["A"].width = 14  # run_date
        ws.column_dimensions["B"].width = 12  # carrier
        ws.column_dimensions["C"].width = 22  # agent_name
        ws.column_dimensions["D"].width = 28  # member_name
        ws.column_dimensions["E"].width = 14  # member_dob
        ws.column_dimensions["F"].width = 8   # state
        ws.column_dimensions["G"].width = 20  # coverage_end_date
        ws.column_dimensions["H"].width = 16  # policy_number

def _resolve_selector(selector_str: str) -> tuple:
    """
    Parse selector strings that may be prefixed with 'xpath:' or 'id:',
    matching the same convention used in the Molina scripts.

    Examples:
        'xpath://button[...]'   → (By.XPATH, '//button[...]')
        'id:someId'             → (By.ID, 'someId')
        'div#odometer'          → (By.CSS_SELECTOR, 'div#odometer')
    """
    if selector_str.startswith("xpath:"):
        return By.XPATH, selector_str[len("xpath:"):]
    if selector_str.startswith("id:"):
        return By.ID, selector_str[len("id:"):]
    return By.CSS_SELECTOR, selector_str


# ── Agents ────────────────────────────────────────────────────────────────────
def _load_agents() -> list[dict]:
    """
    Load the ambetter agent list from config/agents.yaml under key 'ambetter:'.
    Expected format per entry: {name, user, pass}  — same as Molina.
    """
    agents = _AGENTS_CFG.get("ambetter", [])
    if not agents:
        raise ValueError(
            "No agents found under 'ambetter:' in config/agents.yaml.\n"
            "Add agents in the format:\n"
            "  ambetter:\n"
            "    - name: 'Agent Name'\n"
            "      user: 'email@domain.com'\n"
            "      pass: 'password'\n"
        )
    return agents


# ── State helpers ─────────────────────────────────────────────────────────────
STATE_DIR  = ROOT / "data" / "state"
STATE_FILE = STATE_DIR / "ambetter_last_run_date.txt"


def _get_last_run_date() -> date | None:
    if not STATE_FILE.exists():
        return None
    raw = STATE_FILE.read_text().strip()
    try:
        return date.fromisoformat(raw)
    except ValueError:
        log.warning("ambetter | state file malformed ('%s') — treating as first run", raw)
        return None


def _write_state(run_date: date) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(run_date.isoformat())
    log.info("ambetter | state updated → %s", run_date.isoformat())


# ── Chrome setup ──────────────────────────────────────────────────────────────
def _make_dl_dir(agent_name: str, run_date: date) -> Path:
    """
    Per-agent subdirectory prevents CSV collisions when multiple agents
    run in the same session and produce files with identical names.
    """
    safe = agent_name.lower().replace(" ", "_")
    dl = (ROOT / "data" / "raw" / "ambetter"
          / run_date.strftime("%Y-%m")
          / run_date.strftime("%Y-%m-%d")
          / safe)
    dl.mkdir(parents=True, exist_ok=True)
    return dl


def _build_driver(download_dir: Path) -> webdriver.Chrome:
    opts = Options()
    opts.add_argument("--start-maximized")
    opts.add_argument("--disable-notifications")
    opts.add_experimental_option("prefs", {
        "download.default_directory":   str(download_dir),
        "download.prompt_for_download": False,
        "download.directory_upgrade":   True,
        "safebrowsing.enabled":         True,
    })
    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=opts,
    )


# ── Selenium wait helpers ─────────────────────────────────────────────────────
def _wait(driver, selector_str: str, t: int | None = None):
    by, val = _resolve_selector(selector_str)
    return WebDriverWait(driver, t or TIMEOUT).until(
        EC.presence_of_element_located((by, val))
    )


def _clickable(driver, selector_str: str, t: int | None = None):
    by, val = _resolve_selector(selector_str)
    return WebDriverWait(driver, t or TIMEOUT).until(
        EC.element_to_be_clickable((by, val))
    )


# ── ZIP extraction ────────────────────────────────────────────────────────────
def _wait_for_download(dl_dir: Path) -> Path:
    """Block until a complete (non-.crdownload) file appears in dl_dir."""
    deadline = time.time() + DL_TIMEOUT
    while time.time() < deadline:
        candidates = [
            f for f in dl_dir.iterdir()
            if f.is_file() and f.suffix.lower() not in (".crdownload", ".tmp")
        ]
        if candidates:
            return max(candidates, key=lambda p: p.stat().st_mtime)
        time.sleep(1)
    raise TimeoutError(
        f"ambetter | download did not complete within {DL_TIMEOUT}s"
    )


def _extract_zip(zip_path: Path, dest_dir: Path) -> Path:
    """
    Extract the first CSV from the ZIP, then delete the ZIP.
    Ambetter packages exports as single-CSV ZIPs. Deleting the ZIP
    prevents accumulation of duplicates in the archive folder.
    """
    with zipfile.ZipFile(zip_path, "r") as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise ValueError(
                f"ambetter | ZIP contains no CSV: {zip_path.name}\n"
                f"  ZIP contents: {zf.namelist()}"
            )
        extracted = zf.extract(csv_names[0], dest_dir)
    zip_path.unlink()
    log.info("ambetter | extracted '%s' — ZIP deleted", csv_names[0])
    return Path(extracted)


# ── Per-agent portal flows ────────────────────────────────────────────────────
def _login(driver: webdriver.Chrome, agent: dict) -> None:
    driver.get(PORTAL_URL)

    email_el = _clickable(driver, SEL_EMAIL)
    email_el.clear()
    email_el.send_keys(agent["user"])

    pwd_el = _clickable(driver, SEL_PASSWORD)
    pwd_el.clear()
    pwd_el.send_keys(agent["pass"])

    _clickable(driver, SEL_LOGIN_BTN).click()
    log.info("ambetter | [%s] credentials submitted", agent["name"])

    # Dashboard load confirmed when the odometer element is present
    try:
        _wait(driver, SEL_ACTIVE_DIV)
        log.info("ambetter | [%s] authenticated — dashboard loaded", agent["name"])
    except Exception:
        time.sleep(5)
        _wait(driver, SEL_ACTIVE_DIV, t=20)
        log.info("ambetter | [%s] authenticated (slow) — dashboard loaded", agent["name"])


def _scrape_r1(driver: webdriver.Chrome, agent_name: str,
               run_date: date, run_type: str) -> dict:
    el    = _wait(driver, SEL_ACTIVE_DIV)
    count = int(el.text.strip().replace(",", ""))
    log.info("ambetter | [%s] R1 = %d active members", agent_name, count)
    return {
        "run_date":         run_date.isoformat(),
        "run_type":         run_type,
        "carrier":          "Ambetter",
        "agent_name":       agent_name,
        "active_members":   count,
        "status":           "success",
        "error_message":    None,
        "duration_seconds": None,   # filled after full agent run
    }


def _download_cancelled_csv(
    driver: webdriver.Chrome, dl_dir: Path, agent_name: str
) -> Path:
    """
    Two-step Salesforce Visualforce export.

    Step 1: GET the export trigger URL — returns HTML containing a base64
            data URI for the ZIP file embedded directly in the <a> tag.
            Format: href="data:application/zip;base64,<PAYLOAD>"

    Step 2: Decode the base64 payload and write to disk. No second HTTP
            request — the file is fully embedded in the modal HTML.

    Why requests instead of Selenium:
        The Policies SPA page kills the ChromeDriver window handle on
        navigation (Salesforce Lightning security frame). Confirmed across
        3 Selenium approaches. Session cookie transfer from the live browser
        session is the correct bypass.

    Debug behavior:
        On parse failure, saves the raw modal HTML to dl_dir so the
        structure can be inspected without re-running the full agent flow.
    """
    import base64
    import re
    import requests
    from bs4 import BeautifulSoup

    BASE_URL  = "https://broker.ambetterhealth.com"
    STEP1_URL = BASE_URL + "/apex/BC_VFP02_PolicyList_CSV?filter=cancelled&offset=0"

    # ── Build requests session from the live Selenium cookies ─────────────────
    session = requests.Session()
    session.headers.update({
        "User-Agent": driver.execute_script("return navigator.userAgent;"),
        "Referer":    driver.current_url,
        "Accept":     "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    for ck in driver.get_cookies():
        session.cookies.set(ck["name"], ck["value"], domain=ck.get("domain"))

    # ── Step 1: trigger export, receive HTML modal ─────────────────────────────
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    log.info("ambetter | [%s] requesting cancelled export modal", agent_name)
    r = session.get(STEP1_URL, timeout=60, verify=False)

    if r.status_code != 200:
        raise RuntimeError(
            f"ambetter | [{agent_name}] export request failed: HTTP {r.status_code}"
        )

    ct = r.headers.get("Content-Type", "").lower()

    # Fallback: some portal configs might return the file directly (not a modal).
    # Content-Type will be zip/csv/octet-stream in that case.
    if any(t in ct for t in ("zip", "csv", "octet-stream")):
        log.info("ambetter | [%s] file returned directly (no modal)", agent_name)
        return _save_response_file(r, dl_dir, agent_name)

    # ── Step 2: extract base64 data URI from modal HTML ────────────────────────
    debug_path = dl_dir / f"_debug_modal_{agent_name.lower().replace(' ', '_')}.html"
    debug_path.write_bytes(r.content)

    b64_payload = _extract_data_uri_payload(r.text)

    if not b64_payload:
        # Dump diagnostics so the structure can be found without re-running.
        soup  = BeautifulSoup(r.text, "html.parser")
        hrefs = [a.get("href", "")[:80] for a in soup.find_all("a", href=True)]
        raise RuntimeError(
            f"ambetter | [{agent_name}] no base64 data URI found in modal.\n"
            f"  Debug HTML saved: {debug_path}\n"
            f"  All <a> hrefs (truncated): {hrefs}\n"
            f"  → Inspect the HTML and update _extract_data_uri_payload()."
        )

    # ── Decode and write ───────────────────────────────────────────────────────
    try:
        file_bytes = base64.b64decode(b64_payload)
    except Exception as exc:
        raise RuntimeError(
            f"ambetter | [{agent_name}] base64 decode failed: {exc}\n"
            f"  Payload length: {len(b64_payload)} chars\n"
            f"  First 80 chars: {b64_payload[:80]}"
        ) from exc

    safe_name = agent_name.lower().replace(" ", "_")
    out_path  = dl_dir / f"cancelled_{safe_name}.zip"
    out_path.write_bytes(file_bytes)
    log.info(
        "ambetter | [%s] ZIP decoded from modal — %d bytes → %s",
        agent_name, len(file_bytes), out_path.name,
    )

    # Clean up debug file on success
    if debug_path.exists():
        debug_path.unlink()

    return _extract_zip(out_path, dl_dir)


def _extract_data_uri_payload(html: str) -> str | None:
    """
    Find a base64 data URI in an <a href="data:...;base64,..."> tag
    and return the raw base64 string (without the data URI prefix).

    The confirmed Ambetter modal format is:
        <a ... href="data:application/zip;base64,<PAYLOAD>" download="policies.zip">

    Returns None if no matching tag is found.
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("data:") and ";base64," in href:
            # href = "data:<mimetype>;base64,<payload>"
            _, encoded = href.split(";base64,", 1)
            return encoded.strip()

    return None


def _save_response_file(response, dl_dir: Path, agent_name: str) -> Path:
    """
    Write an HTTP response body to disk. Handles ZIP and CSV.
    Used only for the fallback case where the file is returned directly.
    """
    ct  = response.headers.get("Content-Type", "").lower()
    ext = ".zip" if "zip" in ct else ".csv"
    safe_name = agent_name.lower().replace(" ", "_")
    out = dl_dir / f"cancelled_{safe_name}{ext}"
    out.write_bytes(response.content)
    log.info("ambetter | [%s] saved %s (%d bytes)", agent_name, out.name, len(response.content))
    if ext == ".zip":
        return _extract_zip(out, dl_dir)
    return out

def _save_file(response, dl_dir: Path, agent_name: str) -> Path:
    """
    Write an HTTP response to disk. Handles ZIP and CSV.
    Extracted so both step-1 direct returns and step-2 downloads use the same logic.
    """
    ct  = response.headers.get("Content-Type", "").lower()
    ext = ".zip" if "zip" in ct else ".csv"
    safe_name = agent_name.lower().replace(" ", "_")
    out = dl_dir / f"cancelled_{safe_name}{ext}"
    out.write_bytes(response.content)
    log.info("ambetter | [%s] saved %s (%d bytes)", agent_name, out.name, len(response.content))
    if ext == ".zip":
        return _extract_zip(out, dl_dir)
    return out



def _build_r2_records(
    csv_path: Path,
    agent_name: str,
    last_run_date: date | None,
    run_date: date,
) -> list[dict]:
    """
    Parse agent's cancelled CSV and filter to the current period.

    Period logic:
        Policy Term Date >= last_run_date  →  current period only.
        Mon run (state = Mon):  captures Mon–Fri cancellations.
        Fri run (state = Fri):  captures Fri–Mon cancellations.
        No overlap. No gap.

    First run (state file missing):
        All historical cancelled rows included. One-time event. Logged as warning.

    agent_name is sourced from agents.yaml (not from the CSV's own
    Broker Name / Payable Agent columns) to guarantee naming is
    identical between R1 and R2 records.
    """
    df = pd.read_csv(csv_path, dtype=str)
    df.columns = df.columns.str.strip()   # remove accidental whitespace in headers

    required = {COL_TERM_DATE, COL_FIRST_NAME, COL_LAST_NAME}
    missing  = required - set(df.columns)
    if missing:
        raise KeyError(
            f"ambetter | [{agent_name}] R2 CSV missing expected columns: {missing}\n"
            f"  Actual columns: {list(df.columns)}\n"
            f"  Update carriers.ambetter.columns in config/config.yaml."
        )

    df["_term_date"] = pd.to_datetime(df[COL_TERM_DATE], errors="coerce").dt.date

    from datetime import date
    period_start = calculate_period_start()
    period_df = df[df["_term_date"] >= period_start].copy()
    log.info(
        "ambetter | [%s] R2: %d records (Policy Term Date >= %s)",
    agent_name, len(period_df), period_start,
    )

    records: list[dict] = []
    for _, row in period_df.iterrows():
        first      = str(row.get(COL_FIRST_NAME, "")).strip()
        last       = str(row.get(COL_LAST_NAME,  "")).strip()
        policy_num = str(row.get(COL_POLICY_NUM,  "")).strip() or None
        term_date  = row["_term_date"]

        records.append({
            "run_date":          run_date.isoformat(),
            "carrier":           "Ambetter",
            "agent_name":        agent_name,
            "member_name":       f"{first} {last}".strip(),
            "policy_number":     policy_num,
            "last_status":       "Cancelled",
            "coverage_end_date": term_date.isoformat() if term_date else None,
            "member_dob":        str(row.get("Member Date Of Birth", "")).strip() or None,
            "state":             str(row.get("State", "")).strip() or None,
        })

    return records


# ── Single-agent runner ───────────────────────────────────────────────────────
def _run_single_agent(
    agent: dict,
    run_date: date,
    run_type: str,
    last_run_date: date | None,
) -> dict:
    """
    Runs one agent through login → R1 → R2.
    Returns {'r1': dict, 'r2': [...], 'success': bool}.
    3 attempts, backoff 5s / 15s / 45s.
    """
    name    = agent["name"]
    dl_dir  = _make_dl_dir(name, run_date)
    t_start = time.time()
    driver  = None

    for attempt in range(1, 4):
        try:
            log.info("ambetter | [%s] attempt %d/3", name, attempt)
            driver = _build_driver(dl_dir)

            _login(driver, agent)
            r1         = _scrape_r1(driver, name, run_date, run_type)
            csv_path   = _download_cancelled_csv(driver, dl_dir, name)
            r2_records = _build_r2_records(csv_path, name, last_run_date, run_date)

            elapsed = round(time.time() - t_start, 2)
            r1["duration_seconds"] = elapsed
            driver.quit()

            log.info(
                "ambetter | [%s] OK — %d active | %d deactivated | %.1fs",
                name, r1["active_members"], len(r2_records), elapsed,
            )
            return {"r1": r1, "r2": r2_records, "success": True}

        except Exception as exc:
            log.error("ambetter | [%s] attempt %d failed: %s", name, attempt, exc)
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass
                driver = None

            if attempt < 3:
                delay = RETRY_DELAYS[attempt - 1]
                log.info("ambetter | [%s] retrying in %ds", name, delay)
                time.sleep(delay)
            else:
                log.error("ambetter | [%s] all 3 attempts exhausted", name)
                return {
                    "r1": _failure_r1(name, run_date, run_type, t_start, str(exc)),
                    "r2": [],
                    "success": False,
                }

    return {
        "r1": _failure_r1(agent["name"], run_date, run_type, t_start, "unexpected loop exit"),
        "r2": [],
        "success": False,
    }


def _failure_r1(agent_name: str, run_date: date, run_type: str,
                t_start: float, error: str) -> dict:
    return {
        "run_date":         run_date.isoformat(),
        "run_type":         run_type,
        "carrier":          "Ambetter",
        "agent_name":       agent_name,
        "active_members":   None,
        "status":           "failed",
        "error_message":    error,
        "duration_seconds": round(time.time() - t_start, 2),
    }


# ── Main entry point ──────────────────────────────────────────────────────────
def run_ambetter(dry_run: bool = False, agent_index: int | None = None) -> dict:
    """
    Called by main.py in Phase 8.

    Returns {'r1': [...], 'r2': [...]}

    State update rule:
        Written only if ALL agents succeed — same conservative rule as Molina.
        Any failure → state not updated → next run re-processes the same period.
    """
    agents   = _load_agents()
    run_date = date.today()
    run_type = "Monday" if run_date.weekday() == 0 else "Friday"
    last_run = _get_last_run_date()

    if agent_index is not None:
        if agent_index >= len(agents):
            raise IndexError(
                f"--agent {agent_index} out of range — "
                f"{len(agents)} agents loaded (0-based)"
            )
        agents = [agents[agent_index]]
        log.info("ambetter | single-agent mode: [%s]", agents[0]["name"])

    all_r1:    list[dict] = []
    all_r2:    list[dict] = []
    failed:    list[str]  = []
    succeeded: list[str]  = []

    for agent in agents:
        result = _run_single_agent(agent, run_date, run_type, last_run)
        all_r1.append(result["r1"])
        all_r2.extend(result["r2"])
        (succeeded if result["success"] else failed).append(agent["name"])

    total_active = sum(
        r["active_members"] or 0 for r in all_r1 if r["status"] == "success"
    )
    log.info(
        "ambetter | complete — %d/%d agents OK | %d active | %d deactivated",
        len(succeeded), len(agents), total_active, len(all_r2),
    )
    if failed:
        log.warning("ambetter | failed agents: %s", ", ".join(failed))

    all_ok = len(failed) == 0
    if all_ok and not dry_run:
        _write_state(run_date)
    elif dry_run:
        log.info("ambetter | --dry-run: state file NOT updated")
    else:
        log.warning(
            "ambetter | state NOT updated — %d agent(s) failed. "
            "Next run will re-process the same period.",
            len(failed),
        )

    _write_summary_xlsx(all_r1, run_date)
    _append_deactivated_xlsx(all_r2)
    return {"r1": all_r1, "r2": all_r2}


# ── Standalone logging ────────────────────────────────────────────────────────
def _setup_logging() -> None:
    log_dir = ROOT / "logs"
    log_dir.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_dir / f"run_{ts}.log", encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )

def _write_summary_xlsx(r1_records: list[dict], run_date) -> None:
    """
    Write per-agent active member summary to data/output/ambetter_all_agents.xlsx.
    Only includes successful records (status == 'success').
    Mirrors the output format of molina_all_agents.xlsx and oscar_all_agents.xlsx.
    """
    import openpyxl
    from openpyxl.styles import Font

    rows = [
        {"Agent": r["agent_name"], "Active Members": r["active_members"]}
        for r in r1_records
        if r["status"] == "success"
    ]
    if not rows:
        log.warning("ambetter | no successful R1 records — skipping XLSX write")
        return

    df = (
        pd.DataFrame(rows)
        .sort_values("Active Members", ascending=False)
        .reset_index(drop=True)
    )

    output_dir = ROOT / "data" / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "ambetter_all_agents.xlsx"

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Ambetter Active Members")
        ws = writer.sheets["Ambetter Active Members"]
        ws.column_dimensions["A"].width = 30
        ws.column_dimensions["B"].width = 18

    log.info(
        "ambetter | summary saved → %s (%d agents, %d total active)",
        output_path, len(df), df["Active Members"].sum(),
    )

# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ambetter bot — standalone run")
    parser.add_argument("--agent",   type=int, default=None,
                        help="Run a single agent by 0-based index (e.g. --agent 0)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Skip state file update")
    args = parser.parse_args()

    _setup_logging()
    result = run_ambetter(dry_run=args.dry_run, agent_index=args.agent)

    print("\n── R1  Active Members ──────────────────────────────────────────────")
    for r in result["r1"]:
        mark = "✓" if r["status"] == "success" else "✗"
        print(f"  {mark} {r['agent_name']:<25}  {r['active_members']}")

    r2 = result["r2"]
    print(f"\n── R2  Deactivated This Period  ({len(r2)} records) ──────────────────")
    for r in r2:
        print(f"  {r['agent_name']:<25}  {r['member_name']:<30}  {r['coverage_end_date']}")
