"""
molina_downloader.py — Phase 1 (Multi-Agent)

Selenium automation for the Molina EvolveNXT portal.
Loops through all agents in config/agents.yaml → logs in per agent →
downloads CSV → calls molina_report.py → combines R1 + R2 records.

IMPORTANT: Molina is agent-level login, NOT broker-level.
Each of the 19 agents has their own username/password.
One login = one CSV = that agent's members only.
Credentials stored in config/agents.yaml (gitignored).

Reusable patterns established here (copied verbatim into Phases 2–8):
  - setup_logging()       → identical structure in all phases
  - with_retry()          → identical in all phases
  - setup_driver()        → identical Chrome prefs in all phases
  - wait_for_download()   → reused in all CSV-download phases
  - _wait()               → reused in all Selenium phases
  - State file read/write → identical pattern in all phases

Standalone usage:
  python scripts/molina_downloader.py             # full run
  python scripts/molina_downloader.py --dry-run   # no Sheets write, no state update
  python scripts/molina_downloader.py --agent 3   # run agent index 3 only (0-based)
"""

import os
import pandas as pd
import sys
import time
import logging
from pathlib import Path
from datetime import date, datetime

import yaml
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from datetime import date, timedelta

# Project root on sys.path so sibling imports work
sys.path.insert(0, str(Path(__file__).parent.parent))
from scripts.molina_report import process_csv

load_dotenv()

# ─── Paths & config ───────────────────────────────────────────────────────────
ROOT        = Path(__file__).parent.parent
CONFIG_PATH = ROOT / "config" / "config.yaml"
AGENTS_PATH = ROOT / "config" / "agents.yaml"
STATE_DIR   = ROOT / "data" / "state"
LOGS_DIR    = ROOT / "logs"

with open(CONFIG_PATH) as _f:
    CFG = yaml.safe_load(_f)

MCFG      = CFG["carriers"]["molina"]
SEL_CFG   = CFG["selenium"]
SELECTORS = MCFG["selectors"]

TODAY    = date.today()
RUN_DATE = TODAY.isoformat()
RUN_TYPE = "Monday" if TODAY.weekday() == 0 else "Friday"


def _load_agents() -> list[dict]:
    """
    Loads Molina agent list from config/agents.yaml.
    Each entry must have: name, user, pass.
    Raises FileNotFoundError with clear instructions if agents.yaml is missing.
    """
    if not AGENTS_PATH.exists():
        raise FileNotFoundError(
            f"Agent credentials file not found: {AGENTS_PATH}\n"
            "Copy config/agents.yaml.example → config/agents.yaml and fill in credentials.\n"
            "This file is gitignored — it never gets committed."
        )

    with open(AGENTS_PATH) as f:
        agents_cfg = yaml.safe_load(f)

    agents = agents_cfg.get("molina")
    if not agents:
        raise ValueError(
            "No 'molina' section found in config/agents.yaml.\n"
            "See config/agents.yaml.example for the expected format."
        )

    # Validate each agent has required fields
    for i, agent in enumerate(agents):
        for field in ("name", "user", "pass"):
            if not agent.get(field):
                raise ValueError(
                    f"Agent #{i} in agents.yaml is missing required field '{field}'.\n"
                    f"Agent entry: {agent}"
                )

    return agents


# ─────────────────────────────────────────────────────────────────────────────
# REUSABLE PATTERN 1 — Logging
# Copy this block unchanged into every phase script.
# ─────────────────────────────────────────────────────────────────────────────

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

def setup_logging(carrier: str = "MOLINA") -> tuple[logging.Logger, Path]:
    """
    Creates a per-run log file at logs/run_YYYYMMDD_HHMM.log.
    Format: timestamp | CARRIER | LEVEL | message
    Returns (logger, log_file_path).
    """
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOGS_DIR / f"run_{datetime.now().strftime('%Y%m%d_%H%M')}.log"

    fmt = logging.Formatter(f"%(asctime)s | {carrier} | %(levelname)s | %(message)s")

    log = logging.getLogger(f"downloader.{carrier.lower()}")
    log.setLevel(logging.DEBUG)

    # Avoid duplicate handlers if called multiple times in the same process
    if not log.handlers:
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(fmt)
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(fmt)
        log.addHandler(fh)
        log.addHandler(ch)

    return log, log_file


# ─────────────────────────────────────────────────────────────────────────────
# REUSABLE PATTERN 2 — Retry
# Copy with_retry() unchanged into every phase script.
# NOTE: Auth failures must NOT use this. Detect wrong credentials separately
#       and raise immediately — retrying a bad password locks accounts.
# ─────────────────────────────────────────────────────────────────────────────

_BACKOFF = [5, 15, 45]   # seconds — architecture standard


def with_retry(
    func,
    operation_name: str = "operation",
    max_attempts: int = 3,
    backoff: list[int] = _BACKOFF,
    log: logging.Logger = None,
):
    """
    Calls func() up to max_attempts times with exponential backoff.
    Returns func()'s return value on success.
    Raises RuntimeError with diagnostics after all attempts fail.

    Usage:
        csv_path = with_retry(
            lambda: step_download_csv(driver, download_path, log),
            operation_name="csv_download",
            log=log,
        )
    """
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            return func()
        except Exception as exc:
            last_exc = exc
            if log:
                log.warning(
                    f"{operation_name}: attempt {attempt}/{max_attempts} failed — {exc}"
                )
            if attempt < max_attempts:
                wait = backoff[attempt - 1]
                if log:
                    log.info(f"Waiting {wait}s before retry…")
                time.sleep(wait)

    raise RuntimeError(
        f"{operation_name} failed after {max_attempts} attempts. "
        f"Last error: {last_exc}\n"
        "Per no-spiral rule: diagnose root cause before attempting again. "
        "Do NOT call with_retry a 4th time."
    ) from last_exc


# ─────────────────────────────────────────────────────────────────────────────
# REUSABLE PATTERN 3 — Chrome driver setup
# Copy setup_driver() unchanged into every phase script.
# The download_path arg changes per carrier — everything else stays the same.
# ─────────────────────────────────────────────────────────────────────────────

def setup_driver(download_path: Path) -> webdriver.Chrome:
    """
    Returns a Chrome WebDriver configured for silent auto-download.

    Key prefs:
    - download.prompt_for_download: False → suppresses the Save As dialog
      that appears when the Molina portal's pol_search() triggers a download.
    - download.default_directory   → Chrome saves directly here, no user input needed.
    """
    prefs = {
        "download.default_directory":  str(download_path.resolve()),
        "download.prompt_for_download": False,
        "download.directory_upgrade":   True,
        "safebrowsing.enabled":         True,
        "plugins.always_open_pdf_externally": True,
    }
    options = webdriver.ChromeOptions()
    options.add_experimental_option("prefs", prefs)
    options.add_argument("--start-maximized")
    # Uncomment for headless (no browser window) — test headed first:
    # options.add_argument("--headless=new")
    # options.add_argument("--disable-gpu")

    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    })
    driver.set_page_load_timeout(SEL_CFG["page_load_timeout"])
    return driver


# ─────────────────────────────────────────────────────────────────────────────
# REUSABLE PATTERN 4 — Download polling
# Copy wait_for_download() unchanged into all CSV-download phases.
# ─────────────────────────────────────────────────────────────────────────────

def wait_for_download(directory: Path, timeout: int = None, log=None) -> Path:
    """
    Polls directory until a CSV appears and no .crdownload files remain.
    Chrome writes a .crdownload temp file while a download is in progress;
    its absence confirms the file is fully written.

    Returns the Path of the completed CSV file.
    Raises TimeoutError if the download doesn't complete within timeout seconds.
    """
    timeout = timeout or SEL_CFG["download_timeout"]
    deadline = time.time() + timeout

    while time.time() < deadline:
        csvs        = list(directory.glob("*.csv"))
        in_progress = list(directory.glob("*.crdownload"))
        if csvs and not in_progress:
            completed = max(csvs, key=lambda f: f.stat().st_mtime)
            if log:
                log.info(
                    f"Download complete: {completed.name} "
                    f"({completed.stat().st_size:,} bytes)"
                )
            return completed
        time.sleep(1)

    raise TimeoutError(
        f"Download did not complete within {timeout}s. "
        f"Directory: {directory}\n"
        "Possible causes: portal generated an error instead of a file, "
        "network timeout, or Chrome download blocked by antivirus."
    )


# ─────────────────────────────────────────────────────────────────────────────
# Selenium helpers
# ─────────────────────────────────────────────────────────────────────────────

def _wait(driver: webdriver.Chrome, by: str, selector: str, timeout: int = None):
    """
    Shorthand for WebDriverWait(...).until(element_to_be_clickable).
    timeout defaults to config value.
    """
    t = timeout or SEL_CFG["element_timeout"]
    return WebDriverWait(driver, t).until(
        EC.element_to_be_clickable((by, selector))
    )


def _parse_selector(selector_str: str) -> tuple[str, str]:
    """
    Parses 'id:login_id' → (By.ID, 'login_id')
    Parses 'xpath://div[...]' → (By.XPATH, '//div[...]')
    Parses 'css:.my-class' → (By.CSS_SELECTOR, '.my-class')
    """
    prefix, _, value = selector_str.partition(":")
    mapping = {
        "id":    By.ID,
        "xpath": By.XPATH,
        "css":   By.CSS_SELECTOR,
        "name":  By.NAME,
    }
    if prefix not in mapping:
        raise ValueError(f"Unknown selector prefix '{prefix}' in '{selector_str}'")
    return mapping[prefix], value


# ─────────────────────────────────────────────────────────────────────────────
# Portal steps
# ─────────────────────────────────────────────────────────────────────────────

def step_login(driver: webdriver.Chrome, username: str, password: str,
               log: logging.Logger) -> None:
    """
    Step 1: Navigate to login page and submit credentials.
    Does NOT retry — auth failures must be caught and logged, not retried
    (repeated wrong credentials can lock the portal account).

    Changed from v1: accepts username/password as args instead of reading
    from os.environ. Each agent has their own credentials.
    """
    url = MCFG["portal_url"]
    log.info(f"Loading login page: {url}")
    driver.get(url)

    by, sel = _parse_selector(SELECTORS["username_field"])
    _wait(driver, by, sel).send_keys(username)

    by, sel = _parse_selector(SELECTORS["password_field"])
    _wait(driver, by, sel).send_keys(password)

    by, sel = _parse_selector(SELECTORS["submit_button"])
    _wait(driver, by, sel).click()

    log.info("Credentials submitted")


def step_handle_password_modal(driver: webdriver.Chrome, log: logging.Logger) -> None:
    """
    Step 1b: Dismiss the 'Your Password will expire soon' modal if it appears.
    Clicks 'CONTINUE WITH LOGIN'. No-op if modal is not present (timeout=5s).

    This modal appears on some logins due to HIPAA password rotation policy.
    It appears BEFORE carrier selection.
    """
    try:
        by, sel = _parse_selector(SELECTORS["password_modal_continue"])
        btn = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((by, sel))
        )
        import re
        onclick = btn.get_attribute("onclick")
        url_match = re.search(r"location\.href\s*=\s*\('([^']+)'\)", onclick)
        if url_match:
            sso_url = url_match.group(1)
            log.info(f"Extracted SSO URL from button: {sso_url}")
            if not sso_url.startswith("http"):
                sso_url = f"https://account.evolvenxt.com/{sso_url.lstrip('/')}"
            driver.get(sso_url)
        else:
            log.warning(f"Could not extract URL from onclick: {onclick} — falling back to click")
            driver.execute_script("arguments[0].click();", btn)

        time.sleep(3)
        log.info(f"Post-SSO URL: {driver.current_url}")
    except TimeoutException:
        log.debug("No password expiry modal — proceeding")


def step_select_molina_carrier(driver: webdriver.Chrome, log: logging.Logger) -> None:
    """
    Step 2: Click the Molina carrier card if a carrier selection screen appears.

    Three valid post-login states, all handled:
      A) Already on molina.evolvenxt.com  → nothing to do
      B) On account.evolvenxt.com/user/home.htm (single-carrier account)
         → no card to click; proceed directly, marketplace navigation
           will use the configured molina domain
      C) Carrier selection screen with multiple cards
         → click the Molina card, wait for redirect to molina domain
    """
    current = driver.current_url
    log.debug(f"Post-login URL: {current}")

    # State A — already on the carrier portal
    if MCFG["portal_domain"] in current:
        log.info("Already on Molina portal — carrier selection not needed")
        return

    # State B — single-carrier account lands on home page, no card shown
    if "/user/home.htm" in current or "/user/home" in current:
        log.info(
            "Single-carrier account detected (landed on home.htm). "
            "Skipping carrier selection — will navigate directly to Molina domain."
        )
        return

    # State C — multi-carrier selection screen
    try:
        by, sel = _parse_selector(SELECTORS["molina_carrier_card"])
        card = WebDriverWait(driver, 8).until(
            EC.element_to_be_clickable((by, sel))
        )
        card.click()
        log.info("Molina carrier card clicked — waiting for portal redirect…")
        WebDriverWait(driver, 20).until(
            lambda d: MCFG["portal_domain"] in d.current_url
        )
        log.info(f"Carrier portal loaded: {driver.current_url}")

    except TimeoutException:
        # Last-chance check — maybe redirect happened but slowly
        if MCFG["portal_domain"] in driver.current_url:
            log.info("Molina portal loaded (delayed redirect)")
        else:
            raise TimeoutError(
                f"Could not reach {MCFG['portal_domain']}. "
                f"Current URL: {driver.current_url}\n"
                "Expected one of: carrier selection screen, home.htm, "
                "or molina.evolvenxt.com after login."
            )


def step_navigate_to_marketplace(driver: webdriver.Chrome, log: logging.Logger) -> None:
    """
    Step 3+4: Navigate directly to Marketplace Search by URL.

    Always builds the URL from the configured portal_domain, not from the
    current URL's netloc. This is critical for single-carrier accounts that
    land on account.evolvenxt.com/user/home.htm — their current netloc would
    produce the wrong base URL.

    Waits for the download button to confirm the page loaded with member data.
    """
    marketplace_url = (
        f"https://{MCFG['portal_domain']}{MCFG['paths']['marketplace_search']}"
    )
    log.info(f"Navigating to Marketplace Search: {marketplace_url}")
    driver.get(marketplace_url)

    import time; time.sleep(3)
    log.info(f"DEBUG — After navigation URL: {driver.current_url}")

    by, sel = _parse_selector(SELECTORS["download_button"])
    _wait(driver, by, sel, timeout=SEL_CFG["element_timeout"])
    log.info("Marketplace Search page ready")


def step_download_csv(
    driver: webdriver.Chrome, download_path: Path, log: logging.Logger
) -> Path:
    """
    Step 5: Click the Download button and wait for the CSV to land.

    The button calls pol_search() which triggers a server-side export.
    Chrome auto-saves to download_path (no Save As dialog) because of the
    prefs set in setup_driver().

    Returns: Path of the downloaded CSV file.
    """
    by, sel = _parse_selector(SELECTORS["download_button"])
    btn = _wait(driver, by, sel)
    btn.click()
    log.info("Download button clicked — waiting for CSV…")

    csv_path = wait_for_download(download_path, log=log)
    return csv_path


# ─────────────────────────────────────────────────────────────────────────────
# State file
# ─────────────────────────────────────────────────────────────────────────────

def read_last_run_date() -> str | None:
    """Returns ISO date string of last successful Molina run, or None."""
    state_file = STATE_DIR / "molina_last_run_date.txt"
    if state_file.exists():
        val = state_file.read_text(encoding="utf-8").strip()
        return val if val else None
    return None


def write_last_run_date(run_date: str) -> None:
    """
    Updates the state file ONLY on full success.
    Never called on partial failure — per architecture state safety rule.
    """
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    (STATE_DIR / "molina_last_run_date.txt").write_text(run_date, encoding="utf-8")


# ─────────────────────────────────────────────────────────────────────────────
# Single-agent flow
# ─────────────────────────────────────────────────────────────────────────────

def _run_single_agent(
    agent: dict,
    agent_index: int,
    last_run_date: str | None,
    dry_run: bool,
    log: logging.Logger,
) -> dict:
    """
    Runs the full portal flow for ONE agent.

    Returns:
        {
            'agent_name': str,
            'r1': [R1 dicts],
            'r2': [R2 dicts],
            'status': 'success' | 'failed',
            'error': str | None,
            'csv_path': Path | None,
            'duration_seconds': float,
        }

    Why a fresh browser per agent:
      Each agent has different credentials → different portal session.
      Reusing a browser would require logout/re-login, which is fragile
      (Molina's session handling has redirects and modals). A fresh
      ChromeDriver per agent is cleaner and avoids stale cookie issues.
    """
    agent_name = agent["name"]
    start = time.time()
    log.info(f"── Agent {agent_index + 1}: {agent_name} ──────────────────────")

    # Each agent gets an isolated download folder to prevent CSV filename collisions.
    # Chrome names every download identically (e.g., "report.csv"), so without
    # isolation agent 2's file would overwrite agent 1's before processing.
    download_path = _build_download_path(agent_name)
    log.info(f"Download path: {download_path}")

    driver = None
    csv_path = None

    # ── Portal automation ─────────────────────────────────────────────────
    try:
        driver = setup_driver(download_path)

        # Login — no retry (auth failures must not retry)
        try:
            step_login(driver, agent["user"], agent["pass"], log)
            step_handle_password_modal(driver, log)
        except Exception as exc:
            log.error(f"[{agent_name}] Login failed (not retrying — auth failure): {exc}")
            return _agent_result(agent_name, "failed", f"Login failed: {exc}", start)

        # Carrier selection — retryable
        with_retry(
            lambda: step_select_molina_carrier(driver, log),
            operation_name=f"carrier_selection ({agent_name})",
            log=log,
        )

        # Navigate to Marketplace Search — retryable
        with_retry(
            lambda: step_navigate_to_marketplace(driver, log),
            operation_name=f"marketplace_navigation ({agent_name})",
            log=log,
        )

        # Download CSV — retryable
        csv_path = with_retry(
            lambda: step_download_csv(driver, download_path, log),
            operation_name=f"csv_download ({agent_name})",
            log=log,
        )

    except Exception as exc:
        log.error(f"[{agent_name}] Portal automation failed: {exc}")
        return _agent_result(agent_name, "failed", str(exc), start)

    finally:
        if driver:
            driver.quit()
            log.debug(f"[{agent_name}] WebDriver closed")

    # ── CSV processing ────────────────────────────────────────────────────
    try:
        r1_records, r2_records = process_csv(
            csv_path,
            run_date=RUN_DATE,
            run_type=RUN_TYPE,
            last_run_date=last_run_date,
            write_xlsx=False,  # Never write per-agent XLSX — combined XLSX at the end
        )
    except Exception as exc:
        log.error(f"[{agent_name}] CSV processing failed: {exc}")
        return _agent_result(agent_name, "failed", f"CSV processing: {exc}", start)

    duration = round(time.time() - start, 2)
    active_count = sum(r["active_members"] for r in r1_records)
    log.info(
        f"[{agent_name}] Done — {active_count} active, "
        f"{len(r2_records)} deactivated, {duration}s"
    )

    return {
        "agent_name": agent_name,
        "r1": r1_records,
        "r2": r2_records,
        "status": "success",
        "error": None,
        "csv_path": csv_path,
        "duration_seconds": duration,
    }


def _agent_result(agent_name: str, status: str, error: str, start: float) -> dict:
    """Builds a standardised result dict for a failed agent."""
    duration = round(time.time() - start, 2)
    return {
        "agent_name": agent_name,
        "r1": [{
            "run_date":        RUN_DATE,
            "run_type":        RUN_TYPE,
            "carrier":         "Molina",
            "agent_name":      agent_name,
            "active_members":  0,
            "status":          "failed",
            "error_message":   error,
            "duration_seconds": duration,
        }],
        "r2": [],
        "status": status,
        "error": error,
        "csv_path": None,
        "duration_seconds": duration,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────────────

def run_molina(dry_run: bool = False, agent_filter: int | None = None) -> dict:
    """
    Orchestrates the full Molina run across all agents.
    Called by main.py (Phase 8) or standalone.

    Args:
        dry_run:      If True, skips Sheets write and state file update.
        agent_filter:  If set, run only this agent index (0-based). For testing.

    Returns:
        {
            'r1': [combined R1 records from all agents],
            'r2': [combined R2 records from all agents],
            'status': 'success' | 'partial' | 'failed',
            'error':  str | None,
            'agents_succeeded': [names],
            'agents_failed':    [names],
        }

    State update rule:
        State file (molina_last_run_date.txt) is updated ONLY when ALL agents
        succeed. On partial failure, next run's R2 may produce duplicates for
        already-processed agents — this is acceptable. Missing deactivations
        (from skipped agents) is NOT acceptable.
    """
    log, log_file = setup_logging("MOLINA")
    total_start = time.time()

    # ── Pre-flight: load agents ───────────────────────────────────────────
    try:
        agents = _load_agents()
    except (FileNotFoundError, ValueError) as exc:
        log.error(str(exc))
        return {
            "r1": [], "r2": [],
            "status": "failed",
            "error": str(exc),
            "agents_succeeded": [],
            "agents_failed": [],
        }

    # Apply agent filter if specified (for testing individual agents)
    if agent_filter is not None:
        if agent_filter < 0 or agent_filter >= len(agents):
            log.error(
                f"--agent {agent_filter} is out of range. "
                f"Valid range: 0–{len(agents) - 1} ({len(agents)} agents loaded)"
            )
            return {
                "r1": [], "r2": [],
                "status": "failed",
                "error": f"Agent index {agent_filter} out of range",
                "agents_succeeded": [],
                "agents_failed": [],
            }
        agents = [agents[agent_filter]]
        log.info(f"Agent filter active: running only agent #{agent_filter} ({agents[0]['name']})")

    last_run_date = read_last_run_date()
    log.info(f"Run: {RUN_DATE} ({RUN_TYPE}) | last_run: {last_run_date or 'none'}")
    log.info(f"Agents to process: {len(agents)}")
    log.info(f"Log file: {log_file}")

    # ── Loop through agents ───────────────────────────────────────────────
    all_r1 = []
    all_r2 = []
    agents_succeeded = []
    agents_failed = []

    for i, agent in enumerate(agents):
        result = _run_single_agent(agent, i, last_run_date, dry_run, log)

        all_r1.extend(result["r1"])
        all_r2.extend(result["r2"])

        if result["status"] == "success":
            agents_succeeded.append(result["agent_name"])
        else:
            agents_failed.append(result["agent_name"])
            log.warning(
                f"Agent '{result['agent_name']}' failed: {result['error']}. "
                "Continuing with next agent."
            )

    # ── Stamp total duration on all R1 records ────────────────────────────
    total_duration = round(time.time() - total_start, 2)
    # Each agent's R1 records already have their own duration_seconds.
    # We don't overwrite — the per-agent timing is more useful for debugging.

    # ── Determine overall status ──────────────────────────────────────────
    if not agents_failed:
        overall_status = "success"
    elif not agents_succeeded:
        overall_status = "failed"
    else:
        overall_status = "partial"

    total_active = sum(r["active_members"] for r in all_r1 if r.get("status") == "success")

    log.info("=" * 65)
    log.info(
        f"Molina complete — {len(agents_succeeded)}/{len(agents_succeeded) + len(agents_failed)} agents | "
        f"{total_active} active | {len(all_r2)} deactivated | {total_duration}s"
    )
    if agents_failed:
        log.warning(f"Failed agents: {', '.join(agents_failed)}")

    # ── Update state — ONLY on full success ───────────────────────────────
    # Rationale: if agent 15 fails, we don't update the state date. Next run,
    # all agents run against the same last_run_date. Agents 1-14 and 16-19
    # may produce some R2 duplicates (they see the same terminated members
    # again), but agent 15's deactivations won't be missed. Duplicates are
    # recoverable; missed data is invisible.
    if overall_status == "success" and not dry_run:
        write_last_run_date(RUN_DATE)
        log.info(f"State updated → molina_last_run_date.txt = {RUN_DATE}")
    elif overall_status == "partial" and not dry_run:
        log.warning(
            "PARTIAL SUCCESS — state file NOT updated. "
            f"Failed agents ({', '.join(agents_failed)}) need to succeed before "
            "the state date advances. Next run may produce R2 duplicates for "
            "successful agents — this is by design."
        )
    elif dry_run:
        log.info("Dry run — state file NOT updated")

    # ── Write combined XLSX (all agents) ──────────────────────────────────
    # Skipped on dry run. This replaces the per-agent XLSX from v1.
    if not dry_run and overall_status in ("success", "partial"):
        try:
            _write_combined_xlsx(all_r1)
            _append_deactivated_xlsx(all_r2)  # ← agregar esta línea
        except Exception as exc:
            log.warning(f"XLSX write failed (non-fatal): {exc}")

    return {
        "r1":     all_r1,
        "r2":     all_r2,
        "status": overall_status,
        "error":  None if overall_status == "success" else f"Failed: {', '.join(agents_failed)}",
        "agents_succeeded": agents_succeeded,
        "agents_failed":    agents_failed,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _build_download_path(agent_name: str) -> Path:
    """
    Each agent gets their own subdirectory under the date folder.
    This prevents CSV filename collisions — Chrome names every Molina
    download identically, so agent2's file would overwrite agent1's
    without isolation.

    Sanitizes the agent name to remove characters that are invalid in
    Windows paths (the production machine runs Windows).
    """
    safe_name = "".join(c if c.isalnum() or c in (' ', '-', '_') else '_' for c in agent_name).strip()
    path = (
        ROOT / "data" / "raw" / "molina"
        / TODAY.strftime("%Y-%m")
        / TODAY.strftime("%Y-%m-%d")
        / safe_name
    )
    path.mkdir(parents=True, exist_ok=True)
    return path


def _write_combined_xlsx(r1_records: list[dict]) -> None:
    """
    Writes a combined XLSX with all agents' active member counts.
    This replaces the per-agent XLSX from molina_report.py v1.
    Only includes successful records.
    """
    import pandas as pd

    success_records = [r for r in r1_records if r.get("status") == "success"]
    if not success_records:
        return

    df = pd.DataFrame(success_records)[["agent_name", "active_members"]]
    df.columns = ["Agent", "Active Members"]
    df = df.sort_values("Active Members", ascending=False)

    output_path = ROOT / "data" / "output" / "molina_all_agents.xlsx"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Active Members")


def _failed_result(error_msg: str, start: float) -> dict:
    """Returns a standardised failed-run dict for main.py to consume."""
    duration = round(time.time() - start, 2)
    return {
        "r1": [{
            "run_date":        RUN_DATE,
            "run_type":        RUN_TYPE,
            "carrier":         "Molina",
            "agent_name":      None,
            "active_members":  0,
            "status":          "failed",
            "error_message":   error_msg,
            "duration_seconds": duration,
        }],
        "r2":     [],
        "status": "failed",
        "error":  error_msg,
        "agents_succeeded": [],
        "agents_failed":    [],
    }


# ─────────────────────────────────────────────────────────────────────────────
# Standalone entry point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv

    # Parse --agent N for single-agent testing
    agent_filter = None
    if "--agent" in sys.argv:
        idx = sys.argv.index("--agent")
        if idx + 1 < len(sys.argv):
            try:
                agent_filter = int(sys.argv[idx + 1])
            except ValueError:
                print(f"Error: --agent requires an integer, got '{sys.argv[idx + 1]}'")
                sys.exit(1)
        else:
            print("Error: --agent requires a number (0-based index)")
            sys.exit(1)

    if dry_run:
        print("[DRY RUN] No Sheets write, no state update\n")
    if agent_filter is not None:
        print(f"[AGENT FILTER] Running only agent #{agent_filter}\n")

    result = run_molina(dry_run=dry_run, agent_filter=agent_filter)

    print("\n" + "=" * 65)
    print(f"  STATUS: {result['status'].upper()}")
    if result.get("agents_succeeded"):
        print(f"  Succeeded: {len(result['agents_succeeded'])} agents")
    if result.get("agents_failed"):
        print(f"  Failed:    {len(result['agents_failed'])} — {', '.join(result['agents_failed'])}")
    print("=" * 65)

    if result["status"] in ("success", "partial"):
        r1 = result["r1"]
        r2 = result["r2"]

        success_r1 = [r for r in r1 if r.get("status") == "success"]
        failed_r1  = [r for r in r1 if r.get("status") == "failed"]
        total_active = sum(r["active_members"] for r in success_r1)

        print(f"\nR1 — Active Members  ({len(success_r1)} agents, {total_active} total)")
        print("-" * 65)
        for r in success_r1:
            print(f"  {r['agent_name']:<34} {r['active_members']:>4} members")

        if failed_r1:
            print(f"\n  FAILED ({len(failed_r1)}):")
            for r in failed_r1:
                print(f"  ✗ {r['agent_name'] or 'unknown':<32} {r['error_message'][:50]}")

        print(f"\nR2 — Deactivated This Period  ({len(r2)} members)")
        print("-" * 65)
        if r2:
            header = f"  {'Member':<38} {'Agent':<26} {'End Date'}"
            print(header)
            print("  " + "-" * 75)
            for r in r2[:25]:
                print(
                    f"  {r['member_name']:<38} "
                    f"{r['agent_name']:<26} "
                    f"{r['coverage_end_date']}"
                )
            if len(r2) > 25:
                print(f"  … and {len(r2) - 25} more")
        else:
            print("  No deactivations detected this period")

    else:
        print(f"\n  Error: {result['error']}")
        print("\n  Troubleshooting:")
        print("    1. Does config/agents.yaml exist with valid Molina credentials?")
        print("    2. Is Chrome installed and up to date?")
        print("    3. Does https://account.evolvenxt.com/ load in a normal browser?")
