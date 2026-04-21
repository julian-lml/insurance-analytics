# CLAUDE.md — Insurance Analytics Automation
## Project Context, Architecture Decisions & Engineering Log

---

## 1. PROJECT PURPOSE

Manually-operated reporting pipeline run every **Monday and Friday** that logs into 5
insurance carrier portals, extracts member data per agent, and produces two reports:

| Report | Content | Destination |
|--------|---------|-------------|
| **R1 — Active Members** | Count of active members per agent per carrier | Contingency table (pivot) in Google Sheets + individual XLSX per carrier |
| **R2 — Deactivated This Period** | Member-level list of who was lost since the last day of the previous month | `data/output/deactivated_members.xlsx` (append-only, for manager outreach) |

**The contingency table is the primary deliverable.** Every other component exists to feed it.

### Operating Model (confirmed April 2026)

The pipeline runs **manually** via the GUI launcher, not on an automated schedule.
Carrier portals require human interaction (MFA approvals, VPN setup, password resets,
SMS codes) that makes fully unattended execution unreliable. The GUI provides full
control: one button per carrier, single-agent reruns, and a separate "Push to Sheets"
button that the operator triggers only when all carriers are verified.

**Two-step flow:**
```
Step 1 — Run bots (one carrier at a time, reruns as needed)
  Each bot writes to its own carrier XLSX file on disk.
  deactivated_members.xlsx accumulates R2 records across all carriers.
  Bots are completely independent of Google Sheets.

Step 2 — Push to Google Sheets (separate, intentional action)
  sheets_writer.py reads from the XLSX files on disk.
  Triggered manually via [Push to Sheets] button in the GUI.
  Run only when all carriers for the day are complete and verified.
```

This separation means a single failed agent does not block the Sheets push. Fix the
failed agent, rerun it, then push when everything looks correct.

---

## 2. FINAL DELIVERABLE SHAPE

```
data/output/                                    ← Written by bots (Step 1)
├── molina_all_agents.xlsx    ← R1 current state for Molina agents. Merge-on-write.
├── ambetter_all_agents.xlsx  ← R1 current state for Ambetter agents.
├── cigna_all_agents.xlsx     ← R1 current state for Cigna agents.
├── oscar_all_agents.xlsx     ← R1 current state for Oscar agents.
├── united_all_agents.xlsx    ← R1 current state for United agents.
└── deactivated_members.xlsx  ← R2 all carriers, append-only, deduped.

Google Sheets — "April 2026"                    ← Written by sheets_writer (Step 2)
├── Tab: Summary (OVERWRITE each push)
│     Agent         | Ambetter | Cigna | Molina | Oscar | United | Total
│     Brandon       |       95 |     - |      6 |    16 |     22 |  139
│     Felipe        |      461 |    72 |     53 |   166 |    228 |  980
│     Total         |    6,939 |   558 |    757 | 2,555 |  1,711 | 12,520
│
├── Tab: Deactivated This Period (APPEND each push — net-new only, deduped)
│     run_date | carrier | agent_name | member_name | member_dob | state | coverage_end_date | policy_number
│
└── Tab: Active Members (APPEND each push — audit trail for Looker Studio)
      run_date | run_type | carrier | agent_name | active_members | status
```

**carrier_all_agents.xlsx files are not ephemeral snapshots.** They hold the current
authoritative R1 state per carrier. The merge-on-write rule (§8.22, §15) ensures
single-agent reruns update one agent's row without touching any other.

Missing carrier data (failed run) = **blank cell** in Sheets, never zero.
Google Sheets is the permanent history store. XLSX files are the run-day working state.

---

## 3. CARRIERS & AUTOMATION STATUS

| Carrier | Portal URL | Auth | R1 Method | R2 Method | Library | Phase | Status |
|---------|-----------|------|-----------|-----------|---------|-------|--------|
| **Molina** | account.evolvenxt.com | user+pass, no 2FA | CSV download → sum active statuses | Same CSV, Terminated rows | Selenium | 1 | ✅ DONE |
| **Ambetter** | broker.ambetterhealth.com | user+pass, no 2FA | Dashboard odometer | requests+cookies → base64 ZIP | Selenium + requests | 2 | ✅ DONE |
| **Oscar** | accounts.hioscar.com | MS Authenticator (boss phone) SEMI-AUTO | CSV → sum Lives non-Inactive | Same CSV, Inactive rows + date filter | **Playwright** | 4 | ✅ DONE |
| **Cigna** | cignaforbrokers.com | USA VPN + email 2FA (webmail.ligagent.com) | BOB filter → Total results | Portal export: Terminated + date filter | **Playwright** | 5 | ✅ DONE |
| **United** | uhcjarvis.com | MS Auth (boss phone) SEMI-AUTO / SMS for new agents (see §8.21) | Dashboard count | file_extract — policyTermDate filter | **Playwright** | 6 | ✅ DONE |

**Login architecture:** ALL carriers use **agent-level login** (one session per agent).
Molina: 15 agents. Ambetter: 16 agents. Oscar: 13 agents. Cigna: 12 agents. United: 12
agents (3 skip_bot — see §8.19; some new agents may require SMS 2FA — see §8.21).
Credentials in `config/agents.yaml` (gitignored).

---

## 4. FILE STRUCTURE

```
scripts/
├── __init__.py            ← Makes scripts/ a Python package. Required for clean imports.
├── utils.py               ← SHARED INFRASTRUCTURE. All bots import from here. See §15.
├── molina_report.py       ← Domain logic: ONE Molina CSV → R1 + R2. Never run directly.
├── molina_downloader.py   ← Selenium orchestrator for Molina (15 agents).
├── ambetter_bot.py        ← Selenium + requests bypass for Ambetter (16 agents).
├── sheets_writer.py       ← Reads from XLSX files → pushes to Google Sheets. Standalone.
├── oscar_bot.py           ← Playwright semi-auto for Oscar (13 agents).
├── cigna_bot.py           ← Playwright + VPN + email 2FA for Cigna (12 agents).
├── united_bot.py          ← Playwright semi-auto + persistent profiles for United.
└── launcher.py            ← tkinter GUI: run center + push to Sheets. See §16.

config/
├── config.yaml            ← Portal URLs, selectors, column names. Single source of truth.
└── agents.yaml            ← Carrier credentials per agent. GITIGNORED. Never commit.

credentials/
└── service_account.json   ← Google service account key. GITIGNORED. Never commit.

data/
├── raw/{carrier}/{YYYY-MM}/{YYYY-MM-DD}/{agent_name}/   ← Raw CSVs. Kept 90 days.
├── output/
│   ├── molina_all_agents.xlsx     ← R1 current state. Merge-on-write. Not ephemeral.
│   ├── ambetter_all_agents.xlsx
│   ├── cigna_all_agents.xlsx
│   ├── oscar_all_agents.xlsx
│   ├── united_all_agents.xlsx
│   └── deactivated_members.xlsx   ← R2 append-only. NEVER overwrite.
├── state/
│   ├── molina_last_run_date.txt
│   ├── ambetter_last_run_date.txt
│   └── cigna_last_run_date.txt
└── chrome_profiles/{agent_name}/  ← Persistent Chrome profiles for United bot.

logs/run_YYYYMMDD_HHMMSS_{carrier}.log   ← Seconds prevent filename collision.
status/last_run.json
```

### .gitignore (required entries — verify these are present)
```
config/agents.yaml
credentials/
data/raw/
data/output/
data/state/
data/chrome_profiles/
logs/
.env
```

---

## 5. DATA SCHEMAS

### R1 — Active Members
```python
{
    "run_date":         "2026-03-27",
    "run_type":         "Friday",
    "carrier":          "Ambetter",
    "agent_name":       "Brandon Kaplan",  # ALWAYS from agents.yaml
    "active_members":   95,
    "status":           "success",         # "success" | "failed"
    "error_message":    None,
    "duration_seconds": 42.3,
}
```

### R2 — Deactivated This Period
```python
{
    "run_date":          "2026-04-07",
    "carrier":           "Ambetter",
    "agent_name":        "Brandon Kaplan",  # ALWAYS from agents.yaml, never from CSV
    "member_name":       "Emily Rink",
    "member_dob":        "07/24/2000",      # null for Cigna — United has dateOfBirth
    "state":             "SC",
    "coverage_end_date": "2026-03-31",
    "policy_number":     "U70066328",       # United: composite key until ID confirmed (§8.20)
    "last_status":       "Cancelled",
    "detection_method":  "download_filter", # "file_extract" | "download_filter" | "portal_export"
}
```

---

## 6. R2 DATE SCOPING LOGIC

### period_start Definition (UPDATED Phase R — April 2026)

**`period_start` = fixed historical date from `config/config.yaml` → `r2.start_date`.**

Current value: `"2025-12-01"`.

**This function lives in `scripts/utils.py` — the single authoritative definition.
Import it. Never copy it into bot files.**

```python
# scripts/utils.py — SINGLE SOURCE OF TRUTH
def get_r2_start_date() -> date:
    """Fixed historical cutoff read from config/config.yaml r2.start_date."""
    config = yaml.safe_load((ROOT / "config" / "config.yaml").read_text())
    return date.fromisoformat(config["r2"]["start_date"])
```

```yaml
# config/config.yaml
r2:
  start_date: "2025-12-01"
```

**Why a fixed historical date and not a rolling monthly window?**
Carrier portals publish cancellations with days-to-weeks of lag. A rolling window
(previous-month-end) silently drops records that arrive late. The fixed cutoff keeps
the window wide enough to absorb every late-arriving cancellation. The dedup key
`(carrier, policy_number, coverage_end_date) keep="first"` makes repeated historical
captures safe — existing rows always win.

Confirmed from live data (April 7, 2026):
- **Molina:** 100% of terminations stamped `End_Date = last day of month`.
- **Oscar:** 97% of inactive rows stamped `Coverage end date = last day of month`.
- **Ambetter, Cigna, United:** Real cancellation dates. Dedup key handles re-capture.

**Rolling the start date forward:** edit `config/config.yaml` → `r2.start_date`.
No code change required. Retire the old date once you're confident every carrier has
published all cancellations for the preceding period (~4–6 weeks of lag).

### R2 Deduplication

**Dedup key:** `(carrier, policy_number, coverage_end_date)` — existing rows always win.

`coverage_end_date` in the key handles re-enrollment: a member who cancels, re-enrolls,
and cancels again gets a different end date — correctly captured as a new R2 record.

**`append_deactivated_xlsx()` lives in `scripts/utils.py`. Import it. Never copy it.**

Same dedup logic in `sheets_writer.py`: load existing Sheets rows once into pandas,
filter net-new, write once. Never query Sheets in a loop.

**Rules:**
- R2 runs on every bot execution. Calendar window is the only filter.
- State files never used as R2 date filters.
- `agent_name` always from `agents.yaml`.

---

## 7. ENGINEERING STANDARDS (NON-NEGOTIABLE)

### Operational Standards
| Standard | Rule |
|----------|------|
| **Error isolation** | One agent or carrier failure never blocks others |
| **Retry** | 3 attempts, backoff 5s / 15s / 45s. Auth failures do NOT retry. |
| **Logging** | `logs/run_YYYYMMDD_HHMMSS_{carrier}.log` — seconds prevent filename collision |
| **Config** | URLs, selectors, column names → `config/config.yaml`. Credentials → `agents.yaml`. Never hardcoded. |
| **Standalone** | Every bot runs standalone. Does not call sheets_writer. Does not update state on --dry-run. |
| **Dry run** | `--dry-run`: full portal flow, no XLSX write, no state update |
| **No-spiral rule** | 3 failed attempts → stop, diagnose, propose 2 alternatives. Never attempt 4. |
| **State safety** | State files updated ONLY after fully successful carrier run (all agents) |
| **Output safety** | `deactivated_members.xlsx` append-only — never overwritten. Deduped on every append. |
| **agent_name** | Always from `agents.yaml`. Never from CSV columns. |
| **Playwright waits** | Auto-wait only. Never `page.wait_for_timeout()` or bare `time.sleep()` for element waits. |
| **Async boundary** | Playwright bots expose sync wrapper: `def run_X(dry_run, agent_filter): return asyncio.run(...)` |
| **R2 dedup** | Key: `(carrier, policy_number, coverage_end_date)`. Existing rows always win. |

### Architecture Standards (added April 2026 — see §15 for full rationale)
| Standard | Rule |
|----------|------|
| **DRY** | Shared functions live in `scripts/utils.py`. Never copy them into bot files. |
| **Shared imports** | Every bot imports `get_r2_start_date`, `append_deactivated_xlsx`, `write_r1_xlsx`, `run_type`, `setup_logging`, `with_retry` from `utils.py`. |
| **Column names in config** | ALL carrier column names in `config/config.yaml` under `carriers.{carrier}.columns`. Not as Python constants in bot files. |
| **No import-time side effects** | Never open files (agents.yaml, config.yaml) at module level. Load inside functions only. |
| **Module boundary** | Each bot's public API is one function: `run_{carrier}(dry_run, agent_filter)`. Everything else is private (`_` prefix). |
| **No credentials in output** | Never print passwords. Log usernames only. Sanitize agent dicts before any debug logging. |
| **Merge-on-write for R1 XLSX** | `write_r1_xlsx()` always merges. Load existing, remove rows for agents in current run, append new. Never overwrite entire file. Single-agent reruns are safe. |
| **sheets_writer reads from XLSX** | `sheets_writer.py` reads `data/output/*.xlsx` on disk. It does not receive in-memory records from bots. It is always a separate manual step. |

---

## 8. CRITICAL ISSUES & RESOLUTIONS

### 8.1 Ambetter R2 — Salesforce Lightning SPA Kills ChromeDriver (RESOLVED)

**Root cause:** Salesforce Lightning reinitializes browser context on any SPA navigation.
**Resolution:** Bypass Selenium. Use `requests` + cookies transferred from live session.
Export endpoint returns HTML with entire ZIP as base64 data URI in `<a>` tag. Decoded
directly — no second HTTP request.

```python
session = requests.Session()
session.headers.update({
    "User-Agent": driver.execute_script("return navigator.userAgent;"),
    "Referer":    driver.current_url,
})
for ck in driver.get_cookies():
    session.cookies.set(ck["name"], ck["value"], domain=ck.get("domain"))
r = session.get(BASE_URL + "?filter=cancelled&offset=0", timeout=60)
# Parse base64 ZIP from response HTML <a> tag
```

---

### 8.2 Ambetter — Historical Flood on First Run (RESOLVED)

R2 always uses `calculate_period_start()`. Calendar window only, every run.

---

### 8.3 Ambetter CSV — `Payable Agent` ≠ Individual Agent (PERMANENT)

`Payable Agent` = `Plan Advisors, LLC`. Never use. Always source `agent_name` from `agents.yaml`.

---

### 8.4 R2 Count Discrepancy vs R1 Delta (EXPLAINED)

Molina and Oscar stamp all terminations at month-end. Ambetter and Cigna use real dates.
`calculate_period_start()` anchored to last day of previous month captures both correctly.
Dedup key prevents double-counting across runs.

**Ambetter data lag:** Cancellations appear in the export days to weeks after members
drop from the active count. The dedup key handles re-capture when delayed records appear.

---

### 8.5 Molina — Carrier Selection Flakiness (NON-BLOCKING)

Fails attempt 1, succeeds on retry. `with_retry()` handles it.

---

### 8.6 Molina — `_build_r2_records()` Silent Column Drop (RESOLVED)

Field names corrected to match R2 schema. Recovery script `inject_molina_r2.py` used.

---

### 8.7 Google Sheets — Service Account storageQuotaExceeded (RESOLVED)

New Google Cloud projects block service account Drive file creation (misleading error).
Workaround: create monthly sheet manually, register ID in `.env` as `SHEET_ID_APRIL_2026=<id>`.

---

### 8.8 Google Sheets — Missing Headers on Append Tabs (RESOLVED)

Both sheet-retrieval paths now call `_ensure_tab_headers()`. Belt-and-suspenders guard
reads A1 before every append — writes headers if empty.

---

### 8.9 sheets_writer.py — Standalone Test Mode (RESOLVED)

`python scripts/sheets_writer.py` with no args prints usage and exits.
`--dry-run` resolves sheet IDs and logs what would be written without any API calls.

---

### 8.10 Oscar — Modal Dismissal Uses Probe Loop (PERMANENT)

Button classes are hashed and change on every Oscar deployment. Never hardcode them.

```python
buttons = await page.locator("button:visible").all()
if buttons:
    await buttons[-1].click()
```

---

### 8.11 Oscar / Playwright — `downloads_path` Not Valid (RESOLVED)

Remove from `browser.new_context()`. Use `download.save_as()` directly.

---

### 8.12 Cigna — `member_dob` Not Available (PERMANENT)

Cigna export does not include DOB. All Cigna R2 records: `member_dob = null`.

---

### 8.13 Oscar — MFA URL Check (RESOLVED)

Target `"accounts.hioscar.com"` — previous check produced false positives.

---

### 8.14 Single-Agent Recovery

```bash
python scripts/molina_downloader.py --agent 14
python scripts/ambetter_bot.py --agent 15
python scripts/oscar_bot.py --agent 2
python scripts/cigna_bot.py --agent 2
python scripts/united_bot.py --agent 2
```

Uses `write_r1_xlsx()` from utils.py — merges, never resets. Other agents' rows preserved.
State file will NOT update on single-agent runs. Acceptable by design.

---

### 8.15 Cigna Filters — Cannot Be Automated (PERMANENT)

Angular SPA ignores programmatic checkbox events. Filters are semi-manual.
Bot pauses → human applies filter → presses ENTER → bot reads result and exports.
Do not attempt to re-automate. Constraint is Angular's event system.

---

### 8.16 Ambetter R2 — Pagination Bug (RESOLVED)

Endpoint returns 334 rows per page via `offset` parameter. Loop until page < 334 rows.

```python
PAGE_SIZE = 334
offset = 0
while True:
    all_rows.append(page_df)
    if len(page_df) < PAGE_SIZE:
        break
    offset += PAGE_SIZE
```

---

### 8.17 United — Delta Diff Not Needed (RESOLVED before build)

`policyTermDate` IS present in live Jarvis export. United uses `file_extract`. No state file.

---

### 8.18 Ambetter — Login Selector Changed (RESOLVED April 2026)

Placeholder changed from `"Email"` to `"Username"`. Fix is in `config/config.yaml` only.
Diagnostic: `python scripts/ambetter_bot.py --debug-selectors`

---

### 8.19 United — Playwright Bot Detection (PERMANENT)

Persistent Chrome profiles per agent under `data/chrome_profiles/{agent_name}/`.
Login fully manual: bot opens page, prints username, human logs in, presses ENTER.

**Agents with `skip_bot: true` (manual data entry — no bot):**
- Mike Lavernia (`mlaverniauhc`)
- Tony Montenegro (`tonymontenegro`)
- Yusbel Ortega (`yusbellhealth@lvupagent.com`)

---

### 8.20 Security Issues — Code Review April 2026 (RESOLVED Phase R — except §8.20.5)

**CRITICAL — united_bot.py prints passwords to terminal.** ✅ RESOLVED (Phase R)
Replaced with username print + note to use password manager.

**CRITICAL — logs/ and data/ directories not in .gitignore.** ✅ RESOLVED (Phase R)
`files/.gitignore` now covers `logs/`, `data/raw/`, `data/output/`, `data/state/`,
`data/chrome_profiles/`, `credentials/`, `config/agents.yaml`, `.env`.

**HIGH — agents.yaml loaded at module level in ambetter_bot.py.** ✅ RESOLVED (Phase R)
`yaml.safe_load()` moved inside `_load_agents()`.

**HIGH — Oscar/Cigna/United column names hardcoded in Python files.** ✅ RESOLVED (Phase R)
All three now read from `config/config.yaml` → `carriers.{carrier}.columns`.

**§8.20.5 — MEDIUM — United R2 uses name-based composite `policy_number`.** (OPEN)
Confirm real subscriber ID column from next live United export.

---

### 8.21 United — SMS 2FA for New Agents (OPEN — April 2026)

**Situation:** Some new United agents do not have Microsoft Authenticator configured.
Their MFA is delivered via SMS.

**Resolution path:**
- **Path A (preferred):** Boss associates Microsoft Authenticator to new agent accounts.
  No code change required.
- **Path B (fallback):** Bot pauses and prompts for manual SMS code entry:
  ```python
  print(f"[United] [{agent_name}] SMS code sent to registered phone.")
  code = input("Enter SMS code: ").strip()
  await page.fill(SEL_SMS_CODE_FIELD, code)
  await page.click(SEL_SMS_SUBMIT)
  ```
  Requires confirming the SMS input field selector from the portal before implementing.

**Status:** Waiting on boss to confirm whether MS Auth setup is possible for new agents.
Do not implement Path B until Path A is confirmed impossible.

---

### 8.22 R1 XLSX Merge Bug — Inconsistent Across Bots (ACTIVE)

**Problem:** `--agent N` on some bots resets the entire carrier XLSX, losing all other
agents' data. Inconsistent implementation across phases 1–6.

**Resolution:** `write_r1_xlsx()` moves to `scripts/utils.py` as the single
authoritative implementation. Behavior: load existing file → remove rows for agents
in current run → append new rows → save. Never overwrites entire file.

```python
# scripts/utils.py
def write_r1_xlsx(r1_records: list[dict], carrier: str, log=None) -> None:
    """Merge-on-write. Safe for single-agent reruns and full runs."""
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
    except Exception as exc:
        (log or logging.getLogger(__name__)).warning(
            "%s | R1 XLSX write failed (non-fatal): %s", carrier, exc
        )
```

---

### 8.23 R2 Start Date — Rolling Window Retired (RESOLVED Phase R — April 2026)

**Problem:** `calculate_period_start()` returned the last day of the previous month.
Ambetter, Oscar, and Cigna publish cancellations with days-to-weeks of lag. A rolling
monthly window silently dropped records whose `coverage_end_date` fell before the
cutoff by the time the portal published them. Example: a March 20 cancellation
published on April 8 was captured; the same cancellation published on May 2 was not.

**Resolution:** `get_r2_start_date()` replaces `calculate_period_start()`. It reads
a fixed historical date from `config/config.yaml` → `r2.start_date`. Current value
`"2025-12-01"`. See §6 for the full rationale.

**Safety:** the dedup key `(carrier, policy_number, coverage_end_date)` keep="first"
makes repeated historical captures idempotent — already-captured rows stay put.

**Operator action:** roll the start date forward by editing config.yaml when confident
every carrier has published all cancellations for the trailing 4–6 week lag window.

---

## 9. CARRIER COLUMN MAPS (live data — confirmed)

### Ambetter
```yaml
policy_term_date: "Policy Term Date"
first_name:       "Insured First Name"
last_name:        "Insured Last Name"
policy_number:    "Policy Number"
state:            "State"
member_dob:       "Member Date Of Birth"
broker_name:      "Broker Name"
payable_agent:    "Payable Agent"    # = "Plan Advisors, LLC" — DO NOT USE
```

### Molina
```yaml
status:        "Status"             # Active | Pending Payment | Pending Binder | Terminated
member_count:  "Member_Count"
address:       "Address1"           # Household dedup key
end_date:      "End_date"           # Always last day of month
broker_first:  "Broker_First_Name"
broker_last:   "Broker_Last_Name"
subscriber_id: "Subscriber_ID"
```

Molina household dedup: `Member_Count > 1` → dedup by `Address1` per agent.

### Oscar
```yaml
policy_status:     "Policy status"      # Active | Inactive | Grace period | Delinquent
lives:             "Lives"
coverage_end_date: "Coverage end date"  # Always last day of month for Inactive
member_name:       "Member name"
date_of_birth:     "Date of birth"
state:             "State"
member_id:         "Member ID"          # Use as policy_number in R2 schema
```

R1 counts: Active, Grace period, Delinquent (everything except Inactive).

### Cigna
```yaml
termination_date: "Termination Date"
first_name:       "Primary First Name"
last_name:        "Primary Last Name"
policy_number:    "Subscriber ID (Detail Case #)"
state:            "State"
member_dob:       null                  # NOT available (PERMANENT — §8.12)
writing_agent:    "Writing Agent"
policy_status:    "Policy Status"       # "Terminated" for R2
```

### United (confirmed April 2026)
```yaml
termination_date: "policyTermDate"     # YYYY-MM-DD — parse with errors="coerce"
first_name:       "memberFirstName"
last_name:        "memberLastName"
state:            "memberState"
member_dob:       "dateOfBirth"         # Available — format MM/DD/YYYY
writing_agent:    "agentName"           # DO NOT USE
policy_status:    "planStatus"          # 'A' = Active, 'I' = Inactive/Terminated
policy_number:    null                  # ⚠ No subscriber ID confirmed — composite key
```

R2 filter: `planStatus == 'I'` AND `policyTermDate >= calculate_period_start()`
Header detection: scan rows 0–9 for `"memberFirstName"` (row 0 blank, row 1 legal disclaimer).

---

## 10. STATE FILE RULES

| File | Purpose | Update Rule |
|------|---------|-------------|
| `molina_last_run_date.txt` | First-run detection | Full success only (all 15 agents) |
| `ambetter_last_run_date.txt` | First-run detection | Full success only (all 16 agents) |
| `cigna_last_run_date.txt` | First-run detection | Full success only |

United has no state file. R2 uses `calculate_period_start()` + dedup key.

---

## 11. TECHNOLOGY STACK

| Carrier | Library | Rationale |
|---------|---------|-----------|
| Molina | **Selenium** | Production. Working. Do not touch until Phase 9. |
| Ambetter | **Selenium + requests** | Production. requests bypass handles SPA. Do not touch until Phase 9. |
| Oscar | **Playwright** | ✅ Production. |
| Cigna | **Playwright** | ✅ Production. |
| United | **Playwright** | ✅ Production. |
| Molina + Ambetter | **Playwright** (Phase 9) | After pipeline stable in production. |

### Playwright Boilerplate
```python
async def _run_carrier_async(dry_run, agent_filter):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(accept_downloads=True)
        # NOTE: downloads_path is NOT a valid param — omit it
        ...

def run_carrier(dry_run: bool = False, agent_filter: int | None = None):
    """Sync wrapper. Called by launcher."""
    return asyncio.run(_run_carrier_async(dry_run, agent_filter))
```

### requirements.txt
```
selenium
webdriver-manager
requests
beautifulsoup4
playwright              # pip install playwright && playwright install chromium
pandas
openpyxl
pyyaml
python-dotenv
google-api-python-client
google-auth
```

---

## 12. PHASES STATUS

| # | Deliverable | Status | Notes |
|---|-------------|--------|-------|
| 1 | Molina: portal download + R1 + R2 | ✅ DONE | Selenium |
| 2 | Ambetter: dashboard R1 + cancelled CSV R2 | ✅ DONE | Selenium + requests |
| 3 | Google Sheets writer | ✅ DONE | sheets_writer.py — now reads from XLSX |
| 4 | Oscar: semi-auto + R1 + R2 | ✅ DONE | Playwright |
| 5 | Cigna: VPN + email 2FA + R1 + R2 | ✅ DONE | Playwright |
| 6 | United: semi-auto + file_extract R2 | ✅ DONE | Playwright |
| **R** | **utils.py refactor + security fixes + r2 start date fix** | ✅ DONE | See §8.20, §8.22, §8.23 |
| **GUI** | **tkinter launcher — run center** | **⏳ NEXT** | Replaces Phase 8 |
| 7 | Looker Studio dashboard (6 pages) | ⏳ After GUI | Needs 2+ Sheets pushes |
| 9 | Migrate Molina + Ambetter to Playwright | 🔮 Future | After 4+ stable runs |

**Phase 8 (main.py orchestrator + Task Scheduler) is RETIRED.**
The GUI launcher is the run center. Manual operation is the intentional design.

---

## 13. GOOGLE SHEETS SETUP (PERMANENT REFERENCE)

| Item | Value |
|------|-------|
| Google Cloud Project | `insurance-analytics-491923` |
| Service Account | `sheets-writer@insurance-analytics-491923.iam.gserviceaccount.com` |
| JSON Key Path | `credentials/service_account.json` (gitignored) |
| Drive Folder | `Insurance Analytics — Member Reports` |
| Drive Folder ID | `1ZHkof6dLJ9HgPPLy5_5Q1Nnmjv-csTU1` |
| April 2026 Sheet ID | `1XKqoMNMalPRX90nEKQxXYM351FJKlmrpMDZlFoqP-IE` |

**Monthly sheet setup procedure:**
1. Drive folder → New → Google Sheets → rename to `"May 2026"`
2. Rename default tab to `Summary`
3. Add tab: `Deactivated This Period`
4. Add tab: `Active Members`
5. Share with service account (Editor)
6. Copy sheet ID → add to `.env` as `SHEET_ID_MAY_2026=<id>`

**sheets_writer.py reads from XLSX files on disk.** Verify all carrier XLSX files have
data before pushing. Run `sheets_writer.py --dry-run` to preview what will be written.

**Looker Studio requirements:**
- All 5 carriers must have at least one Sheets push in the current month.
- `Active Members` tab needs at least 2 different `run_date` values for trend pages.
- `Deactivated This Period` needs at least a few R2 rows for churn analysis.

---

## 14. STANDARD RUN PROCEDURE

### Normal run day (Monday or Friday)

1. Open the GUI: `python scripts/launcher.py`
2. Run each carrier with its [Run] button. Approve MFA when prompted.
3. If any agent fails, use [Run Agent N] to rerun that agent alone. The carrier XLSX
   merges the result — other agents are unaffected.
4. Verify all carrier XLSX files in `data/output/` look correct (row counts, no blanks
   in unexpected places).
5. Click [Push to Google Sheets] when all carriers are complete and verified.
6. Confirm the push succeeded in the log panel.

### Off-schedule run (missed Monday or Friday)

Same procedure. `period_start` stays as last day of previous month.
Off-schedule runs are operational exceptions — `calculate_period_start()` is correct.

### Single-agent rerun (via terminal)

```bash
python scripts/{carrier}_bot.py --agent N
```

The carrier XLSX is updated for that agent only. Other agents' rows preserved.
Re-push to Sheets after the rerun if the daily push was already done.

---

## 15. ARCHITECTURE PRINCIPLES (Added April 2026)

### Why These Principles Exist

During phases 1–6, shared functions were copy-pasted into every file. By Phase 6,
`calculate_period_start` existed in 6 places, `_append_deactivated_xlsx` in 5, and they
had diverged. `scripts/utils.py` is the permanent fix. All future shared infrastructure
belongs there.

### The Three Categories of Code

| Category | What it is | Where it lives |
|----------|-----------|----------------|
| **Infrastructure** | Retry, logging, file I/O, date math, dedup, XLSX writes — no business rules | `scripts/utils.py` |
| **Domain** | Carrier-specific: column names, active status definitions, R1/R2 filter logic, portal navigation | `scripts/{carrier}_bot.py` + `config/config.yaml` |
| **Orchestration** | Triggering bots, pushing to Sheets, displaying run status | `scripts/launcher.py` |

**If a function contains no carrier-specific logic → `utils.py`.**
**If it contains carrier-specific logic → bot file or config.yaml.**
**If it triggers/displays → `launcher.py`.**

### Dependency Direction

```
After Phase R:
  molina_downloader.py  ──► utils.py
  ambetter_bot.py       ──► utils.py
  oscar_bot.py          ──► utils.py
  cigna_bot.py          ──► utils.py
  united_bot.py         ──► utils.py
  sheets_writer.py      ──► utils.py   (logging helpers)
  launcher.py           ──► all bots   (calls run_X)
  launcher.py           ──► sheets_writer.py
```

### Module Boundary Rule

Each bot has exactly one public function. The `agent_filter` parameter handles both full
runs and single-agent reruns — the same function, not two separate ones.

```python
def run_oscar(dry_run: bool = False, agent_filter: int | None = None):
    """Sync wrapper. Called by launcher for both [Run] and [Run Agent N]."""
    return asyncio.run(_run_oscar_async(dry_run, agent_filter))
```

### scripts/utils.py — Authoritative Contents

```python
get_r2_start_date() -> date
    # Fixed historical R2 cutoff read from config/config.yaml r2.start_date.
    # See §6 and §8.23. Replaces calculate_period_start (retired Phase R).

run_type() -> str
    # "Monday", "Friday", or "Manual".

setup_logging(carrier: str) -> logging.Logger
    # logs/run_YYYYMMDD_HHMMSS_{carrier}.log + stdout.
    # Seconds in filename prevent collision.

with_retry(func, operation_name, max_attempts=3, log=None)
    # 3-attempt retry, 5/15/45s backoff. Never use for auth.

write_r1_xlsx(r1_records, carrier, log=None) -> None
    # Merge-on-write. Load → remove current agents → append → save.
    # Never resets. Safe for single-agent reruns.

append_deactivated_xlsx(r2_records, carrier, log=None) -> None
    # Append + dedup. Key: (carrier, policy_number, coverage_end_date).
    # Existing rows win. Null policy_number guard included.
```

### sheets_writer.py — Public API (UPDATED Phase R)

```python
push_to_sheets(dry_run: bool = False) -> None
    # Reads data/output/{carrier}_all_agents.xlsx (R1) and
    # data/output/deactivated_members.xlsx (R2), pushes to Google Sheets.
    # Never called by bots. Launcher's [Push to Sheets] button invokes it.
    # Replaces write_run(r1_records, r2_records, dry_run) (retired Phase R).
```

---

## 16. GUI LAUNCHER SPECIFICATION (Phase GUI)

The launcher replaces the terminal as the operator interface. Full control, no command
knowledge required, handoff-ready.

### Buttons

| Button | Action |
|--------|--------|
| **[Run]** per carrier | Launches bot subprocess. Streams stdout to log panel. Disables while running. |
| **[Run Agent N]** per carrier | Prompts for agent index. Runs `--agent N`. Merges result into carrier XLSX. |
| **[Run All Carriers]** | Runs all 5 bots sequentially. Does NOT auto-push to Sheets. |
| **[Push to Google Sheets]** | Runs `sheets_writer.py` (reads XLSX → writes Sheets). Separate intentional step. |
| **[Open Output Folder]** | `os.startfile("data/output/")` |
| **[Open Google Sheet]** | Opens current month's Sheet URL. Reads from `.env`. |
| **[View Last Log]** | Opens most recent `logs/` file in default text editor. |

### Status Per Carrier

- ✅ — carrier XLSX modified today, expected agent count present
- ⚠ — XLSX modified before today, or agent count below expected, or 3 skip_bot agents (United)
- ❌ — XLSX missing or last run logged failures
- — — XLSX does not exist (never run)

### Key Design Rules

- **[Push to Google Sheets] is always a separate, explicit button.** Never auto-triggered.
- **Single-agent rerun is safe** because `write_r1_xlsx()` merges, never resets.
- **Log panel** streams subprocess stdout using threading. Never blocks UI.
- **United row** shows: "3 agents require manual entry (Mike, Tony, Yusbel)"
- **All buttons disable** while any subprocess is running.

---

*Last updated: April 21, 2026*
*Phase R complete: utils.py refactor + security fixes + R2 start date fix. See §8.20, §8.22, §8.23.
Key module changes: `calculate_period_start()` → `get_r2_start_date()` (fixed historical date
from config.yaml); `sheets_writer.write_run(r1, r2, dry_run)` → `push_to_sheets(dry_run)` (reads
XLSX from disk); all 5 bots consume shared infrastructure from `scripts/utils.py`. Oscar/Cigna/
United column names + selectors moved to config.yaml. Password print in united_bot.py removed.
`data/chrome_profiles/` added to .gitignore.*

*Previously: April 20, 2026 — Operating model confirmed as manual (GUI-driven, no Task Scheduler).
Pipeline flow updated to Option A: bots write XLSX → sheets_writer reads XLSX → Sheets.
Phase 8 (orchestrator + Task Scheduler) RETIRED — GUI replaces it. §8.21 SMS 2FA open
issue added for United new agents (awaiting boss confirmation on MS Auth setup).
§8.22 R1 XLSX merge bug documented with authoritative fix in utils.py. §7 updated with
merge-on-write and sheets_writer-reads-XLSX standards. §14 rewritten as manual run
procedure. §15 updated: write_r1_xlsx added to utils.py, orchestration now points to
launcher.py. §16 GUI Launcher Specification added. Phase 12 updated: Phase 8 retired,
GUI added, order confirmed as R → GUI → 7 → 9.*
