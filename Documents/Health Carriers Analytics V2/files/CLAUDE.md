# CLAUDE.md — Insurance Analytics Automation
## Project Context, Architecture Decisions & Engineering Log

---

## 1. PROJECT PURPOSE

Bi-weekly automation pipeline (every **Monday and Friday**) that logs into 5 insurance
carrier portals, extracts member data per agent, and produces two reports:

| Report | Content | Destination |
|--------|---------|-------------|
| **R1 — Active Members** | Count of active members per agent per carrier | Contingency table (pivot) in Google Sheets + individual XLSX per carrier |
| **R2 — Deactivated This Period** | Member-level list of who was lost since the last run | `data/output/deactivated_members.xlsx` (append-only, for manager outreach) |

**The contingency table is the primary deliverable.** Every other component exists to feed it.

---

## 2. FINAL DELIVERABLE SHAPE

```
Google Sheets — "April 2026"
├── Tab: Summary (OVERWRITE each run)
│     Agent         | Ambetter | Cigna | Molina | Oscar | United | Total
│     Brandon       |       95 |     - |      6 |    16 |     22 |  139
│     Felipe        |      461 |    72 |     53 |   166 |    228 |  980
│     Total         |    6,939 |   558 |    757 | 2,555 |  1,711 | 12,520
│
├── Tab: Deactivated This Period (APPEND each run — net-new only, deduped)
│     run_date | carrier | agent_name | member_name | member_dob | state | coverage_end_date | policy_number
│
└── Tab: Active Members (APPEND each run — audit trail for Looker Studio)
      run_date | run_type | carrier | agent_name | active_members | status

data/output/
├── molina_all_agents.xlsx       ← R1 per agent (Molina)
├── ambetter_all_agents.xlsx     ← R1 per agent (Ambetter)
└── deactivated_members.xlsx     ← R2 all carriers, append-only, deduped
```

Missing carrier data (failed run) = **blank cell**, never zero.

---

## 3. CARRIERS & AUTOMATION STATUS

| Carrier | Portal URL | Auth | R1 Method | R2 Method | Library | Phase | Status |
|---------|-----------|------|-----------|-----------|---------|-------|--------|
| **Molina** | account.evolvenxt.com | user+pass, no 2FA | CSV download → sum active statuses | Same CSV, Terminated rows | Selenium | 1 | ✅ DONE |
| **Ambetter** | broker.ambetterhealth.com | user+pass, no 2FA | Dashboard odometer | requests+cookies → base64 ZIP | Selenium + requests | 2 | ✅ DONE |
| **Oscar** | accounts.hioscar.com | MS Authenticator (boss phone) SEMI-AUTO | CSV → sum Lives non-Inactive | Same CSV, Inactive rows + date filter | **Playwright** | 4 | ✅ DONE |
| **Cigna** | cignaforbrokers.com | USA VPN + email 2FA (webmail.ligagent.com) | BOB filter → Total results | Portal export: Terminated + date filter | **Playwright** | 5 | ⏳ NEXT |
| **United** | uhcjarvis.com | MS Authenticator (boss phone) SEMI-AUTO | Dashboard count | Delta diff vs state file | **Playwright** | 6 | ⏳ |

### Phase Order Rationale
- **Oscar before Cigna:** Oscar is the simplest new carrier (semi-auto, single CSV, no 2FA).
  Correct surface to learn Playwright before tackling Cigna's VPN+2FA complexity.
- **Oscar before United:** `oscar_report.py` already existed. Phase 4 wrapped automation around it.
- **United last among new carriers:** Delta diff logic is the complexity, not the browser layer.
- **Molina + Ambetter stay on Selenium:** Both production and working. Migration in Phase 9 only.

**Login architecture:** ALL carriers use **agent-level login** (one session per agent).
Molina: 15 agents. Ambetter: 16 agents. Oscar: 13 agents. Credentials in `config/agents.yaml` (gitignored).

---

## 4. FILE STRUCTURE

```
scripts/
├── molina_downloader.py   ← Selenium. Run this for Molina.
├── molina_report.py       ← Imported by downloader (process_csv). Never run directly.
├── ambetter_bot.py        ← Selenium + requests. Run this for Ambetter.
├── sheets_writer.py       ← Phase 3: Google Sheets writer + Summary pivot. DONE.
├── oscar_bot.py           ← Phase 4: Playwright. DONE.
├── cigna_bot.py           ← Phase 5: Playwright
├── united_bot.py          ← Phase 6: Playwright
└── main.py                ← Phase 8: orchestrator

config/
├── agents.yaml            ← credentials per carrier per agent (GITIGNORED)
├── agents.yaml.example    ← template committed to repo
└── config.yaml            ← portal URLs, selectors, column mappings, timeouts

data/
├── raw/{carrier}/{YYYY-MM}/{YYYY-MM-DD}/{agent_name}/
├── output/
│   ├── molina_all_agents.xlsx
│   ├── ambetter_all_agents.xlsx
│   └── deactivated_members.xlsx
└── state/
    ├── molina_last_run_date.txt
    ├── ambetter_last_run_date.txt
    ├── cigna_last_run_date.txt
    └── united_last_run.json

logs/run_YYYYMMDD_HHMM.log
status/last_run.json
```

---

## 5. DATA SCHEMAS

### R1 — Active Members
```python
{
    "run_date":         "2026-03-27",
    "run_type":         "Friday",
    "carrier":          "Ambetter",
    "agent_name":       "Brandon Kaplan",
    "active_members":   95,
    "status":           "success",       # "success" | "failed"
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
    "member_dob":        "07/24/2000",      # null for Cigna — not available in export
    "state":             "SC",
    "coverage_end_date": "2026-03-31",
    "policy_number":     "U70066328",
    "last_status":       "Cancelled",
    "detection_method":  "download_filter",  # "file_extract" | "download_filter" | "delta_diff"
}
```

---

## 6. R2 DATE SCOPING LOGIC

### period_start Definition (UPDATED April 2026)

**`period_start` = last day of the previous month.**

Rationale confirmed from live data (April 7, 2026 run):
- **Molina:** 100% of terminated members showed `End_Date = 2026-03-31`, regardless of
  actual cancellation date. Portal records all March cancellations at month-end.
- **Oscar:** 97% of inactive members showed `Coverage end date = 2026-03-31`. Same pattern.
- **Ambetter:** Mixed — actual cancellation dates reflected (Mar 31, Apr 1, Apr 2, Apr 3).
  Real dates, not end-of-month rounding.
- **Cigna:** Mixed — actual cancellation dates reflected (Mar 31, Apr 1, Apr 2).
  Real dates, not end-of-month rounding.

**Conclusion:** Using `last Friday` as period_start misses the bulk of Molina and Oscar
deactivations, which are always stamped at month-end. Anchoring to last day of previous
month captures everything correctly for all carriers.

```python
from datetime import date
from calendar import monthrange

def calculate_period_start(today: date = None) -> date:
    """
    Returns the last day of the previous month.

    Rationale: Molina and Oscar stamp all terminations at end-of-month regardless
    of actual cancellation date. Ambetter and Cigna use real dates but the wider
    window does not cause duplicates — dedup key handles re-capture.

    Dedup prevents double-counting across runs. See section 8.11.
    """
    if today is None:
        today = date.today()
    first_of_this_month = today.replace(day=1)
    last_of_prev_month = first_of_this_month - timedelta(days=1)
    return last_of_prev_month
```

### R2 Deduplication (CRITICAL — read before touching append logic)

Because `period_start` reaches back to end-of-previous-month, consecutive runs in the
same month would re-capture the same terminated members. Deduplication prevents this.

**Dedup key:** `(carrier, policy_number, coverage_end_date)`

Using `coverage_end_date` in the key handles the edge case where a member cancels,
re-enrolls, and cancels again — their second cancellation will have a different end date
and will correctly appear as a new R2 record.

**Implementation rule:**
```python
def _append_deactivated_xlsx(new_records: list[dict], path: Path) -> None:
    new_df = pd.DataFrame(new_records)
    if path.exists():
        existing_df = pd.read_excel(path)
        combined = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined = new_df
    # Dedup — keep first occurrence (existing rows win over new)
    combined = combined.drop_duplicates(
        subset=["carrier", "policy_number", "coverage_end_date"],
        keep="first"
    )
    combined.to_excel(path, index=False)
```

The same dedup logic applies before appending to Google Sheets — load existing rows
from the "Deactivated This Period" tab, filter new records against them, append only
the net-new rows. **Never query Sheets for dedup in a loop** — load once, filter in
pandas, write once.

**Rules:**
- R2 **always runs** including first run. Calendar window is the only filter.
- State files **never** used as R2 date filters. Exception: United delta diff.
- `agent_name` **always** from `agents.yaml`. Ambetter's `Payable Agent` column = `Plan Advisors, LLC` (broker entity) — never use it.

---

## 7. ENGINEERING STANDARDS (NON-NEGOTIABLE)

| Standard | Rule |
|----------|------|
| **Error isolation** | One carrier failure never blocks others |
| **Retry** | 3 attempts, backoff 5s / 15s / 45s. Auth failures do NOT retry. |
| **Logging** | `logs/run_YYYYMMDD_HHMM.log` — timestamp \| carrier \| agent \| result \| duration |
| **Config** | URLs, selectors, column names → `config/config.yaml`. Credentials → `agents.yaml`. Never hardcoded. |
| **Standalone** | Every module runs standalone without writing to Sheets or updating state |
| **Dry run** | `--dry-run`: full run, no Sheets write, no state update |
| **No-spiral rule** | 3 failed attempts → stop, diagnose, propose 2 alternatives. Never attempt 4. |
| **State safety** | State files updated ONLY after fully successful carrier run |
| **Output safety** | `deactivated_members.xlsx` append-only — never overwritten. Deduped on every append. |
| **agent_name** | Always from `agents.yaml`. Never from CSV columns. |
| **Playwright waits** | Auto-wait only. Never `page.wait_for_timeout()`. |
| **Async boundary** | Playwright bots expose sync wrapper: `def run_X(): return asyncio.run(_async())` |
| **R2 dedup** | Key: `(carrier, policy_number, coverage_end_date)`. Existing rows always win. |

---

## 8. CRITICAL ISSUES & RESOLUTIONS

### 8.1 Ambetter R2 — Salesforce Lightning SPA Kills ChromeDriver (RESOLVED)

**Problem:** Policies SPA kills ChromeDriver window handle on any navigation.
Three Selenium approaches failed (JS click, driver.get, new tab).

**Root cause:** Salesforce Lightning security frame reinitializes browser context on load.
Page-level — not fixable with Selenium.

**Resolution:** Bypass Selenium. Use `requests` + cookies from live session.
Export URL returns HTML modal with entire ZIP embedded as base64 data URI in `<a>` tag.
No second HTTP request — decode directly.

```python
session = requests.Session()
session.headers.update({
    "User-Agent": driver.execute_script("return navigator.userAgent;"),
    "Referer":    driver.current_url,
})
for ck in driver.get_cookies():
    session.cookies.set(ck["name"], ck["value"], domain=ck.get("domain"))

r = session.get(
    "https://broker.ambetterhealth.com/apex/BC_VFP02_PolicyList_CSV"
    "?filter=cancelled&offset=0", timeout=60
)
# Response HTML contains: <a href="data:application/zip;base64,PAYLOAD" download="policies.zip">
soup = BeautifulSoup(r.text, "html.parser")
for a in soup.find_all("a", href=True):
    if a["href"].startswith("data:") and ";base64," in a["href"]:
        _, encoded = a["href"].split(";base64,", 1)
        file_bytes = base64.b64decode(encoded.strip())
```

---

### 8.2 Ambetter — Historical Flood on First Run (RESOLVED)

Fixed: R2 always uses `calculate_period_start()`. Calendar window only, every run.
Dedup key prevents re-capture across runs. See section 6.

---

### 8.3 Ambetter CSV — `Payable Agent` ≠ Individual Agent (PERMANENT)

`Payable Agent` = `Plan Advisors, LLC`. Never use. Always source `agent_name` from `agents.yaml`.

---

### 8.4 R2 Count Discrepancy vs R1 Delta (EXPLAINED — April 2026)

Previously hypothesized as mid-period cancellations getting future term dates. Now
confirmed: Molina and Oscar stamp ALL terminations at end-of-month regardless of actual
date. Ambetter and Cigna use real cancellation dates.

**Resolution:** `calculate_period_start()` now returns last day of previous month.
Dedup key `(carrier, policy_number, coverage_end_date)` prevents double-counting across
runs. See section 6 for full explanation.

---

### 8.5 Molina — Carrier Selection Flakiness (NON-BLOCKING)

Fails attempt 1, succeeds on retry. `with_retry()` handles it.

---

### 8.6 Molina — `_build_r2_records()` Silent Column Drop (RESOLVED)

**Problem:** Internal field names in `_build_r2_records()` were mismatched to R2 schema
column names. pandas silently dropped member_name, member_dob, state, and policy_number.

**Resolution:** Field names corrected to match R2 schema exactly. A one-time recovery
script (`inject_molina_r2.py`) was used to backfill missed records.

---

### 8.7 Google Sheets — Service Account storageQuotaExceeded on File Creation (RESOLVED)

**Problem:** `drive.files().create()` returned `403 storageQuotaExceeded` even though
the personal Drive account had 14.98 GB free.

**Root cause:** New Google Cloud projects block service accounts from creating files
via the Drive API due to abuse prevention. The error message is misleading — it is not
a quota issue, it is a new-project restriction.

**Resolution:** Create the monthly sheet manually in Drive once, then register its ID
in `.env` as `SHEET_ID_APRIL_2026=<id>`. Modified `get_or_create_month_sheet()` to
check for this env var first and skip the Drive create call entirely if found.
For production, a new sheet is created manually at the start of each month and its ID
added to `.env`. The Drive search + create path remains in code for future use.

---

### 8.8 Google Sheets — Missing Headers on Append Tabs (RESOLVED)

**Problem:** `Deactivated This Period` and `Active Members` tabs had no header row
after first `--test` run. Data started on row 1.

**Root cause:** `get_or_create_month_sheet()` called `_ensure_tab_headers()` only on
the pinned-ID path, not the Drive-search path. The existing-sheet path returned the ID
bare with no header initialization.

**Resolution:** Two fixes applied:
1. Both sheet-retrieval paths now call `_ensure_tab_headers()`
2. Belt-and-suspenders guard added to `append_deactivated()` and `append_r1_log()`:
   reads A1:Z1 before every append — if empty, writes headers first. Protects against
   manually cleared headers and any future path that bypasses initialization.

---

### 8.9 sheets_writer.py — Standalone Test Mode (RESOLVED)

`python scripts/sheets_writer.py` with no args now prints usage and exits (no writes).
Test fixture only runs with explicit `--test` flag.
`--test --dry-run` resolves sheet ID and logs what would be written without any API writes.

---

### 8.10 Oscar — Modal Dismissal Uses Probe Loop (PERMANENT)

Oscar's button classes are hashed and change on every deployment. Never hardcode button
text or class selectors. Use a probe loop targeting the last visible button:

```python
# Find all visible buttons, click the last one (modal dismiss)
buttons = await page.locator("button:visible").all()
if buttons:
    await buttons[-1].click()
```

Confirmed working across multiple Oscar portal deployments.

---

### 8.11 Oscar / Playwright — `downloads_path` Not Valid (RESOLVED)

`downloads_path` is not a valid parameter in the installed Playwright version.
Remove it from `browser.new_context()`. Use `download.save_as()` directly.

```python
# WRONG
context = await browser.new_context(accept_downloads=True, downloads_path=str(dl_dir))

# CORRECT
context = await browser.new_context(accept_downloads=True)
# ...
await download.save_as(dl_dir / download.suggested_filename)
```

---

### 8.12 Cigna — `member_dob` Not Available in Export (PERMANENT)

The Cigna Book of Business export does not include date of birth. All Cigna R2 records
will have `member_dob = null`. This is a known data gap — do not attempt to derive it
from other columns. Document clearly in any Cigna R2 output.

---

### 8.13 Oscar — MFA URL Check (RESOLVED)

MFA URL check must target `"accounts.hioscar.com"` — prior check produced false
positives. Confirmed correct pattern after Phase 4 debugging.

---

### 8.15 Cigna Filters — Cannot Be Automated (PERMANENT)

Angular SPA does not register programmatic checkbox changes unless the element receives
a real browser interaction event. Both R1 (Medical filter) and R2 (Terminated + date
filter) automation failed — automated clicks set the DOM state but Angular never fired
the underlying data query, so results were always unfiltered.

**Resolution:** Both filters are semi-manual, consistent with the project's semi-auto
philosophy (2FA, MS Authenticator). Bot pauses at each step with printed instructions.
Human applies the filter and clicks Apply, bot reads the count label and (for R2) clicks
Export Filtered.

R1 flow: pause → human applies Medical/Active filter → ENTER → bot reads `label.pr-5`
R2 flow: pause → human applies Terminated + date filter → ENTER → bot exports

**Do not attempt to re-automate these filters.** The constraint is Angular's event
system, not selector brittleness. No selector change or wait strategy can fix it.

Selectors removed from bot constants and config.yaml:
- `filter_medical` (`label[for='productTypesMedical']`)
- `filter_terminated` (`label[for='policyStatusesTerminated']`)
- `filter_active` (`label[for='policyStatusesActive']`)
- `apply_button` (`button.btn.btn-primary`)
- `termination_date_select` (`select#terminationDate`)
- `termination_date_from` (`input#terminationDateFrom`)

---

### 8.14 Single-Agent Recovery

```bash
python scripts/molina_downloader.py --agent 14
python scripts/ambetter_bot.py --agent 15
python scripts/oscar_bot.py --agent 2
```

State file will NOT update on single-agent runs. Acceptable by design.

---

## 9. CONFIRMED COLUMN NAMES

### Ambetter (live CSV, all cancelled)
```yaml
policy_term_date: "Policy Term Date"      # Real cancellation date — NOT end-of-month
first_name:       "Insured First Name"
last_name:        "Insured Last Name"
policy_number:    "Policy Number"
state:            "State"
member_dob:       "Member Date Of Birth"
broker_name:      "Broker Name"           # Use this for agent matching, not "Payable Agent"
payable_agent:    "Payable Agent"         # = "Plan Advisors, LLC" — DO NOT USE
```

### Molina (live CSV)
```yaml
status:       "Status"           # Active | Pending Payment | Pending Binder | Terminated
member_count: "Member_Count"
address:      "Address1"         # dedup key for multi-member households
end_date:     "End_date"         # Always last day of month — NOT real cancellation date
broker_first: "Broker_First_Name"
broker_last:  "Broker_Last_Name"
subscriber_id: "Subscriber_ID"
```

Molina dedup: `Member_Count > 1` → dedup by `Address1`. Single-member rows never deduped.

### Oscar (live CSV)
```yaml
policy_status:     "Policy status"      # Active | Inactive | Grace period | Delinquent
lives:             "Lives"              # R1: sum where status != Inactive
coverage_end_date: "Coverage end date"  # Always last day of month for Inactive — NOT real date
member_name:       "Member name"
date_of_birth:     "Date of birth"
state:             "State"
member_id:         "Member ID"          # Use as policy_number in R2 schema
```

Oscar R1 statuses that count toward active: Active, Grace period, Delinquent (everything except Inactive).

### Cigna (live XLSX export)
```yaml
termination_date: "Termination Date"    # Real cancellation date — NOT end-of-month
first_name:       "Primary First Name"
last_name:        "Primary Last Name"
policy_number:    "Subscriber ID (Detail Case #)"
state:            "State"
member_dob:       null                  # NOT available in Cigna export
writing_agent:    "Writing Agent"       # Use for agent matching
policy_status:    "Policy Status"       # "Terminated" for R2 records
```

---

## 10. STATE FILE RULES

| File | Purpose | Update Rule |
|------|---------|-------------|
| `molina_last_run_date.txt` | First-run detection | Full success only (all 15 agents) |
| `ambetter_last_run_date.txt` | First-run detection | Full success only (all 16 agents) |
| `cigna_last_run_date.txt` | First-run detection | Full success only |
| `united_last_run.json` | Previous inactive list for delta diff | Full success only |

---

## 11. TECHNOLOGY STACK

### Hybrid Browser Architecture (Decided March 2026)

| Carrier | Library | Rationale |
|---------|---------|-----------|
| Molina | **Selenium** | Production. Working. Do not touch. |
| Ambetter | **Selenium + requests** | Production. requests bypass handles SPA. Do not touch. |
| Oscar | **Playwright** | ✅ Production. Validates Playwright pattern for Cigna + United. |
| Cigna | **Playwright** | After Oscar confirms pattern — NEXT |
| United | **Playwright** | Delta diff is hard part, not browser |
| Molina + Ambetter | **Playwright** (Phase 9) | After all 5 carriers live |

**Puppeteer rejected:** Node.js only. All logic is Python.

### Playwright Boilerplate

```python
from playwright.async_api import async_playwright
import asyncio

async def _run_agent_async(agent: dict, dl_dir: Path) -> dict:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            accept_downloads=True,
            # NOTE: downloads_path is NOT a valid param in installed version — omit it
        )
        page = await context.new_page()
        await page.goto(PORTAL_URL)
        await page.fill(SEL_EMAIL, agent["user"])   # auto-waits
        await page.fill(SEL_PASS,  agent["pass"])
        await page.click(SEL_LOGIN_BTN)             # auto-waits
        async with page.expect_download() as dl_info:
            await page.click(SEL_DOWNLOAD_BTN)
        download = await dl_info.value
        await download.save_as(dl_dir / download.suggested_filename)
        await browser.close()

def run_oscar(dry_run: bool = False) -> dict:
    return asyncio.run(_run_oscar_async(dry_run))
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

| # | Deliverable | Library | Status |
|---|-------------|---------|--------|
| 1 | Molina: portal download + R1 + R2 | Selenium | ✅ DONE |
| 2 | Ambetter: dashboard R1 + cancelled CSV R2 | Selenium + requests | ✅ DONE |
| 3 | Google Sheets: Summary pivot + dual-tab writer | — | ✅ DONE |
| 4 | Oscar: semi-auto + R1 + R2 | **Playwright** | ✅ DONE |
| 5 | Cigna: VPN + email 2FA + R1 + R2 | **Playwright** | ⏳ NEXT |
| 6 | United: semi-auto + delta R2 | **Playwright** | ⏳ |
| 7 | Looker Studio dashboard (6 pages) | — | ⏳ |
| 8 | main.py orchestrator + Windows Task Scheduler | — | ⏳ |
| 9 | Migrate Molina + Ambetter to Playwright | **Playwright** | 🔮 Future |

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

**Monthly sheet setup procedure (repeat at start of each month):**
1. Go to Drive folder → New → Google Sheets → rename to `"May 2026"` (or current month)
2. Rename default tab to `Summary`
3. Add tab: `Deactivated This Period`
4. Add tab: `Active Members`
5. Share sheet with service account email (Editor)
6. Copy sheet ID from URL → add to `.env` as `SHEET_ID_MAY_2026=<id>`

---

## 14. MANUAL RUN PROCEDURE (MISSED SCHEDULED RUN)

When a scheduled Monday or Friday run is missed and executed on another day:

1. `period_start` stays as last day of previous month — do not change it.
   The window opens slightly wider on a late run. Dedup prevents double-counting.
2. Download portal files manually (one per agent, per carrier).
3. Place in `data/raw/{carrier}/YYYY-MM-DD/{agent_name}/`.
4. Run `python scripts/main.py --dry-run` to verify output before committing.
5. Run `python scripts/main.py` to write to Sheets.
6. Note actual run date in `status/last_run.json`.

**Do not modify `calculate_period_start()` to handle Tuesday/Wednesday runs.**
The function is correct. Off-schedule runs are operational exceptions, not design cases.
The overhead of adding weekday-edge logic outweighs the benefit for a rare scenario.

---

*Last updated: April 7, 2026*
*Key changes: Oscar Phase 4 complete. period_start revised to last day of previous month
based on confirmed carrier behavior (Molina/Oscar = end-of-month dates; Ambetter/Cigna = real dates).
R2 dedup key added: (carrier, policy_number, coverage_end_date). Cigna member_dob confirmed null.
Section 9 expanded with Oscar and Cigna column names.*
