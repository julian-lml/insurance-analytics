# PROMPTS.md — Claude Code Ready-to-Paste Prompts
## Insurance Analytics Automation — Phase-by-Phase

> **How to use:**
> 1. Place `CLAUDE.md` in your project root — Claude Code reads it automatically
> 2. Start every session with the **MASTER CONTEXT PROMPT** below
> 3. Then paste the prompt for the phase or debug scenario you need
> 4. After each session update `## 12. PHASES STATUS` in `CLAUDE.md`
>
> **Current phase order:** R (utils.py) → GUI+R3 (launcher + roster) → 7 (Looker Studio) → 9 (Playwright migration)

---

## ── MASTER CONTEXT PROMPT ──────────────────────────────────────────

*Paste this as your FIRST message in every Claude Code session.*

```
Before writing any code, read CLAUDE.md completely. Do not skip any section.
Then confirm you have read it by answering ALL of the following:

1. What are R1, R2, and R3? What is each report's content, frequency, destination,
   and output file? Which reports run in regular mode vs roster mode?

2. Which carriers use Playwright and which use Selenium? Why does that split exist
   and when does it change?

3. What does get_r2_start_date() return, and why did it replace calculate_period_start()?
   Where is the value stored and how do you change it?

4. Where does agent_name always come from? Why does this matter for Ambetter specifically?

5. What is the R2 dedup key? What is the R3 dedup key? Why are they different?

6. What is scripts/utils.py? List every function that must live there.
   What is the rule about shared functions vs carrier-specific logic?

7. Describe the two-step regular flow and the three-step roster flow.
   What does sheets_writer read from? Does it receive in-memory records from bots?

8. What is write_r1_xlsx() and why does it merge instead of overwrite?
   What is write_active_members_xlsx() and when is it called?

9. What does --mode regular vs --mode roster do to each bot?
   For Ambetter, United, and Cigna — what specifically changes between modes?
   For Molina and Oscar — what changes between modes?

10. What is the current phase status and order?
    Why are Phase GUI and Phase R3 combined into one session?
    Why is Phase 8 retired?

11. Name the two CRITICAL security issues from §8.20 and the fix for each.

12. What are the two sections of the GUI launcher? What buttons are in each section?
    Which buttons trigger confirmation dialogs? What warning does the roster dialog show?

13. What does a null/blank carrier value mean in the Excel sheet, and how do you distinguish
    a data gap (carry forward) from a deactivated account (zero out)? See §8.26.

Do not write any code until you have answered all 13 questions correctly.
If CLAUDE.md does not exist, stop and tell me — do not proceed.

Architecture rules that apply to everything you build:
- Shared functions live in scripts/utils.py. Never copy them into bot files.
- Column names belong in config/config.yaml, not as Python constants in bot files.
- Never open agents.yaml or config.yaml at module level — only inside functions.
- Never print passwords. Log usernames only.
- Log filenames include seconds: run_YYYYMMDD_HHMMSS_{carrier}.log
- Each bot's public API: run_{carrier}(dry_run, agent_filter, mode='regular').
- sheets_writer.py reads from XLSX files on disk. Never called by bots.
- write_r1_xlsx() always merges. Never resets. Safe for 1-agent and full runs.
- write_active_members_xlsx() only called when mode='roster'. Never in regular mode.
- R3 is additive. mode='regular' output is identical whether or not R3 exists.
- agent_name always from agents.yaml. Never from CSV columns.
- Null carrier column = data gap (carry forward) OR deactivated account (zero out). See §8.26.
  Two+ consecutive nulls for one agent on one carrier = confirm deactivation before carrying forward.
```

---

## ── PHASE R — utils.py Refactor + Security ─────────────────────────

*Run before any other new development. Phase R is DONE — this prompt is for reference.*

```
Read CLAUDE.md completely and answer the master context questions before writing code.
PHASE R — utils.py refactor. No new features. Move existing functions to shared module.
Apply security fixes from §8.20 in this same session.

WHAT TO CREATE:
  scripts/__init__.py   ← empty file, makes scripts/ a Python package
  scripts/utils.py      ← all shared infrastructure

FUNCTIONS THAT MOVE TO utils.py:

  1. get_r2_start_date() -> date                            ← REPLACES calculate_period_start
     Reads fixed historical date from config/config.yaml → r2.start_date.
     Currently in: all 5 bots as calculate_period_start (diverged copies).
     See §6 and §8.23 in CLAUDE.md for full rationale.

  2. run_type() -> str
     Currently in: oscar_bot.py, cigna_bot.py, united_bot.py

  3. setup_logging(carrier: str) -> logging.Logger
     Filename: run_YYYYMMDD_HHMMSS_{carrier}.log (seconds prevent collision)

  4. with_retry(func, operation_name, max_attempts=3, log=None)
     Currently in: molina_downloader.py only

  5. append_deactivated_xlsx(r2_records, carrier, log=None) -> None
     Currently in: all 5 bots (diverged). Use most complete version.
     Dedup key: (carrier, policy_number, coverage_end_date). Existing rows win.

  6. write_r1_xlsx(r1_records, carrier, log=None) -> None        ← NEW
     Merge-on-write. Load → remove current agents → append → save. See §8.22.

  7. write_active_members_xlsx(r3_records, carrier, log=None) -> None  ← NEW STUB
     Output: data/output/active_members_all.xlsx
     Dedup key: (carrier, policy_number, run_date). Existing rows win.
     In Phase R: implement fully — the function body is needed by Phase GUI+R3.
     R3 schema: run_date, carrier, agent_name, member_first_name, member_last_name,
       member_dob, state, policy_number, plan_name, coverage_start_date, policy_status

SECURITY FIXES:
  1. united_bot.py: remove password print, replace with username + password manager note
  2. Verify .gitignore: logs/, data/raw/, data/output/, data/state/,
     data/chrome_profiles/, config/agents.yaml, credentials/, .env

UPDATE EACH BOT:
  - Delete all local definitions of functions above
  - Add imports from scripts.utils
  - Replace ad-hoc XLSX write logic with write_r1_xlsx(r1_records, carrier)
  - Update run_X() signature: def run_X(dry_run=False, agent_filter=None, mode='regular')
    mode parameter: in Phase R, 'roster' is a no-op that logs "roster mode coming in Phase GUI+R3"

UPDATE sheets_writer.py:
  - Reads from data/output/*.xlsx on disk (not in-memory records)
  - Public API: push_to_sheets(dry_run=False, mode='regular')
    mode='roster' stub: logs "roster push coming in Phase GUI+R3" and returns

RULES:
  - Move functions exactly. Do not change business logic at the same time.
  - Do not touch molina_report.py domain logic.

VERIFICATION (all must pass before committing):
  python scripts/molina_downloader.py --agent 0 --dry-run
  python scripts/ambetter_bot.py --agent 0 --dry-run
  python scripts/oscar_bot.py --agent 0 --dry-run
  python scripts/cigna_bot.py --agent 0 --dry-run
  python scripts/united_bot.py --agent 0 --dry-run
  python scripts/sheets_writer.py --dry-run

DONE WHEN:
  ✓ scripts/__init__.py exists (empty)
  ✓ scripts/utils.py has all 7 functions
  ✓ All 5 bots import from utils.py — no local copies remain
  ✓ write_r1_xlsx() used in every bot for R1 XLSX output
  ✓ write_active_members_xlsx() implemented in utils.py (full body, not stub)
  ✓ All bots have run_X(dry_run, agent_filter, mode='regular') signature
  ✓ sheets_writer.py reads from XLSX on disk
  ✓ sheets_writer.py has push_to_sheets(dry_run, mode) signature
  ✓ All 6 verification commands pass
  ✓ Password print removed from united_bot.py
  ✓ .gitignore verified
  ✓ Committed: "refactor: phase R — utils.py + security fixes"
```

---

## ── PHASE GUI+R3 — Launcher + Active Roster ────────────────────────

*Start only after Phase R is committed and all 6 verification commands pass.*

```
Read CLAUDE.md completely and answer all 12 master context questions before writing code.
Phase R complete. Building Phase GUI+R3 in a single session.

This phase has two parts built together:
  PART A — R3 bot logic: add mode='roster' behavior to all 5 bots
  PART B — GUI launcher: two-section tkinter interface
  PART C — sheets_writer roster push

════════════════════════════════════════════════════════════════════
PART A — R3 BOT LOGIC
Add mode='roster' to each bot. mode='regular' behavior is UNCHANGED.
R3 is additive — never modifies R1 or R2.
════════════════════════════════════════════════════════════════════

R3 SCHEMA (defined in CLAUDE.md §5):
  run_date, carrier, agent_name, member_first_name, member_last_name,
  member_dob, state, policy_number, plan_name, coverage_start_date, policy_status

── MOLINA ──────────────────────────────────────────────────────────
No portal navigation changes. Same CSV used for R1+R2 also provides R3.

mode='roster': after R1+R2, generate R3 records from the already-downloaded CSV:
  Filter: Status in ["Active", "Pending Payment", "Pending Binder"]
  Columns (from config/config.yaml carriers.molina.columns):
    agent_name:           always from agents.yaml
    member_first_name:    parse from name columns (Broker_First_Name pattern for members)
    member_last_name:     parse from name columns
    member_dob:           "dob" → format MM/DD/YYYY
    state:                "State.1" or "State"
    policy_number:        "Subscriber_ID"
    plan_name:            "Product"
    coverage_start_date:  "Effective_date"
    policy_status:        "Status"
  Call: write_active_members_xlsx(r3_records, 'Molina', log)

── OSCAR ────────────────────────────────────────────────────────────
No portal navigation changes. Same CSV used for R1+R2 also provides R3.

mode='roster': after R1+R2, generate R3 records from the already-downloaded CSV:
  Filter: Policy status in ["Active", "Grace period", "Delinquent"]
  Columns (from config/config.yaml carriers.oscar.columns):
    agent_name:           always from agents.yaml
    member_first_name:    first word of "Member name"
    member_last_name:     remainder of "Member name"
    member_dob:           "Date of birth" → format MM/DD/YYYY
    state:                "State"
    policy_number:        "Member ID"
    plan_name:            "Plan"
    coverage_start_date:  "Coverage start date"
    policy_status:        "Policy status"
  Call: write_active_members_xlsx(r3_records, 'Oscar', log)

── AMBETTER ────────────────────────────────────────────────────────
mode='regular' (UNCHANGED):
  requests bypass URL: filter=cancelled → R2 only

mode='roster' (NEW):
  requests bypass URL: https://broker.ambetterhealth.com/apex/BC_VFP02_PolicyList_CSV?filter=all&offset=0
  Same session/cookie pattern as regular mode.
  Pagination: identical structure to current (offset parameter).
  IMPORTANT: Confirm actual page size on first request — filter=all may not return 334
  rows per page. Add logging: log page size of first response before looping.
  After full download, split in Python:

  R2: Policy Status == "Terminated"
      AND Policy Term Date >= get_r2_start_date()
      → append_deactivated_xlsx(r2_records, 'Ambetter', log)

  R3: Policy Status == "Active"
      Columns (all from config/config.yaml carriers.ambetter.columns):
        agent_name:           always from agents.yaml (NEVER from "Payable Agent" — §8.3)
        member_first_name:    "Insured First Name"
        member_last_name:     "Insured Last Name"
        member_dob:           "Member Date Of Birth"
        state:                "State"
        policy_number:        "Policy Number"
        plan_name:            "Plan Name"
        coverage_start_date:  "Policy Effective Date"
        policy_status:        "Policy Status"
      → write_active_members_xlsx(r3_records, 'Ambetter', log)

  Log after split: total rows downloaded, R2 count, R3 count, net new appended each.

── UNITED ───────────────────────────────────────────────────────────
mode='regular' (UNCHANGED):
  Jarvis BOB downloaded with planStatus=I filter → R2 only

mode='roster' (NEW):
  Remove planStatus filter from Jarvis BOB download → download full book.
  Header detection (scan rows 0–9 for "memberFirstName") unchanged.
  After download, split in Python:

  R2: planStatus == "I"
      AND policyTermDate >= get_r2_start_date()
      → append_deactivated_xlsx(r2_records, 'United', log)

  R3: planStatus == "A"
      Columns (from config/config.yaml carriers.united.columns):
        agent_name:           always from agents.yaml
        member_first_name:    "memberFirstName"
        member_last_name:     "memberLastName"
        member_dob:           "dateOfBirth"
        state:                "memberState"
        policy_number:        null (§8.20.5 — unconfirmed)
        plan_name:            "product"
        coverage_start_date:  null (not in Jarvis export)
        policy_status:        "Active" (derived from planStatus == "A")
      → write_active_members_xlsx(r3_records, 'United', log)

  skip_bot agents (Mike, Tony, Yusbel): in roster mode, log manual reminder only.
  Do not attempt automation — same rule as regular mode.

── CIGNA ────────────────────────────────────────────────────────────
mode='regular' (UNCHANGED):
  Bot pauses → operator selects "Terminated" filter → presses ENTER → R2 only.

mode='roster' (NEW):
  Update pause message:
    Regular: "Apply TERMINATED filter in portal, then press ENTER"
    Roster:  "Select ALL POLICIES (no status filter) in portal, then press ENTER"

  After export, split in Python:
  R2: "Policy Status" == "Terminated"
      AND "Termination Date" >= get_r2_start_date()
      → append_deactivated_xlsx(r2_records, 'Cigna', log)

  R3: "Policy Status" == "Active"
      Columns (from config/config.yaml carriers.cigna.columns):
        agent_name:           always from agents.yaml
        member_first_name:    "Primary First Name"
        member_last_name:     "Primary Last Name"
        member_dob:           null (PERMANENT — §8.12)
        state:                "State"
        policy_number:        "Subscriber ID (Detail Case #)"
        plan_name:            "Plan Name"
        coverage_start_date:  "Effective Date"
        policy_status:        "Policy Status"
      → write_active_members_xlsx(r3_records, 'Cigna', log)

════════════════════════════════════════════════════════════════════
PART B — UPDATE sheets_writer.py
════════════════════════════════════════════════════════════════════

push_to_sheets(dry_run=False, mode='regular'):

  mode='regular' (EXISTING — unchanged behavior):
    Reads carrier XLSX → builds R1 pivot → writes Summary tab (overwrite)
    Reads deactivated_members.xlsx → writes Deactivated tab (append + dedup)
    Reads carrier XLSX active counts → writes Active Members tab (append)

  mode='roster' (NEW):
    Reads data/output/active_members_all.xlsx
    Writes Google Sheets tab: "Active Roster – {Month} {Year}"
    Tab is OVERWRITTEN on every roster push — it is a point-in-time snapshot.
    If tab does not exist, create it with headers first (_ensure_tab_headers pattern).
    Log: total rows written, carrier breakdown, sheet tab name.

  CLI: python scripts/sheets_writer.py --mode regular
       python scripts/sheets_writer.py --mode roster
       python scripts/sheets_writer.py --dry-run --mode regular
       python scripts/sheets_writer.py --dry-run --mode roster

════════════════════════════════════════════════════════════════════
PART C — GUI LAUNCHER (scripts/launcher.py)
Build from scratch. tkinter only. No external GUI libraries.
════════════════════════════════════════════════════════════════════

UI LAYOUT:
┌──────────────────────────────────────────────────────────────────┐
│  Limitless Insurance Group — Run Center                          │
│                                                                  │
│  ── REGULAR RUN (R1 + R2) ── Twice weekly / daily ────────────── │
│                                                                  │
│  [▶ Run All Carriers]             [☁ Push to Google Sheets]      │
│                                                                  │
│  Carrier   Status       Last Run      Agents    Action           │
│  ────────────────────────────────────────────────────────────    │
│  Molina    ✅ Today     Apr 24 09:04  15/15     [Run]  [↩]       │
│  Ambetter  ✅ Today     Apr 24 09:18  16/16     [Run]  [↩]       │
│  Oscar     ⚠ Yesterday Apr 23 09:11  12/13     [Run]  [↩]       │
│  Cigna     ✅ Today     Apr 24 09:35  12/12     [Run]  [↩]       │
│  United    ⚠ Manual    Apr 24 09:52   9/12     [Run]  [↩]       │
│                                                                  │
│  ── MONTHLY ROSTER (R3) ── Once a month ─────────────────────── │
│                                                                  │
│  [📋 Run Roster — All Carriers]    [📤 Push Roster to Sheets]    │
│  Last roster run: never                                          │
│                                                                  │
│  ── UTILITIES ────────────────────────────────────────────────── │
│  [📂 Open Output Folder]  [📊 Open Google Sheet]  [📋 Last Log]  │
│                                                                  │
│  ┌─ Log Output ──────────────────────────────────────────────┐   │
│  │ 09:04:12 | MOLINA | INFO | agent 1/15 Brandon K OK        │   │
│  └────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘

BUTTON BEHAVIORS:

[Run] per carrier:
  - sys.executable scripts/{carrier}_bot.py --mode regular
  - Streams stdout to log panel (threading). Disables while running.
  - Status badge + last-run timestamp update on completion.

[↩] (single-agent rerun):
  - Prompts for agent index via dialog
  - sys.executable scripts/{carrier}_bot.py --agent N --mode regular
  - Safe because write_r1_xlsx() merges — other agents unaffected.

[▶ Run All Carriers]:
  - All 5 bots sequentially: Molina → Ambetter → Cigna → United → Oscar
  - mode=regular. Does NOT auto-push to Sheets.
  - All buttons disable during run.

[☁ Push to Google Sheets]:
  - Confirmation: "Push R1 + R2 data to Sheets?"
  - sys.executable scripts/sheets_writer.py --mode regular
  - Streams output to log panel.

[📋 Run Roster — All Carriers]:
  - Confirmation: "Run MONTHLY ROSTER for all carriers?
    This downloads full Book of Business data and takes longer than a regular run."
  - All 5 bots sequentially with --mode roster
  - Updates "Last roster run: {date}" label on success
  - All buttons disable during run.

[📤 Push Roster to Sheets]:
  - Confirmation: "Push active member roster to Sheets?
    This will OVERWRITE the Active Roster tab for {current_month}."
  - sys.executable scripts/sheets_writer.py --mode roster
  - Streams output to log panel.

[📂 Open Output Folder]: os.startfile(str(ROOT / "data" / "output"))
[📊 Open Google Sheet]: open current month Sheet URL from .env in browser
[📋 Last Log]: open most recent file in logs/ in default text editor

STATUS BADGE LOGIC:
  ✅ = XLSX modified today AND row count >= expected agent count
  ⚠  = XLSX modified before today, OR agent count below expected
  ❌ = XLSX exists but last run logged failures (status/last_run.json)
  —  = XLSX does not exist

Expected counts: Molina:15 Ambetter:16 Oscar:13 Cigna:12 United:12 (3 always manual)
United row always shows: "(Mike, Tony, Yusbel require manual entry)"
"Last roster run" label reads from status/last_roster_run.txt. Shows "never" if missing.

TECHNICAL REQUIREMENTS:
  - tkinter only — no external GUI libraries
  - subprocess.Popen for all scripts — non-blocking, real-time output
  - Threading for stdout capture — never block UI thread
  - sys.executable for all subprocess calls — ensures venv Python
  - python-dotenv to read .env for Sheet URL
  - All paths relative to project root (resolve from launcher.py location)
  - Handle missing status files gracefully (first run has no status/last_run.json)

RULES FOR THIS SESSION:
  - mode='regular' behavior in all bots MUST be identical to Phase R output.
    Run --agent 0 --dry-run --mode regular and confirm no behavioral change.
  - R3 is additive. R1 and R2 are never modified.
  - agent_name always from agents.yaml. Never from portal CSV columns.
  - write_active_members_xlsx() imported from scripts.utils — never redefined in bots.
  - Column names from config/config.yaml — never hardcoded in bot files.
  - No-spiral rule: 3 failed attempts → stop, propose 2 alternatives.

VERIFICATION (all must pass before committing):
  python scripts/molina_downloader.py --agent 0 --dry-run
  python scripts/molina_downloader.py --agent 0 --dry-run --mode roster
  python scripts/ambetter_bot.py --agent 0 --dry-run --mode regular
  python scripts/ambetter_bot.py --agent 0 --dry-run --mode roster
  python scripts/oscar_bot.py --agent 0 --dry-run --mode roster
  python scripts/cigna_bot.py --agent 0 --dry-run
  python scripts/united_bot.py --agent 0 --dry-run --mode roster
  python scripts/sheets_writer.py --dry-run --mode regular
  python scripts/sheets_writer.py --dry-run --mode roster
  python scripts/launcher.py   ← window opens, two sections visible, no errors

DONE WHEN:
  ✓ All 5 bots: mode='regular' behavior unchanged, mode='roster' generates R3 records
  ✓ Ambetter roster: filter=all URL, R2+R3 split, pagination confirmed/logged
  ✓ United roster: full BOB download, planStatus A vs I split
  ✓ Cigna roster: pause message updated, Active vs Terminated split
  ✓ Molina + Oscar: R3 generated from existing download, zero extra portal traffic
  ✓ data/output/active_members_all.xlsx written on roster run
  ✓ sheets_writer --mode roster pushes "Active Roster – {Month} {Year}" (overwrite)
  ✓ launcher.py opens with two sections (Regular Run + Monthly Roster)
  ✓ All Regular Run buttons work with --mode regular
  ✓ [Run Roster] and [Push Roster to Sheets] work correctly
  ✓ Confirmation dialogs appear for all push/roster actions
  ✓ United row shows manual agent note
  ✓ "Last roster run" label updates after successful roster run
  ✓ All 10 verification commands pass
  ✓ Committed: "feat: phase GUI+R3 — launcher + active roster"
```

---

## ── PHASE 7 — Looker Studio Dashboard ─────────────────────────────

*Start only after GUI+R3 is committed and the pipeline has pushed to Sheets at least
twice (regular) and at least once (roster).*

```
Read CLAUDE.md completely and answer the master context questions before writing code.
GUI+R3 complete. Pipeline pushed to Sheets. Building Looker Studio dashboard.

PREREQUISITES (verify before starting):
  - Active Members tab: at least 2 different run_date values
  - Deactivated This Period tab: at least 5 rows
  - Active Roster tab: at least one roster push complete
  - Spot-check: run_date is YYYY-MM-DD, active_members is a number,
    carrier spelling is exactly: Ambetter, Cigna, Molina, Oscar, United HC
  - Active Roster tab: member_dob blank (not "null" text) for Cigna rows

KNOWN NULL FIELDS (must display as blank — never as error):
  - member_dob: null for all Cigna R2 and R3 records (permanent — §8.12)
  - policy_number: United uses null until subscriber ID confirmed (§8.20.5)

DATA SOURCES:
  Tab "Summary":               agents × carriers pivot, latest run_date
  Tab "Active Members":        run_date | run_type | carrier | agent_name | active_members | status
  Tab "Deactivated This Period": run_date | carrier | agent_name | member_name |
                                member_dob | state | coverage_end_date | policy_number
  Tab "Active Roster – {Month}": run_date | carrier | agent_name | member_first_name |
                                member_last_name | member_dob | state | policy_number |
                                plan_name | coverage_start_date | policy_status

5 PAGES:

Page 1 — Current Snapshot
  Contingency table: agents × carriers, most recent run_date.
  Grand total row + column. Missing = blank, not zero.

Page 2 — Portfolio Trend
  Line chart: x=run_date, y=SUM(active_members), one line per carrier, 90 days.

Page 3 — Agent Rankings
  Table: agents sorted by total members. Carrier filter control.

Page 4 — Churn Analysis
  Table from Deactivated tab: member_name | agent | carrier | coverage_end_date | state
  Date range filter. Scorecard: total deactivated in period.
  Nulls as blank.

Page 5 — Active Roster
  Table from Active Roster tab: full member-level detail.
  Filters: carrier, agent_name, state, policy_status.
  Scorecard: total active members in filtered view.
  member_dob blank for Cigna rows.

FORMAT: Step-by-step UI walkthrough — not code.
Include: data source connection, tab blending for Page 2, sharing view-only for manager.

DONE WHEN:
  ✓ All 5 pages display with real data
  ✓ Page 1 pivot matches Google Sheets Summary tab
  ✓ Page 5 shows member-level detail with carrier/agent/state filters
  ✓ Cigna member_dob shows blank (not error) in Pages 4 and 5
  ✓ Shared view-only link confirmed working for manager
```

---

## ── PHASE 9 — Playwright Migration ─────────────────────────────────

*Start only after pipeline has run successfully at least 4 consecutive times.*

```
Read CLAUDE.md completely. Phase 9. Pipeline in production with 4+ stable runs.

Migrate Molina and Ambetter from Selenium to Playwright.
Test Ambetter first — if Playwright handles the SPA natively, the requests bypass
can be removed. If it crashes, keep the bypass, replace only the Selenium layer.

Rules:
  - Replace all WebDriverWait / _clickable() / _wait() with auto-wait
  - Never use page.wait_for_timeout()
  - Keep all business logic identical (both mode='regular' and mode='roster')
  - Import from scripts.utils — do not redefine locally
  - Run --agent 0 --dry-run --mode regular AND --mode roster, confirm output identical
  - Do NOT pass downloads_path to browser.new_context()

DONE WHEN:
  ✓ ambetter_bot.py runs with Playwright, both modes identical to Selenium version
  ✓ molina_downloader.py runs with Playwright, both modes identical to Selenium version
  ✓ Selenium + webdriver-manager removed from requirements.txt
  ✓ Committed: "phase-9: full migration to playwright complete"
```

---

## ── DEBUGGING PROMPTS ───────────────────────────────────────────────

### DEBUG-1 — Element Not Found

```
DEBUG — Element Not Found
Read CLAUDE.md first.

Phase: [N] | Script: [filename.py] | Carrier: [name] | Mode: [regular/roster] | Library: [Selenium/Playwright]

Error: [paste full traceback]
Failing code (10–15 lines): [paste]
Already tried: [describe]

1. Diagnose root cause — not just "element not found"
2. Give alternative selector strategy
3. Show how to print current DOM at failure point
4. If Playwright: confirm auto-wait, not wait_for_timeout
5. If selector fix: update config/config.yaml, not the Python file
```

---

### DEBUG-2 — Stuck After 3 Attempts (No-Spiral Rule)

```
DEBUG — No-Spiral Escalation
Read CLAUDE.md first.

Goal: [one sentence]
Attempt 1: [description + exact error]
Attempt 2: [description + exact error]
Attempt 3: [description + exact error]

Do NOT suggest attempt 4.
1. Diagnose ROOT CAUSE — why are all three failing?
2. Give 2 completely different approaches with pros, cons, build time, risk
3. Recommend one and explain why
```

---

### DEBUG-3 — R2 Wrong Member Count

```
DEBUG — R2 Count Discrepancy
Read CLAUDE.md §6 and §8 first.

Carrier: [name] | Expected: ~[N] | Script produced: [N]
Today: [date] | get_r2_start_date() returned: [date]
Sample R2 records (first 5): [paste]
Log lines showing row counts before/after date filter: [paste]

Investigate in order:
1. Is get_r2_start_date() imported from utils.py or defined locally?
2. Is the date filter using the correct column name from CLAUDE.md §9?
3. Molina/Oscar? End-of-month stamping — confirm filter date is correct.
4. Ambetter/Cigna/United? Real dates — check column values vs filter.
5. Dedup dropping expected records? Check existing deactivated_members.xlsx.
6. Ambetter: pagination issue? (§8.16) Log page sizes each iteration.
```

---

### DEBUG-4 — Playwright Async Issue

```
DEBUG — Playwright Async/Sync Boundary Error
Read CLAUDE.md §11 first.

Error: [paste traceback]

Check:
1. Does bot expose sync wrapper: def run_X(dry_run, agent_filter, mode): return asyncio.run(...)?
2. Is async_playwright used as: async with async_playwright() as p?
3. Is asyncio.run() called inside an already-running event loop?
4. Is downloads_path passed to browser.new_context()? Remove it.
```

---

### DEBUG-5 — XLSX Write Failure

```
DEBUG — XLSX Output File Error
Read CLAUDE.md §8.22 and §15 first.

Script: [filename.py] | Mode: [regular/roster]
File: [{carrier}_all_agents / deactivated_members / active_members_all]
Error: [paste traceback]

Rules:
- XLSX write failure is never fatal — log as WARNING and continue
- write_r1_xlsx() and write_active_members_xlsx() must come from scripts.utils
- write_active_members_xlsx() only called in mode='roster'
- deactivated_members.xlsx and active_members_all.xlsx are append-only
- If file is open in Excel: close it and re-run
```

---

### DEBUG-6 — ImportError After Refactor

```
DEBUG — ImportError or NameError
Read CLAUDE.md §15 first.

Error: [paste full error]
Script: [filename.py]

Check in order:
1. Does scripts/__init__.py exist? (empty file — required)
2. Import correct?
   from scripts.utils import get_r2_start_date, append_deactivated_xlsx,
       write_r1_xlsx, write_active_members_xlsx, run_type, setup_logging, with_retry
3. Local definition shadowing the import?
   grep "def get_r2_start_date\|def append_deactivated\|def write_r1\|def write_active" in bot file
4. Run from project root?
```

---

### DEBUG-7 — R2 Duplicates Across Runs

```
DEBUG — Duplicate Deactivated Members
Read CLAUDE.md §6 first.

Symptom: same member appears twice in deactivated_members.xlsx.

Confirm append_deactivated_xlsx() is from scripts.utils — not a local copy.
Must include:
  combined = combined.drop_duplicates(
      subset=["carrier", "policy_number", "coverage_end_date"],
      keep="first"
  )

Apply same dedup fix in sheets_writer.py Deactivated tab append.
```

---

### DEBUG-8 — Single-Agent Rerun Resets Carrier XLSX

```
DEBUG — Single-agent rerun overwrites other agents' data
Read CLAUDE.md §8.22 and §15 first.

Symptom: running --agent N causes other agents to disappear.

Fix:
1. Confirm write_r1_xlsx() imported from scripts.utils in this bot.
2. Confirm every R1 write calls write_r1_xlsx(r1_records, carrier_name).
3. Confirm no other pd.ExcelWriter in the bot writes carrier XLSX directly.

write_r1_xlsx() pattern: load existing → remove current agents → append → save.
```

---

### DEBUG-9 — R3 Wrong Active Count or Missing Records

```
DEBUG — R3 Active Roster Issue
Read CLAUDE.md §5, §6 (R3 section), §9 first.

Carrier: [name] | Mode: roster | Expected: ~[N] active | Script produced: [N]
Sample R3 records (first 5): [paste]

Investigate in order:
1. Is mode='roster' being passed correctly? Log mode value at bot entry.
2. Is write_active_members_xlsx() being called? Check log output.
3. Is write_active_members_xlsx() imported from scripts.utils — not redefined?
4. Is the active status filter correct for this carrier? (See CLAUDE.md §6 R3 table)
5. Ambetter only: was filter=all used (not filter=cancelled)? Log the URL.
6. United only: was planStatus filter removed? Check BOB download size vs regular run.
7. Cigna only: did operator select "All Policies" (not Terminated) at pause?
8. Dedup dropping expected records? Check existing active_members_all.xlsx.
```

---

## ── MAINTENANCE PROMPTS ──────────────────────────────────────────────

### MAINT-1 — Add New Agent

```
Read CLAUDE.md first.
Carrier: [name] | New agent: name=[name], user=[email], pass=[password]

1. Add entry to config/agents.yaml under [carrier]: section
2. python scripts/[carrier]_bot.py --agent [new_index] --dry-run --mode regular
3. Confirm R1 record shows correct agent_name from agents.yaml
4. If carrier supports R3: python scripts/[carrier]_bot.py --agent [new_index] --dry-run --mode roster
```

---

### MAINT-2 — Broken Selector After Portal Update

```
Read CLAUDE.md first.
Carrier: [name] | Mode: [regular/roster] | Broken selector: [value] | Error: [paste]

1. Open portal manually in Chrome → DevTools → find stable selector
2. Update config/config.yaml — NOT the Python file
3. Run --agent 0 --dry-run to confirm
4. Document in CLAUDE.md §8

Oscar: button classes are hashed — use probe loop, never hardcode. See §8.10.
```

---

### MAINT-3 — Monthly Sheet Setup

```
Read CLAUDE.md §13 first.
1. Drive folder → New → Google Sheets → rename "[Month] [Year]"
2. Rename default tab to "Summary"
3. Add tabs: "Deactivated This Period", "Active Members"
   (Active Roster tab is created automatically by sheets_writer --mode roster)
4. Share with service account (Editor)
5. Copy sheet ID → add to .env as SHEET_ID_[MONTH]_[YEAR]=<id>
6. python scripts/sheets_writer.py --dry-run --mode regular
7. python scripts/sheets_writer.py --dry-run --mode roster
```

---

### MAINT-4 — Roll Forward R2 Start Date

```
Read CLAUDE.md §6 first.

When to roll forward: confident all carriers have published cancellations
for the period being retired (~4–6 weeks of lag window).

Steps:
1. Edit config/config.yaml → r2.start_date: "YYYY-MM-DD"
2. No code changes required.
3. python scripts/sheets_writer.py --dry-run --mode regular
   Confirm R2 row count looks reasonable (not a huge drop or flood).
4. Commit: "config: roll r2.start_date forward to YYYY-MM-DD"
5. Document in CLAUDE.md §6 as current value.
```

---

*Last updated: April 27, 2026*
*Master context updated to 13 questions — adds §8.26 null carrier value handling (deactivated
account vs data gap). Architecture rules updated with null handling guideline.
Phase GUI+R3 marked DONE. Phase 7 (Looker Studio) is now NEXT.
Phase order: R ✅ → GUI+R3 ✅ → 7 ⏳ → 9 🔮*
