# PROMPTS.md — Claude Code Ready-to-Paste Prompts
## Insurance Analytics Automation — Phase-by-Phase

> **How to use:**
> 1. Place `CLAUDE.md` in your project root — Claude Code reads it automatically
> 2. Start every session with the **MASTER CONTEXT PROMPT** below
> 3. Then paste the prompt for the phase or debug scenario you need
> 4. After each session update `## 12. PHASES STATUS` in `CLAUDE.md`

---

## ── MASTER CONTEXT PROMPT ──────────────────────────────────────────

*Paste this as your FIRST message in every Claude Code session.*

```
Before writing any code, read CLAUDE.md completely. Do not skip any section.
Then confirm you have read it by answering ALL of the following:

1. What are the two reports this system produces and where does each one go?

2. Which carriers use Playwright and which use Selenium? Why does that split exist
   and when does it change?

3. What does calculate_period_start() return, and why was it changed from the original
   Friday/Monday logic? Which carriers drove that decision and why?

4. Where does agent_name come from — the CSV or agents.yaml? Why does this matter
   for Ambetter specifically?

5. What is the R2 dedup key and why does it include coverage_end_date?

6. What is scripts/utils.py? List every function that must live there. What is the
   rule about shared functions vs carrier-specific logic?

7. Describe the two-step pipeline flow (Step 1: bots, Step 2: sheets_writer).
   What does sheets_writer read from? Does it receive in-memory records from bots?

8. What is write_r1_xlsx() and why does it merge instead of overwrite?
   What breaks if a bot resets the entire carrier XLSX during a single-agent rerun?

9. What is the current phase status — what is done, what is next, in what order?
   Why is Phase 8 (Task Scheduler) retired?

10. Name the two CRITICAL security issues from §8.20 and the fix for each.

Do not write any code until you have answered all 10 questions correctly.
If CLAUDE.md does not exist, stop and tell me — do not proceed.

Architecture rules that apply to everything you build:
- Shared functions (calculate_period_start, append_deactivated_xlsx, write_r1_xlsx,
  run_type, setup_logging, with_retry) live in scripts/utils.py. Never copy them.
- Column names belong in config/config.yaml, not as Python constants in bot files.
- Never open agents.yaml or config.yaml at module level — only inside functions.
- Never print passwords. Log usernames only.
- Log filenames include seconds: run_YYYYMMDD_HHMMSS_{carrier}.log
- Each bot's public API: run_{carrier}(dry_run, agent_filter). Everything else is private.
- sheets_writer.py reads from XLSX files on disk. It is never called by bots.
- write_r1_xlsx() always merges. Never resets. Safe for 1-agent and full runs.
```

---

## ── PHASE R — utils.py Refactor ────────────────────────────────────

*Run before any other new development.*

```
Read CLAUDE.md completely and answer the master context questions before writing code.
PHASE R — utils.py refactor. No new features. Move existing functions to a shared module.
Also apply the security fixes from §8.20 during this same session.

WHAT TO CREATE:
  scripts/__init__.py   ← empty file, makes scripts/ a Python package
  scripts/utils.py      ← all shared infrastructure (see below)

FUNCTIONS THAT MOVE TO utils.py:

  1. calculate_period_start(today=None) -> date
     Currently in: molina_report.py, molina_downloader.py, ambetter_bot.py,
                   oscar_bot.py, cigna_bot.py, united_bot.py (6 copies)

  2. run_type() -> str
     Currently in: oscar_bot.py (as _run_type), cigna_bot.py, united_bot.py

  3. setup_logging(carrier: str) -> logging.Logger
     Currently in: molina_downloader.py, oscar_bot.py, cigna_bot.py, united_bot.py
     New filename: run_YYYYMMDD_HHMMSS_{carrier}.log (add seconds to prevent collision)

  4. with_retry(func, operation_name, max_attempts=3, log=None)
     Currently in: molina_downloader.py only

  5. append_deactivated_xlsx(r2_records, carrier, log=None) -> None
     Currently in: molina_downloader.py, ambetter_bot.py, oscar_bot.py,
                   cigna_bot.py, united_bot.py (5 copies — already diverged)
     Use the most complete version (null policy_number guard from Ambetter/Oscar).

  6. write_r1_xlsx(r1_records, carrier, log=None) -> None  ← NEW FUNCTION
     Does not exist yet in any bot — currently each bot has its own ad-hoc write logic.
     Authoritative merge-on-write implementation (see §8.22 in CLAUDE.md for full code).
     Load existing → remove rows for agents in current run → append → save.
     Never overwrites entire file. Safe for --agent N reruns.

SECURITY FIXES TO APPLY IN THE SAME SESSION:
  1. united_bot.py: remove print(f"     Password: {agent['pass']}")
     Replace with: print(f"     Account: {agent['user']}")
                   print(f"     (Use your password manager for the password)")
  2. Verify .gitignore has: logs/, data/raw/, data/output/, data/state/

UPDATE EACH BOT FILE:
  - Delete all local definitions of the functions above
  - Add to top of each bot:
      from scripts.utils import (
          calculate_period_start,
          append_deactivated_xlsx,
          write_r1_xlsx,
          run_type,
          setup_logging,
          with_retry,
      )
  - Replace each bot's ad-hoc XLSX write logic with write_r1_xlsx(r1_records, carrier)
  - Update run_X() signature to: def run_X(dry_run=False, agent_filter=None)

UPDATE sheets_writer.py:
  - sheets_writer.py now reads from XLSX files on disk instead of receiving in-memory
    records. It must:
    1. Read each carrier XLSX from data/output/{carrier}_all_agents.xlsx
    2. Build R1 records from those files
    3. Read deactivated_members.xlsx for R2 records
    4. Push both to Google Sheets (same logic as before, different data source)
  - Public API becomes: push_to_sheets(dry_run=False) — no parameters for records.

RULES FOR THIS SESSION:
  - Move functions exactly as they exist. Do not change business logic at the same time.
  - If implementations differ across files, use the most complete version in utils.py.
  - Do not touch molina_report.py domain logic — only its calculate_period_start import.

VERIFICATION (run each after the session — all must pass with no errors):
  python scripts/molina_downloader.py --agent 0 --dry-run
  python scripts/ambetter_bot.py --agent 0 --dry-run
  python scripts/oscar_bot.py --agent 0 --dry-run
  python scripts/cigna_bot.py --agent 0 --dry-run
  python scripts/united_bot.py --agent 0 --dry-run
  python scripts/sheets_writer.py --dry-run

DONE WHEN:
  ✓ scripts/__init__.py exists (empty)
  ✓ scripts/utils.py contains all 6 functions with correct implementations
  ✓ All 5 bot files import from utils.py — no local copies remain
  ✓ write_r1_xlsx() used in every bot for R1 XLSX output
  ✓ sheets_writer.py reads from XLSX files — not from in-memory records
  ✓ sheets_writer.py has push_to_sheets(dry_run=False) as its public API
  ✓ All 6 verification commands pass without ImportError or NameError
  ✓ Password print removed from united_bot.py
  ✓ .gitignore verified
  ✓ Committed: "refactor: phase R — utils.py + security fixes"
```

---

## ── PHASE GUI — tkinter Launcher ────────────────────────────────────

*Start only after Phase R is committed and all 6 verification commands pass.*

```
Read CLAUDE.md completely and answer the master context questions before writing code.
Phase R complete. Building the tkinter GUI launcher. This replaces Phase 8 entirely.
The GUI is the run center — no Task Scheduler, no main.py orchestrator.

Build: scripts/launcher.py
Library: tkinter (built into Python — no install needed)

PIPELINE FLOW THE GUI IMPLEMENTS:
  Step 1: Run bots individually → each writes to its carrier XLSX file
  Step 2: Operator clicks [Push to Google Sheets] when ready → sheets_writer runs
  These are always separate steps. Never auto-trigger the push after a bot run.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
UI LAYOUT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

┌─────────────────────────────────────────────────────────────┐
│           Insurance Analytics — Run Center                  │
│                                                             │
│  [▶ Run All Carriers]        [☁ Push to Google Sheets]      │
│                                                             │
│  Carrier    Status        Last Run      Agents   Action     │
│  ──────────────────────────────────────────────────────     │
│  Molina     ✅ Today      Apr 21 09:04   15/15   [Run] [↩]  │
│  Ambetter   ✅ Today      Apr 21 09:18   16/16   [Run] [↩]  │
│  Oscar      ⚠ Yesterday  Apr 20 09:11   12/13   [Run] [↩]  │
│  Cigna      ✅ Today      Apr 21 09:35   12/12   [Run] [↩]  │
│  United     ⚠ 3 manual   Apr 21 09:52    9/12   [Run] [↩]  │
│                                                             │
│  ──────────────────────────────────────────────────────     │
│  [📂 Open Output Folder]  [📊 Open Google Sheet]            │
│  [📋 View Last Log]                                         │
│                                                             │
│  ┌─ Log Output ────────────────────────────────────────┐    │
│  │ 09:04:12 | MOLINA | INFO | agent 1/15 Brandon K OK  │    │
│  │ 09:04:55 | MOLINA | INFO | agent 2/15 Felipe V OK   │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘

[↩] = single-agent rerun button. Clicking it prompts: "Agent index (0-based):"
     then runs: python scripts/{carrier}_bot.py --agent N

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BUTTON BEHAVIORS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

[Run] per carrier:
  - subprocess.Popen: python scripts/{carrier}_bot.py
  - Streams stdout to log panel in real time (threading)
  - Button disables while running, re-enables on completion
  - Status badge and last-run timestamp update after completion

[↩] (single-agent rerun) per carrier:
  - Prompts for agent index via a small dialog
  - subprocess.Popen: python scripts/{carrier}_bot.py --agent N
  - Same streaming output as [Run]
  - Safe because write_r1_xlsx() merges — other agents unaffected

[Run All Carriers]:
  - Runs all 5 bots sequentially: Molina → Ambetter → Cigna → United → Oscar
  - Does NOT auto-push to Sheets after completion
  - All buttons disable during run

[☁ Push to Google Sheets]:
  - subprocess.Popen: python scripts/sheets_writer.py
  - Streams output to log panel
  - Separate, intentional action — never triggered automatically
  - Shows confirmation dialog before running: "Push all carrier data to Sheets?"

[📂 Open Output Folder]: os.startfile(str(ROOT / "data" / "output"))
[📊 Open Google Sheet]: open current month sheet URL in browser (from .env)
[📋 View Last Log]: open most recent file in logs/ in default text editor

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STATUS BADGE LOGIC
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Read from carrier XLSX file in data/output/:
  ✅  = XLSX modified today AND row count >= expected agent count
  ⚠   = XLSX modified before today, OR agent count below expected,
        OR carrier is United (always show "3 manual agents" note)
  ❌  = XLSX exists but last run logged failures (check status/last_run.json)
  —   = XLSX does not exist (never run)

Show: last modified timestamp, actual agent row count in file.
United: always append "(Mike, Tony, Yusbel require manual entry)"

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
TECHNICAL REQUIREMENTS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

- tkinter only — no external GUI libraries
- subprocess.Popen for all scripts — non-blocking, real-time output
- Threading for stdout capture — never block the UI thread
- Read .env with python-dotenv for sheet ID
- All paths relative to project root (resolve from launcher.py location)
- Use sys.executable to ensure the venv Python is used, not system Python

Script commands:
  Molina:         python scripts/molina_downloader.py
  Ambetter:       python scripts/ambetter_bot.py
  Oscar:          python scripts/oscar_bot.py
  Cigna:          python scripts/cigna_bot.py
  United:         python scripts/united_bot.py
  Push to Sheets: python scripts/sheets_writer.py
  Single-agent:   python scripts/{carrier}_bot.py --agent N

DONE WHEN:
  ✓ Window opens, all 5 carriers show correct status badges and agent counts
  ✓ [Run] launches bot, streams output, updates status on completion
  ✓ [↩] prompts for agent index, runs single-agent rerun, merges XLSX correctly
  ✓ [Run All] runs all 5 in sequence, does NOT auto-push
  ✓ [Push to Google Sheets] shows confirmation dialog, runs sheets_writer, streams output
  ✓ [Open Output Folder], [Open Google Sheet], [View Last Log] all work
  ✓ All buttons disable while any subprocess is running
  ✓ United row shows manual agent note
  ✓ Committed: "feat: launcher — GUI run center (replaces Phase 8)"
```

---

## ── PHASE 7 — Looker Studio Dashboard ────────────────────────────────

```
Read CLAUDE.md completely and answer the master context questions before writing code.
GUI complete. Pipeline has been pushed to Google Sheets at least twice.

PREREQUISITES (verify before starting this session):
  - All 5 carriers have at least one successful Sheets push in the current month
  - "Active Members" tab has data from at least 2 different run_date values
  - "Deactivated This Period" tab has at least a few R2 rows
  - Spot-check: run_date is YYYY-MM-DD, active_members is a number, carrier
    spelling is exactly: Ambetter, Cigna, Molina, Oscar, United

KNOWN NULL FIELDS (Looker Studio must handle gracefully — no error on null):
  - member_dob: null for all Cigna records (permanent — see §8.12)
  - policy_number: United uses name composite "First_Last" until real ID confirmed

DATA SOURCES:
  Monthly Sheets: connect both current month and previous month for MoM comparison
  Tab "Active Members":  run_date | run_type | carrier | agent_name | active_members | status
  Tab "Deactivated This Period":
                         run_date | carrier | agent_name | member_name |
                         member_dob | state | coverage_end_date | policy_number

6 PAGES:

Page 1 — Current Snapshot
  Contingency table: agents × carriers, most recent run_date only
  Grand total row + column. Missing carrier = blank, not zero.
  Filter: run_date defaulting to max.

Page 2 — Trend Over Time
  Line chart: x=run_date, y=SUM(active_members), one line per carrier, trailing 90 days.

Page 3 — Carrier Summary
  Bar chart: total active per carrier, current month.
  Scorecard: total active across all carriers.

Page 4 — Agent Rankings
  Table: agents sorted descending by total members across all carriers.
  Carrier filter control.

Page 5 — Month-over-Month
  Table: agent | this month | last month | delta | % change
  Calculated fields for delta and %. Red/green conditional formatting.
  Requires data blending across monthly sheets.

Page 6 — Churn Analysis
  Table: member_name | agent | carrier | coverage_end_date | state | member_dob | policy_number
  Date range filter on coverage_end_date.
  Scorecard: total deactivated in selected period.
  Nulls display as blank (not "null" text).

FORMAT: Step-by-step UI walkthrough — not code.

Include:
  - How to connect Google Sheets as a data source
  - Data blending setup for Page 5 (cross-sheet MoM)
  - Calculated field formulas for delta and %
  - Default date filter to current month
  - Sharing as view-only for manager
  - Known Looker Studio limitations for this use case

DONE WHEN:
  ✓ All 6 pages display with real data
  ✓ Page 1 pivot matches Google Sheets Summary tab
  ✓ Page 6 shows null member_dob as blank for Cigna rows, not as error
  ✓ Date filters work on all pages
  ✓ Shared view-only link confirmed working for manager
```

---

## ── PHASE 9 — Playwright Migration ─────────────────────────────────

```
Read CLAUDE.md completely. Phase 9. Pipeline in production with 4+ stable runs.

Migrate Molina and Ambetter from Selenium to Playwright.
Test Ambetter first — if Playwright handles the SPA natively, the requests bypass
can be removed. If it crashes, keep the bypass, replace only the Selenium layer.

Rules:
  - Replace all WebDriverWait / _clickable() / _wait() with auto-wait
  - Never use page.wait_for_timeout()
  - Keep all business logic identical
  - Import from scripts.utils — do not redefine locally
  - Run --agent 0 --dry-run and confirm output identical to Selenium version
  - Do NOT pass downloads_path to browser.new_context()

DONE WHEN:
  ✓ ambetter_bot.py runs with Playwright, identical output to Selenium version
  ✓ molina_downloader.py runs with Playwright, identical output to Selenium version
  ✓ Selenium + webdriver-manager removed from requirements.txt
  ✓ Committed: "phase-9: full migration to playwright complete"
```

---

## ── DEBUGGING PROMPTS ───────────────────────────────────────────────

### DEBUG-1 — Element Not Found

```
DEBUG — Element Not Found
Read CLAUDE.md first.

Phase: [N] | Script: [filename.py] | Carrier: [name] | Library: [Selenium / Playwright]

Error: [paste full traceback]
Failing code (10–15 lines): [paste]
Already tried: [describe]

1. Diagnose root cause — not just "element not found"
2. Give alternative selector strategy
3. Show how to print current DOM at the failure point
4. If Playwright: confirm auto-wait is used, not wait_for_timeout
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
Read CLAUDE.md §6 and §8.4 first.

Carrier: [name] | Expected: ~[N] | Script produced: [N]
Today: [weekday, date] | calculate_period_start() returned: [date]
Sample R2 records (first 5): [paste]
Log lines showing row counts before/after date filter: [paste]

Investigate in order:
1. Is calculate_period_start() imported from utils.py or defined locally?
   A local override would be a bug — there should be exactly one definition.
2. Is the date filter using the correct column name from CLAUDE.md §9?
3. Molina/Oscar? Terminations stamped at month-end — confirm period_start = last day.
4. Ambetter/Cigna? Real dates — check actual column values vs filter date.
5. Any records filtered by dedup before expected? Check existing deactivated_members.xlsx.
6. Pagination issue? (Ambetter only — §8.16)
```

---

### DEBUG-4 — Playwright Async Issue

```
DEBUG — Playwright Async/Sync Boundary Error
Read CLAUDE.md §11 first.

Error: [paste traceback]

Check:
1. Does bot expose sync wrapper: def run_X(dry_run, agent_filter): return asyncio.run(...)?
2. Is async_playwright used as: async with async_playwright() as p?
3. Is asyncio.run() called inside an already-running event loop?
4. Is downloads_path passed to browser.new_context()? Remove it — not valid.
```

---

### DEBUG-5 — XLSX Write Failure

```
DEBUG — XLSX Output File Error
Read CLAUDE.md §8.22 first.

Script: [filename.py]
File: [{carrier}_all_agents / deactivated_members]
Error: [paste traceback]

Rules to enforce:
- XLSX write failure is never fatal — log as WARNING and continue
- write_r1_xlsx() must be called from scripts.utils, not defined locally
- It merges — never resets — load → remove current agents → append → save
- deactivated_members.xlsx is append-only — existing_df loaded before concat
- If file is open in Excel: close it and re-run
```

---

### DEBUG-6 — ImportError After Refactor

```
DEBUG — ImportError or NameError after utils.py refactor
Read CLAUDE.md §15 first.

Error: [paste full ImportError or NameError]
Script: [filename.py]

Check in order:
1. Does scripts/__init__.py exist? (empty file — required)
2. Is import correct at top of bot file?
   from scripts.utils import calculate_period_start, append_deactivated_xlsx,
       write_r1_xlsx, run_type, setup_logging, with_retry
3. Is there still a local definition in the bot file that shadows the import?
   grep "def calculate_period_start\|def _append_deactivated\|def write_r1" in bot file
4. Was script run from project root? (cd to project root before python scripts/...)
```

---

### DEBUG-7 — R2 Duplicates Across Runs

```
DEBUG — Duplicate Deactivated Members
Read CLAUDE.md §6 first.

Symptom: same member appears twice in deactivated_members.xlsx.

Root cause: period_start reaches back to last day of previous month.
Without dedup, consecutive runs re-capture the same terminated members.

Confirm append_deactivated_xlsx() is called from scripts.utils — not a local copy.
The authoritative implementation must include:
  combined = combined.drop_duplicates(
      subset=["carrier", "policy_number", "coverage_end_date"],
      keep="first"  # existing rows win
  )

Apply same fix to Google Sheets append in sheets_writer.py.
```

---

### DEBUG-8 — Single-Agent Rerun Resets Carrier XLSX

```
DEBUG — Single-agent rerun overwrites other agents' data
Read CLAUDE.md §8.22 and §15 first.

Symptom: running --agent N causes other agents to disappear from the carrier XLSX.

Root cause: bot is not using write_r1_xlsx() from utils.py, or the local write
logic is still overwriting the entire file instead of merging.

Fix:
1. Confirm write_r1_xlsx() is imported from scripts.utils in this bot file.
2. Confirm every R1 write in the bot calls write_r1_xlsx(r1_records, carrier_name).
3. Confirm there is no other pd.ExcelWriter call in the bot that writes to the
   carrier XLSX file without going through write_r1_xlsx().

write_r1_xlsx() must follow this pattern exactly:
  load existing → remove rows for agents in current run → append new → save
  Never pd.ExcelWriter without first loading and merging.
```

---

## ── MAINTENANCE PROMPTS ──────────────────────────────────────────────

### MAINT-1 — Add New Agent

```
Read CLAUDE.md first.
Carrier: [name] | New agent: name=[name], user=[email], pass=[password]

1. Add entry to config/agents.yaml under [carrier]: section
2. Run: python scripts/[carrier]_bot.py --agent [new_index] --dry-run
3. Confirm R1 record shows correct agent_name from agents.yaml
```

---

### MAINT-2 — Broken Selector After Portal Update

```
Read CLAUDE.md first.
Carrier: [name] | Broken selector: [value] | Error: [paste]

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
2. Rename default tab to "Summary", add "Deactivated This Period", "Active Members"
3. Share with service account (Editor)
4. Copy sheet ID → add to .env as SHEET_ID_[MONTH]_[YEAR]=<id>
5. Run: python scripts/sheets_writer.py --dry-run
   Confirm correct sheet ID resolves and write targets look right.
```

---

*Last updated: April 20, 2026*
*Key changes: Master context expanded to 10 questions — adds two-step flow, write_r1_xlsx,
sheets_writer reads from XLSX, Phase 8 retirement. Phase R prompt updated to include
write_r1_xlsx (new function) and sheets_writer refactor. Phase GUI prompt updated with
[Push to Google Sheets] button, [↩] single-agent rerun button, and two-step flow rules.
Phase 8 prompt retired. DEBUG-8 added for single-agent rerun resetting XLSX.
Phase order throughout: R → GUI → 7 → 9.*
