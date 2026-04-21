# PRE_PHASE_CHECKLIST.md
## Questions to Answer Before Starting Each Phase

> Answer every question in a phase's section before opening Claude Code.
> If you cannot answer a question, resolve it first.
>
> **Current phase order:** R (utils.py) → GUI (launcher) → 7 (Looker Studio) → 9 (Playwright migration)
> **Phase 8 (Task Scheduler) is retired.** The GUI launcher is the run center.

---

## PHASE R — utils.py Refactor + Security Fixes
*Do this before any new development. No new features. Pure cleanup.*

### Code Inventory (run these commands before the session)
- [ ] How many files define `calculate_period_start()`?
  `grep -r "def calculate_period_start" scripts/`
  **Expected: 6. Answer:** _______________

- [ ] How many files define `_append_deactivated_xlsx()`?
  `grep -r "def _append_deactivated_xlsx" scripts/`
  **Expected: 5. Answer:** _______________

- [ ] Does any bot define its own XLSX write logic instead of a shared function?
  `grep -rn "pd.ExcelWriter\|to_excel" scripts/` — list which files
  **Answer:** _______________

- [ ] Does `scripts/__init__.py` exist?
  **Answer (yes / no):** _______________

- [ ] Does `scripts/utils.py` exist?
  **Answer (yes / no):** _______________

### Security Checks
- [ ] Does `united_bot.py` print the password?
  `grep -n "Password\|agent\['pass'\]" scripts/united_bot.py`
  **If yes: this line must be removed during the session.**

- [ ] Open `.gitignore`. Confirm these entries exist:
  - [ ] `config/agents.yaml`
  - [ ] `credentials/`
  - [ ] `data/raw/`
  - [ ] `data/output/`
  - [ ] `data/state/`
  - [ ] `data/chrome_profiles/`
  - [ ] `logs/`
  - [ ] `.env`

### Readiness
- [ ] All 5 carrier bots currently in a working state on main branch?
  (Never refactor broken code. Fix first, then refactor.)
- [ ] Do you understand the merge-on-write rule for write_r1_xlsx()? (§8.22 in CLAUDE.md)
  If not, re-read §8.22 and §15 before opening Claude Code.
- [ ] Do you understand that sheets_writer.py will be updated to read from XLSX files,
  not receive in-memory records? (§7 architecture standard in CLAUDE.md)

### New Function Confirmation
- [ ] `write_r1_xlsx()` does not exist yet — it will be created in utils.py during Phase R.
  Its behavior: load existing XLSX → remove rows for agents in current run → append → save.
  Confirm you understand why this is different from the current ad-hoc write logic.
  **Answer (understood / need clarification):** _______________

### Verification Plan
After the session, run each command and confirm it passes before committing:
- [ ] `python scripts/molina_downloader.py --agent 0 --dry-run`
- [ ] `python scripts/ambetter_bot.py --agent 0 --dry-run`
- [ ] `python scripts/oscar_bot.py --agent 0 --dry-run`
- [ ] `python scripts/cigna_bot.py --agent 0 --dry-run`
- [ ] `python scripts/united_bot.py --agent 0 --dry-run`
- [ ] `python scripts/sheets_writer.py --dry-run`

---

## PHASE GUI — tkinter Launcher
*Start only after Phase R is committed and all 6 verification commands pass.*

### Prerequisites
- [ ] Phase R complete — `scripts/utils.py` exists with all 6 functions.
- [ ] All 5 bots pass `--agent 0 --dry-run` without errors.
- [ ] `sheets_writer.py` has `push_to_sheets(dry_run=False)` as its public API.
- [ ] Password print removed from `united_bot.py`.
- [ ] `.gitignore` has all required entries.

### Understanding the Two-Step Flow
- [ ] The GUI implements two separate steps — confirm you understand both:
  - **Step 1:** [Run] buttons → bots write to XLSX files on disk
  - **Step 2:** [Push to Google Sheets] → sheets_writer reads XLSX → pushes to Sheets
  - These steps are NEVER automatically chained. The operator chooses when to push.
  **Answer (understood / questions):** _______________

- [ ] What is the [↩] single-agent rerun button supposed to do?
  **Answer:** _______________
  *Expected: prompts for agent index, runs `--agent N`, XLSX merges correctly*

### Status Badge Logic
- [ ] Do you know the expected agent count per carrier?
  (Used to determine ✅ vs ⚠ status badges)
  - Molina: ___ agents
  - Ambetter: ___ agents
  - Oscar: ___ agents
  - Cigna: ___ agents
  - United: ___ agents (3 are always manual — Mike, Tony, Yusbel)

### Technical Readiness
- [ ] Does `status/last_run.json` exist? (If not, launcher must handle missing file gracefully.)
  **Answer:** _______________

- [ ] What is the correct `.env` key for the current month's sheet ID?
  **Answer (e.g., SHEET_ID_APRIL_2026):** _______________

- [ ] Do you want a confirmation dialog before [Push to Google Sheets] runs?
  **Answer (yes / no):** _______________

---

## PHASE 7 — Looker Studio Dashboard
*Start only after GUI is committed and the pipeline has pushed to Sheets at least twice.*

### Data Readiness (open the Google Sheet and verify before the session)
- [ ] **Active Members tab** — does it have data from at least **2 different run_date values**?
  (Trend page needs at least 2 points. A single date is just a dot, not a line.)
  **Answer:** _______________

- [ ] **Active Members tab** — do all 5 carriers appear?
  Check: `=COUNTIF(C:C,"Molina")` etc. for each carrier name.
  List any missing carriers: _______________

- [ ] **Active Members tab** — spot-check 3 rows:
  - Is `run_date` in YYYY-MM-DD format (not MM/DD/YYYY)?
  - Is `active_members` a number (not text)?
  - Is `carrier` spelled exactly: Ambetter, Cigna, Molina, Oscar, United?
  **Answer:** _______________

- [ ] **Deactivated This Period tab** — at least 5 rows present?
  **Answer:** _______________

- [ ] **Deactivated This Period tab** — are any Cigna rows present?
  If yes, confirm `member_dob` column is blank (not "null" text) for those rows.
  **Answer:** _______________

### Google Account Alignment
- [ ] Can you access `lookerstudio.google.com` with your Google account?

- [ ] Is this the same Google account that owns the Drive folder?
  If different: the Sheet must be explicitly shared with this account — not just
  with the service account. Verify this before the session.
  **Answer (same account / different account — sharing confirmed):** _______________

- [ ] Can you open the current month's Sheet in this account right now?
  **Answer:** _______________

### Design Decisions
- [ ] Dashboard access for manager: view-only or can they apply filters?
  **Answer:** _______________

- [ ] Sharing method: public link or restricted to specific email addresses?
  **Answer:** _______________

- [ ] Company color scheme or branding to follow?
  **Answer:** _______________

### Data Gaps to Understand Before Presenting to Manager
These are known and documented. Make sure you can explain them:
- [ ] `member_dob` is null for all Cigna records — Page 6 will show blank in that column for Cigna.
- [ ] United `policy_number` is currently a name composite (e.g., "Emily_Rink") — not a real ID.
  It will display in Page 6 but cannot be used to cross-reference other systems.
- [ ] Ambetter has a data lag — cancelled members appear in R2 days to weeks after dropping from R1.
  This is not a bug — it is how Ambetter's system processes cancellations.
- [ ] United has 3 agents (Mike, Tony, Yusbel) with manual data entry. Their R1 counts may be
  absent or entered manually on days when the bot cannot retrieve their data.

---

## PHASE 9 — Playwright Migration (Molina + Ambetter)
*Start only after the pipeline has run successfully at least 4 consecutive times.*

### Readiness
- [ ] GUI complete and the pipeline has run at least 4 consecutive times without errors?
- [ ] Are you working on a feature branch (not main)?
  **Branch name:** _______________

### Ambetter SPA Test (do this manually before the session)
- [ ] Run: `playwright codegen broker.ambetterhealth.com`
- [ ] Log in, then try navigating to the Policies section.
- [ ] Does it load without killing the browser session?
  **Answer (yes it works / no it crashes):** _______________
- [ ] If yes: requests bypass can be replaced with direct Playwright navigation.
- [ ] If no: keep requests bypass, replace Selenium browser layer only.

### Parallel Execution (Ambetter only)
- [ ] Run all 16 Ambetter agents in parallel, or keep sequential?
  Parallel is faster but uses more memory. If one agent crashes, it may affect others.
  **Answer (parallel / sequential):** _______________

---

*Last updated: April 20, 2026*
*Key changes: Phase R checklist updated — added write_r1_xlsx new function confirmation,
added sheets_writer refactor understanding check, added verification for all 6 commands
(including sheets_writer). Phase GUI checklist updated — added two-step flow understanding
check, [↩] single-agent rerun confirmation, expected agent counts per carrier.
Phase 7 checklist expanded — added spot-check for date format, carrier name spelling,
Cigna null confirmation, Google account alignment check, data gap awareness section.
Phase 8 retired — removed from checklist. Phase order updated throughout.*
