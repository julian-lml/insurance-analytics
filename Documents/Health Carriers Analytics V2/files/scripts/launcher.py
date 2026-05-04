"""
scripts/launcher.py — Phase GUI+R3
Limitless Insurance Group — Run Center

Two-section tkinter GUI:
  REGULAR RUN (R1 + R2) — runs frequently (twice weekly / daily)
  MONTHLY ROSTER (R3)   — runs once per month

All subprocess calls use sys.executable to ensure the venv Python is used.
stdout is streamed to the log panel via a background thread + queue.
All buttons disable while any subprocess is running.

See CLAUDE.md §16 for full specification.
"""

from __future__ import annotations

import json
import os
import queue
import subprocess
import sys
import threading
import webbrowser
from datetime import date, datetime
from pathlib import Path

import tkinter as tk
from tkinter import messagebox, simpledialog, ttk

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

# ─── Carrier definitions ──────────────────────────────────────────────────────

CARRIERS = [
    {"name": "Molina",   "script": "molina_downloader.py", "expected": 15},
    {"name": "Ambetter", "script": "ambetter_bot.py",       "expected": 16},
    {"name": "Oscar",    "script": "oscar_bot.py",          "expected": 13},
    {"name": "Cigna",    "script": "cigna_bot.py",          "expected": 12},
    {"name": "United",   "script": "united_bot.py",         "expected": 12},
]

RUN_ALL_ORDER = ["Molina", "Ambetter", "Cigna", "United", "Oscar"]

SCRIPTS_DIR      = ROOT / "scripts"
OUTPUT_DIR       = ROOT / "data" / "output"
STATUS_DIR       = ROOT / "status"
LOGS_DIR         = ROOT / "logs"
LAST_ROSTER_FILE = STATUS_DIR / "last_roster_run.txt"
LAST_RUN_JSON    = STATUS_DIR / "last_run.json"

UNITED_MANUAL_NOTE = "(Mike, Tony, Yusbel require manual entry)"


# ─── Status badge helpers ─────────────────────────────────────────────────────

def _read_last_run_json() -> dict:
    if LAST_RUN_JSON.exists():
        try:
            return json.loads(LAST_RUN_JSON.read_text())
        except Exception:
            pass
    return {}


def _write_last_run_json(carrier: str, status: str) -> None:
    STATUS_DIR.mkdir(parents=True, exist_ok=True)
    data = _read_last_run_json()
    data[carrier.lower()] = {
        "status":    status,
        "timestamp": datetime.now().isoformat(timespec="seconds"),
    }
    LAST_RUN_JSON.write_text(json.dumps(data, indent=2))


def _carrier_badge(carrier_name: str, expected: int) -> tuple[str, str, str, str]:
    """
    Returns (badge, status_text, last_run_str, agent_count_str).

    Badge rules (CLAUDE.md §16):
      ✅ = XLSX modified today AND agent_count >= expected
      ⚠  = XLSX modified before today OR agent_count < expected
      ❌ = last_run.json records a failure for this carrier
      —  = XLSX does not exist
    """
    xlsx = OUTPUT_DIR / f"{carrier_name.lower()}_all_agents.xlsx"

    if not xlsx.exists():
        return "—", "Never run", "—", "—"

    last_run_data = _read_last_run_json()
    carrier_run   = last_run_data.get(carrier_name.lower(), {})
    if carrier_run.get("status") == "failed":
        ts  = carrier_run.get("timestamp", "")[:16].replace("T", " ")
        return "❌", "Failed", ts, "—"

    mtime   = datetime.fromtimestamp(xlsx.stat().st_mtime)
    is_today = mtime.date() == date.today()
    last_run_str = mtime.strftime("%b %d %H:%M")

    agent_count = 0
    try:
        import pandas as pd
        df = pd.read_excel(xlsx, engine="openpyxl")
        if "agent_name" in df.columns:
            agent_count = int(df["agent_name"].nunique())
    except Exception:
        pass

    count_str = f"{agent_count}/{expected}"

    if is_today and agent_count >= expected:
        badge = "✅"
        status_text = "Today"
    else:
        badge = "⚠"
        status_text = "Today" if is_today else "Stale"

    return badge, status_text, last_run_str, count_str


def _read_last_roster_date() -> str:
    if LAST_ROSTER_FILE.exists():
        val = LAST_ROSTER_FILE.read_text(encoding="utf-8").strip()
        return val if val else "never"
    return "never"


def _write_last_roster_date() -> None:
    STATUS_DIR.mkdir(parents=True, exist_ok=True)
    LAST_ROSTER_FILE.write_text(date.today().isoformat(), encoding="utf-8")


# ─── Application ─────────────────────────────────────────────────────────────

class LauncherApp:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("Limitless Insurance Group — Run Center")
        self.root.resizable(True, True)

        self._log_queue: queue.Queue[str] = queue.Queue()
        self._running   = False            # True while any subprocess is active
        self._all_btns: list[tk.Widget] = []   # every button, for bulk disable/enable

        self._build_ui()
        self._refresh_badges()
        self._poll_log_queue()

    # ── UI construction ───────────────────────────────────────────────────────

    def _build_ui(self) -> None:
        pad = {"padx": 8, "pady": 4}

        # ── REGULAR RUN section ───────────────────────────────────────────────
        reg_frame = ttk.LabelFrame(
            self.root,
            text=" REGULAR RUN (R1 + R2) — Twice weekly / daily ",
        )
        reg_frame.pack(fill="x", padx=10, pady=(10, 4))

        top_row = ttk.Frame(reg_frame)
        top_row.pack(fill="x", **pad)

        self._btn_run_all = ttk.Button(
            top_row, text="▶  Run All Carriers",
            command=self._on_run_all,
        )
        self._btn_run_all.pack(side="left", padx=(0, 8))

        self._btn_push = ttk.Button(
            top_row, text="☁  Push to Google Sheets",
            command=self._on_push_sheets,
        )
        self._btn_push.pack(side="left")

        self._all_btns += [self._btn_run_all, self._btn_push]

        # Carrier table headers
        hdr = ttk.Frame(reg_frame)
        hdr.pack(fill="x", padx=8, pady=(4, 0))
        for col, width in [("Carrier", 10), ("Status", 12), ("Last Run", 14), ("Agents", 7), ("", 16)]:
            ttk.Label(hdr, text=col, font=("TkDefaultFont", 9, "bold"), width=width, anchor="w").pack(side="left")
        ttk.Separator(reg_frame, orient="horizontal").pack(fill="x", padx=8)

        # Per-carrier rows
        self._carrier_widgets: dict[str, dict] = {}
        for c in CARRIERS:
            self._build_carrier_row(reg_frame, c)

        # ── MONTHLY ROSTER section ────────────────────────────────────────────
        roster_frame = ttk.LabelFrame(
            self.root,
            text=" MONTHLY ROSTER (R3) — Once a month ",
        )
        roster_frame.pack(fill="x", padx=10, pady=4)

        roster_top = ttk.Frame(roster_frame)
        roster_top.pack(fill="x", **pad)

        self._btn_roster = ttk.Button(
            roster_top, text="📋  Run Roster — All Carriers",
            command=self._on_run_roster,
        )
        self._btn_roster.pack(side="left", padx=(0, 8))

        self._btn_push_roster = ttk.Button(
            roster_top, text="📤  Push Roster to Sheets",
            command=self._on_push_roster,
        )
        self._btn_push_roster.pack(side="left")

        self._all_btns += [self._btn_roster, self._btn_push_roster]

        last_roster = _read_last_roster_date()
        self._lbl_last_roster = ttk.Label(
            roster_frame,
            text=f"Last roster run: {last_roster}",
            foreground="#555555",
        )
        self._lbl_last_roster.pack(anchor="w", padx=8, pady=(0, 4))

        # ── UTILITIES section ─────────────────────────────────────────────────
        util_frame = ttk.LabelFrame(self.root, text=" Utilities ")
        util_frame.pack(fill="x", padx=10, pady=4)

        util_row = ttk.Frame(util_frame)
        util_row.pack(fill="x", **pad)

        btn_folder = ttk.Button(
            util_row, text="📂  Open Output Folder",
            command=self._on_open_folder,
        )
        btn_folder.pack(side="left", padx=(0, 6))

        btn_sheet = ttk.Button(
            util_row, text="📊  Open Google Sheet",
            command=self._on_open_sheet,
        )
        btn_sheet.pack(side="left", padx=(0, 6))

        btn_log = ttk.Button(
            util_row, text="📋  Last Log",
            command=self._on_open_last_log,
        )
        btn_log.pack(side="left")

        self._all_btns += [btn_folder, btn_sheet, btn_log]

        # ── Log output panel ──────────────────────────────────────────────────
        log_frame = ttk.LabelFrame(self.root, text=" Log Output ")
        log_frame.pack(fill="both", expand=True, padx=10, pady=(4, 10))

        self._log_text = tk.Text(
            log_frame, height=14, state="disabled",
            font=("Courier", 9), wrap="word", bg="#1e1e1e", fg="#d4d4d4",
        )
        scroll = ttk.Scrollbar(log_frame, command=self._log_text.yview)
        self._log_text.configure(yscrollcommand=scroll.set)
        scroll.pack(side="right", fill="y")
        self._log_text.pack(fill="both", expand=True, padx=4, pady=4)

    def _build_carrier_row(self, parent: ttk.LabelFrame, carrier: dict) -> None:
        name = carrier["name"]
        row  = ttk.Frame(parent)
        row.pack(fill="x", padx=8, pady=1)

        badge_lbl = ttk.Label(row, text="—", width=2)
        badge_lbl.pack(side="left")

        name_lbl = ttk.Label(row, text=name, width=9, anchor="w")
        name_lbl.pack(side="left")

        status_lbl = ttk.Label(row, text="—", width=12, anchor="w")
        status_lbl.pack(side="left")

        run_lbl = ttk.Label(row, text="—", width=14, anchor="w")
        run_lbl.pack(side="left")

        count_lbl = ttk.Label(row, text="—", width=7, anchor="w")
        count_lbl.pack(side="left")

        btn_run = ttk.Button(
            row, text="Run",
            command=lambda n=name: self._on_run_carrier(n),
            width=6,
        )
        btn_run.pack(side="left", padx=(0, 4))

        btn_rerun = ttk.Button(
            row, text="↩",
            command=lambda n=name: self._on_rerun_agent(n),
            width=3,
        )
        btn_rerun.pack(side="left")

        self._all_btns += [btn_run, btn_rerun]

        # United note
        if name == "United":
            note_lbl = ttk.Label(row, text=UNITED_MANUAL_NOTE, foreground="#888888",
                                 font=("TkDefaultFont", 8))
            note_lbl.pack(side="left", padx=(8, 0))

        self._carrier_widgets[name] = {
            "badge":  badge_lbl,
            "status": status_lbl,
            "run":    run_lbl,
            "count":  count_lbl,
        }

    # ── Badge refresh ─────────────────────────────────────────────────────────

    def _refresh_badges(self) -> None:
        for c in CARRIERS:
            badge, status, last_run, count = _carrier_badge(c["name"], c["expected"])
            w = self._carrier_widgets[c["name"]]
            w["badge"].configure(text=badge)
            w["status"].configure(text=status)
            w["run"].configure(text=last_run)
            w["count"].configure(text=count)

        last_roster = _read_last_roster_date()
        self._lbl_last_roster.configure(text=f"Last roster run: {last_roster}")

    # ── Subprocess runner ─────────────────────────────────────────────────────

    def _set_running(self, state: bool) -> None:
        self._running = state
        btn_state = "disabled" if state else "normal"
        for btn in self._all_btns:
            try:
                btn.configure(state=btn_state)
            except Exception:
                pass

    def _run_subprocess(
        self,
        args: list[str],
        label: str,
        on_done: callable | None = None,
    ) -> None:
        """Launch a subprocess, stream its stdout to the log panel in a thread."""
        self._set_running(True)
        self._log(f"\n{'─' * 60}")
        self._log(f"  Starting: {label}")
        self._log(f"{'─' * 60}")

        def _worker():
            try:
                proc = subprocess.Popen(
                    args,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                    cwd=str(ROOT),
                )
                for line in proc.stdout:
                    self._log_queue.put(line.rstrip())
                proc.wait()
                exit_code = proc.returncode
            except Exception as exc:
                self._log_queue.put(f"[LAUNCHER ERROR] {exc}")
                exit_code = -1

            self._log_queue.put(f"\n[{label}] exited with code {exit_code}")
            self.root.after(0, lambda: self._on_subprocess_done(exit_code, label, on_done))

        threading.Thread(target=_worker, daemon=True).start()

    def _run_sequence(
        self,
        steps: list[tuple[list[str], str]],
        on_all_done: callable | None = None,
    ) -> None:
        """Run a list of (args, label) subprocess steps sequentially."""
        if not steps:
            self._set_running(False)
            if on_all_done:
                on_all_done()
            return

        args, label = steps[0]
        remaining   = steps[1:]

        def next_step():
            self._run_sequence(remaining, on_all_done)

        self._run_subprocess(args, label, on_done=next_step)

    def _on_subprocess_done(self, exit_code: int, label: str, on_done: callable | None) -> None:
        carrier_name = label.split()[0] if label else ""
        status = "success" if exit_code == 0 else "failed"
        # Write run status to last_run.json for badge logic
        for c in CARRIERS:
            if c["name"].lower() == carrier_name.lower():
                _write_last_run_json(c["name"], status)
                break
        self._refresh_badges()
        self._set_running(False)
        if on_done:
            self._set_running(True)   # more steps coming — stay locked
            on_done()

    # ── Log panel ─────────────────────────────────────────────────────────────

    def _log(self, text: str) -> None:
        self._log_queue.put(text)

    def _poll_log_queue(self) -> None:
        try:
            while True:
                line = self._log_queue.get_nowait()
                self._log_text.configure(state="normal")
                self._log_text.insert("end", line + "\n")
                self._log_text.see("end")
                self._log_text.configure(state="disabled")
        except queue.Empty:
            pass
        self.root.after(50, self._poll_log_queue)

    # ── Button handlers — Regular Run ─────────────────────────────────────────

    def _on_run_carrier(self, carrier_name: str) -> None:
        if self._running:
            return
        script = next(c["script"] for c in CARRIERS if c["name"] == carrier_name)
        args   = [sys.executable, str(SCRIPTS_DIR / script), "--mode", "regular"]
        self._run_subprocess(args, f"{carrier_name} regular run")

    def _on_rerun_agent(self, carrier_name: str) -> None:
        if self._running:
            return
        agent_idx = simpledialog.askinteger(
            "Single-agent rerun",
            f"Agent index for {carrier_name} (0-based):",
            parent=self.root, minvalue=0,
        )
        if agent_idx is None:
            return
        script = next(c["script"] for c in CARRIERS if c["name"] == carrier_name)
        args   = [
            sys.executable, str(SCRIPTS_DIR / script),
            "--agent", str(agent_idx),
            "--mode", "regular",
        ]
        self._run_subprocess(args, f"{carrier_name} agent {agent_idx} rerun")

    def _on_run_all(self) -> None:
        if self._running:
            return
        steps = []
        for name in RUN_ALL_ORDER:
            script = next(c["script"] for c in CARRIERS if c["name"] == name)
            steps.append((
                [sys.executable, str(SCRIPTS_DIR / script), "--mode", "regular"],
                f"{name} regular run",
            ))
        self._set_running(True)
        self._run_sequence(steps)

    def _on_push_sheets(self) -> None:
        if self._running:
            return
        if not messagebox.askokcancel(
            "Push to Google Sheets",
            "Push R1 + R2 data to Google Sheets?\n\n"
            "This will overwrite the Summary tab and append to Deactivated This Period.",
            parent=self.root,
        ):
            return
        args = [sys.executable, str(SCRIPTS_DIR / "sheets_writer.py"), "--mode", "regular"]
        self._run_subprocess(args, "Sheets push (regular)")

    # ── Button handlers — Monthly Roster ─────────────────────────────────────

    def _on_run_roster(self) -> None:
        if self._running:
            return
        if not messagebox.askokcancel(
            "Run Monthly Roster",
            "Run MONTHLY ROSTER for all carriers?\n\n"
            "This downloads full Book of Business data and takes longer than a regular run.\n\n"
            "Cigna will pause — select 'All Policies' (no filter) when prompted.\n"
            "United and Ambetter will download full BOB.",
            parent=self.root,
        ):
            return

        steps = []
        for name in RUN_ALL_ORDER:
            script = next(c["script"] for c in CARRIERS if c["name"] == name)
            steps.append((
                [sys.executable, str(SCRIPTS_DIR / script), "--mode", "roster"],
                f"{name} roster run",
            ))

        def _after_roster():
            _write_last_roster_date()
            self._lbl_last_roster.configure(
                text=f"Last roster run: {date.today().isoformat()}"
            )
            self._log("[LAUNCHER] Roster run complete. Review data/output/active_members_all.xlsx before pushing to Sheets.")

        self._set_running(True)
        self._run_sequence(steps, on_all_done=_after_roster)

    def _on_push_roster(self) -> None:
        if self._running:
            return
        month_str = date.today().strftime("%B %Y")
        if not messagebox.askokcancel(
            "Push Roster to Sheets",
            f"Push active member roster to Google Sheets?\n\n"
            f"This will OVERWRITE the 'Active Roster – {month_str}' tab.\n"
            "Ensure you have reviewed active_members_all.xlsx first.",
            parent=self.root,
        ):
            return
        args = [sys.executable, str(SCRIPTS_DIR / "sheets_writer.py"), "--mode", "roster"]
        self._run_subprocess(args, "Sheets push (roster)")

    # ── Button handlers — Utilities ───────────────────────────────────────────

    def _on_open_folder(self) -> None:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        os.startfile(str(OUTPUT_DIR))

    def _on_open_sheet(self) -> None:
        today    = date.today()
        month_uc = today.strftime("%B").upper()
        env_key  = f"SHEET_ID_{month_uc}_{today.year}"
        sheet_id = os.getenv(env_key, "").strip()
        if sheet_id:
            webbrowser.open(f"https://docs.google.com/spreadsheets/d/{sheet_id}")
        else:
            messagebox.showwarning(
                "Sheet ID not configured",
                f"Add {env_key}=<sheet_id> to your .env file.",
                parent=self.root,
            )

    def _on_open_last_log(self) -> None:
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        logs = sorted(LOGS_DIR.glob("*.log"), key=lambda p: p.stat().st_mtime, reverse=True)
        if not logs:
            messagebox.showinfo("No logs", "No log files found in logs/.", parent=self.root)
            return
        os.startfile(str(logs[0]))


# ─── Entry point ─────────────────────────────────────────────────────────────

def main() -> None:
    root = tk.Tk()
    app  = LauncherApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
