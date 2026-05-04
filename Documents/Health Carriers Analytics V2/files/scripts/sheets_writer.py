"""
scripts/sheets_writer.py — Phase 3 (refactored Phase R)
Google Sheets writer: reads carrier XLSX files from disk, pushes to Sheets.

Public API (called by launcher):
    push_to_sheets(dry_run=False) → None

Reads from:
    data/output/{carrier}_all_agents.xlsx   (one per carrier — R1 current state)
    data/output/deactivated_members.xlsx    (all carriers — R2 append-only)

Writes to:
    Google Sheet "{Month} {Year}"
      ├── Summary (overwrite each push)
      ├── Deactivated This Period (append net-new only, deduped)
      └── Active Members (append — R1 audit trail)

Bots never call this module. The launcher's "Push to Sheets" button invokes it
only after the operator has verified all carrier XLSX files for the day.

Env vars (.env):
    GOOGLE_SERVICE_ACCOUNT_JSON  service account JSON (default: credentials/service_account.json)
    DRIVE_FOLDER_ID              Drive folder that receives monthly sheets
    SHEET_ID_{YEAR}              pin an existing sheet ID (e.g. SHEET_ID_2026=…)

Standalone usage:
    python scripts/sheets_writer.py             # push live
    python scripts/sheets_writer.py --dry-run   # log what would be written, no API writes
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build

load_dotenv()

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scripts.utils import setup_logging

log = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────────────────────

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

CARRIER_ORDER = ["Ambetter", "Cigna", "Molina", "Oscar", "United"]

TAB_SUMMARY     = "Summary"
TAB_DEACTIVATED = "Deactivated This Period"
TAB_ACTIVE_LOG  = "Active Members"

SUMMARY_HEADERS = ["Agent"] + CARRIER_ORDER + ["Total"]

R1_LOG_HEADERS = [
    "run_date", "run_type", "carrier", "agent_name",
    "active_members", "status",
]

R2_HEADERS = [
    "run_date", "carrier", "agent_name", "member_name",
    "member_dob", "state", "coverage_end_date", "policy_number",
]

R3_HEADERS = [
    "run_date", "carrier", "agent_name",
    "member_first_name", "member_last_name", "member_dob",
    "state", "policy_number", "plan_name", "coverage_start_date", "policy_status",
]

OUTPUT_DIR = ROOT / "data" / "output"
R2_XLSX    = OUTPUT_DIR / "deactivated_members.xlsx"
R3_XLSX    = OUTPUT_DIR / "active_members_all.xlsx"


# ─── XLSX readers ─────────────────────────────────────────────────────────────

def _read_r1_from_disk() -> list[dict]:
    """
    Load R1 records from every {carrier}_all_agents.xlsx file in data/output/.
    Missing files are skipped (logged, not fatal). Returns records as list of dicts.
    """
    records: list[dict] = []
    for carrier in CARRIER_ORDER:
        path = OUTPUT_DIR / f"{carrier.lower()}_all_agents.xlsx"
        if not path.exists():
            log.warning("R1 XLSX missing — skipping %s (%s)", carrier, path.name)
            continue
        df = pd.read_excel(path, engine="openpyxl")
        log.info("R1 loaded: %s — %d rows", carrier, len(df))
        records.extend(df.to_dict(orient="records"))
    return records


def _read_r2_from_disk() -> list[dict]:
    """
    Load every row from data/output/deactivated_members.xlsx.
    Dedup against Sheets happens inside append_deactivated().
    """
    if not R2_XLSX.exists():
        log.warning("R2 XLSX missing — %s (no R2 push this run)", R2_XLSX.name)
        return []
    df = pd.read_excel(R2_XLSX, engine="openpyxl")
    log.info("R2 loaded: %d rows from %s", len(df), R2_XLSX.name)
    return df.to_dict(orient="records")


def _read_r3_from_disk() -> list[dict]:
    """Load every row from data/output/active_members_all.xlsx for roster push."""
    if not R3_XLSX.exists():
        log.warning("R3 XLSX missing — %s (run --mode roster first)", R3_XLSX.name)
        return []
    df = pd.read_excel(R3_XLSX, engine="openpyxl")
    log.info("R3 loaded: %d rows from %s", len(df), R3_XLSX.name)
    return df.to_dict(orient="records")


# ─── Auth ─────────────────────────────────────────────────────────────────────

def _get_services():
    """Build and return (sheets_service, drive_service) from service account JSON."""
    sa_path = ROOT / os.getenv(
        "GOOGLE_SERVICE_ACCOUNT_JSON", "credentials/service_account.json"
    )
    if not sa_path.exists():
        raise FileNotFoundError(
            f"Service account JSON not found: {sa_path}\n"
            "Steps to fix:\n"
            "  1. Create a Google Cloud project\n"
            "  2. Enable Sheets + Drive APIs\n"
            "  3. Create a service account, download the JSON key\n"
            "  4. Place the key at credentials/service_account.json\n"
            "  5. Share your Drive folder with the service account email (Editor)"
        )
    creds = service_account.Credentials.from_service_account_file(
        str(sa_path), scopes=SCOPES
    )
    sheets = build("sheets", "v4", credentials=creds)
    drive  = build("drive",  "v3", credentials=creds)
    return sheets, drive


# ─── Sheet management ─────────────────────────────────────────────────────────

def _month_sheet_name(year: int, month: int) -> str:
    """'April 2026', 'May 2026', etc."""
    return date(year, month, 1).strftime("%B %Y")


def _find_sheet_in_folder(drive, folder_id: str, name: str) -> str | None:
    """Return the file ID if a Sheets file named `name` exists in the folder."""
    query = (
        f"name='{name}' "
        f"and mimeType='application/vnd.google-apps.spreadsheet' "
        f"and '{folder_id}' in parents "
        f"and trashed=false"
    )
    resp = drive.files().list(q=query, fields="files(id,name)").execute()
    files = resp.get("files", [])
    return files[0]["id"] if files else None


def _create_month_sheet(sheets, drive, folder_id: str, name: str) -> str:
    """
    Create a new Sheets file in the Drive folder.
    Sets up 3 tabs (Summary, Deactivated This Period, Active Members) with headers.
    Returns the file ID.
    """
    file_meta = {
        "name": name,
        "mimeType": "application/vnd.google-apps.spreadsheet",
        "parents": [folder_id],
    }
    file = drive.files().create(body=file_meta, fields="id", supportsAllDrives=True).execute()
    sheet_id = file["id"]
    log.info(f"Created new sheet '{name}' (id={sheet_id})")

    meta = sheets.spreadsheets().get(spreadsheetId=sheet_id).execute()
    default_gid = meta["sheets"][0]["properties"]["sheetId"]

    sheets.spreadsheets().batchUpdate(
        spreadsheetId=sheet_id,
        body={
            "requests": [
                {
                    "updateSheetProperties": {
                        "properties": {"sheetId": default_gid, "title": TAB_SUMMARY},
                        "fields": "title",
                    }
                },
                {"addSheet": {"properties": {"title": TAB_DEACTIVATED}}},
                {"addSheet": {"properties": {"title": TAB_ACTIVE_LOG}}},
            ]
        },
    ).execute()

    sheets.spreadsheets().values().batchUpdate(
        spreadsheetId=sheet_id,
        body={
            "valueInputOption": "RAW",
            "data": [
                {"range": f"'{TAB_SUMMARY}'!A1",     "values": [SUMMARY_HEADERS]},
                {"range": f"'{TAB_DEACTIVATED}'!A1", "values": [R2_HEADERS]},
                {"range": f"'{TAB_ACTIVE_LOG}'!A1",  "values": [R1_LOG_HEADERS]},
            ],
        },
    ).execute()

    log.info(f"Initialised 3 tabs with headers in '{name}'")
    return sheet_id


def _read_a1(sheets, sheet_id: str, tab: str) -> str:
    """Return the value of cell A1 in the given tab, or '' if empty."""
    resp = sheets.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range=f"'{tab}'!A1",
    ).execute()
    values = resp.get("values", [])
    return values[0][0] if (values and values[0]) else ""


def _ensure_tab_headers(sheets, sheet_id: str) -> None:
    """
    Write header rows to tabs whose A1 does not match the expected header.
    Compares A1 against the first header string — distinguishes correct headers
    from empty cells AND from data rows written without headers (false non-empty).
    Safe to call every run — no-op when headers are already correct.
    """
    tab_headers = [
        (TAB_SUMMARY,     SUMMARY_HEADERS),
        (TAB_DEACTIVATED, R2_HEADERS),
        (TAB_ACTIVE_LOG,  R1_LOG_HEADERS),
    ]
    writes = []
    for tab, headers in tab_headers:
        a1 = _read_a1(sheets, sheet_id, tab)
        if a1 != headers[0]:
            writes.append({"range": f"'{tab}'!A1", "values": [headers]})

    if writes:
        sheets.spreadsheets().values().batchUpdate(
            spreadsheetId=sheet_id,
            body={"valueInputOption": "RAW", "data": writes},
        ).execute()
        written = [w["range"].split("'")[1] for w in writes]
        log.info(f"_ensure_tab_headers: wrote headers to {written}")
    else:
        log.info("_ensure_tab_headers: all tabs already have headers — skipped")


def get_or_create_month_sheet(year: int, month: int) -> str:
    """
    Return the Sheets file ID for the given year/month.

    Resolution order:
      1. Env var SHEET_ID_{MONTH}_{YEAR} — skips Drive entirely.
      2. Drive folder search — finds existing sheet by name.
      3. Drive file create — creates sheet with 3 tabs + headers.
    """
    env_key = f"SHEET_ID_{year}"
    pinned_id = os.getenv(env_key, "").strip()
    if pinned_id:
        log.info(f"Using pinned sheet ID from env {env_key}={pinned_id}")
        sheets, _ = _get_services()
        _ensure_tab_headers(sheets, pinned_id)
        return pinned_id

    folder_id = os.getenv("DRIVE_FOLDER_ID", "").strip()
    if not folder_id:
        raise ValueError(
            "DRIVE_FOLDER_ID is not set in .env\n"
            "Create a Google Drive folder, share it with your service account email,\n"
            "then paste the folder ID (from the URL) into .env"
        )

    sheets, drive = _get_services()
    name = _month_sheet_name(year, month)

    existing_id = _find_sheet_in_folder(drive, folder_id, name)
    if existing_id:
        log.info(f"Found existing sheet '{name}' (id={existing_id})")
        _ensure_tab_headers(sheets, existing_id)
        return existing_id

    return _create_month_sheet(sheets, drive, folder_id, name)


# ─── Summary pivot ────────────────────────────────────────────────────────────

def _build_pivot_rows(r1_records: list[dict]) -> list[list]:
    """
    Build the Summary contingency table as a list of lists.

    Rules:
      - Only 'success' records contribute (failed runs → blank cell, never zero)
      - Rows: agents sorted alphabetically, then Total row
      - Columns: Agent | Ambetter | Cigna | Molina | Oscar | United | Total
      - Missing carrier for an agent → "" (blank), not 0
    """
    success = [r for r in r1_records if r.get("status") == "success"]

    if not success:
        return [SUMMARY_HEADERS]

    df = pd.DataFrame(success)
    pivot = df.pivot_table(
        index="agent_name",
        columns="carrier",
        values="active_members",
        aggfunc="sum",
    )

    pivot = pivot.reindex(columns=CARRIER_ORDER)
    pivot = pivot.sort_index()
    pivot.index.name  = None
    pivot.columns.name = None

    pivot["Total"] = pivot.sum(axis=1, min_count=1)

    total_row = pivot.sum(axis=0, min_count=1)
    total_row.name = "Total"
    pivot = pd.concat([pivot, total_row.to_frame().T])

    rows = [SUMMARY_HEADERS]
    for agent_name, row in pivot.iterrows():
        out_row = [agent_name]
        for val in row:
            if pd.isna(val):
                out_row.append("")
            else:
                out_row.append(int(val))
        rows.append(out_row)

    return rows


def write_summary(r1_records: list[dict], sheet_id: str) -> None:
    """
    Completely overwrites the Summary tab with the current pivot.
    Called every push — Summary is never appended, always rebuilt.
    """
    sheets, _ = _get_services()

    sheets.spreadsheets().values().clear(
        spreadsheetId=sheet_id,
        range=f"'{TAB_SUMMARY}'!A:Z",
    ).execute()

    rows = _build_pivot_rows(r1_records)
    sheets.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=f"'{TAB_SUMMARY}'!A1",
        valueInputOption="RAW",
        body={"values": rows},
    ).execute()

    agent_rows = max(0, len(rows) - 2)
    log.info(
        f"write_summary: {agent_rows} agents, "
        f"{len([r for r in r1_records if r.get('status') == 'success'])} success records "
        f"(sheet={sheet_id})"
    )


# ─── Append helpers ───────────────────────────────────────────────────────────

def _records_to_rows(records: list[dict], headers: list[str]) -> list[list]:
    """Convert a list of dicts to ordered rows matching the given header list."""
    return [
        [str(r.get(h) if r.get(h) is not None else "") for h in headers]
        for r in records
    ]


def append_deactivated(r2_records: list[dict], sheet_id: str) -> None:
    """
    Append net-new R2 rows to 'Deactivated This Period'.
    Loads existing rows ONCE into pandas, deduplicates in-process, writes only
    truly new rows — never queries Sheets in a loop.
    Dedup key: (carrier, policy_number, coverage_end_date), keep='first'.
    """
    if not r2_records:
        log.info("append_deactivated: empty list — nothing written")
        return

    found_count = len(r2_records)
    sheets, _ = _get_services()

    if _read_a1(sheets, sheet_id, TAB_DEACTIVATED) != R2_HEADERS[0]:
        sheets.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range=f"'{TAB_DEACTIVATED}'!A1",
            valueInputOption="RAW",
            body={"values": [R2_HEADERS]},
        ).execute()
        log.info("append_deactivated: wrote missing headers to row 1")

    resp = sheets.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range=f"'{TAB_DEACTIVATED}'!A:Z",
    ).execute()
    existing_values = resp.get("values", [])

    if len(existing_values) > 1:
        existing_df = pd.DataFrame(existing_values[1:], columns=existing_values[0])
    else:
        existing_df = pd.DataFrame(columns=R2_HEADERS)

    rows_before = len(existing_df)

    new_df = pd.DataFrame(r2_records)
    new_df = new_df.reindex(columns=R2_HEADERS)
    new_df = new_df.fillna("").astype(str)

    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    combined_df = combined_df.drop_duplicates(
        subset=["carrier", "policy_number", "coverage_end_date"],
        keep="first",
    )

    net_new = len(combined_df) - rows_before
    already_in_file = found_count - net_new

    log.info(
        "sheets | R2 | found=%d | already_in_file=%d | net_new_appended=%d",
        found_count, already_in_file, net_new,
    )

    if net_new == 0:
        return

    net_new_df = combined_df.iloc[rows_before:]
    rows = [
        [str(v) if v is not None else "" for v in row]
        for row in net_new_df.values.tolist()
    ]

    sheets.spreadsheets().values().append(
        spreadsheetId=sheet_id,
        range=f"'{TAB_DEACTIVATED}'!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows},
    ).execute()


def append_r1_log(r1_records: list[dict], sheet_id: str) -> None:
    """
    Append raw R1 rows to 'Active Members' (audit trail for Looker Studio).
    Empty list → no-op.
    """
    if not r1_records:
        log.info("append_r1_log: empty list — nothing written")
        return

    sheets, _ = _get_services()

    if _read_a1(sheets, sheet_id, TAB_ACTIVE_LOG) != R1_LOG_HEADERS[0]:
        sheets.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range=f"'{TAB_ACTIVE_LOG}'!A1",
            valueInputOption="RAW",
            body={"values": [R1_LOG_HEADERS]},
        ).execute()
        log.info("append_r1_log: wrote missing headers to row 1")

    rows = _records_to_rows(r1_records, R1_LOG_HEADERS)
    sheets.spreadsheets().values().append(
        spreadsheetId=sheet_id,
        range=f"'{TAB_ACTIVE_LOG}'!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows},
    ).execute()
    log.info(f"append_r1_log: {len(rows)} rows written (sheet={sheet_id})")


# ─── Roster tab push (overwrite — point-in-time snapshot) ────────────────────

def _roster_tab_name(year: int, month: int) -> str:
    """'Active Roster – April 2026', etc."""
    return "Active Roster – " + date(year, month, 1).strftime("%B %Y")


def push_roster_tab(r3_records: list[dict], sheet_id: str) -> None:
    """
    Overwrite the 'Active Roster – {Month} {Year}' tab with current R3 data.
    Creates the tab if it does not exist. Point-in-time snapshot — never appended.
    """
    if not r3_records:
        log.warning("push_roster_tab: no R3 records — nothing written")
        return

    today    = date.today()
    tab_name = _roster_tab_name(today.year, today.month)
    sheets, _ = _get_services()

    # Ensure the tab exists — create it if not
    meta   = sheets.spreadsheets().get(spreadsheetId=sheet_id).execute()
    titles = [s["properties"]["title"] for s in meta.get("sheets", [])]
    if tab_name not in titles:
        sheets.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": tab_name}}}]},
        ).execute()
        log.info("push_roster_tab: created tab '%s'", tab_name)

    # Overwrite: clear then write headers + data
    sheets.spreadsheets().values().clear(
        spreadsheetId=sheet_id,
        range=f"'{tab_name}'!A:Z",
    ).execute()

    rows = [R3_HEADERS] + _records_to_rows(r3_records, R3_HEADERS)
    sheets.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=f"'{tab_name}'!A1",
        valueInputOption="RAW",
        body={"values": rows},
    ).execute()

    carrier_counts = {}
    for r in r3_records:
        carrier_counts[r.get("carrier", "?")] = carrier_counts.get(r.get("carrier", "?"), 0) + 1

    log.info(
        "push_roster_tab: wrote %d rows to '%s' — carriers: %s",
        len(r3_records), tab_name, carrier_counts,
    )


# ─── Dashboard JSON export ───────────────────────────────────────────────────

def export_dashboard_json(r1_records: list[dict]) -> None:
    """
    Merge new R1 records into dashboard/dashboard_data.json.
    Loads existing file (history), updates with new records, writes back.
    Runs in both live and dry-run modes — local file write only, no Sheets API.
    """
    CARRIER_NAMES = ["Ambetter", "Cigna", "Molina", "Oscar", "United HC"]
    DASH_DIR = ROOT / "dashboard"
    DASH_DIR.mkdir(exist_ok=True)
    out_path = DASH_DIR / "dashboard_data.json"

    if out_path.exists():
        with out_path.open(encoding="utf-8") as fh:
            existing = json.load(fh)
    else:
        existing = {
            "meta": {"generated": "", "first_date": "", "latest_date": "",
                     "total_dates": 0, "total_agents": 0, "carriers": CARRIER_NAMES},
            "dates": [], "run_types": {}, "agents": [],
            "carriers": CARRIER_NAMES, "D": {},
        }

    D_out = existing.get("D", {})
    rt_out = existing.get("run_types", {})

    for rec in r1_records:
        if rec.get("status") != "success":
            continue
        count = int(rec.get("active_members") or 0)
        if count <= 0:
            continue
        try:
            ds = str(rec.get("run_date", ""))[:10]
            d_obj = date.fromisoformat(ds)
            date_key = f"{d_obj.month:02d}/{d_obj.day:02d}"
        except Exception:
            continue
        agent   = str(rec.get("agent_name", "")).strip()
        carrier = str(rec.get("carrier",    "")).strip()
        run_type = str(rec.get("run_type",  "Manual")).strip()
        if not agent or not carrier:
            continue
        CARRIER_NORMALIZE = {"United": "United HC"}
        carrier = CARRIER_NORMALIZE.get(carrier, carrier)
        D_out.setdefault(date_key, {}).setdefault(agent, {})[carrier] = count
        rt_out[date_key] = run_type

    all_dates  = sorted(D_out.keys())
    all_agents = sorted({a for d in D_out.values() for a in d})

    payload = {
        "meta": {
            "generated":   str(date.today()),
            "first_date":  all_dates[0]  if all_dates else "",
            "latest_date": all_dates[-1] if all_dates else "",
            "total_dates":  len(all_dates),
            "total_agents": len(all_agents),
            "carriers": CARRIER_NAMES,
        },
        "dates":     all_dates,
        "run_types": {k: rt_out[k] for k in sorted(rt_out)},
        "agents":    all_agents,
        "carriers":  CARRIER_NAMES,
        "D":         {k: D_out[k] for k in all_dates},
    }

    with out_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)

    log.info(
        "dashboard_data.json exported -> dashboard/dashboard_data.json (%d dates, %d agents)",
        len(all_dates), len(all_agents),
    )


# ─── Top-level entry point ────────────────────────────────────────────────────

def push_to_sheets(dry_run: bool = False, mode: str = "regular") -> None:
    """
    Read XLSX files from disk, push to Google Sheets.

    mode='regular' (default):
        Reads carrier R1 XLSX + deactivated_members.xlsx (R2).
        Overwrites Summary tab, appends Deactivated and Active Members tabs.

    mode='roster':
        Reads active_members_all.xlsx (R3).
        Overwrites 'Active Roster – {Month} {Year}' tab (point-in-time snapshot).

    Never called by bots. Launcher buttons invoke it.
    """
    global log
    log = setup_logging("SHEETS")

    today    = date.today()
    sheet_id = get_or_create_month_sheet(today.year, today.month)

    if mode == "roster":
        r3_records = _read_r3_from_disk()
        tab_name   = _roster_tab_name(today.year, today.month)
        if dry_run:
            log.info("DRY RUN (roster) — Sheets write skipped")
            log.info("  sheet target:           '%s' (id=%s)", _month_sheet_name(today.year, today.month), sheet_id)
            log.info("  would overwrite tab:    '%s'", tab_name)
            log.info("  would write R3 records: %d", len(r3_records))
            carrier_counts = {}
            for r in r3_records:
                carrier_counts[r.get("carrier", "?")] = carrier_counts.get(r.get("carrier", "?"), 0) + 1
            log.info("  carrier breakdown:      %s", carrier_counts)
            export_dashboard_json(_read_r1_from_disk())
            return
        push_roster_tab(r3_records, sheet_id)
        export_dashboard_json(_read_r1_from_disk())
        log.info("push_to_sheets (roster) complete")
        return

    # Regular mode
    r1_records = _read_r1_from_disk()
    r2_records = _read_r2_from_disk()

    if dry_run:
        log.info("DRY RUN — all Sheets writes skipped")
        log.info(f"  sheet target:             '{_month_sheet_name(today.year, today.month)}' (id={sheet_id})")
        log.info(f"  would write_summary:      {len(r1_records)} R1 records")
        log.info(f"  would append_deactivated: {len(r2_records)} R2 records")
        log.info(f"  would append_r1_log:      {len(r1_records)} R1 records")
        export_dashboard_json(r1_records)
        return

    write_summary(r1_records, sheet_id)
    append_deactivated(r2_records, sheet_id)
    append_r1_log(r1_records, sheet_id)
    export_dashboard_json(r1_records)
    log.info("push_to_sheets complete")


# ─── Standalone CLI ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Push carrier XLSX files to Google Sheets",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  python scripts/sheets_writer.py\n"
            "    → read data/output/*.xlsx, push to Sheets\n\n"
            "  python scripts/sheets_writer.py --dry-run\n"
            "    → log what would be written, no API writes\n\n"
            "env vars required (.env):\n"
            "  SHEET_ID_2026         (or DRIVE_FOLDER_ID if not pinning)\n"
            "  GOOGLE_SERVICE_ACCOUNT_JSON\n"
        ),
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--mode", choices=["regular", "roster"], default="regular",
                        help="regular: push R1+R2 | roster: overwrite Active Roster tab")
    args = parser.parse_args()

    push_to_sheets(dry_run=args.dry_run, mode=args.mode)
