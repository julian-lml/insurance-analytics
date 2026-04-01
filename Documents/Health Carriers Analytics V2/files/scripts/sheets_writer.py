"""
scripts/sheets_writer.py — Phase 3
Google Sheets writer: Summary pivot + Deactivated This Period + Active Members log.

Public API (what main.py calls in Phase 8):
    write_run(r1_records, r2_records, dry_run=False) → None

Lower-level functions (also importable):
    get_or_create_month_sheet(year, month) → sheet_id
    write_summary(r1_records, sheet_id)   → None
    append_deactivated(r2_records, sheet_id) → None
    append_r1_log(r1_records, sheet_id)   → None

Env vars (.env):
    GOOGLE_SERVICE_ACCOUNT_JSON  path to service account JSON (default: credentials/service_account.json)
    DRIVE_FOLDER_ID              Google Drive folder that receives monthly sheets
    SHEET_ID_{MONTH}_{YEAR}      pin an existing sheet ID and skip Drive file creation entirely
                                 e.g. SHEET_ID_APRIL_2026=1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms
                                 Use when Drive returns storageQuotaExceeded on file creation.

Standalone test:
    python scripts/sheets_writer.py
    python scripts/sheets_writer.py --dry-run
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build

load_dotenv()

ROOT = Path(__file__).resolve().parent.parent

# ─── Logging ──────────────────────────────────────────────────────────────────

def setup_logging() -> None:
    log_dir = ROOT / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"run_{datetime.now().strftime('%Y%m%d_%H%M')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | sheets | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


log = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────────────────────

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Canonical carrier column order — matches CLAUDE.md deliverable shape
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

    # Get the auto-created Sheet1's numeric sheetId so we can rename it
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

    # Write header rows to all three tabs
    sheets.spreadsheets().values().batchUpdate(
        spreadsheetId=sheet_id,
        body={
            "valueInputOption": "RAW",
            "data": [
                {
                    "range": f"'{TAB_SUMMARY}'!A1",
                    "values": [SUMMARY_HEADERS],
                },
                {
                    "range": f"'{TAB_DEACTIVATED}'!A1",
                    "values": [R2_HEADERS],
                },
                {
                    "range": f"'{TAB_ACTIVE_LOG}'!A1",
                    "values": [R1_LOG_HEADERS],
                },
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
      1. Env var SHEET_ID_{MONTH}_{YEAR} (e.g. SHEET_ID_APRIL_2026) — skips Drive entirely.
         Use this when Drive file creation is blocked (storageQuotaExceeded abuse prevention).
      2. Drive folder search — finds existing sheet by name.
      3. Drive file create — creates sheet with 3 tabs + headers.

    Idempotent — safe to call multiple times per run.
    """
    month_name = date(year, month, 1).strftime("%B").upper()  # e.g. "APRIL"
    env_key = f"SHEET_ID_{month_name}_{year}"                 # e.g. "SHEET_ID_APRIL_2026"
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

    # Ensure all carriers present as columns in canonical order;
    # carriers with no data stay as NaN (→ blank cell)
    pivot = pivot.reindex(columns=CARRIER_ORDER)
    pivot = pivot.sort_index()          # agents alphabetical
    pivot.index.name  = None
    pivot.columns.name = None

    # Total column per agent — sum non-NaN only (min_count=1 keeps NaN if all blank)
    pivot["Total"] = pivot.sum(axis=1, min_count=1)

    # Total row — sum per carrier across all agents
    total_row = pivot.sum(axis=0, min_count=1)
    total_row.name = "Total"
    pivot = pd.concat([pivot, total_row.to_frame().T])

    # Serialise: NaN → "", numbers → int
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
    Called every run — Summary is never appended, always rebuilt.
    """
    sheets, _ = _get_services()

    # Clear the entire tab first (rows may shrink between runs)
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

    # -1 for header, -1 for Total row = agent data rows
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
    Append R2 rows to 'Deactivated This Period'.
    Empty list → no-op (no API call, no error).
    Writes headers to row 1 first if the tab is empty.
    """
    if not r2_records:
        log.info("append_deactivated: empty list — nothing written")
        return

    sheets, _ = _get_services()

    if _read_a1(sheets, sheet_id, TAB_DEACTIVATED) != R2_HEADERS[0]:
        sheets.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range=f"'{TAB_DEACTIVATED}'!A1",
            valueInputOption="RAW",
            body={"values": [R2_HEADERS]},
        ).execute()
        log.info("append_deactivated: wrote missing headers to row 1")

    rows = _records_to_rows(r2_records, R2_HEADERS)
    sheets.spreadsheets().values().append(
        spreadsheetId=sheet_id,
        range=f"'{TAB_DEACTIVATED}'!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows},
    ).execute()
    log.info(f"append_deactivated: {len(rows)} rows written (sheet={sheet_id})")


def append_r1_log(r1_records: list[dict], sheet_id: str) -> None:
    """
    Append raw R1 rows to 'Active Members' (audit trail for Looker Studio).
    Empty list → no-op.
    Writes headers to row 1 first if the tab is empty.
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


# ─── Top-level entry point ────────────────────────────────────────────────────

def write_run(
    r1_records: list[dict],
    r2_records: list[dict],
    dry_run: bool = False,
) -> None:
    """
    Top-level function. This is what main.py calls in Phase 8.

    Flow:
        get_or_create_month_sheet  → sheet_id
        write_summary              → overwrites Summary tab
        append_deactivated         → appends to Deactivated This Period
        append_r1_log              → appends to Active Members
    """
    today = date.today()
    sheet_id = get_or_create_month_sheet(today.year, today.month)

    if dry_run:
        log.info("DRY RUN — all Sheets writes skipped")
        log.info(f"  sheet target:             '{_month_sheet_name(today.year, today.month)}' (id={sheet_id})")
        log.info(f"  would write_summary:      {len(r1_records)} R1 records")
        log.info(f"  would append_deactivated: {len(r2_records)} R2 records")
        log.info(f"  would append_r1_log:      {len(r1_records)} R1 records")
        return

    write_summary(r1_records, sheet_id)
    append_deactivated(r2_records, sheet_id)
    append_r1_log(r1_records, sheet_id)
    log.info("write_run complete")


# ─── Standalone test ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Phase 3 — Sheets writer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  python scripts/sheets_writer.py\n"
            "    → prints this help (no writes)\n\n"
            "  python scripts/sheets_writer.py --test\n"
            "    → writes 2-agent test fixture to the live sheet\n\n"
            "  python scripts/sheets_writer.py --test --dry-run\n"
            "    → resolves sheet ID and logs what would be written, no API writes\n\n"
            "env vars required (.env):\n"
            "  SHEET_ID_APRIL_2026   (or DRIVE_FOLDER_ID if not pinning)\n"
            "  GOOGLE_SERVICE_ACCOUNT_JSON\n"
        ),
    )
    parser.add_argument("--test",    action="store_true", help="Write 2-agent fixture data to the live sheet")
    parser.add_argument("--dry-run", action="store_true", help="Resolve sheet ID but skip all Sheets writes (use with --test)")
    args = parser.parse_args()

    if not args.test:
        parser.print_help()
        sys.exit(0)

    setup_logging()

    # 2-agent × 2-carrier test — Brandon missing Cigna (failed) → blank cell
    TEST_R1 = [
        {
            "run_date": date.today().isoformat(),
            "run_type": "Monday",
            "carrier": "Ambetter",
            "agent_name": "Brandon Kaplan",
            "active_members": 95,
            "status": "success",
            "error_message": None,
            "duration_seconds": 42.3,
        },
        {
            "run_date": date.today().isoformat(),
            "run_type": "Monday",
            "carrier": "Molina",
            "agent_name": "Brandon Kaplan",
            "active_members": 6,
            "status": "success",
            "error_message": None,
            "duration_seconds": 18.1,
        },
        {
            "run_date": date.today().isoformat(),
            "run_type": "Monday",
            "carrier": "Ambetter",
            "agent_name": "Felipe Ramirez",
            "active_members": 461,
            "status": "success",
            "error_message": None,
            "duration_seconds": 38.7,
        },
        {
            "run_date": date.today().isoformat(),
            "run_type": "Monday",
            "carrier": "Cigna",
            "agent_name": "Felipe Ramirez",
            "active_members": 72,
            "status": "success",
            "error_message": None,
            "duration_seconds": 22.4,
        },
        # Cigna failed for Brandon — excluded from pivot → blank cell, not zero
        {
            "run_date": date.today().isoformat(),
            "run_type": "Monday",
            "carrier": "Cigna",
            "agent_name": "Brandon Kaplan",
            "active_members": None,
            "status": "failed",
            "error_message": "timeout waiting for element",
            "duration_seconds": 45.0,
        },
    ]

    TEST_R2 = [
        {
            "run_date": date.today().isoformat(),
            "carrier": "Ambetter",
            "agent_name": "Brandon Kaplan",
            "member_name": "Emily Rink",
            "member_dob": "07/24/2000",
            "state": "SC",
            "coverage_end_date": "2026-03-24",
            "policy_number": "U70066328",
            "last_status": "Cancelled",
            "detection_method": "download_filter",
        },
        {
            "run_date": date.today().isoformat(),
            "carrier": "Molina",
            "agent_name": "Felipe Ramirez",
            "member_name": "Carlos Mendoza",
            "member_dob": "03/15/1985",
            "state": "FL",
            "coverage_end_date": "2026-03-28",
            "policy_number": "M90012345",
            "last_status": "Terminated",
            "detection_method": "download_filter",
        },
    ]

    write_run(TEST_R1, TEST_R2, dry_run=args.dry_run)
