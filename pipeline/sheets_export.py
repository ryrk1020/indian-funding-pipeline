"""Google Sheets exporter — idempotent replace of a worksheet.

Auth: service account JSON file (path in GOOGLE_SERVICE_ACCOUNT_FILE). The
sheet must be shared with the service account's client_email as Editor.

Idempotency: each run clears the worksheet and writes header + rows in one
`update` call. No appending, no stale rows.

Kept out of pipeline/exporter.py so the CSV/JSON path stays zero-dep — gspread
is only loaded when Sheets export is actually invoked.
"""
from __future__ import annotations

from pathlib import Path

from loguru import logger

from pipeline.exporter import EXPORT_COLUMNS, _fetch_rows
from pipeline.storage import Storage

_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


class SheetsExportError(RuntimeError):
    pass


def export_sheets(
    storage: Storage,
    sheet_id: str,
    service_account_file: Path | str,
    worksheet_name: str = "rounds",
    min_confidence: float = 0.0,
) -> int:
    """Replace `worksheet_name` with current funding_rounds. Returns row count."""
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError as e:
        raise SheetsExportError(
            "gspread / google-auth not installed. Run: pip install gspread google-auth"
        ) from e

    sa_path = Path(service_account_file)
    if not sa_path.exists():
        raise SheetsExportError(f"service account file not found: {sa_path}")

    creds = Credentials.from_service_account_file(str(sa_path), scopes=_SCOPES)
    client = gspread.authorize(creds)

    try:
        sheet = client.open_by_key(sheet_id)
    except Exception as e:
        raise SheetsExportError(f"failed to open sheet {sheet_id}: {e}") from e

    # Get or create worksheet
    try:
        ws = sheet.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet(title=worksheet_name, rows=1000, cols=len(EXPORT_COLUMNS))

    rows = _fetch_rows(storage, min_confidence=min_confidence)
    payload = [EXPORT_COLUMNS] + [
        [_cell(r.get(c)) for c in EXPORT_COLUMNS] for r in rows
    ]

    ws.clear()
    if payload:
        ws.update(
            values=payload,
            range_name=f"A1:{_col_letter(len(EXPORT_COLUMNS))}{len(payload)}",
        )
    logger.info("sheets: wrote {} rows to '{}' in sheet {}", len(rows), worksheet_name, sheet_id)
    return len(rows)


def _cell(v: object) -> str:
    if v is None:
        return ""
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    return str(v)


def _col_letter(n: int) -> str:
    """1 → A, 26 → Z, 27 → AA."""
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s
