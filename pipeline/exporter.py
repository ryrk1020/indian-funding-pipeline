"""Flat exports of the funding_rounds table.

CSV + JSON are idempotent: each run writes a fresh snapshot. Sheets is handled
in `pipeline.sheets_export` (keeps this file dependency-light).

Row shape:
  round_id, company_name, sector, stage, amount, currency, amount_usd,
  announced_on, lead_investor, investors, sources, summary, confidence,
  extraction_method, created_at, updated_at

Multi-value fields (investors, sources) are pipe-joined so a downstream BI
tool can split them back without CSV quoting nightmares.
"""
from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from pipeline.storage import Storage

EXPORT_COLUMNS = [
    "round_id",
    "company_name",
    "sector",
    "stage",
    "amount",
    "currency",
    "amount_usd",
    "announced_on",
    "lead_investor",
    "investors",
    "sources",
    "summary",
    "confidence",
    "extraction_method",
    "created_at",
    "updated_at",
]


def _fetch_rows(storage: Storage, min_confidence: float = 0.0) -> list[dict[str, Any]]:
    """Join funding_rounds with investors + sources. Ordered by confidence desc."""
    q = """
    SELECT
        fr.round_id,
        fr.company_name,
        fr.company_json,
        fr.stage,
        fr.amount,
        fr.currency,
        fr.amount_usd,
        fr.announced_on,
        fr.summary,
        fr.confidence,
        fr.extraction_method,
        fr.created_at,
        fr.updated_at,
        (SELECT GROUP_CONCAT(name, ' | ')
         FROM round_investors ri WHERE ri.round_id = fr.round_id) AS investors,
        (SELECT name FROM round_investors ri
         WHERE ri.round_id = fr.round_id AND ri.lead = 1 LIMIT 1) AS lead_investor,
        (SELECT GROUP_CONCAT(article_url, ' | ')
         FROM round_sources rs WHERE rs.round_id = fr.round_id) AS sources
    FROM funding_rounds fr
    WHERE fr.confidence >= ?
    ORDER BY fr.confidence DESC, fr.announced_on DESC
    """
    out: list[dict[str, Any]] = []
    with storage.connect() as c:
        for row in c.execute(q, (min_confidence,)).fetchall():
            d = dict(row)
            # Unpack sector from company_json; keep the rest flat
            try:
                company = json.loads(d.pop("company_json") or "{}")
            except json.JSONDecodeError:
                company = {}
            d["sector"] = company.get("sector")
            out.append({k: d.get(k) for k in EXPORT_COLUMNS})
    return out


def export_csv(storage: Storage, path: Path, min_confidence: float = 0.0) -> int:
    rows = _fetch_rows(storage, min_confidence=min_confidence)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=EXPORT_COLUMNS)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: "" if v is None else v for k, v in r.items()})
    return len(rows)


def export_json(storage: Storage, path: Path, min_confidence: float = 0.0) -> int:
    rows = _fetch_rows(storage, min_confidence=min_confidence)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Split pipe-joined fields back into arrays for JSON — more natural there
    for r in rows:
        r["investors"] = r["investors"].split(" | ") if r["investors"] else []
        r["sources"] = r["sources"].split(" | ") if r["sources"] else []
    with path.open("w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2, default=str)
    return len(rows)


def export_xlsx(storage: Storage, path: Path, min_confidence: float = 0.0) -> int:
    """Excel workbook with one sheet 'rounds'. Frozen header, auto-sized columns,
    conditional formatting on confidence (red below 0.35, green above 0.85).
    """
    from openpyxl import Workbook
    from openpyxl.formatting.rule import CellIsRule
    from openpyxl.styles import Font, PatternFill
    from openpyxl.utils import get_column_letter

    rows = _fetch_rows(storage, min_confidence=min_confidence)
    path.parent.mkdir(parents=True, exist_ok=True)

    wb = Workbook()
    ws = wb.active
    ws.title = "rounds"

    ws.append(EXPORT_COLUMNS)
    for cell in ws[1]:
        cell.font = Font(bold=True)
        cell.fill = PatternFill("solid", fgColor="E8E8E8")
    ws.freeze_panes = "A2"

    for r in rows:
        ws.append([_xlsx_cell(r.get(c)) for c in EXPORT_COLUMNS])

    # Auto-size columns (cap at 50 chars — long summaries otherwise blow out)
    for idx, col_name in enumerate(EXPORT_COLUMNS, start=1):
        letter = get_column_letter(idx)
        max_len = max(
            [len(col_name)] + [len(str(r.get(col_name) or "")) for r in rows]
        )
        ws.column_dimensions[letter].width = min(max_len + 2, 50)

    # Confidence column conditional formatting
    conf_col = EXPORT_COLUMNS.index("confidence") + 1
    conf_letter = get_column_letter(conf_col)
    if rows:
        rng = f"{conf_letter}2:{conf_letter}{len(rows) + 1}"
        ws.conditional_formatting.add(rng, CellIsRule(
            operator="lessThan", formula=["0.35"],
            fill=PatternFill("solid", fgColor="FADBD8"),
        ))
        ws.conditional_formatting.add(rng, CellIsRule(
            operator="greaterThanOrEqual", formula=["0.85"],
            fill=PatternFill("solid", fgColor="D5F5E3"),
        ))

    wb.save(path)
    return len(rows)


def _xlsx_cell(v: object) -> object:
    """Return a value Excel can render natively (numbers as numbers, dates as strings)."""
    if v is None:
        return ""
    if isinstance(v, (int, float)):
        return v
    return str(v)
