"""CSV/JSON exporters: header shape, row count, multi-value joining, idempotency."""
from __future__ import annotations

import csv
import json
from datetime import date, datetime

from config.schemas import ArticleRaw, Company, FundingRound, FundingStage, Investor
from pipeline.exporter import EXPORT_COLUMNS, export_csv, export_json, export_xlsx
from pipeline.storage import Storage


def _seed(tmp_path) -> Storage:
    s = Storage(db_path=tmp_path / "ex.db")
    s.upsert_article(ArticleRaw(
        source="inc42", url="https://inc42.com/a", title="t", text="x",
        fetched_at=datetime(2026, 4, 17),
    ))
    s.upsert_article(ArticleRaw(
        source="entrackr", url="https://entrackr.com/a", title="t", text="x",
        fetched_at=datetime(2026, 4, 17),
    ))
    s.upsert_round(FundingRound(
        round_id="r1" * 8,
        company=Company(name="Hocco", sector="Food"),
        stage=FundingStage.SERIES_C,
        amount_usd=12_000_000,
        announced_on=date(2026, 4, 17),
        investors=[Investor(name="Sixth Sense", lead=True), Investor(name="Accel")],
        sources=["https://inc42.com/a"],
        confidence=0.95,
        summary="Hocco raised Series C.",
    ), source_urls=["https://inc42.com/a", "https://entrackr.com/a"], dedup=False)
    s.upsert_round(FundingRound(
        round_id="r2" * 8,
        company=Company(name="TraqCheck"),
        stage=FundingStage.SEED,
        amount_usd=500_000,
        announced_on=date(2026, 4, 14),
        sources=["https://inc42.com/a"],
        confidence=0.40,
    ), source_urls=["https://inc42.com/a"], dedup=False)
    return s


def test_csv_has_all_columns_and_rows(tmp_path) -> None:
    s = _seed(tmp_path)
    out = tmp_path / "rounds.csv"
    n = export_csv(s, out)
    assert n == 2
    with out.open("r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert list(rows[0].keys()) == EXPORT_COLUMNS
    # Ordered by confidence desc
    assert rows[0]["company_name"] == "Hocco"
    assert rows[1]["company_name"] == "TraqCheck"
    assert set(rows[0]["investors"].split(" | ")) == {"Sixth Sense", "Accel"}
    assert rows[0]["lead_investor"] == "Sixth Sense"
    assert rows[0]["sector"] == "Food"
    assert rows[0]["sources"].count(" | ") == 1  # two sources, one delimiter


def test_json_unpacks_arrays(tmp_path) -> None:
    s = _seed(tmp_path)
    out = tmp_path / "rounds.json"
    n = export_json(s, out)
    assert n == 2
    data = json.loads(out.read_text(encoding="utf-8"))
    assert isinstance(data[0]["investors"], list)
    assert set(data[0]["investors"]) == {"Sixth Sense", "Accel"}
    assert isinstance(data[0]["sources"], list)
    assert len(data[0]["sources"]) == 2


def test_min_confidence_filter(tmp_path) -> None:
    s = _seed(tmp_path)
    out = tmp_path / "hi.csv"
    n = export_csv(s, out, min_confidence=0.8)
    assert n == 1  # only Hocco (0.95) passes; TraqCheck (0.40) filtered


def test_export_is_idempotent(tmp_path) -> None:
    s = _seed(tmp_path)
    out = tmp_path / "x.csv"
    assert export_csv(s, out) == 2
    first = out.read_text(encoding="utf-8")
    assert export_csv(s, out) == 2
    second = out.read_text(encoding="utf-8")
    assert first == second


def test_xlsx_writes_workbook(tmp_path) -> None:
    from openpyxl import load_workbook

    s = _seed(tmp_path)
    out = tmp_path / "rounds.xlsx"
    n = export_xlsx(s, out)
    assert n == 2
    wb = load_workbook(out)
    ws = wb["rounds"]
    header = [c.value for c in ws[1]]
    assert header == EXPORT_COLUMNS
    # 2 data rows + 1 header = 3 physical rows
    assert ws.max_row == 3
    # Confidence column holds floats, not strings
    conf_col = EXPORT_COLUMNS.index("confidence") + 1
    assert isinstance(ws.cell(row=2, column=conf_col).value, float)
