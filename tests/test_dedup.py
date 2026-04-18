"""Cross-source dedup: normalize + fuzzy match + amount/date windows."""
from __future__ import annotations

from datetime import date, datetime

from config.schemas import ArticleRaw, Company, FundingRound
from pipeline.dedup import (
    _amounts_close,
    _dates_close,
    find_existing_round_id,
    normalize_company_name,
)
from pipeline.storage import Storage


def _seed_article(s: Storage, url: str, source: str = "inc42") -> None:
    s.upsert_article(ArticleRaw(
        source=source,
        url=url,
        title="x",
        text="x",
        fetched_at=datetime(2026, 4, 17),
    ))


def test_normalize_strips_suffixes() -> None:
    assert normalize_company_name("Bluestone Jewellery Pvt Ltd") == "bluestone jewellery"
    assert normalize_company_name("TraqCheck Technologies") == "traqcheck"
    assert normalize_company_name("Acme, Inc.") == "acme"
    assert normalize_company_name("BlueStone") == normalize_company_name("Bluestone")


def test_normalize_collapses_spaces() -> None:
    assert normalize_company_name("  The   Hosteller  ") == "the hosteller"


def test_amounts_close_within_15pct() -> None:
    assert _amounts_close(100.0, 110.0) is True   # 9.1% of 110
    assert _amounts_close(100.0, 130.0) is False  # 23% of 130
    assert _amounts_close(None, 100.0) is True    # missing side → permissive


def test_dates_close_within_3d() -> None:
    assert _dates_close(date(2026, 4, 14), date(2026, 4, 17)) is True
    assert _dates_close(date(2026, 4, 14), date(2026, 4, 18)) is False
    assert _dates_close(None, date(2026, 4, 14)) is True


def _mk_round(name: str, amount_usd: float, d: date, rid: str) -> FundingRound:
    return FundingRound(
        round_id=rid,
        company=Company(name=name),
        amount_usd=amount_usd,
        announced_on=d,
        sources=["https://inc42.com/x"],
        confidence=0.9,
    )


def test_find_existing_matches_fuzzy(tmp_path) -> None:
    s = Storage(db_path=tmp_path / "t.db")
    _seed_article(s, "https://inc42.com/x")
    r1 = _mk_round("Bluestone", 29_000_000, date(2026, 4, 14), "aaaaaaaaaaaaaaaa")
    s.upsert_round(r1, source_urls=["https://inc42.com/x"], dedup=False)
    # Re-report same round with corporate suffix + amount off by 5% + date off by 1 day
    r2 = _mk_round("Bluestone Jewellery Pvt Ltd", 28_000_000, date(2026, 4, 15), "bbbbbbbbbbbbbbbb")
    with s.connect() as c:
        existing = find_existing_round_id(c, r2)
    assert existing == "aaaaaaaaaaaaaaaa"


def test_find_existing_no_match_when_amount_differs(tmp_path) -> None:
    s = Storage(db_path=tmp_path / "t.db")
    _seed_article(s, "https://inc42.com/h")
    r1 = _mk_round("Hocco", 12_000_000, date(2026, 4, 17), "cccccccccccccccc")
    s.upsert_round(r1, source_urls=["https://inc42.com/h"], dedup=False)
    # Same company, very different amount → different round
    r2 = _mk_round("Hocco", 2_000_000, date(2026, 4, 17), "dddddddddddddddd")
    with s.connect() as c:
        existing = find_existing_round_id(c, r2)
    assert existing is None


def test_upsert_with_dedup_merges_into_existing(tmp_path) -> None:
    s = Storage(db_path=tmp_path / "t.db")
    _seed_article(s, "https://inc42.com/h")
    _seed_article(s, "https://entrackr.com/h", source="entrackr")
    r1 = _mk_round("The Hosteller", 18_000_000, date(2026, 4, 16), "eeeeeeeeeeeeeeee")
    s.upsert_round(r1, source_urls=["https://inc42.com/h"], dedup=False)
    r2 = _mk_round("Hosteller", 18_200_000, date(2026, 4, 16), "ffffffffffffffff")
    effective = s.upsert_round(r2, source_urls=["https://entrackr.com/h"], dedup=True)
    assert effective == "eeeeeeeeeeeeeeee"
    # Both source URLs should point at the one round
    with s.connect() as c:
        urls = [r["article_url"] for r in c.execute(
            "SELECT article_url FROM round_sources WHERE round_id = ?",
            ("eeeeeeeeeeeeeeee",),
        ).fetchall()]
    assert set(urls) == {"https://inc42.com/h", "https://entrackr.com/h"}
