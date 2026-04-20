"""Entity resolution: canonical company + investor names."""
from __future__ import annotations

from datetime import date, datetime

from config.schemas import ArticleRaw, Company, FundingRound, Investor
from pipeline.entity_resolution import (
    normalize_investor_name,
    resolve_company,
    resolve_investor,
    seed_aliases,
)
from pipeline.storage import Storage


def _seed_article(s: Storage, url: str, source: str = "inc42") -> None:
    s.upsert_article(ArticleRaw(
        source=source, url=url, title="t", text="x",
        fetched_at=datetime(2026, 4, 17),
    ))


def _mk_round(name: str, rid: str, invs: list[Investor] | None = None) -> FundingRound:
    return FundingRound(
        round_id=rid,
        company=Company(name=name),
        amount_usd=10_000_000,
        announced_on=date(2026, 4, 14),
        investors=invs or [],
        sources=["https://inc42.com/x"],
        confidence=0.9,
    )


def test_normalize_investor_lowercases_and_strips_suffixes() -> None:
    assert normalize_investor_name("Accel Partners Pvt Ltd") == "accel partners"
    assert normalize_investor_name("IvyCap Ventures LLP") == "ivycap ventures"
    assert normalize_investor_name("  Tiger Global  ") == "tiger global"


def test_resolve_company_registers_new_entity(tmp_path) -> None:
    s = Storage(db_path=tmp_path / "t.db")
    with s.connect() as c:
        assert resolve_company(c, "Hocco") == "Hocco"
        # Alias row exists after resolution
        row = c.execute(
            "SELECT canonical FROM company_aliases WHERE alias_norm = 'hocco'"
        ).fetchone()
        assert row["canonical"] == "Hocco"


def test_resolve_company_fuzzy_matches_suffix_variant(tmp_path) -> None:
    s = Storage(db_path=tmp_path / "t.db")
    _seed_article(s, "https://inc42.com/h")
    s.upsert_round(_mk_round("Hocco", "aaaaaaaaaaaaaaaa"), source_urls=["https://inc42.com/h"], dedup=False)
    with s.connect() as c:
        # Same company, corporate suffix. Should resolve to "Hocco".
        assert resolve_company(c, "Hocco Ice Cream Pvt Ltd") == "Hocco"


def test_resolve_company_distinct_names_not_merged(tmp_path) -> None:
    s = Storage(db_path=tmp_path / "t.db")
    _seed_article(s, "https://inc42.com/h")
    s.upsert_round(_mk_round("Hocco", "aaaaaaaaaaaaaaaa"), source_urls=["https://inc42.com/h"], dedup=False)
    with s.connect() as c:
        assert resolve_company(c, "Zomato") == "Zomato"


def test_resolve_investor_via_seed_alias(tmp_path) -> None:
    s = Storage(db_path=tmp_path / "t.db")
    with s.connect() as c:
        # Seed data includes "sequoia capital india" → "Peak XV Partners"
        assert resolve_investor(c, "Sequoia Capital India") == "Peak XV Partners"
        assert resolve_investor(c, "Accel Partners") == "Accel"


def test_resolve_investor_fuzzy_matches_existing_canonical(tmp_path) -> None:
    s = Storage(db_path=tmp_path / "t.db")
    _seed_article(s, "https://inc42.com/x")
    s.upsert_round(
        _mk_round("SomeCo", "bbbbbbbbbbbbbbbb", invs=[Investor(name="IvyCap Ventures")]),
        source_urls=["https://inc42.com/x"],
        dedup=False,
    )
    with s.connect() as c:
        # Slight variation — should fuzzy-match the existing canonical.
        assert resolve_investor(c, "IvyCap Ventures LLP") == "IvyCap Ventures"


def test_seed_aliases_idempotent(tmp_path) -> None:
    s = Storage(db_path=tmp_path / "t.db")
    with s.connect() as c:
        inserted1 = seed_aliases(c)
        inserted2 = seed_aliases(c)
        # Second call inserts nothing — everything's already there.
        assert inserted2 == 0
        # And sanity: first call also inserted 0 because Storage init already seeded.
        assert inserted1 == 0


def test_upsert_round_canonicalizes_on_insert(tmp_path) -> None:
    """Full integration: dirty input → clean canonical names in DB."""
    s = Storage(db_path=tmp_path / "t.db")
    _seed_article(s, "https://inc42.com/h")
    r = _mk_round(
        "Hocco Ice Cream Pvt Ltd",
        "cccccccccccccccc",
        invs=[Investor(name="Sequoia Capital India", lead=True), Investor(name="Accel Partners")],
    )
    # First run: seeds Hocco canonical via the raw name.
    s.upsert_round(_mk_round("Hocco", "dddddddddddddddd"), source_urls=["https://inc42.com/h"], dedup=False)
    s.upsert_round(r, source_urls=["https://inc42.com/h"], dedup=True)
    with s.connect() as c:
        rows = c.execute(
            "SELECT company_name FROM funding_rounds"
        ).fetchall()
        names = {row["company_name"] for row in rows}
        # Both records now carry "Hocco" — the canonical form.
        assert names == {"Hocco"}
        invs = {row["name"] for row in c.execute("SELECT name FROM round_investors").fetchall()}
        assert "Peak XV Partners" in invs
        assert "Accel" in invs
