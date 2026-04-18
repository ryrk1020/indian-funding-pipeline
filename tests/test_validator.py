"""Validator: EnrichmentResult → FundingRound."""
from __future__ import annotations

from datetime import date

from config.schemas import Currency, EnrichmentResult, FundingStage, Investor
from pipeline.validator import build_funding_round, ensure_amount_usd


def test_ensure_amount_usd_inr() -> None:
    r = EnrichmentResult(
        company_name="X",
        amount=83_000_000,  # 83 million INR = 1 million USD at 1/83
        currency=Currency.INR,
        confidence=0.9,
    )
    r = ensure_amount_usd(r)
    assert r.amount_usd == 1_000_000.0


def test_ensure_amount_usd_preserves_existing() -> None:
    r = EnrichmentResult(amount=100, currency=Currency.INR, amount_usd=999, confidence=0.5)
    r = ensure_amount_usd(r)
    assert r.amount_usd == 999  # not overwritten


def test_build_round_skips_when_no_company() -> None:
    r = EnrichmentResult(company_name=None, confidence=0.1)
    assert build_funding_round(r, article_url="https://x/y") is None


def test_build_round_basic() -> None:
    r = EnrichmentResult(
        company_name="TraqCheck",
        stage=FundingStage.SERIES_A,
        amount=8_000_000,
        currency=Currency.USD,
        amount_usd=8_000_000,
        announced_on=date(2026, 4, 14),
        investors=[Investor(name="IvyCap Ventures", lead=True)],
        summary="TraqCheck raised $8M Series A led by IvyCap.",
        confidence=0.93,
        extraction_method="llm",
    )
    fr = build_funding_round(r, article_url="https://inc42.com/x/y")
    assert fr is not None
    assert fr.company.name == "TraqCheck"
    assert fr.stage is FundingStage.SERIES_A
    assert fr.amount_usd == 8_000_000
    assert fr.announced_on == date(2026, 4, 14)
    assert fr.investors[0].lead is True
    assert str(fr.sources[0]) == "https://inc42.com/x/y"
    # deterministic round_id
    assert len(fr.round_id) == 16
    # Same inputs → same round_id (idempotent dedup key)
    fr2 = build_funding_round(r, article_url="https://entrackr.com/other/url")
    assert fr2 is not None and fr2.round_id == fr.round_id


def test_round_id_changes_with_amount() -> None:
    r1 = EnrichmentResult(company_name="X", announced_on=date(2026, 4, 1), amount_usd=1_000_000, confidence=0.8)
    r2 = EnrichmentResult(company_name="X", announced_on=date(2026, 4, 1), amount_usd=2_000_000, confidence=0.8)
    fr1 = build_funding_round(r1, article_url="https://a/")
    fr2 = build_funding_round(r2, article_url="https://a/")
    assert fr1.round_id != fr2.round_id  # type: ignore[union-attr]
