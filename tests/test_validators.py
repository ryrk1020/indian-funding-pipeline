"""Hardened Pydantic validators on EnrichmentResult and FundingRound."""
from __future__ import annotations

from datetime import date, timedelta

import pytest
from pydantic import ValidationError

from config.schemas import (
    Company,
    EnrichmentResult,
    FundingRound,
    FundingStage,
    Investor,
)


def test_enrichment_rejects_future_date() -> None:
    with pytest.raises(ValidationError):
        EnrichmentResult(announced_on=date.today() + timedelta(days=7), confidence=0.5)


def test_enrichment_rejects_absurd_amount() -> None:
    with pytest.raises(ValidationError):
        EnrichmentResult(amount_usd=100_000_000_000, confidence=0.5)


def test_enrichment_strips_company_name_punct() -> None:
    r = EnrichmentResult(company_name="  TraqCheck. ", confidence=0.5)
    assert r.company_name == "TraqCheck"


def test_enrichment_empty_company_becomes_none() -> None:
    # strip-then-check-empty: a pure-punct name becomes None
    r = EnrichmentResult(company_name=" . ", confidence=0.5)
    assert r.company_name is None


def test_enrichment_dedupes_investors_case_insensitive() -> None:
    r = EnrichmentResult(
        investors=[
            Investor(name="Accel", lead=False),
            Investor(name="accel", lead=True),  # should upgrade to lead
            Investor(name="IvyCap Ventures"),
        ],
        confidence=0.8,
    )
    assert len(r.investors) == 2
    accel = next(i for i in r.investors if i.name.lower() == "accel")
    assert accel.lead is True


def test_funding_round_rejects_future_date() -> None:
    with pytest.raises(ValidationError):
        FundingRound(
            round_id="x" * 16,
            company=Company(name="X"),
            announced_on=date.today() + timedelta(days=1),
            sources=["https://x/y"],
        )


def test_funding_round_dedupes_investors() -> None:
    fr = FundingRound(
        round_id="x" * 16,
        company=Company(name="X"),
        stage=FundingStage.SEED,
        investors=[
            Investor(name="Sequoia"),
            Investor(name="sequoia", lead=True),
        ],
        sources=["https://x/y"],
    )
    assert len(fr.investors) == 1
    assert fr.investors[0].lead is True
