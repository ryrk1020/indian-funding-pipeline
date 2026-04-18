"""Turn EnrichmentResult + ArticleRaw → FundingRound and persist it.

Two transforms:
  1. Normalize currency & compute amount_usd if the LLM didn't
  2. Compute deterministic round_id from (company, date, amount_usd)

Day 4 adds cross-source dedup — for now a single ArticleRaw maps to one round,
and the storage upsert folds duplicates across sources via round_id collision.
"""
from __future__ import annotations

from datetime import date

from loguru import logger

from config.schemas import (
    Company,
    Currency,
    EnrichmentResult,
    FundingRound,
    FundingStage,
)

# Rough FX rates. Accurate enough for sorting / analytics at this scale;
# anything serious would pull daily rates from an API.
USD_PER_UNIT: dict[Currency, float] = {
    Currency.USD: 1.0,
    Currency.INR: 1 / 83.0,
    Currency.EUR: 1 / 0.92,
    Currency.GBP: 1 / 0.79,
    Currency.OTHER: 0.0,  # unknown — don't fabricate
}


def ensure_amount_usd(result: EnrichmentResult) -> EnrichmentResult:
    if result.amount_usd is not None:
        return result
    if result.amount is None or result.currency is None:
        return result
    rate = USD_PER_UNIT.get(result.currency)
    if not rate:
        return result
    result.amount_usd = round(result.amount * rate, 2)
    return result


def build_funding_round(
    result: EnrichmentResult,
    *,
    article_url: str,
    fallback_company: str | None = None,
    fallback_announced: date | None = None,
) -> FundingRound | None:
    """Return a FundingRound, or None if there isn't enough signal."""
    result = ensure_amount_usd(result)

    company_name = result.company_name or fallback_company
    if not company_name:
        logger.debug("build_funding_round: no company — skipping {}", article_url)
        return None

    announced = result.announced_on or fallback_announced
    round_id = FundingRound.compute_round_id(
        company_name=company_name,
        announced_on=announced,
        amount_usd=result.amount_usd,
    )

    return FundingRound(
        round_id=round_id,
        company=Company(name=company_name),
        stage=result.stage or FundingStage.UNDISCLOSED,
        amount=result.amount,
        currency=result.currency,
        amount_usd=result.amount_usd,
        announced_on=announced,
        investors=result.investors,
        sources=[article_url],  # upsert will merge additional sources
        summary=result.summary,
        confidence=result.confidence,
        extraction_method=result.extraction_method,
    )
