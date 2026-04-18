"""Tests for the OpenRouter enricher with a mocked async client."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from config.schemas import Currency, FundingStage
from pipeline.enricher import (
    EnricherConfig,
    EnrichmentError,
    OpenRouterEnricher,
    _coerce_json,
    _sanitize_llm_payload,
    _strip_code_fence,
)


def _make_enricher_with_response(raw: str) -> OpenRouterEnricher:
    e = OpenRouterEnricher.__new__(OpenRouterEnricher)  # skip __init__ (no openai deps)
    e.cfg = EnricherConfig(api_key="test-key", model="m", referer="r", app_name="a")
    e._models = ["m"]
    e._model_idx = 0
    mock_client = MagicMock()
    completion = SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content=raw))]
    )
    mock_client.chat.completions.create = AsyncMock(return_value=completion)
    e._client = mock_client  # type: ignore[attr-defined]
    return e


def test_strip_code_fence_removes_json_block() -> None:
    assert _strip_code_fence("```json\n{\"a\":1}\n```") == '{"a":1}'
    assert _strip_code_fence("```\n{\"a\":1}\n```") == '{"a":1}'
    assert _strip_code_fence('{"a":1}') == '{"a":1}'


def test_coerce_json_handles_prose_wrapped() -> None:
    raw = 'Sure! Here is the data:\n{"company_name":"X"}\nHope that helps.'
    assert _coerce_json(raw) == {"company_name": "X"}


def test_sanitize_empty_string_to_none() -> None:
    out = _sanitize_llm_payload({"company_name": "", "confidence": 0.5})
    assert out["company_name"] is None


def test_sanitize_string_numeric_coerced() -> None:
    out = _sanitize_llm_payload({"amount": "1,500,000", "confidence": "0.8"})
    assert out["amount"] == 1_500_000 and out["confidence"] == 0.8


def test_sanitize_investor_string_items_normalized() -> None:
    out = _sanitize_llm_payload({"investors": ["IvyCap Ventures", {"name": "Accel", "lead": True}], "confidence": 0.9})
    assert out["investors"] == [
        {"name": "IvyCap Ventures", "lead": False},
        {"name": "Accel", "lead": True},
    ]


def test_sanitize_confidence_clamp() -> None:
    assert _sanitize_llm_payload({"confidence": 2.5})["confidence"] == 1.0
    assert _sanitize_llm_payload({"confidence": -0.3})["confidence"] == 0.0


@pytest.mark.asyncio
async def test_enrich_parses_valid_response() -> None:
    raw = (
        '{"company_name":"TraqCheck","sector":"HR tech","stage":"series_a",'
        '"amount":8000000,"currency":"USD","amount_usd":8000000,'
        '"announced_on":"2026-04-14","investors":[{"name":"IvyCap Ventures","lead":true}],'
        '"location":"London","summary":"TraqCheck raised $8M Series A.","confidence":0.93}'
    )
    e = _make_enricher_with_response(raw)
    r = await e.enrich("TraqCheck Raises $8 Mn", "Full article body...")
    assert r.company_name == "TraqCheck"
    assert r.stage is FundingStage.SERIES_A
    assert r.currency is Currency.USD
    assert r.amount_usd == 8_000_000
    assert r.confidence == pytest.approx(0.93)
    assert r.extraction_method == "llm"


@pytest.mark.asyncio
async def test_enrich_raises_on_malformed_json() -> None:
    e = _make_enricher_with_response("not json at all")
    with pytest.raises(EnrichmentError):
        await e.enrich("t", "x")


@pytest.mark.asyncio
async def test_enrich_raises_when_no_api_key() -> None:
    e = OpenRouterEnricher.__new__(OpenRouterEnricher)
    e.cfg = EnricherConfig(api_key="", model="m", referer="r", app_name="a")
    with pytest.raises(EnrichmentError):
        await e.enrich("t", "x")
