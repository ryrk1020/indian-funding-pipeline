"""LLM-based enrichment of ArticleRaw → EnrichmentResult via OpenRouter.

Uses the OpenAI SDK pointed at OpenRouter's OpenAI-compatible endpoint. Free
models vary in JSON-mode support, so we:
  - Ask for strict JSON with a strong schema-in-prompt
  - `response_format={"type": "json_object"}` when the model honors it
  - Parse defensively; extraction failure → regex fallback upstream

The enricher does NOT call the DB — it just turns (title, text) into an
EnrichmentResult. Persistence is the validator's job.
"""
from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass

from loguru import logger
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential,
)

from config.schemas import EnrichmentResult
from config.settings import settings


class EnrichmentError(RuntimeError):
    pass


SYSTEM_PROMPT = """You extract structured Indian startup funding data from news articles.

Return strict JSON matching this schema (no prose, no markdown fences):
{
  "company_name": "string | null",
  "sector": "string | null",
  "stage": "pre_seed | seed | pre_series_a | series_a | series_b | series_c | series_d | series_e_plus | bridge | debt | grant | ipo | acquisition | undisclosed",
  "amount": "number | null",
  "currency": "USD | INR | EUR | GBP | OTHER | null",
  "amount_usd": "number | null",
  "announced_on": "YYYY-MM-DD | null",
  "investors": [{"name": "string", "lead": true}],
  "location": "string | null",
  "summary": "1-2 sentence factual summary",
  "confidence": "0.0 to 1.0"
}

Rules:
- If the article is NOT about a specific funding round, set company_name to null, stage to "undisclosed", and confidence to 0.1 or less.
- Use the stated currency for "amount" and "currency". Convert to USD in "amount_usd" using: 1 USD = 83 INR, 1 USD = 1.08 EUR, 1 USD = 0.79 GBP.
- "lead" is true only if the text explicitly says "led by" or "lead investor".
- "investors" only lists named entities (VCs, angels, corporates). Skip vague phrases like "existing investors" unless specifically named.
- "confidence" reflects how clearly the article conveys the funding details — 0.9+ only when company, amount, stage, and investors are all unambiguous.
- Return ONLY the JSON object. No explanation, no markdown.
"""

USER_TEMPLATE = """Article title: {title}

Article text:
{text}

Return the JSON object now.
"""


@dataclass
class EnricherConfig:
    api_key: str
    model: str
    referer: str
    app_name: str
    request_timeout: float = 45.0
    max_text_chars: int = 8000  # truncate very long articles


def _strip_code_fence(s: str) -> str:
    s = s.strip()
    if s.startswith("```"):
        s = re.sub(r"^```(?:json)?\s*", "", s)
        s = re.sub(r"\s*```$", "", s)
    return s.strip()


def _coerce_json(raw: str) -> dict:
    raw = _strip_code_fence(raw)
    # Some models add stray prose before/after; grab outermost {...}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if not m:
            raise
        return json.loads(m.group(0))


def _transient_openai_exc() -> tuple[type[Exception], ...]:
    """Transient errors worth retrying on the *same* model (network / 5xx)."""
    try:
        from openai import APIConnectionError, APITimeoutError, InternalServerError
        return (APITimeoutError, APIConnectionError, InternalServerError)
    except ImportError:
        return ()


def _exhausted_openai_exc() -> tuple[type[Exception], ...]:
    """Errors that mean this model is done — switch to the next one."""
    try:
        from openai import NotFoundError, RateLimitError
        return (RateLimitError, NotFoundError)
    except ImportError:
        return ()


class OpenRouterEnricher:
    def __init__(self, config: EnricherConfig | None = None) -> None:
        if config is None:
            config = EnricherConfig(
                api_key=settings.openrouter_api_key,
                model=settings.openrouter_model,
                referer=settings.openrouter_referer,
                app_name=settings.openrouter_app_name,
            )
        self.cfg = config
        self._models: list[str] = settings.model_chain
        # Advances permanently when a model is exhausted so later articles skip it.
        self._model_idx: int = 0

        from openai import AsyncOpenAI

        self._client = AsyncOpenAI(
            api_key=config.api_key,
            base_url="https://openrouter.ai/api/v1",
            default_headers={
                "HTTP-Referer": config.referer,
                "X-Title": config.app_name,
            },
            timeout=config.request_timeout,
        )

    @property
    def active_model(self) -> str:
        return self._models[self._model_idx]

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_random_exponential(multiplier=1, max=20),
        retry=retry_if_exception_type((TimeoutError, asyncio.TimeoutError, *_transient_openai_exc())),
        reraise=True,
    )
    async def _chat(self, model: str, title: str, text: str) -> str:
        user = USER_TEMPLATE.format(title=title, text=text[: self.cfg.max_text_chars])
        resp = await self._client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user},
            ],
            response_format={"type": "json_object"},
            temperature=0.1,
            max_tokens=800,
        )
        return resp.choices[0].message.content or ""

    async def enrich(self, title: str, text: str) -> EnrichmentResult:
        if not self.cfg.api_key:
            raise EnrichmentError("OPENROUTER_API_KEY not set")

        exhausted = _exhausted_openai_exc()
        last_exc: Exception = EnrichmentError("no models configured")

        while self._model_idx < len(self._models):
            model = self.active_model
            try:
                raw = await self._chat(model, title, text)
            except exhausted as e:
                logger.warning("model {} exhausted ({}), switching to next", model, type(e).__name__)
                self._model_idx += 1
                last_exc = e
                continue
            except Exception as e:
                raise EnrichmentError(f"chat_failed [{model}]: {e}") from e

            try:
                payload = _coerce_json(raw)
            except (json.JSONDecodeError, ValueError) as e:
                logger.warning("enrich: JSON parse failed on {}: {}", model, e)
                raise EnrichmentError(f"json_parse_failed: {e}") from e

            payload = _sanitize_llm_payload(payload)
            try:
                return EnrichmentResult(**payload, extraction_method="llm")
            except Exception as e:
                logger.warning("enrich: pydantic coerce failed: {}", e)
                raise EnrichmentError(f"schema_mismatch: {e}") from e

        raise EnrichmentError(
            f"all {len(self._models)} model(s) exhausted — last error: {last_exc}"
        ) from last_exc


def _sanitize_llm_payload(p: dict) -> dict:
    """Cheap schema-friendliness fixes: blank → None, numeric coercion, etc."""
    out: dict = {}
    for k, v in p.items():
        if v == "" or v == "null":
            out[k] = None
        else:
            out[k] = v
    # numeric coercion
    for k in ("amount", "amount_usd", "confidence"):
        v = out.get(k)
        if isinstance(v, str):
            cleaned = re.sub(r"[^\d.\-]", "", v)
            try:
                out[k] = float(cleaned) if cleaned else None
            except ValueError:
                out[k] = None
    # investors shape: list of dicts with name + lead
    inv = out.get("investors") or []
    norm: list[dict] = []
    if isinstance(inv, list):
        for x in inv:
            if isinstance(x, str):
                norm.append({"name": x, "lead": False})
            elif isinstance(x, dict) and x.get("name"):
                norm.append({"name": x["name"], "lead": bool(x.get("lead", False))})
    out["investors"] = norm
    # stage / currency: normalize case
    if isinstance(out.get("stage"), str):
        out["stage"] = out["stage"].strip().lower()
    if isinstance(out.get("currency"), str):
        out["currency"] = out["currency"].strip().upper()
    # confidence clamp
    c = out.get("confidence")
    if isinstance(c, (int, float)):
        out["confidence"] = max(0.0, min(1.0, float(c)))
    else:
        out["confidence"] = 0.0
    return out
