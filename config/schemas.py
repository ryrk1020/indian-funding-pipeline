"""Pydantic v2 models used across the pipeline.

Two layers:
- `ArticleRaw` is what scrapers produce (source + URL + raw HTML/text).
- `FundingRound` is what the enricher produces after LLM extraction + validation.

Every external boundary (scraper output, LLM output, exporter input) goes through
one of these. This is the place that decides what "clean" means.
"""
from __future__ import annotations

import hashlib
import re
from datetime import UTC, date, datetime
from enum import StrEnum
from typing import Annotated

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    StringConstraints,
    field_validator,
    model_validator,
)


def _utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)

NonEmptyStr = Annotated[str, StringConstraints(min_length=1, strip_whitespace=True)]


class FundingStage(StrEnum):
    PRE_SEED = "pre_seed"
    SEED = "seed"
    PRE_SERIES_A = "pre_series_a"
    SERIES_A = "series_a"
    SERIES_B = "series_b"
    SERIES_C = "series_c"
    SERIES_D = "series_d"
    SERIES_E_PLUS = "series_e_plus"
    BRIDGE = "bridge"
    DEBT = "debt"
    GRANT = "grant"
    IPO = "ipo"
    ACQUISITION = "acquisition"
    UNDISCLOSED = "undisclosed"


class Currency(StrEnum):
    USD = "USD"
    INR = "INR"
    EUR = "EUR"
    GBP = "GBP"
    OTHER = "OTHER"


class Investor(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: NonEmptyStr
    lead: bool = False

    @field_validator("name")
    @classmethod
    def _strip(cls, v: str) -> str:
        return re.sub(r"\s+", " ", v).strip()


class Company(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: NonEmptyStr
    sector: str | None = None
    city: str | None = None
    country: str = "India"
    website: HttpUrl | None = None

    @field_validator("name")
    @classmethod
    def _strip(cls, v: str) -> str:
        return re.sub(r"\s+", " ", v).strip()


class ArticleRaw(BaseModel):
    """What a scraper produces. No interpretation, just raw capture."""

    model_config = ConfigDict(extra="forbid")

    source: NonEmptyStr
    url: HttpUrl
    title: NonEmptyStr
    published_at: datetime | None = None
    author: str | None = None
    html: str = ""
    text: NonEmptyStr
    fetched_at: datetime = Field(default_factory=_utcnow)

    @property
    def article_id(self) -> str:
        return hashlib.sha256(str(self.url).encode("utf-8")).hexdigest()[:16]


class EnrichmentResult(BaseModel):
    """Direct output of the LLM call. May be partial — validator upgrades to FundingRound."""

    model_config = ConfigDict(extra="forbid")

    company_name: str | None = None
    sector: str | None = None
    stage: FundingStage | None = None
    amount: float | None = None
    currency: Currency | None = None
    amount_usd: float | None = None
    announced_on: date | None = None
    investors: list[Investor] = Field(default_factory=list)
    location: str | None = None
    summary: str | None = None
    confidence: float = Field(ge=0.0, le=1.0, default=0.0)
    extraction_method: str = "llm"  # "llm" | "regex_fallback" | "manual"

    @field_validator("amount", "amount_usd")
    @classmethod
    def _non_negative(cls, v: float | None) -> float | None:
        if v is not None and v < 0:
            raise ValueError("amount must be non-negative")
        return v

    @field_validator("amount_usd")
    @classmethod
    def _plausible_usd(cls, v: float | None) -> float | None:
        if v is not None and v > 50_000_000_000:
            raise ValueError("amount_usd implausibly large (>$50B)")
        return v

    @field_validator("announced_on")
    @classmethod
    def _not_future(cls, v: date | None) -> date | None:
        if v is not None and v > _utcnow().date():
            raise ValueError(f"announced_on {v} is in the future")
        return v

    @field_validator("company_name")
    @classmethod
    def _clean_company(cls, v: str | None) -> str | None:
        if v is None:
            return None
        v = re.sub(r"\s+", " ", v).strip().strip(".,;:")
        return v or None

    @model_validator(mode="after")
    def _dedupe_investors(self) -> EnrichmentResult:
        seen: dict[str, Investor] = {}
        for inv in self.investors:
            key = inv.name.lower()
            if key not in seen:
                seen[key] = inv
            elif inv.lead and not seen[key].lead:
                seen[key] = inv  # upgrade to lead
        self.investors = list(seen.values())
        return self


class FundingRound(BaseModel):
    """The clean, deduplicated, export-ready record. One row per funding event."""

    model_config = ConfigDict(extra="forbid")

    round_id: NonEmptyStr  # deterministic hash of (company, amount, date)
    company: Company
    stage: FundingStage = FundingStage.UNDISCLOSED
    amount: float | None = None
    currency: Currency | None = None
    amount_usd: float | None = None
    announced_on: date | None = None
    investors: list[Investor] = Field(default_factory=list)
    sources: list[HttpUrl] = Field(default_factory=list, min_length=1)
    summary: str | None = None
    confidence: float = Field(ge=0.0, le=1.0, default=0.0)
    extraction_method: str = "llm"
    created_at: datetime = Field(default_factory=_utcnow)
    updated_at: datetime = Field(default_factory=_utcnow)

    @model_validator(mode="after")
    def _stamp_update(self) -> FundingRound:
        self.updated_at = _utcnow()
        return self

    @field_validator("announced_on")
    @classmethod
    def _not_future(cls, v: date | None) -> date | None:
        if v is not None and v > _utcnow().date():
            raise ValueError(f"announced_on {v} is in the future")
        return v

    @field_validator("amount_usd")
    @classmethod
    def _plausible_usd(cls, v: float | None) -> float | None:
        if v is not None and v > 50_000_000_000:
            raise ValueError("amount_usd implausibly large (>$50B)")
        return v

    @model_validator(mode="after")
    def _dedupe_investors_round(self) -> FundingRound:
        seen: dict[str, Investor] = {}
        for inv in self.investors:
            key = inv.name.lower()
            if key not in seen or (inv.lead and not seen[key].lead):
                seen[key] = inv
        self.investors = list(seen.values())
        return self

    @staticmethod
    def compute_round_id(
        company_name: str,
        announced_on: date | None,
        amount_usd: float | None,
    ) -> str:
        key = "|".join(
            [
                company_name.lower().strip(),
                announced_on.isoformat() if announced_on else "",
                f"{amount_usd:.2f}" if amount_usd is not None else "",
            ]
        )
        return hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]


class RunRecord(BaseModel):
    """One row in run_log per pipeline invocation per source."""

    model_config = ConfigDict(extra="forbid")

    run_id: NonEmptyStr
    source: NonEmptyStr
    started_at: datetime
    finished_at: datetime | None = None
    articles_seen: int = 0
    articles_new: int = 0
    articles_failed: int = 0
    enriched_ok: int = 0
    enriched_failed: int = 0
    schema_drift_flag: bool = False
    error: str | None = None
