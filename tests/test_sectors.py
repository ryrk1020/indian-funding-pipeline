"""Sector taxonomy + normalization."""
from __future__ import annotations

from pipeline.sectors import (
    SECTOR_ALIASES,
    SECTOR_LABELS,
    normalize_sector,
    sector_choices_for_prompt,
    sector_label,
)


def test_normalize_exact_alias() -> None:
    assert normalize_sector("fintech") == "fintech"
    assert normalize_sector("Financial Services") == "fintech"
    assert normalize_sector("SaaS") == "saas"
    assert normalize_sector("healthcare") == "healthtech"
    assert normalize_sector("EV") == "mobility"


def test_normalize_handles_whitespace_and_case() -> None:
    assert normalize_sector("  FinTech  ") == "fintech"
    assert normalize_sector("ARTIFICIAL INTELLIGENCE") == "ai_ml"


def test_normalize_strips_decoration() -> None:
    assert normalize_sector("fintech platform") == "fintech"
    assert normalize_sector("edtech startup") == "edtech"
    assert normalize_sector("logistics industry") == "logistics"


def test_normalize_fuzzy_match() -> None:
    # Typos / near-matches still resolve
    assert normalize_sector("fin-tech") == "fintech"
    assert normalize_sector("health tech") == "healthtech"


def test_normalize_unknown_becomes_other() -> None:
    assert normalize_sector("quantum widgetry of the 4th dimension") == "other"


def test_normalize_empty_returns_none() -> None:
    assert normalize_sector(None) is None
    assert normalize_sector("") is None
    assert normalize_sector("   ") is None


def test_all_aliases_resolve_to_known_slug() -> None:
    for alias, slug in SECTOR_ALIASES.items():
        assert slug in SECTOR_LABELS, f"alias '{alias}' → unknown slug '{slug}'"


def test_label_for_slug() -> None:
    assert sector_label("fintech") == "FinTech"
    assert sector_label("ai_ml") == "AI/ML"
    assert sector_label(None) == ""
    assert sector_label("") == ""


def test_prompt_list_includes_core_sectors() -> None:
    choices = sector_choices_for_prompt()
    for key in ("fintech", "saas", "healthtech", "edtech", "other"):
        assert key in choices
