"""Tests for the deterministic regex fallback extractor."""
from __future__ import annotations

from datetime import date

from config.schemas import Currency, FundingStage
from pipeline.regex_fallback import (
    extract_amount,
    extract_company_name,
    extract_investors,
    extract_stage,
    regex_extract,
)


class TestAmount:
    def test_usd_million(self) -> None:
        native, ccy, usd = extract_amount("raised $8 Mn in Series A")
        assert native == 8_000_000
        assert ccy is Currency.USD
        assert usd == 8_000_000

    def test_usd_million_word(self) -> None:
        native, ccy, _ = extract_amount("Company raises $2.5 million seed")
        assert native == 2_500_000 and ccy is Currency.USD

    def test_usd_billion(self) -> None:
        native, ccy, _ = extract_amount("valued at $1.2 Bn after raising $100 Mn")
        # The first match wins — $1.2 Bn
        assert native == 1_200_000_000 and ccy is Currency.USD

    def test_inr_crore(self) -> None:
        native, ccy, usd = extract_amount("raised Rs 150 Cr in Series B funding")
        assert native == 1_500_000_000  # 150 crore = 1.5 billion rupees
        assert ccy is Currency.INR
        # ~ 1.5e9 / 83 ≈ 18.07 Mn USD
        assert usd is not None and 17_000_000 < usd < 19_000_000

    def test_inr_lakh(self) -> None:
        native, ccy, _ = extract_amount("grant of Rs 25 lakh from MeitY")
        assert native == 2_500_000 and ccy is Currency.INR

    def test_inr_with_rupee_symbol(self) -> None:
        native, ccy, _ = extract_amount("raised ₹22 Cr to develop autonomous vehicles")
        assert native == 220_000_000 and ccy is Currency.INR

    def test_no_amount(self) -> None:
        assert extract_amount("TraqCheck hires new CTO") == (None, None, None)


class TestStage:
    def test_pre_seed(self) -> None:
        assert extract_stage("leads pre-seed round in FIFTH SENSE") is FundingStage.PRE_SEED

    def test_pre_series_a(self) -> None:
        assert extract_stage("pre-Series A led by Big Global JSC") is FundingStage.PRE_SERIES_A

    def test_series_b(self) -> None:
        assert extract_stage("Series B round led by PROMAFT") is FundingStage.SERIES_B

    def test_ipo(self) -> None:
        assert extract_stage("Company files for IPO with SEBI") is FundingStage.IPO

    def test_acquisition(self) -> None:
        assert extract_stage("Razorpay acquires BillMe") is FundingStage.ACQUISITION

    def test_unknown(self) -> None:
        assert extract_stage("Company hires new CTO") is FundingStage.UNDISCLOSED


class TestCompanyName:
    def test_title_with_raises(self) -> None:
        assert extract_company_name("TraqCheck Raises $8 Mn") == "TraqCheck"

    def test_multiword(self) -> None:
        assert extract_company_name(
            "The Hosteller Raises Rs 150 Cr in Series B round led by PROMAFT"
        ) == "The Hosteller"

    def test_bags(self) -> None:
        assert extract_company_name("Palmonas Bags $40 Mn From Xponentia Capital") == "Palmonas"

    def test_no_verb_fallback(self) -> None:
        # Feature articles — no raise verb. Fallback returns leading caps.
        out = extract_company_name("Jio Financial Services Q4 PAT Dips 14%")
        assert out and out.startswith("Jio Financial Services")


class TestInvestors:
    def test_led_by(self) -> None:
        invs = extract_investors("raised $8 Mn in Series A led by IvyCap Ventures")
        assert len(invs) == 1 and invs[0].name == "IvyCap Ventures" and invs[0].lead

    def test_led_by_and_participation(self) -> None:
        invs = extract_investors(
            "Series B led by PROMAFT Partners with participation from V3 Ventures"
        )
        names = {i.name for i in invs}
        assert "PROMAFT Partners" in names
        assert "V3 Ventures" in names
        leads = {i.name for i in invs if i.lead}
        assert leads == {"PROMAFT Partners"}


class TestEndToEnd:
    def test_full_extraction_high_confidence(self) -> None:
        title = "TraqCheck Raises $8 Mn To Build AI Agents For Recruitment"
        text = (
            "Enterprise tech startup TraqCheck has raised $8 Mn in its Series A "
            "funding round led by IvyCap Ventures, with participation from "
            "existing investors."
        )
        r = regex_extract(title, text, published_at=date(2026, 4, 14))
        assert r.company_name == "TraqCheck"
        assert r.stage is FundingStage.SERIES_A
        assert r.amount == 8_000_000 and r.currency is Currency.USD
        assert r.announced_on == date(2026, 4, 14)
        assert any(i.name == "IvyCap Ventures" and i.lead for i in r.investors)
        assert r.confidence >= 0.8
        assert r.extraction_method == "regex_fallback"

    def test_low_confidence_when_no_amount_or_stage(self) -> None:
        r = regex_extract("TraqCheck Hires New CTO", "TraqCheck announced...")
        assert r.confidence < 0.5
