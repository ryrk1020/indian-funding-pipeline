"""Static HTML dashboard generator.

Renders `pipeline/templates/dashboard.html.j2` with the current funding_rounds
snapshot embedded as JSON. The output is a single self-contained file that:
  - Works by double-click (no server) — `file:///.../index.html`
  - Ships to GitHub Pages with zero config
  - Uses Apache ECharts 5 (charts), Alpine.js 3 (reactivity), Tailwind (styling)
    — all via CDN, so the file is small and updates independently.

Emits to wherever `--out` points; the daily workflow copies the result to
`docs/index.html` for GitHub Pages.
"""
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape

from pipeline.exporter import _fetch_rows, build_public_rows
from pipeline.storage import Storage

TEMPLATE_DIR = Path(__file__).parent / "templates"
TEMPLATE_NAME = "dashboard.html.j2"


def _dashboard_rows(raw_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Rows tailored for the dashboard JS: keeps raw stage + numeric usd for filters."""
    out: list[dict[str, Any]] = []
    for r in build_public_rows(raw_rows):
        out.append({
            "company": r["company_name"],
            "sector": r["sector"],
            "sector_raw": r["sector_raw"],
            "stage": r["stage"],
            "stage_raw": r["stage_raw"],
            "amount": r["amount"],
            "amount_usd": r["amount_usd"],
            "currency": r["currency"],
            "announced_on": r["announced_on"],
            "lead_investor": r["lead_investor"],
            "investors": [x for x in r["investors"].split(" | ") if x] if r["investors"] else [],
            "sources": [x for x in r["sources"].split(" | ") if x] if r["sources"] else [],
            "confidence": r["confidence"],
            "summary": r["summary"],
            "city": r["city"],
        })
    return out


def _meta(rows: list[dict[str, Any]]) -> dict[str, Any]:
    stages = sorted({r["stage_raw"] for r in rows if r.get("stage_raw")})
    sectors = sorted({r["sector"] for r in rows if r.get("sector")})
    amounts = [r["amount_usd"] for r in rows if r.get("amount_usd")]
    # Precompute investor aggregates so the template doesn't need to loop in JS.
    inv_counts: dict[str, int] = {}
    inv_leads: dict[str, int] = {}
    co_pairs: dict[str, int] = {}
    for r in rows:
        names = r.get("investors") or []
        lead = r.get("lead_investor") or ""
        for n in names:
            inv_counts[n] = inv_counts.get(n, 0) + 1
            if n == lead:
                inv_leads[n] = inv_leads.get(n, 0) + 1
        for i, a in enumerate(names):
            for b in names[i + 1:]:
                key = " || ".join(sorted([a, b]))
                co_pairs[key] = co_pairs.get(key, 0) + 1
    top_investors = [
        {"name": n, "deals": c, "leads": inv_leads.get(n, 0)}
        for n, c in sorted(inv_counts.items(), key=lambda x: -x[1])[:25]
    ]
    co_investments = [
        {"pair": k.split(" || "), "count": v}
        for k, v in sorted(co_pairs.items(), key=lambda x: -x[1])[:25]
        if v >= 2
    ]
    # Companies grouped: one row per company with round list (stage progression).
    by_company: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        by_company.setdefault(r["company"], []).append(r)
    companies = [
        {
            "name": name,
            "rounds": sorted(
                [
                    {
                        "stage": rr["stage"],
                        "stage_raw": rr["stage_raw"],
                        "amount": rr["amount"],
                        "amount_usd": rr["amount_usd"],
                        "announced_on": rr["announced_on"],
                        "lead_investor": rr["lead_investor"],
                        "confidence": rr["confidence"],
                    }
                    for rr in rs
                ],
                key=lambda x: x["announced_on"] or "",
            ),
            "total_usd": sum(
                (rr["amount_usd"] or 0) for rr in rs if rr.get("amount_usd")
            ),
            "sector": rs[0]["sector"] if rs else "",
        }
        for name, rs in sorted(by_company.items(), key=lambda x: -len(x[1]))
    ]
    return {
        "generated_at": datetime.now(UTC).strftime("%b %d, %Y %H:%M UTC"),
        "total_rounds": len(rows),
        "total_usd": sum(amounts) if amounts else 0,
        "stages_present": stages,
        "sectors_present": sectors,
        "max_amount_usd": max(amounts) if amounts else 0,
        "top_investors": top_investors,
        "co_investments": co_investments,
        "companies": companies,
        "unique_companies": len(by_company),
        "unique_investors": len(inv_counts),
    }


def render_dashboard(storage: Storage, min_confidence: float = 0.0) -> str:
    """Return rendered HTML. Use `export_dashboard` to write it to disk."""
    raw = _fetch_rows(storage, min_confidence=min_confidence)
    rows = _dashboard_rows(raw)
    meta = _meta(rows)

    env = Environment(
        loader=FileSystemLoader(str(TEMPLATE_DIR)),
        autoescape=select_autoescape(["html", "j2"]),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    tmpl = env.get_template(TEMPLATE_NAME)
    return tmpl.render(
        rounds_json=json.dumps(rows, ensure_ascii=False, default=str),
        meta_json=json.dumps(meta, ensure_ascii=False, default=str),
        meta=meta,
    )


def export_dashboard(
    storage: Storage,
    path: Path,
    min_confidence: float = 0.0,
) -> int:
    """Render the dashboard to `path`. Returns the number of rounds included."""
    html = render_dashboard(storage, min_confidence=min_confidence)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(html, encoding="utf-8")
    raw = _fetch_rows(storage, min_confidence=min_confidence)
    return len(raw)
