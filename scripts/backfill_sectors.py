"""One-shot: re-normalize the sector field on every existing funding_round.

Two passes:
  1. Read each row's stored `enrichment_json` (from articles table via
     round_sources join). If the LLM extracted a sector string we hadn't
     canonicalized, map it to the taxonomy now.
  2. If no enrichment_json is available, leave `sector` null — we don't want
     to burn LLM credits here. Run `python -m pipeline.run enrich` with the
     updated prompt to fill gaps.

Usage:
    python -m scripts.backfill_sectors [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import sys

from loguru import logger

from pipeline.sectors import normalize_sector, sector_label
from pipeline.storage import Storage


def backfill(storage: Storage, *, dry_run: bool = False) -> dict[str, int]:
    updated = 0
    skipped = 0
    already_ok = 0
    with storage.connect() as c:
        rounds = c.execute(
            "SELECT round_id, company_json FROM funding_rounds"
        ).fetchall()
        for rnd in rounds:
            try:
                company = json.loads(rnd["company_json"] or "{}")
            except json.JSONDecodeError:
                company = {}
            current = (company.get("sector") or "").strip().lower()

            llm_sector: str | None = None
            urls = c.execute(
                "SELECT article_url FROM round_sources WHERE round_id = ?",
                (rnd["round_id"],),
            ).fetchall()
            for u in urls:
                row = c.execute(
                    "SELECT enrichment_json FROM articles WHERE url = ?",
                    (u["article_url"],),
                ).fetchone()
                if not row or not row["enrichment_json"]:
                    continue
                try:
                    payload = json.loads(row["enrichment_json"])
                except json.JSONDecodeError:
                    continue
                if payload.get("sector"):
                    llm_sector = str(payload["sector"])
                    break

            candidate = normalize_sector(llm_sector or current or None)
            if not candidate:
                skipped += 1
                continue
            if candidate == current:
                already_ok += 1
                continue
            company["sector"] = candidate
            new_json = json.dumps(company, ensure_ascii=False)
            logger.info(
                "round {}: sector '{}' → '{}' ({})",
                rnd["round_id"], current or "None", candidate, sector_label(candidate),
            )
            if not dry_run:
                c.execute(
                    "UPDATE funding_rounds SET company_json = ? WHERE round_id = ?",
                    (new_json, rnd["round_id"]),
                )
            updated += 1
    return {"updated": updated, "already_ok": already_ok, "skipped": skipped}


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dry-run", action="store_true", help="Show changes without writing.")
    args = p.parse_args()

    storage = Storage()
    stats = backfill(storage, dry_run=args.dry_run)
    print(
        f"sectors backfill: {stats['updated']} updated, "
        f"{stats['already_ok']} already ok, {stats['skipped']} skipped "
        f"(no sector signal){' [DRY RUN]' if args.dry_run else ''}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
