"""One-shot: canonicalize company + investor names across existing rounds.

After Phase 2 ships, the alias tables start filling automatically on each new
insert. This script backfills everything already in the DB:

  1. For every funding_rounds.company_name, call resolve_company() and update
     the row (and company_json) if the canonical differs.
  2. For every round_investors.name, call resolve_investor() and update.

If the resolution collapses two rounds onto the same (company, date, amount)
key, we do NOT merge them here — that's dedup's job. The IDs stay; the names
just get aligned. Run `python -m pipeline.run enrich` afterwards if you want
dedup to sweep.

Usage:
    python -m scripts.backfill_entities [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import sys

from loguru import logger

from pipeline.entity_resolution import resolve_company, resolve_investor
from pipeline.storage import Storage


def backfill(storage: Storage, *, dry_run: bool = False) -> dict[str, int]:
    company_updated = 0
    investor_updated = 0

    with storage.connect() as c:
        rounds = c.execute(
            "SELECT round_id, company_name, company_json FROM funding_rounds"
        ).fetchall()
        for rnd in rounds:
            raw = rnd["company_name"]
            if not raw:
                continue
            canon = resolve_company(c, raw)
            if canon == raw:
                continue
            try:
                company = json.loads(rnd["company_json"] or "{}")
            except json.JSONDecodeError:
                company = {}
            company["name"] = canon
            logger.info("company: '{}' → '{}'", raw, canon)
            if not dry_run:
                c.execute(
                    "UPDATE funding_rounds SET company_name = ?, company_json = ? "
                    "WHERE round_id = ?",
                    (canon, json.dumps(company, ensure_ascii=False), rnd["round_id"]),
                )
            company_updated += 1

        invs = c.execute(
            "SELECT round_id, name, lead FROM round_investors"
        ).fetchall()
        for row in invs:
            raw = row["name"]
            if not raw:
                continue
            canon = resolve_investor(c, raw)
            if canon == raw:
                continue
            logger.info("investor: '{}' → '{}'", raw, canon)
            if not dry_run:
                # Can't change PK in place; delete + re-insert (ignore dup).
                c.execute(
                    "DELETE FROM round_investors WHERE round_id = ? AND name = ?",
                    (row["round_id"], raw),
                )
                c.execute(
                    "INSERT OR IGNORE INTO round_investors (round_id, name, lead) "
                    "VALUES (?, ?, ?)",
                    (row["round_id"], canon, row["lead"]),
                )
            investor_updated += 1
    return {"companies": company_updated, "investors": investor_updated}


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dry-run", action="store_true", help="Show changes without writing.")
    args = p.parse_args()
    storage = Storage()
    stats = backfill(storage, dry_run=args.dry_run)
    print(
        f"entities backfill: {stats['companies']} companies renamed, "
        f"{stats['investors']} investors renamed"
        f"{' [DRY RUN]' if args.dry_run else ''}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
