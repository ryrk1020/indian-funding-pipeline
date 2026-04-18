# Loom demo script — 2-minute walkthrough

Target length: **~2:00**. Record at 1440p if possible.

Pre-flight (do this before hitting record):
- Terminal in `E:\scraper\` with venv activated
- `.env` already has `OPENROUTER_API_KEY` set
- Reset the DB so enrichment runs fresh:
  ```bash
  /c/Python313/python.exe -c "import sqlite3; c=sqlite3.connect('data/funding.db'); c.execute(\"UPDATE articles SET enrichment_status='pending'\"); c.execute('DELETE FROM round_sources'); c.execute('DELETE FROM round_investors'); c.execute('DELETE FROM funding_rounds'); c.commit()"
  ```
- Two tabs open in editor: `README.md` (architecture diagram) + `pipeline/run.py`
- One browser tab open at the GitHub repo's `exports/sample_rounds.csv`

---

## 0:00 – 0:20 — Problem

> "Indian startup funding news is fragmented across Inc42, Entrackr, YourStory, and others — no public API, every site has its own HTML. Analysts burn hours a week copy-pasting from articles into spreadsheets. I built a pipeline that turns that into a single deduplicated, confidence-scored table and runs unattended every morning."

Action: Show the README top, scroll to the architecture Mermaid diagram.

---

## 0:20 – 0:50 — Design

> "Three scrapers, each deliberately demonstrating a different capability. Inc42 uses the WordPress sitemap for archive discovery. Entrackr needs Playwright to scroll the homepage, but its detail pages are static. YourStory fingerprints the TLS handshake, so both list and detail go through Playwright."
>
> "Raw articles go into SQLite. Then the enricher hits OpenRouter — I'm using a free gpt-oss-120b model — in parallel under a semaphore. If the LLM confidence is low, a deterministic regex extractor merges in, but with a weighted blend so regex can't mask a confident LLM 'no.'"
>
> "Then Pydantic validators, fuzzy cross-source dedup on rapidfuzz token-set ratio, and finally CSV, JSON, and Excel output with green/red confidence highlighting."

Action: Point at the Mermaid diagram as each stage is mentioned.

---

## 0:50 – 1:30 — Live demo

> "Let me run it end to end."

```bash
python -m pipeline.run enrich --limit 10
```

> "Ten articles. LLM calls are parallel — you can see the timestamps overlap."

(Wait ~30s. Point at the confidence merge log lines — "low confidence 0.10 — merging regex".)

```bash
python -m pipeline.run health
```

> "Health shows per-source status — OK, WARN, STALE, DRIFT, CRITICAL. It flags silent breakage, not just exceptions."

```bash
python -m pipeline.run export --format all
```

> "CSV, JSON, and Excel all written in one shot."

Open `data/exports/rounds_<date>.xlsx` in Excel briefly.

> "Excel has the confidence column color-coded. Green above 0.85 — trustworthy. Red below 0.35. Middle is the regex-merged rows where the LLM flagged the article as probably not funding."

---

## 1:30 – 1:55 — Automation + story

Switch to browser on the GitHub repo → `.github/workflows/daily.yml`.

> "There's a daily cron that runs the whole thing, uploads exports as artifacts, and auto-commits a fresh sample file back to the repo. CI runs ruff and 71 tests on every push."

Switch to `exports/sample_rounds.csv` on GitHub.

> "This CSV is produced by that workflow. Reviewers can see real output without running anything."

---

## 1:55 – 2:00 — Close

> "Stack is HTTPX, Playwright, Pydantic, SQLite, OpenRouter, rapidfuzz, tenacity, GitHub Actions. Repo and README link below."

End.

---

## Fallback commands if something breaks on-camera

If `enrich` hits a 429 or 404 on the model:
```bash
python -m pipeline.run enrich --use-regex-only --limit 10
```

If Playwright browser isn't installed:
```bash
python -m playwright install chromium
```

If the DB is empty (no articles scraped yet):
```bash
python -m pipeline.run scrape --source inc42 --limit 3
```
