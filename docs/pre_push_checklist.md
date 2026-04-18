# Pre-push checklist

Run through this before pushing to GitHub. Each item has been verified once on 2026-04-18.

## 1. Local verification

```bash
# Lint — must report 0 errors
python -m ruff check .

# Tests — 71 tests must all pass
python -m pytest -q

# End-to-end smoke (fresh DB)
rm -f data/funding.db data/funding.db-wal data/funding.db-shm
python -m pipeline.run scrape  --limit 8
python -m pipeline.run enrich  --limit 16
python -m pipeline.run export  --format all
python -m pipeline.run health
```

Expected: non-zero rounds in `data/exports/rounds_YYYYMMDD.csv`, all sources
reporting `OK` (or `STALE` only for inc42 if its sitemap is empty that day).

## 2. Secret scan

```bash
# Nothing here should list your .env or a real API key
git status --ignored
grep -rE 'sk-or-v1-[a-zA-Z0-9]{30,}|sk-ant-' . \
  --exclude-dir=.venv --exclude-dir=__pycache__ --exclude=.env
```

`.env` must appear under *Ignored files*, never in *Untracked*. Confirm
`.env.example` contains only placeholder keys.

## 3. Refresh committed sample exports

```bash
cp data/exports/rounds_*.csv  exports/sample_rounds.csv
cp data/exports/rounds_*.json exports/sample_rounds.json
cp data/exports/rounds_*.xlsx exports/sample_rounds.xlsx
```

These three files are explicitly allow-listed in `.gitignore`.

## 4. Initial git push

```bash
git init
git add .
git status                      # review — no .env, no data/*.db
git commit -m "Indian startup funding pipeline — initial commit"
git branch -M main
git remote add origin git@github.com:<user>/<repo>.git
git push -u origin main
```

## 5. GitHub Secrets (for daily workflow)

Repo → **Settings → Secrets and variables → Actions → New repository secret**:

| Name                 | Value                                         |
| -------------------- | --------------------------------------------- |
| `OPENROUTER_API_KEY` | Your OpenRouter key (`sk-or-v1-...`)          |

Optional (only if you wire up Sheets export later):

| `GOOGLE_SERVICE_ACCOUNT_JSON` | Full JSON body of the service account file |
| `GOOGLE_SHEET_ID`             | The `d/<id>/edit` id of the target sheet   |

## 6. Verify workflows

1. Push triggers `.github/workflows/ci.yml` → **Actions** tab, check ruff + pytest both pass.
2. Manually trigger `.github/workflows/daily.yml`:
   - **Actions** → `daily` → **Run workflow** → `main` → **Run workflow**.
   - Watch the run; on success it uploads `exports-artifact` and auto-commits refreshed `exports/sample_rounds.*`.
3. Confirm the scheduled cron (`0 2 * * *` UTC) appears on the workflow page.

## 7. README sanity

- Mermaid diagram renders on the GitHub repo page.
- `exports/sample_rounds.csv` is visible and non-empty.
- No references to deleted files or stale commands.

## Known caveats

- **OpenRouter free tier caps at 50 requests/day and 20/minute.** Full
  enrichment of >50 articles in a day will hit the daily cap; rely on
  regex-fallback or add $10 of credits to lift the day cap to 1000.
- **Inc42 sitemap sometimes returns 0 candidates** (paginated sitemap rolled
  over). Other two sources compensate. Schema-drift monitoring will flag a
  real regression separately.
- **First-run scrape is slow** (~60s for 16 articles) because Playwright
  boots a Chromium instance per source.
