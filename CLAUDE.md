# CLAUDE.md — `yl-hb-dz` (Deezer enrichment)

Conventions shared across the `yl-hb-*` fleet live in
[`SCRAPER-CLAUDE-TEMPLATE.md`](../SCRAPER-CLAUDE-TEMPLATE.md) — read both.

## ⚠️ READ FIRST: SCHEMA MISMATCH WITH LIVE DB

The TypeScript scrapers in `src/` reference `talent_profiles`,
`social_profiles`, and `media_profiles`. **None of those tables exist
on the live Supabase project (`oerfmtjpwrefxuitsphl`)** — live uses
`hb_talent`, `hb_socials`, `hb_media`. The scheduled GitHub Actions
workflows therefore likely fail on the first write (or, if the script
swallows errors, run to "completion" without persisting anything).

This repo is in the same broken state as `yl-hb-dtp` and `yl-hb-tadb`.
Until resolved, treat the workflows as broken:

1. **Rewrite to target `hb_talent` / `hb_socials` / `hb_media`** (recommended), or
2. **Retire** if Deezer enrichment is now done elsewhere.

The Python script `enrich_deezer_airtable.py` is a separate, Airtable-only
flow that does not have the schema-mismatch problem.

## What this repo does (intent)

Two complementary flows:

1. **Supabase enrichment** — TS scrapers in `src/` that fetch Deezer
   artist + album metadata via RapidAPI and upsert into Supabase
   (currently broken — see schema mismatch above).
2. **Airtable sync** — Python script (`enrich_deezer_airtable.py`)
   that mirrors a subset of the same data into Airtable.

## Stack

**Mixed.** TS scrapers under `src/` (Node 20 + ts-node, service-role
Supabase) plus a Python Airtable sync at the root (`requirements.txt`).
Two GitHub Actions workflows.

## Repo layout

```
src/
  deezer-social-enrichment.ts        # TS — currently targets legacy schema
  deezer-media-enrichment.ts         # TS — same
  supabase.ts                        # service-role client
enrich_deezer_airtable.py            # standalone Airtable mirror
requirements.txt                     # for the Python script
overnight-deezer-social.sh           # local-runner wrapper
overnight-deezer-media.sh            # local-runner wrapper
.github/workflows/
  deezer-unified-enrichment.yml      # main TS scrapers
  deezer-airtable-sync.yml           # Python script
package.json
tsconfig.json
```

## Supabase auth

Standard fleet convention — `SUPABASE_URL` + `SUPABASE_SERVICE_KEY`
in `src/supabase.ts`. The Airtable sync script doesn't use Supabase.

## Workflow lifecycle convention

Both workflows call `log_workflow_run` start + result with hardcoded
GitHub workflow ids (this repo's local convention).

## Tables this repo intends to touch

| Legacy table | Operation | Live equivalent |
|---|---|---|
| `talent_profiles` | UPSERT | `public.hb_talent` |
| `social_profiles` | UPSERT | `public.hb_socials` |
| `media_profiles` | UPSERT (albums, releases) | `public.hb_media` |

The Python sync also writes to a Deezer-specific Airtable view.

## Running locally

```bash
# TS scrapers (currently broken vs live schema):
npm install
npx ts-node --transpile-only src/deezer-social-enrichment.ts

# Python Airtable sync:
pip install -r requirements.txt
export AIRTABLE_API_KEY=...
python3 enrich_deezer_airtable.py
```

Required env vars:

```
SUPABASE_URL, SUPABASE_SERVICE_KEY      # TS scrapers
RAPIDAPI_KEY                            # Deezer host on RapidAPI
LIMIT (default 100), SLEEP_MS           # tuning knobs
AIRTABLE_API_KEY                        # Python sync
```

## Per-repo gotchas

- **Schema mismatch (see top of file).**
- **Python and TS flows touch overlapping data** but don't share a
  data model. Easy to make them drift; coordinate any field changes.
- **`overnight-*.sh` shell wrappers** are for local runs, not CI. CI
  invokes the TS files directly.

## Conventions Claude should follow when editing this repo

- **Don't run the TS workflows against `oerfmtjpwrefxuitsphl` until
  the schema rewrite is complete.**
- **Match the model in `yl-hb-am`, `yl-hb-imdb`, `yl-hb-rgm`** when
  rewriting — those are the canonical examples for fleet-aligned
  Supabase scrapers.

## Related repos

- `yl-hb-tadb`, `yl-hb-ml`, `yl-hb-dtp` — same legacy-schema problem.
- `yl-hb-am`, `yl-hb-imdb`, `yl-hb-rgm`, `yl-hb-sp` — fleet-aligned
  models for the rewrite.
