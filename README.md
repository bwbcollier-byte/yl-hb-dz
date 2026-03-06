# yl-hb-dz — Deezer Enrichment Pipeline

Automated enrichment of Deezer social and media profiles using the Deezer API via RapidAPI.

## 🚀 Workflows

| Workflow                     | Table                                | Records | Schedule       |
| ---------------------------- | ------------------------------------ | ------- | -------------- |
| **Deezer Social Enrichment** | `social_profiles` (Deezer type)      | ~52,782 | Daily 3 AM UTC |
| **Deezer Media Enrichment**  | `media_profiles` (with `deezer_url`) | ~1,787  | Daily 4 AM UTC |

## 📦 Setup

### 1. Install Dependencies

```bash
npm install
```

### 2. Create `.env` file

```bash
cp .env.example .env
# Edit .env with your actual keys
```

### 3. GitHub Secrets Required

Go to **Settings > Secrets and Variables > Actions** in the GitHub repo and add:

| Secret                 | Value                                      |
| ---------------------- | ------------------------------------------ |
| `SUPABASE_URL`         | `https://oerfmtjpwrefxuitsphl.supabase.co` |
| `SUPABASE_SERVICE_KEY` | Your Supabase service role key             |
| `RAPIDAPI_KEY_1`       | Your first RapidAPI key                    |
| `RAPIDAPI_KEY_2`       | Your second RapidAPI key                   |
| ...                    | ...                                        |
| `RAPIDAPI_KEY_10`      | Your tenth RapidAPI key                    |

## 🔧 Usage

### Single Run (limited)

```bash
# Social profiles — process 5 records
LIMIT=5 npx ts-node src/deezer-social-enrichment.ts

# Media profiles — process 5 records
LIMIT=5 npx ts-node src/deezer-media-enrichment.ts
```

### Full Run (all records)

```bash
# Social profiles — default 1000 per batch
npx ts-node src/deezer-social-enrichment.ts

# Media profiles — default 1000 per batch
npx ts-node src/deezer-media-enrichment.ts
```

### Overnight Script (continuous loop)

```bash
# Social — runs in loop with 60s sleep between rounds
./overnight-deezer-social.sh

# Media — runs in loop with 60s sleep between rounds
./overnight-deezer-media.sh

# Custom limit per round
LIMIT=5000 ./overnight-deezer-social.sh

# Background mode
nohup ./overnight-deezer-social.sh > social-enrichment.log 2>&1 &
```

### GitHub Actions (Manual Dispatch)

Both workflows support `workflow_dispatch` with an optional `record_limit` input. Trigger from:

- **GitHub UI**: Actions tab → Select workflow → Run workflow
- **Hypebase Web App**: Workflows page → Click Start

## 📊 Processing Order

1. **Unprocessed first** — Records where `last_processed IS NULL`
2. **Oldest processed next** — Records ordered by `last_processed ASC`

This ensures all records get touched before any record is re-processed.

## 🔑 Multi-Key Rotation

The pipeline supports up to **10 RapidAPI keys** rotating round-robin. With 10 keys and ~200ms delay:

- **~50 requests/sec** across keys
- **52,782 social profiles** in ~17 minutes
- **1,787 media profiles** in ~6 minutes

## 📡 Deezer API Endpoints Used

| Endpoint           | Purpose                                                |
| ------------------ | ------------------------------------------------------ |
| `GET /artist/{id}` | Social profile enrichment (fans, albums, images)       |
| `GET /album/{id}`  | Media profile enrichment (tracks, cover, label, genre) |

## 📝 Fields Updated

### Social Profiles (`social_profiles`)

`name`, `username`, `social_url`, `social_image`, `social_about`, `followers_count`, `media_count`, `images` (jsonb), `status`, `dz_check`, `last_checked`, `last_processed`, `processed_updates`, `workflow_logs`

### Media Profiles (`media_profiles`)

`deezer_id`, `deezer_type`, `deezer_fans`, `deezer_genre_id`, `deezer_check`, `cover_art_url`, `track_count`, `release_date`, `label`
