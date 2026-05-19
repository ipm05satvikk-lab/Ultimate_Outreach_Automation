# Creator Scraper

Apify-powered TikTok bio scraper + Claude-based matcher + lightweight CRM,
with optional two-hop linktree following and structured per-dimension scoring.

Upload a list of up to 50,000 creator handles, describe the kind of creator you are
looking for in plain English (with an optional rubric for per-dimension scoring),
and the tool will:

1. Scrape every bio via Apify (deduped against past scrapes within `BIO_REFRESH_DAYS`)
2. Extract emails / phones / links / social handles from each bio
3. Follow `bio_link` URLs through known aggregators (linktr.ee, beacons.ai, etc.)
   and extract any contacts hidden there
4. Have Claude judge each creator against your description (match / no-match,
   confidence, reason, optionally per-rubric-dimension scores)
5. Store everything in Postgres so it persists across redeploys
6. Let you tag, status, note, and export creators, turning the data into a real CRM

## What is inside

```
server.js              Express app + API routes + HTTP Basic Auth + rate limiting
db.js                  Postgres schema (auto-migrates on boot)
workers/scraper.js     Apify worker loop (atomic claims + retries)
workers/evaluator.js   Anthropic evaluator loop (atomic claims + retries + rubric)
workers/linktree.js    Bio-link follower (extracts contacts from linktree-style pages)
lib/extractors.js      Regex-based contact extraction (emails, strict phones, links, socials)
public/                Web UI (index, job detail, CRM)
```

The server runs four things in one Node process: HTTP API + UI, scraper worker,
evaluator worker, and linktree worker. Each worker uses atomic Postgres claims
(`FOR UPDATE SKIP LOCKED`) plus a per-row retry counter, so multiple instances
can run safely on Railway without double-billing Apify or losing rows to
transient errors.

## Deploy to Railway

```bash
# 1. From this directory:
railway login
railway init

# 2. Add the Postgres plugin from the Railway dashboard (one click).
#    It auto-injects DATABASE_URL into your service.

# 3. Set your secrets in Railway dashboard -> Variables:
#       APIFY_TOKEN=apify_api_xxx
#       ANTHROPIC_API_KEY=sk-ant-xxx
#       BASIC_AUTH_USER=admin
#       BASIC_AUTH_PASS=<a long random string>     <-- IMPORTANT
#    (Optional tuning vars are in .env.example)

# 4. Deploy
railway up
```

That is it. Open the generated `*.up.railway.app` URL. The browser will prompt
for the Basic Auth credentials.

**Auth is critical.** If you leave `BASIC_AUTH_PASS` unset, the server will
boot, log a loud warning, and serve every endpoint publicly. A 50,000-handle
job costs around $250 in Apify credits, so do not leave the URL exposed.

## Local dev

```bash
npm install
cp .env.example .env       # fill in the values
# Postgres: easiest is `docker run -p 5432:5432 -e POSTGRES_PASSWORD=pw postgres:16`
# then set DATABASE_URL=postgres://postgres:pw@localhost:5432/postgres in .env
node server.js
# open http://localhost:3000
```

Leave `BASIC_AUTH_PASS` blank for local dev. The server will warn but run.

## Storage architecture (the CRM bit)

The schema is designed so that **creators are long-lived records** and jobs are
just runs against them.

```
creators        ← one row per @handle, ever. The CRM record.
jobs            ← each run: description, optional rubric, status, counts
job_creators    ← which creators were in which job + the per-job verdict
                  (includes scrape_attempts, eval_attempts, in_flight state,
                  rubric_scores JSONB for per-dimension scoring)
contacts        ← emails/phones/links discovered. Many per creator.
                  source tells you where (bio / bio_link / linktree / manual)
creator_tags    ← tags ("priority", "outreached", "responded") for organising
creator_notes   ← free-form notes you add manually
creator_crm     ← CRM status (new/shortlisted/contacted/responded/signed/...),
                  owner, last_contacted, plus a JSONB `custom` field for any
                  fields you invent later
link_fetches    ← bookkeeping for the linktree worker: which bio_links were
                  fetched, when, status code, contacts found, attempt count
```

Why this matters:

- Re-running a different description against the same 10K handles re-uses cached
  bios. You do not pay Apify twice, and you keep all your notes / tags / CRM
  status. Same applies to linktree follows: each unique bio_link is only
  fetched once.
- Every Claude verdict is kept per job, so you can A/B different descriptions
  (or different rubrics) against the same pool.
- The `custom JSONB` column on `creator_crm` is your escape hatch. When you
  decide you also want to track GMV, last reply date, deal value, whatever,
  just stuff it in there. `PATCH /api/creators/:handle` now does a key-level
  JSONB merge so partial updates do not overwrite the whole blob.
- Postgres = SQL, so you can later plug into Retool, Metabase, Looker, Hex,
  Supabase Studio, or just query from the Postgres CLI.

## Rubric scoring

Pass a JSON rubric alongside the description in the job submission form, or as
the `rubric` field in the API:

```json
{
  "credentials": "Bio mentions verifiable medical credential (RN, NP, MD, PharmD, etc.)",
  "audience_fit": "Bio language / hashtags suggest US health-conscious audience",
  "authority_signals": "Bio references clinical practice or hospital affiliation",
  "content_style": "Bio implies educational rather than affiliate-style content"
}
```

Claude will score each creator 0-5 on every dimension you provide, alongside
the usual match boolean. The scores are stored in `job_creators.rubric_scores`
as JSONB and shown in the CRM drawer under each job history entry. The CSV
export includes the rubric_scores column as a JSON string.

Rubrics with more than ~6 dimensions may need `ANTHROPIC_MAX_TOKENS` bumped up
to avoid output truncation on a 20-creator batch.

## Cost guardrails

For 10,000 creators:

| What                            | Estimated cost |
|---------------------------------|---------------:|
| Apify profile scraping          | ~$50 (depends on actor; ~$0.005/profile) |
| Claude Sonnet evaluation        | ~$8-12 (20 creators/call × 500 calls, more if rubric used) |
| Linktree fetches                | $0 (bandwidth only, runs on Railway) |
| Railway hobby plan + Postgres   | ~$5/mo flat |

Re-running the **same** handles with a new description = Apify cost ≈ $0
(within `BIO_REFRESH_DAYS`), only Claude eval cost.

## Reliability features

- **Atomic claims.** Both workers use `FOR UPDATE SKIP LOCKED` and update the
  row to `in_flight` in the same statement. Multiple Railway instances can run
  side by side without double-calling Apify.
- **Per-row retry counter.** `scrape_attempts` and `eval_attempts` are
  incremented on each claim. A row is only marked permanently `failed` after
  `*_MAX_ATTEMPTS` tries (default 3).
- **In-call retry-with-backoff.** A single Apify 503 or Anthropic 529 does not
  consume an attempt: the workers retry up to 3 times with exponential backoff
  before the attempt counter ticks.
- **Stuck-claim reaper.** Rows that have been in `in_flight` longer than
  `*_CLAIM_TIMEOUT_MS` get reset back to `pending` so a crashed worker does not
  permanently block a job.
- **Rate limiting.** 120 GET req/min/IP for read endpoints; 10 POST /api/jobs
  per hour per IP.

## API cheat-sheet

```
POST   /api/jobs                  { name?, description, rubric?, handles: [...] }
                                  or multipart with handles_file
GET    /api/jobs                  recent 100 jobs
GET    /api/jobs/:id              job summary
GET    /api/jobs/:id/results      ?matched=true|false&limit&offset
POST   /api/jobs/:id/cancel

GET    /api/creators              ?status&tag&q&has_email&matched_in_job&limit&offset
GET    /api/creators/:handle      full record + contacts + tags + notes + jobs + link_fetches
PATCH  /api/creators/:handle      { status?, owner?, custom?, last_contacted?, __replace_custom? }
                                  Send {"__clear": true} as the value to explicitly clear a field.
                                  By default `custom` is merged (JSONB ||). Set
                                  __replace_custom: true to replace the blob instead.
POST   /api/creators/:handle/tags { tag }
DELETE /api/creators/:handle/tags/:tag
POST   /api/creators/:handle/notes { note, author? }
POST   /api/creators/:handle/contacts { kind, value, source? }

GET    /api/export.csv?job=ID                  full job export
GET    /api/export.csv?job=ID&matched=true     matched-only export
GET    /api/export.csv?status=shortlisted      CRM export

GET    /healthz                  open (no auth)
```

## Notes / gotchas

- **Apify actor.** Defaults to `clockworks/tiktok-profile-scraper`. If you
  prefer a different actor, set `APIFY_ACTOR`. The `normalizeProfile()`
  function in `workers/scraper.js` handles several common output shapes; if
  you switch to an actor with a very different schema, update that one function.

- **`proxyCountryCode`.** The original code passed the literal string `"None"`,
  which Apify interpreted as a country code. The fix is to omit the field by
  default. Set `APIFY_PROXY_COUNTRY=US` if you need to force a proxy region.

- **Linktree allowlist.** The linktree worker will only fetch URLs whose
  hostname matches the built-in allowlist of bio-link aggregators (linktr.ee,
  beacons.ai, stan.store, bio.link, allmylinks.com, etc.). Add more via
  `LINKTREE_EXTRA_HOSTS=foo.com,bar.io`. Arbitrary personal sites are skipped
  by design (no SSRF surface).

- **Phone false positives.** The phone regex is stricter than the old version:
  it requires either a `+` country-code prefix, parens around the area code,
  or explicit separators between three groups (`415-555-1234`). It also checks
  surrounding context and skips matches near words like "followers", "views",
  "USD", "year". You will still see occasional noise; the CRM lets you delete
  bad contacts manually.

- **False positives in matching.** Claude is stricter than substring matching,
  but for very short bios with vague language it can still be noisy. Tighten
  your description prompt: be explicit about what is NOT a match ("NOT
  supplement brands without credentials", "NOT general wellness affiliates").
  Adding a rubric also helps because the per-dimension scores expose where
  Claude is reaching.

- **`raw_profile` size.** The scraper trims the raw Apify item to a curated
  subset of fields before insert, which keeps DB size manageable at 10K+
  creators. Set `RAW_PROFILE_FULL=true` to keep the full item if you need it.

- **Stuck jobs.** If a worker crashes mid-batch, rows can sit in `in_flight`
  briefly. The reaper resets them after `*_CLAIM_TIMEOUT_MS` (default 10 min).
  If a job is genuinely stuck and you want to nudge it, cancel and re-submit
  with the same handles; cached bios make re-running near-free.
