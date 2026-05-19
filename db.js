// db.js — Postgres connection + schema. Auto-migrates on boot.
//
// CRM-friendly schema design:
//   creators       = the long-lived record (one row per @handle, ever)
//   jobs           = each scraping run (description + optional rubric + status)
//   job_creators   = link table: which creators were in which job + per-job verdict
//                    Includes scrape_attempts / eval_attempts counters so transient
//                    failures can be retried rather than dropped permanently, and an
//                    in_flight state used by the atomic claim queries in the workers.
//   contacts       = emails/phones/links found in bios (many per creator)
//   creator_tags   = tags ("priority", "outreached", "responded") for CRM workflow
//   creator_notes  = free-form notes attached to a creator
//   creator_crm    = CRM status + owner + custom JSONB escape hatch
//   link_fetches   = bookkeeping for the linktree worker: which bio_links we've
//                    pulled, when, what we got back, how many contacts we found.
//
// Migration strategy: every table uses CREATE TABLE IF NOT EXISTS, and every
// later-added column uses ALTER TABLE ... ADD COLUMN IF NOT EXISTS, so old
// databases get upgraded in place on next boot.

const { Pool } = require("pg");

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  // Railway internal Postgres needs SSL off; external needs it on. Toggle via env.
  ssl: process.env.PGSSL === "true" ? { rejectUnauthorized: false } : false,
});

pool.on("error", (err) => console.error("[db] idle client error:", err));

// Base schema. Safe to re-run.
const SCHEMA = `
CREATE TABLE IF NOT EXISTS creators (
  handle           TEXT PRIMARY KEY,
  bio              TEXT,
  bio_link         TEXT,
  follower_count   INTEGER,
  following_count  INTEGER,
  video_count      INTEGER,
  verified         BOOLEAN,
  nickname         TEXT,
  avatar_url       TEXT,
  region           TEXT,
  raw_profile      JSONB,
  scrape_status    TEXT NOT NULL DEFAULT 'pending', -- pending | scraped | failed | not_found
  scrape_error     TEXT,
  scraped_at       TIMESTAMPTZ,
  first_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_creators_scrape_status ON creators (scrape_status);
CREATE INDEX IF NOT EXISTS idx_creators_scraped_at ON creators (scraped_at);

CREATE TABLE IF NOT EXISTS jobs (
  id                       BIGSERIAL PRIMARY KEY,
  name                     TEXT,
  description              TEXT NOT NULL,
  rubric                   JSONB,
  enable_visual            BOOLEAN NOT NULL DEFAULT FALSE,
  enable_transcripts       BOOLEAN NOT NULL DEFAULT FALSE,
  relevant_hashtags        TEXT[],
  max_videos_per_creator   INTEGER NOT NULL DEFAULT 5,
  cost_cap_usd             NUMERIC(10,4) NOT NULL DEFAULT 10.0000,
  cost_spent_usd           NUMERIC(10,4) NOT NULL DEFAULT 0.0000,
  cost_paused_at           TIMESTAMPTZ,
  status                   TEXT NOT NULL DEFAULT 'pending',   -- pending | running | paused | done | cancelled | failed
  total_creators           INTEGER NOT NULL DEFAULT 0,
  scraped_count            INTEGER NOT NULL DEFAULT 0,
  evaluated_count          INTEGER NOT NULL DEFAULT 0,
  matched_count            INTEGER NOT NULL DEFAULT 0,
  error                    TEXT,
  created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at              TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs (status);

CREATE TABLE IF NOT EXISTS job_creators (
  job_id              BIGINT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  handle              TEXT   NOT NULL REFERENCES creators(handle) ON DELETE CASCADE,
  scrape_state        TEXT   NOT NULL DEFAULT 'pending',  -- pending | in_flight | done | skipped | failed
  eval_state          TEXT   NOT NULL DEFAULT 'pending',
  visual_state        TEXT   NOT NULL DEFAULT 'skipped',  -- pending | in_flight | done | skipped | failed
  transcript_state    TEXT   NOT NULL DEFAULT 'skipped',  -- pending | in_flight | done | skipped | failed
  scrape_attempts     INTEGER NOT NULL DEFAULT 0,
  eval_attempts       INTEGER NOT NULL DEFAULT 0,
  visual_attempts     INTEGER NOT NULL DEFAULT 0,
  transcript_attempts INTEGER NOT NULL DEFAULT 0,
  scrape_claimed_at   TIMESTAMPTZ,
  eval_claimed_at     TIMESTAMPTZ,
  visual_claimed_at   TIMESTAMPTZ,
  transcript_claimed_at TIMESTAMPTZ,
  matched             BOOLEAN,
  confidence          NUMERIC(3,2),
  reason              TEXT,
  rubric_scores       JSONB,
  match_signals       JSONB,
  match_sources       TEXT[],
  match_cues          TEXT[],
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  evaluated_at        TIMESTAMPTZ,
  PRIMARY KEY (job_id, handle)
);
CREATE INDEX IF NOT EXISTS idx_job_creators_job_state ON job_creators (job_id, scrape_state, eval_state);
CREATE INDEX IF NOT EXISTS idx_job_creators_matched ON job_creators (job_id, matched);
CREATE INDEX IF NOT EXISTS idx_job_creators_scrape_claim ON job_creators (scrape_state, scrape_claimed_at);
CREATE INDEX IF NOT EXISTS idx_job_creators_eval_claim ON job_creators (eval_state, eval_claimed_at);
CREATE INDEX IF NOT EXISTS idx_job_creators_visual_claim ON job_creators (visual_state, visual_claimed_at);
CREATE INDEX IF NOT EXISTS idx_job_creators_transcript_claim ON job_creators (transcript_state, transcript_claimed_at);

CREATE TABLE IF NOT EXISTS contacts (
  id         BIGSERIAL PRIMARY KEY,
  handle     TEXT NOT NULL REFERENCES creators(handle) ON DELETE CASCADE,
  kind       TEXT NOT NULL,
  value      TEXT NOT NULL,
  source     TEXT,
  found_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (handle, kind, value)
);
CREATE INDEX IF NOT EXISTS idx_contacts_handle ON contacts (handle);
CREATE INDEX IF NOT EXISTS idx_contacts_kind ON contacts (kind);

CREATE TABLE IF NOT EXISTS creator_tags (
  handle    TEXT NOT NULL REFERENCES creators(handle) ON DELETE CASCADE,
  tag       TEXT NOT NULL,
  added_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (handle, tag)
);
CREATE INDEX IF NOT EXISTS idx_creator_tags_tag ON creator_tags (tag);

CREATE TABLE IF NOT EXISTS creator_notes (
  id          BIGSERIAL PRIMARY KEY,
  handle      TEXT NOT NULL REFERENCES creators(handle) ON DELETE CASCADE,
  note        TEXT NOT NULL,
  author      TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_creator_notes_handle ON creator_notes (handle);

CREATE TABLE IF NOT EXISTS creator_crm (
  handle           TEXT PRIMARY KEY REFERENCES creators(handle) ON DELETE CASCADE,
  status           TEXT NOT NULL DEFAULT 'new',
  owner            TEXT,
  last_contacted   TIMESTAMPTZ,
  custom           JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_creator_crm_status ON creator_crm (status);

-- Bio-link follow worker: bookkeeping so we don't re-fetch the same URL repeatedly,
-- and so we can see the yield (emails found) per link.
CREATE TABLE IF NOT EXISTS link_fetches (
  id               BIGSERIAL PRIMARY KEY,
  handle           TEXT NOT NULL REFERENCES creators(handle) ON DELETE CASCADE,
  url              TEXT NOT NULL,
  status           TEXT NOT NULL DEFAULT 'pending',  -- pending | done | failed | skipped
  http_status      INTEGER,
  attempts         INTEGER NOT NULL DEFAULT 0,
  contacts_found   INTEGER NOT NULL DEFAULT 0,
  error            TEXT,
  fetched_at       TIMESTAMPTZ,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (handle, url)
);
CREATE INDEX IF NOT EXISTS idx_link_fetches_status ON link_fetches (status);

-- Vision worker: stores the verdict from running screenshots through the vision
-- model. Per (handle, job) because the description (and thus what counts as a
-- match) varies per job. screenshot_paths is informational only — the actual
-- PNGs live on the worker's filesystem and are not persisted long term.
CREATE TABLE IF NOT EXISTS creator_visual_analyses (
  id               BIGSERIAL PRIMARY KEY,
  handle           TEXT NOT NULL REFERENCES creators(handle) ON DELETE CASCADE,
  job_id           BIGINT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  matched          BOOLEAN,
  confidence       TEXT,
  likely_role      TEXT,
  cues             TEXT[],
  reason           TEXT,
  screenshot_paths TEXT[],
  model            TEXT,
  raw_response     JSONB,
  analyzed_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (handle, job_id)
);
CREATE INDEX IF NOT EXISTS idx_visual_analyses_handle ON creator_visual_analyses (handle);
CREATE INDEX IF NOT EXISTS idx_visual_analyses_job ON creator_visual_analyses (job_id);

-- Videos table: per-creator cache of recent videos. Transcripts persist forever
-- so subsequent jobs reuse them without re-paying Whisper. video_id is TikTok's
-- own ID; UNIQUE (handle, video_id) prevents duplicates across multiple fetches.
CREATE TABLE IF NOT EXISTS videos (
  id                 BIGSERIAL PRIMARY KEY,
  handle             TEXT NOT NULL REFERENCES creators(handle) ON DELETE CASCADE,
  video_id           TEXT NOT NULL,
  video_url          TEXT,
  caption            TEXT,
  hashtags           TEXT[],
  cover_url          TEXT,
  duration_s         INTEGER,
  view_count         BIGINT,
  posted_at          TIMESTAMPTZ,
  transcript         TEXT,
  transcript_status  TEXT NOT NULL DEFAULT 'pending', -- pending | done | skipped | failed
  transcript_error   TEXT,
  transcribed_at     TIMESTAMPTZ,
  fetched_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (handle, video_id)
);
CREATE INDEX IF NOT EXISTS idx_videos_handle ON videos (handle);
CREATE INDEX IF NOT EXISTS idx_videos_transcript_status ON videos (transcript_status);

-- ============================================================================
-- Outreach (v1.5): email sending pipeline. Each "campaign" is a category like
-- "Medical Professional"; it owns 5 stage templates (initial + v1..v4) and a
-- bundle of attachments. Creators get assigned to a campaign and progress
-- through stages one email at a time. Replies and bounces detected via IMAP
-- pause the sequence automatically.
-- ============================================================================

-- One sending identity. Holds SMTP and IMAP credentials for one Gmail/Workspace
-- mailbox. App Password lives here in plaintext for now; for production-grade
-- security we would encrypt it with a key from env, but at single-tenant scale
-- the DB credentials themselves are the security boundary.
CREATE TABLE IF NOT EXISTS email_senders (
  id                   BIGSERIAL PRIMARY KEY,
  name                 TEXT NOT NULL,
  from_email           TEXT NOT NULL,
  from_name            TEXT NOT NULL,
  smtp_host            TEXT NOT NULL DEFAULT 'smtp.gmail.com',
  smtp_port            INTEGER NOT NULL DEFAULT 587,
  smtp_user            TEXT NOT NULL,
  smtp_app_password    TEXT NOT NULL,
  imap_host            TEXT NOT NULL DEFAULT 'imap.gmail.com',
  imap_port            INTEGER NOT NULL DEFAULT 993,
  daily_cap            INTEGER NOT NULL DEFAULT 50,
  sent_today           INTEGER NOT NULL DEFAULT 0,
  sent_today_resets_at TIMESTAMPTZ NOT NULL DEFAULT (now() + interval '24 hours'),
  signature            TEXT,
  active               BOOLEAN NOT NULL DEFAULT TRUE,
  imap_last_uid        INTEGER NOT NULL DEFAULT 0,
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- A category-level campaign. One row per (category, audience). Holds the
-- delay-between-stages config and points at the sender.
CREATE TABLE IF NOT EXISTS email_campaigns (
  id              BIGSERIAL PRIMARY KEY,
  name            TEXT NOT NULL,
  description     TEXT,
  sender_id       BIGINT NOT NULL REFERENCES email_senders(id),
  delay_days_v1   INTEGER NOT NULL DEFAULT 4,
  delay_days_v2   INTEGER NOT NULL DEFAULT 7,
  delay_days_v3   INTEGER NOT NULL DEFAULT 10,
  delay_days_v4   INTEGER NOT NULL DEFAULT 14,
  status          TEXT NOT NULL DEFAULT 'draft',  -- draft | ready | active | paused | archived
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_email_campaigns_status ON email_campaigns (status);

-- 5 rows per campaign: initial, v1, v2, v3, v4. Body uses [PERSONALIZED] as
-- the placeholder marker. personalization_instructions is the prompt fragment
-- that drives Claude when filling it in.
CREATE TABLE IF NOT EXISTS email_templates (
  id                            BIGSERIAL PRIMARY KEY,
  campaign_id                   BIGINT NOT NULL REFERENCES email_campaigns(id) ON DELETE CASCADE,
  stage                         TEXT NOT NULL,                -- 'initial' | 'v1' | 'v2' | 'v3' | 'v4'
  subject                       TEXT NOT NULL,
  body                          TEXT NOT NULL,
  personalization_instructions  TEXT NOT NULL DEFAULT '',
  approved_at                   TIMESTAMPTZ,
  updated_at                    TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (campaign_id, stage)
);

-- Attachments uploaded once per campaign and reused across all stages (or a
-- subset). Stored as bytea so backups are self-contained; at the volumes here
-- (a handful of product images per campaign) this is fine.
CREATE TABLE IF NOT EXISTS campaign_attachments (
  id               BIGSERIAL PRIMARY KEY,
  campaign_id      BIGINT NOT NULL REFERENCES email_campaigns(id) ON DELETE CASCADE,
  filename         TEXT NOT NULL,
  mime_type        TEXT NOT NULL,
  content_bytes    BYTEA NOT NULL,
  size_bytes       INTEGER NOT NULL,
  apply_to_stages  TEXT[] NOT NULL DEFAULT ARRAY['initial','v1','v2','v3','v4']::text[],
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_campaign_attachments_campaign ON campaign_attachments (campaign_id);

-- Assignment of a creator to a campaign + sequence state. current_stage is the
-- NEXT stage to send. status transitions: active -> replied/bounced/completed/
-- paused. UNIQUE on (handle, campaign_id) means a creator can be in multiple
-- campaigns simultaneously, but only once per campaign.
CREATE TABLE IF NOT EXISTS creator_campaigns (
  id              BIGSERIAL PRIMARY KEY,
  handle          TEXT NOT NULL REFERENCES creators(handle) ON DELETE CASCADE,
  campaign_id     BIGINT NOT NULL REFERENCES email_campaigns(id) ON DELETE CASCADE,
  to_email        TEXT NOT NULL,
  current_stage   TEXT NOT NULL DEFAULT 'initial',          -- next stage to send
  status          TEXT NOT NULL DEFAULT 'active',           -- active | paused | replied | bounced | completed | opted_out | failed
  next_send_at    TIMESTAMPTZ,
  replied_at      TIMESTAMPTZ,
  bounced_at      TIMESTAMPTZ,
  completed_at    TIMESTAMPTZ,
  last_error      TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (handle, campaign_id)
);
CREATE INDEX IF NOT EXISTS idx_creator_campaigns_status ON creator_campaigns (status, next_send_at);
CREATE INDEX IF NOT EXISTS idx_creator_campaigns_handle ON creator_campaigns (handle);

-- Every email queued, sent, or failed. parent_email_id chains follow-ups to
-- their original for SMTP threading via In-Reply-To. message_id is the SMTP
-- Message-ID returned by the server after a successful send.
CREATE TABLE IF NOT EXISTS outreach_emails (
  id                     BIGSERIAL PRIMARY KEY,
  handle                 TEXT NOT NULL REFERENCES creators(handle) ON DELETE CASCADE,
  campaign_id            BIGINT NOT NULL REFERENCES email_campaigns(id),
  stage                  TEXT NOT NULL,
  to_email               TEXT NOT NULL,
  subject                TEXT NOT NULL,
  body                   TEXT NOT NULL,                     -- final rendered body
  personalized_content   TEXT,                              -- the AI-generated portion only (audit)
  message_id             TEXT,                              -- set after successful send
  parent_email_id        BIGINT REFERENCES outreach_emails(id),
  thread_root_id         BIGINT REFERENCES outreach_emails(id),
  sender_id              BIGINT NOT NULL REFERENCES email_senders(id),
  status                 TEXT NOT NULL DEFAULT 'draft',     -- draft | approved | queued | sending | sent | failed | bounced | cancelled
  approved_by_user       BOOLEAN NOT NULL DEFAULT FALSE,
  scheduled_at           TIMESTAMPTZ,
  sent_at                TIMESTAMPTZ,
  error                  TEXT,
  retry_count            INTEGER NOT NULL DEFAULT 0,
  generation_attempts    INTEGER NOT NULL DEFAULT 0,
  created_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_outreach_emails_status_scheduled ON outreach_emails (status, scheduled_at);
CREATE INDEX IF NOT EXISTS idx_outreach_emails_message_id ON outreach_emails (message_id);
CREATE INDEX IF NOT EXISTS idx_outreach_emails_handle ON outreach_emails (handle);
CREATE INDEX IF NOT EXISTS idx_outreach_emails_campaign ON outreach_emails (campaign_id);

-- Anything the IMAP listener pulls in. matched_email_id links inbound messages
-- to the outreach_email they replied to, via In-Reply-To header lookup. is_bounce
-- is set heuristically from subject + sender (mailer-daemon, etc).
CREATE TABLE IF NOT EXISTS inbox_messages (
  id                BIGSERIAL PRIMARY KEY,
  sender_id         BIGINT NOT NULL REFERENCES email_senders(id),
  uid               INTEGER NOT NULL,
  message_id        TEXT,
  in_reply_to       TEXT,
  references_header TEXT,
  from_email        TEXT,
  to_email          TEXT,
  subject           TEXT,
  body_snippet      TEXT,
  is_bounce         BOOLEAN NOT NULL DEFAULT FALSE,
  matched_email_id  BIGINT REFERENCES outreach_emails(id),
  received_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (sender_id, uid)
);
CREATE INDEX IF NOT EXISTS idx_inbox_messages_in_reply_to ON inbox_messages (in_reply_to);
CREATE INDEX IF NOT EXISTS idx_inbox_messages_matched ON inbox_messages (matched_email_id);
`;

// Idempotent column adds for databases that pre-date the newer columns. Postgres 9.6+
// supports IF NOT EXISTS on ADD COLUMN, so these are safe to run on every boot.
const COLUMN_ADDS = [
  `ALTER TABLE jobs ADD COLUMN IF NOT EXISTS rubric JSONB`,
  `ALTER TABLE jobs ADD COLUMN IF NOT EXISTS enable_visual BOOLEAN NOT NULL DEFAULT FALSE`,
  `ALTER TABLE jobs ADD COLUMN IF NOT EXISTS enable_transcripts BOOLEAN NOT NULL DEFAULT FALSE`,
  `ALTER TABLE jobs ADD COLUMN IF NOT EXISTS relevant_hashtags TEXT[]`,
  `ALTER TABLE jobs ADD COLUMN IF NOT EXISTS max_videos_per_creator INTEGER NOT NULL DEFAULT 5`,
  `ALTER TABLE jobs ADD COLUMN IF NOT EXISTS cost_cap_usd NUMERIC(10,4) NOT NULL DEFAULT 10.0000`,
  `ALTER TABLE jobs ADD COLUMN IF NOT EXISTS cost_spent_usd NUMERIC(10,4) NOT NULL DEFAULT 0.0000`,
  `ALTER TABLE jobs ADD COLUMN IF NOT EXISTS cost_paused_at TIMESTAMPTZ`,
  `ALTER TABLE job_creators ADD COLUMN IF NOT EXISTS scrape_attempts INTEGER NOT NULL DEFAULT 0`,
  `ALTER TABLE job_creators ADD COLUMN IF NOT EXISTS eval_attempts INTEGER NOT NULL DEFAULT 0`,
  `ALTER TABLE job_creators ADD COLUMN IF NOT EXISTS visual_attempts INTEGER NOT NULL DEFAULT 0`,
  `ALTER TABLE job_creators ADD COLUMN IF NOT EXISTS transcript_attempts INTEGER NOT NULL DEFAULT 0`,
  `ALTER TABLE job_creators ADD COLUMN IF NOT EXISTS scrape_claimed_at TIMESTAMPTZ`,
  `ALTER TABLE job_creators ADD COLUMN IF NOT EXISTS eval_claimed_at TIMESTAMPTZ`,
  `ALTER TABLE job_creators ADD COLUMN IF NOT EXISTS visual_claimed_at TIMESTAMPTZ`,
  `ALTER TABLE job_creators ADD COLUMN IF NOT EXISTS transcript_claimed_at TIMESTAMPTZ`,
  `ALTER TABLE job_creators ADD COLUMN IF NOT EXISTS visual_state TEXT NOT NULL DEFAULT 'skipped'`,
  `ALTER TABLE job_creators ADD COLUMN IF NOT EXISTS transcript_state TEXT NOT NULL DEFAULT 'skipped'`,
  `ALTER TABLE job_creators ADD COLUMN IF NOT EXISTS rubric_scores JSONB`,
  `ALTER TABLE job_creators ADD COLUMN IF NOT EXISTS match_signals JSONB`,
  `ALTER TABLE job_creators ADD COLUMN IF NOT EXISTS match_sources TEXT[]`,
  `ALTER TABLE job_creators ADD COLUMN IF NOT EXISTS match_cues TEXT[]`,
];

async function migrate() {
  const client = await pool.connect();
  try {
    await client.query(SCHEMA);
    for (const stmt of COLUMN_ADDS) {
      await client.query(stmt);
    }
    console.log("[db] schema ready");
  } finally {
    client.release();
  }
}

module.exports = { pool, migrate };
