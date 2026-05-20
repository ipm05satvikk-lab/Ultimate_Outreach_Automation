// workers/scraper.js — Apify-based bio scraper.
//
// Changes from the original:
//   - Atomic claim using FOR UPDATE SKIP LOCKED, so multiple worker instances
//     can run safely without double-calling Apify.
//   - scrape_attempts counter on job_creators with a max-attempts ceiling
//     (default 3). Transient Apify errors are retried instead of dropped.
//   - In-process retry-with-backoff around the Apify call itself, so a single
//     5xx does not consume an attempt.
//   - A "reaper" sweeps any in_flight rows whose claim is older than
//     SCRAPE_CLAIM_TIMEOUT_MS back to pending (handles worker crashes mid-batch).
//   - raw_profile is trimmed to a small subset before persisting, so 10K+
//     creators do not balloon the DB. Set RAW_PROFILE_FULL=true to keep
//     everything (the old behavior).
//   - proxyCountryCode is omitted from the Apify input (the old "None" string
//     was passed through and would be interpreted as a literal country code).
//   - Round-robin across all running jobs instead of strict ORDER BY id LIMIT 1.

const { ApifyClient } = require("apify-client");
const { pool } = require("../db");
const { extractContacts, normalizeText } = require("../lib/extractors");
const { chargeJob, UNIT } = require("../lib/cost");

const APIFY_ACTOR = process.env.APIFY_ACTOR || "clockworks/tiktok-profile-scraper";
const BATCH_SIZE = parseInt(process.env.SCRAPE_BATCH_SIZE || "50", 10);
const REFRESH_DAYS = parseInt(process.env.BIO_REFRESH_DAYS || "30", 10);
const POLL_MS = parseInt(process.env.SCRAPE_POLL_MS || "5000", 10);
const MAX_ATTEMPTS = parseInt(process.env.SCRAPE_MAX_ATTEMPTS || "3", 10);
const CLAIM_TIMEOUT_MS = parseInt(process.env.SCRAPE_CLAIM_TIMEOUT_MS || "600000", 10); // 10 min
const APIFY_TIMEOUT_S = parseInt(process.env.APIFY_TIMEOUT_S || "300", 10);
const RAW_PROFILE_FULL = process.env.RAW_PROFILE_FULL === "true";

let client = null;
function apify() {
  if (!client) {
    if (!process.env.APIFY_TOKEN) throw new Error("APIFY_TOKEN env var is required");
    client = new ApifyClient({ token: process.env.APIFY_TOKEN });
  }
  return client;
}

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

/**
 * Map an Apify profile item (varies by actor) into a uniform shape we store.
 */
function normalizeProfile(item) {
  const author = item.authorMeta || item.author || item.user || item;
  const handle = (author.uniqueId || author.name || author.username || item.uniqueId || "").toLowerCase();
  if (!handle) return null;

  // Trim raw_profile to a useful subset unless RAW_PROFILE_FULL is set.
  let raw;
  if (RAW_PROFILE_FULL) {
    raw = item;
  } else {
    raw = {
      uniqueId: author.uniqueId,
      nickname: author.nickname || author.nickName,
      signature: author.signature || author.bio,
      bioLink: author.bioLink || author.bio_link || author.bioLinkUrl,
      fans: author.fans ?? author.followerCount,
      heart: author.heart ?? author.heartCount,
      video: author.video ?? author.videoCount,
      verified: !!author.verified,
      privateAccount: !!author.privateAccount,
      region: author.region,
      language: author.language,
      ttSeller: !!author.ttSeller,
      avatar: author.avatar || author.avatarLarger || author.avatarMedium,
    };
  }

  return {
    handle,
    bio: normalizeText(author.signature || author.bio || ""),
    bio_link: author.bioLink || author.bio_link || author.bioLinkUrl || null,
    follower_count: author.fans ?? author.followerCount ?? null,
    following_count: author.following ?? author.followingCount ?? null,
    video_count: author.video ?? author.videoCount ?? null,
    verified: !!author.verified,
    nickname: author.nickname || author.nickName || null,
    avatar_url: author.avatar || author.avatarLarger || author.avatarMedium || null,
    region: author.region || null,
    raw,
  };
}

/**
 * Reap: any rows stuck in in_flight beyond CLAIM_TIMEOUT_MS get reset to pending.
 * Cheap to run every loop iteration and protects against crashed workers.
 */
async function reapStaleClaims() {
  await pool.query(
    `UPDATE job_creators
     SET scrape_state='pending'
     WHERE scrape_state='in_flight'
       AND scrape_claimed_at < now() - ($1 || ' milliseconds')::interval`,
    [String(CLAIM_TIMEOUT_MS)]
  );
}

/**
 * Atomically claim a batch of pending handles across ALL running jobs, in
 * round-robin fashion (by lowest job_id first within each pass). Marks the rows
 * scrape_state='in_flight', increments scrape_attempts, and returns them.
 *
 * Uses FOR UPDATE SKIP LOCKED so two workers cannot grab the same row.
 */
async function claimBatch(limit) {
  const { rows } = await pool.query(
    `
    WITH candidate AS (
      SELECT jc.job_id, jc.handle
      FROM job_creators jc
      JOIN jobs j ON j.id = jc.job_id
      WHERE j.status = 'running'
        AND jc.scrape_state = 'pending'
        AND jc.scrape_attempts < $2
      ORDER BY jc.job_id ASC, jc.handle ASC
      LIMIT $1
      FOR UPDATE OF jc SKIP LOCKED
    )
    UPDATE job_creators jc
    SET scrape_state = 'in_flight',
        scrape_attempts = jc.scrape_attempts + 1,
        scrape_claimed_at = now()
    FROM candidate
    WHERE jc.job_id = candidate.job_id AND jc.handle = candidate.handle
    RETURNING jc.job_id, jc.handle, jc.scrape_attempts
    `,
    [limit, MAX_ATTEMPTS]
  );
  return rows;
}

/**
 * For handles already scraped recently we don't need Apify — short-circuit them
 * straight to done. Returns the handles that were short-circuited.
 */
async function shortCircuitCached(claimed) {
  if (!claimed.length) return [];
  const handles = claimed.map((c) => c.handle);
  const { rows } = await pool.query(
    `
    SELECT handle FROM creators
    WHERE handle = ANY($1)
      AND scrape_status = 'scraped'
      AND ($2::int = 0 OR scraped_at > now() - ($2 || ' days')::interval)
    `,
    [handles, REFRESH_DAYS]
  );
  const cached = new Set(rows.map((r) => r.handle));
  if (!cached.size) return [];

  // Mark each cached claim as done within its specific job.
  const cachedClaims = claimed.filter((c) => cached.has(c.handle));
  for (const cc of cachedClaims) {
    await pool.query(
      `UPDATE job_creators SET scrape_state='done', scrape_claimed_at=NULL
       WHERE job_id=$1 AND handle=$2`,
      [cc.job_id, cc.handle]
    );
  }
  return cachedClaims.map((c) => c.handle);
}

/**
 * Call Apify with retry-with-backoff. Each attempt is one Apify run; the loop
 * retries up to 3 times on transient failures (network, 5xx). The caller's
 * scrape_attempts counter is unaffected unless every retry fails.
 */
async function runApifyForHandles(handles) {
  const profiles = handles.map((h) => `https://www.tiktok.com/@${h}`);
  const input = {
    profiles,
    resultsPerPage: 1,
    shouldDownloadVideos: false,
    shouldDownloadCovers: false,
    shouldDownloadSubtitles: false,
  };
  // Allow overriding proxy country via env (e.g. APIFY_PROXY_COUNTRY=US). Default: omit.
  if (process.env.APIFY_PROXY_COUNTRY) {
    input.proxyCountryCode = process.env.APIFY_PROXY_COUNTRY;
  }

  const maxRetries = 3;
  let lastErr;
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
     const run = await apify().actor(APIFY_ACTOR).call(input, { waitSecs: APIFY_TIMEOUT_S });
      const items = [];
      for await (const item of apify().dataset(run.defaultDatasetId).iterate()) {
        items.push(item);
      }
      return items;
    } catch (e) {
      lastErr = e;
      const transient = /timeout|ECONN|ETIMEDOUT|ENOTFOUND|fetch failed|5\d\d/i.test(String(e?.message || e));
      if (!transient || attempt === maxRetries) throw e;
      const backoff = 1000 * Math.pow(4, attempt - 1); // 1s, 4s, 16s
      console.warn(`[scraper] Apify call failed (attempt ${attempt}/${maxRetries}): ${e.message}. Retrying in ${backoff}ms…`);
      await sleep(backoff);
    }
  }
  throw lastErr;
}

async function persistProfile(p) {
  const c = await pool.connect();
  try {
    await c.query("BEGIN");
    await c.query(
      `
      INSERT INTO creators (handle, bio, bio_link, follower_count, following_count,
                            video_count, verified, nickname, avatar_url, region,
                            raw_profile, scrape_status, scrape_error, scraped_at, updated_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'scraped',NULL,now(),now())
      ON CONFLICT (handle) DO UPDATE SET
        bio = EXCLUDED.bio,
        bio_link = EXCLUDED.bio_link,
        follower_count = EXCLUDED.follower_count,
        following_count = EXCLUDED.following_count,
        video_count = EXCLUDED.video_count,
        verified = EXCLUDED.verified,
        nickname = EXCLUDED.nickname,
        avatar_url = EXCLUDED.avatar_url,
        region = EXCLUDED.region,
        raw_profile = EXCLUDED.raw_profile,
        scrape_status = 'scraped',
        scrape_error = NULL,
        scraped_at = now(),
        updated_at = now()
      `,
      [
        p.handle, p.bio, p.bio_link, p.follower_count, p.following_count,
        p.video_count, p.verified, p.nickname, p.avatar_url, p.region, p.raw,
      ]
    );

    const contacts = extractContacts(p.bio, p.bio_link);
    for (const ct of contacts) {
      await c.query(
        `INSERT INTO contacts (handle, kind, value, source)
         VALUES ($1,$2,$3,$4)
         ON CONFLICT (handle, kind, value) DO NOTHING`,
        [p.handle, ct.kind, ct.value, ct.source]
      );
    }

    // If this profile has a bio_link, queue it for the linktree worker. The
    // worker decides whether the domain is on the allowlist; queuing here is cheap.
    if (p.bio_link) {
      await c.query(
        `INSERT INTO link_fetches (handle, url, status)
         VALUES ($1, $2, 'pending')
         ON CONFLICT (handle, url) DO NOTHING`,
        [p.handle, String(p.bio_link)]
      );
    }

    await c.query("COMMIT");
  } catch (e) {
    await c.query("ROLLBACK");
    throw e;
  } finally {
    c.release();
  }
}

/**
 * Release a claim back to pending so it can be retried (when we still have attempts left).
 */
async function releaseClaim(jobId, handle) {
  await pool.query(
    `UPDATE job_creators
     SET scrape_state = CASE WHEN scrape_attempts >= $3 THEN 'failed' ELSE 'pending' END,
         scrape_claimed_at = NULL
     WHERE job_id=$1 AND handle=$2`,
    [jobId, handle, MAX_ATTEMPTS]
  );
}

async function markNotFound(handles, claimed, reason) {
  if (!handles.length) return;
  // Update creators table for any never-seen handles
  await pool.query(
    `INSERT INTO creators (handle, scrape_status, scrape_error, scraped_at)
     SELECT unnest($1::text[]), 'not_found', $2, now()
     ON CONFLICT (handle) DO UPDATE SET
       scrape_status = CASE WHEN creators.scrape_status='scraped' THEN 'scraped' ELSE 'not_found' END,
       scrape_error  = EXCLUDED.scrape_error,
       updated_at    = now()`,
    [handles, reason]
  );
  // Update job_creators: not_found is terminal (no retry — Apify won't suddenly find them).
  for (const c of claimed.filter((c) => handles.includes(c.handle))) {
    await pool.query(
      `UPDATE job_creators SET scrape_state='failed', scrape_claimed_at=NULL
       WHERE job_id=$1 AND handle=$2`,
      [c.job_id, c.handle]
    );
  }
}

async function refreshScrapedCounts(jobIds) {
  for (const id of jobIds) {
    await pool.query(
      `UPDATE jobs SET scraped_count = (
         SELECT count(*) FROM job_creators
         WHERE job_id=$1 AND scrape_state IN ('done','failed','skipped')
       ) WHERE id=$1`,
      [id]
    );
  }
}

async function processBatch() {
  await reapStaleClaims();
  const claimed = await claimBatch(BATCH_SIZE);
  if (!claimed.length) return 0;

  const jobIds = Array.from(new Set(claimed.map((c) => c.job_id)));

  // Cache short-circuit
  const cachedHandles = await shortCircuitCached(claimed);
  const cachedSet = new Set(cachedHandles);
  const toScrape = claimed.filter((c) => !cachedSet.has(c.handle));

  if (!toScrape.length) {
    await refreshScrapedCounts(jobIds);
    return claimed.length;
  }

  // Group by job for the Apify call (we batch handles together since Apify
  // doesn't care which job they belong to).
  const handlesToScrape = Array.from(new Set(toScrape.map((c) => c.handle)));

  let items = [];
  try {
    items = await runApifyForHandles(handlesToScrape);
  } catch (e) {
    console.error(`[scraper] Apify call ultimately failed for ${handlesToScrape.length} handles:`, e.message);
    // Release each claim back to pending (or failed if attempts exhausted)
    for (const c of toScrape) {
      await releaseClaim(c.job_id, c.handle);
      await pool.query(
        `UPDATE creators SET scrape_error=$1, updated_at=now() WHERE handle=$2`,
        [String(e.message).slice(0, 500), c.handle]
      );
    }
    await refreshScrapedCounts(jobIds);
    return claimed.length;
  }

  const profiles = items.map(normalizeProfile).filter(Boolean);
  const returnedHandles = new Set(profiles.map((p) => p.handle));

  // Persist what we got and mark each affected job_creators row as done.
  for (const p of profiles) {
    try {
      await persistProfile(p);
    } catch (e) {
      console.error(`[scraper] persist failed for ${p.handle}:`, e.message);
      // Release every claim for this handle across all claimed jobs
      for (const c of toScrape.filter((c) => c.handle === p.handle)) {
        await releaseClaim(c.job_id, c.handle);
      }
      continue;
    }
    for (const c of toScrape.filter((c) => c.handle === p.handle)) {
      await pool.query(
        `UPDATE job_creators SET scrape_state='done', scrape_claimed_at=NULL
         WHERE job_id=$1 AND handle=$2`,
        [c.job_id, c.handle]
      );
      // Each job that needed this profile is charged once for the bio scrape.
      // A single Apify call billed our account once, but jobs that re-use a
      // newly-scraped profile each pay their share so per-job budgets reflect
      // reality. (Cached short-circuits don't reach this code path.)
      await chargeJob(c.job_id, UNIT.BIO_SCRAPE, `bio_scrape @${p.handle}`);
    }
  }

  // Anything we asked for but didn't get back = not_found (terminal)
  const missing = handlesToScrape.filter((h) => !returnedHandles.has(h));
  if (missing.length) {
    await markNotFound(missing, toScrape, "profile not returned by Apify");
  }

  await refreshScrapedCounts(jobIds);
  return claimed.length;
}

async function loop() {
  if (!process.env.APIFY_TOKEN) {
    console.warn("[scraper] APIFY_TOKEN not set — scraper loop will idle");
    setTimeout(loop, POLL_MS * 6);
    return;
  }
  try {
    await processBatch();
  } catch (e) {
    console.error("[scraper] loop error:", e);
  } finally {
    setTimeout(loop, POLL_MS);
  }
}

function start() {
  console.log(`[scraper] starting — actor=${APIFY_ACTOR} batch=${BATCH_SIZE} poll=${POLL_MS}ms max_attempts=${MAX_ATTEMPTS}`);
  loop();
}

module.exports = { start };
