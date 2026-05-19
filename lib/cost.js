// lib/cost.js — Cost tracking and auto-pause guardrail.
//
// Every worker calls chargeJob(jobId, amountUsd, label) after each successful
// paid operation. The function atomically increments jobs.cost_spent_usd. If
// the running total reaches or exceeds jobs.cost_cap_usd, the job is flipped
// to status='paused' and a reason is recorded, which stops all four workers
// from claiming new rows for it (they filter on j.status='running').
//
// Per-unit costs are estimates based on real provider pricing as of late 2025.
// Slight drift is fine — the cap is a soft ceiling, not a precise meter.

// Lazy-loaded so this module can be required for cost estimation without
// triggering the pg driver (which fails in environments where pg isn't installed).
let _pool;
function getPool() {
  if (!_pool) {
    _pool = require("../db").pool;
  }
  return _pool;
}

// Per-unit costs (USD). Update these if pricing changes.
const UNIT = {
  // Apify clockworks/tiktok-profile-scraper, ~$0.005 per profile
  BIO_SCRAPE: 0.005,
  // Apify clockworks/free-tiktok-scraper for video listing, per profile call
  VIDEO_LIST: 0.001,
  // OpenAI Whisper: $0.006 per minute. We pass actual duration.
  WHISPER_PER_SECOND: 0.006 / 60,
  // OpenAI gpt-4o-mini vision at "low" detail, 2 images per creator: ~$0.0008
  VISION_PER_CREATOR: 0.0008,
  // Claude Sonnet 4.6 eval: ~$0.001 per creator amortized (20-creator batch ~ $0.02)
  EVAL_PER_CREATOR: 0.001,
};

/**
 * Atomically charge a job, then auto-pause it if the cap has been reached.
 * Returns { spent, cap, paused } so the caller can decide whether to keep going.
 *
 * Safe under concurrent workers — the UPDATE ... RETURNING pattern serializes
 * via Postgres row locking. Two workers charging simultaneously will both
 * succeed; the second one sees the post-first-update value and triggers the
 * pause if the cap is crossed.
 */
async function chargeJob(jobId, amountUsd, label) {
  if (!jobId || !(amountUsd > 0)) return null;
  const pool = getPool();
  const { rows } = await pool.query(
    `UPDATE jobs
     SET cost_spent_usd = cost_spent_usd + $2
     WHERE id=$1
     RETURNING cost_spent_usd, cost_cap_usd, status`,
    [jobId, amountUsd]
  );
  const r = rows[0];
  if (!r) return null;

  const spent = Number(r.cost_spent_usd);
  const cap = Number(r.cost_cap_usd);
  let paused = false;

  if (r.status === "running" && spent >= cap) {
    const upd = await pool.query(
      `UPDATE jobs
       SET status='paused',
           cost_paused_at=now(),
           error=$2
       WHERE id=$1 AND status='running'
       RETURNING id`,
      [
        jobId,
        `Cost cap reached: spent $${spent.toFixed(4)} of $${cap.toFixed(2)} (last charge: ${label || "unknown"})`,
      ]
    );
    if (upd.rowCount > 0) {
      paused = true;
      console.warn(`[cost] Job ${jobId} auto-paused — cap $${cap.toFixed(2)} reached (spent $${spent.toFixed(4)} after ${label})`);
    }
  }

  return { spent, cap, paused };
}

/**
 * Estimate the upper-bound cost of a job given its configuration. Used by the
 * UI to show a "this run could cost up to $X" preview before submission.
 */
function estimatedJobCost(cfg, totalCreators) {
  const n = totalCreators || 0;
  let total = 0;
  // Bio scrape on every creator (assume worst case: nothing cached)
  total += UNIT.BIO_SCRAPE * n;
  // Evaluator on every creator
  total += UNIT.EVAL_PER_CREATOR * n;
  // Vision (optional)
  if (cfg.enable_visual) total += UNIT.VISION_PER_CREATOR * n;
  // Transcripts (optional). Per creator: video listing + ~1.5 transcribed
  // videos at ~45s each, after hashtag filtering. If no hashtag filter, the
  // average climbs but Whisper cost still dominates only modestly.
  if (cfg.enable_transcripts) {
    total += UNIT.VIDEO_LIST * n;
    const avgTranscribedVideos = cfg.relevant_hashtags && cfg.relevant_hashtags.length ? 1.5 : 3.0;
    const avgSeconds = 45;
    total += UNIT.WHISPER_PER_SECOND * avgSeconds * avgTranscribedVideos * n;
  }
  return total;
}

module.exports = { UNIT, chargeJob, estimatedJobCost };
