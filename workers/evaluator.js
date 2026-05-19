// workers/evaluator.js — Claude-based matching.
//
// Changes from the original:
//   - Atomic claim using FOR UPDATE SKIP LOCKED (matches scraper.js).
//   - eval_attempts counter on job_creators with a max-attempts ceiling.
//   - Retry-with-backoff around the Anthropic call so a single overload/5xx
//     does not consume an attempt for 20 creators.
//   - Reaper for stuck in_flight rows.
//   - Optional rubric: when jobs.rubric is set, Claude scores each creator on
//     each rubric dimension (returning a JSONB { dim_name: score, ... }) in
//     addition to the match boolean. The verdict format stays the same.
//   - Robust handle matching: Claude's returned "handle" gets normalized
//     (lowercased, leading @ stripped) before lookup.
//   - Round-robin across running jobs.

const Anthropic = require("@anthropic-ai/sdk");
const { pool } = require("../db");
const { normalizeHandle } = require("../lib/extractors");
const { chargeJob, UNIT } = require("../lib/cost");

const MODEL = process.env.ANTHROPIC_MODEL || "claude-sonnet-4-6";
const BATCH = parseInt(process.env.EVAL_BATCH_SIZE || "20", 10);
const POLL_MS = parseInt(process.env.EVAL_POLL_MS || "4000", 10);
const MAX_ATTEMPTS = parseInt(process.env.EVAL_MAX_ATTEMPTS || "3", 10);
const CLAIM_TIMEOUT_MS = parseInt(process.env.EVAL_CLAIM_TIMEOUT_MS || "600000", 10);
const ANTHROPIC_MAX_TOKENS = parseInt(process.env.ANTHROPIC_MAX_TOKENS || "4096", 10);

let anthropic = null;
function client() {
  if (!anthropic) {
    if (!process.env.ANTHROPIC_API_KEY) throw new Error("ANTHROPIC_API_KEY env var is required");
    anthropic = new Anthropic.default({ apiKey: process.env.ANTHROPIC_API_KEY });
  }
  return anthropic;
}

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

async function reapStaleClaims() {
  await pool.query(
    `UPDATE job_creators
     SET eval_state='pending'
     WHERE eval_state='in_flight'
       AND eval_claimed_at < now() - ($1 || ' milliseconds')::interval`,
    [String(CLAIM_TIMEOUT_MS)]
  );
}

/**
 * Atomically claim a batch of creators ready for evaluation, across all running
 * jobs. Joins to creators to pull bio + meta for the prompt in one round trip.
 *
 * When a job has enable_visual=true, we also require visual_state to be in a
 * terminal state (done/skipped/failed) before evaluating, so the evaluator sees
 * the vision verdict and can roll it into match_signals.
 */
async function claimEvalBatch(limit) {
  const { rows } = await pool.query(
    `
    WITH candidate AS (
      SELECT jc.job_id, jc.handle
      FROM job_creators jc
      JOIN jobs j ON j.id = jc.job_id
      JOIN creators c ON c.handle = jc.handle
      WHERE j.status = 'running'
        AND jc.scrape_state = 'done'
        AND jc.eval_state   = 'pending'
        AND jc.eval_attempts < $2
        AND c.scrape_status = 'scraped'
        AND (j.enable_visual     = FALSE OR jc.visual_state     IN ('done','skipped','failed'))
        AND (j.enable_transcripts = FALSE OR jc.transcript_state IN ('done','skipped','failed'))
      ORDER BY jc.job_id ASC, jc.handle ASC
      LIMIT $1
      FOR UPDATE OF jc SKIP LOCKED
    )
    UPDATE job_creators jc
    SET eval_state='in_flight',
        eval_attempts = jc.eval_attempts + 1,
        eval_claimed_at = now()
    FROM candidate
    WHERE jc.job_id=candidate.job_id AND jc.handle=candidate.handle
    RETURNING jc.job_id, jc.handle
    `,
    [limit, MAX_ATTEMPTS]
  );
  if (!rows.length) return [];

  // Fetch creator data for the claimed rows, including up to 3 recent transcripts.
  const handles = Array.from(new Set(rows.map((r) => r.handle)));
  const { rows: creatorRows } = await pool.query(
    `
    SELECT c.handle, c.bio, c.nickname, c.follower_count, c.video_count, c.region,
           array_agg(DISTINCT ct.value) FILTER (WHERE ct.kind='link') AS links,
           array_agg(DISTINCT ct.value) FILTER (WHERE ct.kind='email') AS emails,
           (
             SELECT json_agg(json_build_object(
               'caption', v.caption,
               'hashtags', v.hashtags,
               'transcript', v.transcript
             ) ORDER BY v.posted_at DESC NULLS LAST)
             FROM (
               SELECT caption, hashtags, transcript, posted_at
               FROM videos
               WHERE handle = c.handle AND transcript_status='done' AND transcript IS NOT NULL
               ORDER BY posted_at DESC NULLS LAST
               LIMIT 3
             ) v
           ) AS transcripts
    FROM creators c
    LEFT JOIN contacts ct ON ct.handle = c.handle
    WHERE c.handle = ANY($1)
    GROUP BY c.handle
    `,
    [handles]
  );
  const byHandle = new Map(creatorRows.map((r) => [r.handle, r]));
  return rows.map((r) => ({ job_id: r.job_id, ...(byHandle.get(r.handle) || { handle: r.handle }) }));
}

function buildPrompt(description, rubric, creators) {
  const items = creators
    .map((c, i) => {
      const links = (c.links || []).filter(Boolean).slice(0, 3).join(", ");
      const emails = (c.emails || []).filter(Boolean).slice(0, 2).join(", ");
      const transcripts = Array.isArray(c.transcripts) ? c.transcripts.filter(Boolean) : [];
      const transcriptBlock = transcripts.length
        ? "\nRecent video transcripts:\n" + transcripts
            .map((t, j) => `  [${j + 1}] caption: ${(t.caption || "").slice(0, 120)}\n      hashtags: ${(t.hashtags || []).join(", ").slice(0, 200)}\n      transcript: ${(t.transcript || "").slice(0, 600)}`)
            .join("\n")
        : "";
      return `[${i + 1}] @${c.handle}
nickname: ${c.nickname || ""}
followers: ${c.follower_count ?? "?"}
region: ${c.region || ""}
bio: ${c.bio ? c.bio.replace(/\s+/g, " ").slice(0, 600) : "(empty)"}
links: ${links || "(none)"}
emails: ${emails || "(none)"}${transcriptBlock}`;
    })
    .join("\n\n");

  const dims = rubric && typeof rubric === "object" ? Object.keys(rubric) : [];
  const rubricBlock = dims.length
    ? `

RUBRIC: for each creator also score the following dimensions 0-5 (0 = no signal, 5 = strong signal). Definitions:
${dims.map((d) => `  - ${d}: ${rubric[d]}`).join("\n")}
Include them in each verdict as a "scores" object with one key per dimension.`
    : "";

  const schemaBlock = dims.length
    ? `[
  {"i": 1, "handle": "...", "bio_match": true|false, "bio_cues": ["..."], "transcript_match": true|false|null, "transcript_cues": ["..."], "confidence": 0.0-1.0, "reason": "one short sentence", "scores": {${dims.map((d) => `"${d}": 0-5`).join(", ")}}}
]`
    : `[
  {"i": 1, "handle": "...", "bio_match": true|false, "bio_cues": ["..."], "transcript_match": true|false|null, "transcript_cues": ["..."], "confidence": 0.0-1.0, "reason": "one short sentence"}
]`;

  return `You are screening TikTok creators against a target description.

TARGET DESCRIPTION:
"""
${description}
"""${rubricBlock}

For each numbered creator below, evaluate the BIO and the TRANSCRIPTS (when present) SEPARATELY. Be strict — only mark a signal match=true when there is clear evidence in that specific signal.

- bio_match: true if their bio text indicates a strong fit. bio_cues = the specific short phrases from the BIO that drove your decision (≤4 words each, max 6).
- transcript_match: true if any recent transcript indicates a strong fit. transcript_cues = specific short phrases from the TRANSCRIPTS (NOT bio). If no transcripts are provided for this creator, set transcript_match=null and transcript_cues=[].
- confidence: your overall 0.0-1.0 confidence in the strongest positive signal.
- reason: one short sentence summarizing the decision.

If both signals say false, return false for both with empty cues arrays.

Return ONLY a JSON array, one element per creator, in the same order. No prose. Schema:
${schemaBlock}

CREATORS:
${items}`;
}

function parseClaudeJson(text) {
  const cleaned = text
    .replace(/^[\s\S]*?(\[)/, "$1")
    .replace(/```json|```/g, "")
    .trim();
  try {
    return JSON.parse(cleaned);
  } catch {
    const m = cleaned.match(/\[[\s\S]*\]/);
    if (m) {
      try { return JSON.parse(m[0]); } catch {}
    }
  }
  return null;
}

async function callAnthropic(prompt) {
  const maxRetries = 3;
  let lastErr;
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const resp = await client().messages.create({
        model: MODEL,
        max_tokens: ANTHROPIC_MAX_TOKENS,
        messages: [{ role: "user", content: prompt }],
      });
      return resp.content
        .filter((b) => b.type === "text")
        .map((b) => b.text)
        .join("\n");
    } catch (e) {
      lastErr = e;
      const status = e?.status || e?.response?.status;
      const transient = status === 429 || status === 529 || (status >= 500 && status < 600) ||
        /overloaded|timeout|ECONN|ETIMEDOUT/i.test(String(e?.message || ""));
      if (!transient || attempt === maxRetries) throw e;
      const backoff = 2000 * Math.pow(3, attempt - 1); // 2s, 6s, 18s
      console.warn(`[evaluator] Anthropic call failed (attempt ${attempt}/${maxRetries}, status=${status}): ${e.message}. Retrying in ${backoff}ms…`);
      await sleep(backoff);
    }
  }
  throw lastErr;
}

async function releaseEvalClaim(jobId, handle) {
  await pool.query(
    `UPDATE job_creators
     SET eval_state = CASE WHEN eval_attempts >= $3 THEN 'failed' ELSE 'pending' END,
         eval_claimed_at = NULL
     WHERE job_id=$1 AND handle=$2`,
    [jobId, handle, MAX_ATTEMPTS]
  );
}

async function evaluateOneJobBatch(jobId, description, rubric, creators) {
  const prompt = buildPrompt(description, rubric, creators);
  let text = "";
  try {
    text = await callAnthropic(prompt);
  } catch (e) {
    console.error(`[evaluator] Anthropic ultimately failed:`, e.message);
    for (const c of creators) {
      await releaseEvalClaim(jobId, c.handle);
    }
    return 0;
  }

  const verdicts = parseClaudeJson(text);
  if (!Array.isArray(verdicts)) {
    console.error(`[evaluator] Could not parse JSON. First 500 chars:`, text.slice(0, 500));
    for (const c of creators) {
      await releaseEvalClaim(jobId, c.handle);
    }
    return 0;
  }

  // Build a normalized handle map. Strip leading @ and lowercase.
  const byHandle = new Map();
  for (const v of verdicts) {
    const h = normalizeHandle(v.handle);
    if (h) byHandle.set(h, v);
  }

  let matched = 0;
  for (const c of creators) {
    const v = byHandle.get(c.handle);
    if (!v) {
      // Try matching by index as a fallback if Claude returned same order but
      // mangled the handle (extra punctuation, accidental quoting, etc.)
      const idx = creators.indexOf(c);
      const byIndex = verdicts[idx];
      if (!byIndex) {
        await releaseEvalClaim(jobId, c.handle);
        continue;
      }
      await writeVerdict(jobId, c.handle, byIndex);
      if (byIndex.match) matched++;
      continue;
    }
    await writeVerdict(jobId, c.handle, v);
    if (v.match) matched++;
  }
  return matched;
}

async function writeVerdict(jobId, handle, v) {
  // Bio signal from Claude's text evaluation.
  const bioMatched = v.bio_match === true;
  const bioConf = Math.max(0, Math.min(1, Number(v.confidence) || 0));
  const bioCues = Array.isArray(v.bio_cues)
    ? v.bio_cues.map((s) => String(s).slice(0, 80)).filter(Boolean).slice(0, 8)
    : [];

  // Transcript signal from Claude's text evaluation, when transcripts existed.
  // transcript_match=null means no transcripts were available.
  const transcriptPresent = v.transcript_match !== null && v.transcript_match !== undefined;
  const transcriptMatched = v.transcript_match === true;
  const transcriptCues = Array.isArray(v.transcript_cues)
    ? v.transcript_cues.map((s) => String(s).slice(0, 80)).filter(Boolean).slice(0, 8)
    : [];

  // Visual signal: pulled from creator_visual_analyses.
  const { rows: visRows } = await pool.query(
    `SELECT matched, confidence, cues, likely_role, reason
     FROM creator_visual_analyses
     WHERE handle=$1 AND job_id=$2
     LIMIT 1`,
    [handle, jobId]
  );
  const vis = visRows[0] || null;

  // Build the structured per-signal record.
  const signals = {
    bio: {
      matched: bioMatched,
      confidence: bioConf,
      cues: bioCues,
    },
  };
  if (transcriptPresent) {
    signals.transcript = {
      matched: transcriptMatched,
      confidence: bioConf, // we use one overall confidence number from Claude
      cues: transcriptCues,
    };
  }
  if (vis) {
    signals.visual = {
      matched: vis.matched === true,
      confidence: vis.confidence,
      cues: Array.isArray(vis.cues) ? vis.cues : [],
      likely_role: vis.likely_role || null,
    };
  }

  // Rollup: matched = ANY source matched.
  const sources = [];
  const allCues = [];
  if (signals.bio.matched) { sources.push("bio"); allCues.push(...bioCues); }
  if (signals.transcript?.matched) { sources.push("transcript"); allCues.push(...transcriptCues); }
  if (signals.visual?.matched) { sources.push("visual"); allCues.push(...signals.visual.cues); }
  const overallMatched = sources.length > 0;

  // Overall confidence: prefer bio numeric. Fall back to visual mapping if only
  // visual matched.
  let overallConf = bioConf;
  if (!signals.bio.matched && !signals.transcript?.matched && signals.visual?.matched) {
    overallConf = ({ high: 0.9, medium: 0.7, low: 0.5 }[String(signals.visual.confidence || "").toLowerCase()] || 0.6);
  }

  let reason = String(v.reason || "").slice(0, 250);
  if (signals.visual?.matched && vis?.reason) {
    reason = (overallMatched ? reason + " | " : "") + "VISUAL: " + String(vis.reason).slice(0, 200);
  }

  const scores = v.scores && typeof v.scores === "object" ? JSON.stringify(v.scores) : null;

  await pool.query(
    `UPDATE job_creators
     SET eval_state='done',
         eval_claimed_at=NULL,
         evaluated_at=now(),
         matched=$1,
         confidence=$2,
         reason=$3,
         rubric_scores=$4::jsonb,
         match_signals=$5::jsonb,
         match_sources=$6::text[],
         match_cues=$7::text[]
     WHERE job_id=$8 AND handle=$9`,
    [
      overallMatched,
      overallConf,
      reason.slice(0, 500),
      scores,
      JSON.stringify(signals),
      sources,
      Array.from(new Set(allCues.map((s) => String(s).slice(0, 80)))).slice(0, 20),
      jobId,
      handle,
    ]
  );

  // Charge for the eval. Charge happens after a successful verdict write so
  // failures (which don't write) don't bill.
  await chargeJob(jobId, UNIT.EVAL_PER_CREATOR, `eval @${handle}`);
}

async function refreshJobCounts(jobIds) {
  for (const id of jobIds) {
    await pool.query(
      `UPDATE jobs SET
         evaluated_count = (SELECT count(*) FROM job_creators WHERE job_id=$1 AND eval_state IN ('done','failed','skipped')),
         matched_count   = (SELECT count(*) FROM job_creators WHERE job_id=$1 AND matched IS TRUE)
       WHERE id=$1`,
      [id]
    );
  }
}

async function tryFinishJobs(jobIds) {
  for (const id of jobIds) {
    const { rows } = await pool.query(
      `SELECT j.enable_visual, j.enable_transcripts,
              count(jc.*) FILTER (WHERE jc.scrape_state IN ('pending','in_flight')) AS pending_scrape,
              count(jc.*) FILTER (WHERE jc.scrape_state='done' AND jc.eval_state IN ('pending','in_flight')) AS pending_eval,
              count(jc.*) FILTER (WHERE jc.visual_state     IN ('pending','in_flight')) AS pending_visual,
              count(jc.*) FILTER (WHERE jc.transcript_state IN ('pending','in_flight')) AS pending_transcript
       FROM jobs j
       LEFT JOIN job_creators jc ON jc.job_id=j.id
       WHERE j.id=$1
       GROUP BY j.id, j.enable_visual, j.enable_transcripts`,
      [id]
    );
    const r = rows[0];
    if (!r) continue;
    const visualReady     = !r.enable_visual     || Number(r.pending_visual)     === 0;
    const transcriptReady = !r.enable_transcripts || Number(r.pending_transcript) === 0;
    if (Number(r.pending_scrape) === 0 && Number(r.pending_eval) === 0 && visualReady && transcriptReady) {
      await pool.query(
        `UPDATE jobs SET status='done', finished_at=now() WHERE id=$1 AND status='running'`,
        [id]
      );
    }
  }
}

async function processBatch() {
  await reapStaleClaims();

  // Group claimed rows by job for prompt building (rubric/description are per-job).
  const claimed = await claimEvalBatch(BATCH);
  if (!claimed.length) {
    // Nothing in flight — check whether any running jobs are actually finished.
    const { rows } = await pool.query(`SELECT id FROM jobs WHERE status='running'`);
    if (rows.length) {
      await refreshJobCounts(rows.map((r) => r.id));
      await tryFinishJobs(rows.map((r) => r.id));
    }
    return 0;
  }

  // Pull job descriptions + rubrics for every job we touched
  const jobIds = Array.from(new Set(claimed.map((c) => c.job_id)));
  const { rows: jobs } = await pool.query(
    `SELECT id, description, rubric FROM jobs WHERE id = ANY($1)`,
    [jobIds]
  );
  const jobMap = new Map(jobs.map((j) => [Number(j.id), j]));

  // Evaluate one job at a time (different jobs may have different prompts).
  const byJob = new Map();
  for (const c of claimed) {
    const list = byJob.get(c.job_id) || [];
    list.push(c);
    byJob.set(c.job_id, list);
  }
  for (const [jobId, creators] of byJob) {
    const job = jobMap.get(Number(jobId));
    if (!job) continue;
    await evaluateOneJobBatch(jobId, job.description, job.rubric, creators);
  }

  await refreshJobCounts(jobIds);
  await tryFinishJobs(jobIds);
  return claimed.length;
}

async function loop() {
  if (!process.env.ANTHROPIC_API_KEY) {
    console.warn("[evaluator] ANTHROPIC_API_KEY not set — evaluator loop will idle");
    setTimeout(loop, POLL_MS * 6);
    return;
  }
  try {
    await processBatch();
  } catch (e) {
    console.error("[evaluator] loop error:", e);
  } finally {
    setTimeout(loop, POLL_MS);
  }
}

function start() {
  console.log(`[evaluator] starting — model=${MODEL} batch=${BATCH} poll=${POLL_MS}ms max_attempts=${MAX_ATTEMPTS}`);
  loop();
}

module.exports = { start };
