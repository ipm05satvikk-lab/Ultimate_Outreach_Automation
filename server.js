// server.js — Express app + worker boot.
//
// Changes from the original:
//   - HTTP Basic Auth via BASIC_AUTH_USER / BASIC_AUTH_PASS. If unset, the
//     server prints a loud warning and runs without auth (for local dev only).
//     /healthz is always reachable without auth.
//   - In-memory rate limiting: 120 req/min per IP for read endpoints,
//     10 jobs/hour per IP for POST /api/jobs (the expensive one).
//   - PATCH /api/creators/:handle now supports explicit null clearing via the
//     "__clear" sentinel object, and merges into the custom JSONB blob instead
//     of replacing it. Pass __replace_custom: true to keep the old replace
//     semantics if you really want.
//   - jobs.rubric: POST /api/jobs accepts a rubric object alongside description.
//   - CSV export cap raised to 1,000,000 rows (still hard-capped to avoid OOM).
//   - linktree worker is started alongside scraper and evaluator.

const express = require("express");
const multer = require("multer");
const path = require("path");
const crypto = require("crypto");
const { pool, migrate } = require("./db");
const scraper = require("./workers/scraper");
const evaluator = require("./workers/evaluator");
const linktree = require("./workers/linktree");
const vision = require("./workers/vision");
const videos = require("./workers/videos");
const emailGen = require("./workers/email_generator");
const emailSend = require("./workers/email_sender");
const imapListener = require("./workers/imap_listener");
const { estimatedJobCost } = require("./lib/cost");
const { generateEmail } = require("./lib/email_render");
const { verifyCredentials, invalidateTransporter } = require("./lib/smtp");

const app = express();
const upload = multer({ limits: { fileSize: 10 * 1024 * 1024 } });

app.use(express.json({ limit: "5mb" }));

// ---- Basic Auth -------------------------------------------------------------
const BASIC_USER = process.env.BASIC_AUTH_USER || "";
const BASIC_PASS = process.env.BASIC_AUTH_PASS || "";
const AUTH_ENABLED = !!BASIC_PASS;

function constantTimeEqual(a, b) {
  const ab = Buffer.from(a);
  const bb = Buffer.from(b);
  if (ab.length !== bb.length) return false;
  return crypto.timingSafeEqual(ab, bb);
}

function basicAuth(req, res, next) {
  if (!AUTH_ENABLED) return next();
  if (req.path === "/healthz") return next();
  const hdr = req.headers.authorization || "";
  if (!hdr.startsWith("Basic ")) {
    res.set("WWW-Authenticate", 'Basic realm="creator-scraper"');
    return res.status(401).send("auth required");
  }
  let decoded = "";
  try {
    decoded = Buffer.from(hdr.slice(6), "base64").toString("utf8");
  } catch {
    return res.status(401).send("bad auth header");
  }
  const idx = decoded.indexOf(":");
  if (idx < 0) return res.status(401).send("bad auth header");
  const u = decoded.slice(0, idx);
  const p = decoded.slice(idx + 1);
  if (constantTimeEqual(u, BASIC_USER) && constantTimeEqual(p, BASIC_PASS)) return next();
  res.set("WWW-Authenticate", 'Basic realm="creator-scraper"');
  return res.status(401).send("invalid credentials");
}

app.use(basicAuth);

// ---- Rate limiting ----------------------------------------------------------
// Two buckets per IP:
//   read   - 120 req/min, applied to all GET requests
//   jobs   - 10 req/hour, applied specifically to POST /api/jobs
// Buckets reset by sliding window of timestamps.
const READ_LIMIT = parseInt(process.env.RATE_LIMIT_READ_PER_MIN || "120", 10);
const JOB_LIMIT = parseInt(process.env.RATE_LIMIT_JOBS_PER_HOUR || "10", 10);
const readBuckets = new Map();
const jobBuckets = new Map();

function rateLimit(bucketMap, windowMs, limit, key) {
  const now = Date.now();
  const arr = (bucketMap.get(key) || []).filter((t) => now - t < windowMs);
  if (arr.length >= limit) {
    bucketMap.set(key, arr);
    return false;
  }
  arr.push(now);
  bucketMap.set(key, arr);
  return true;
}

function ipOf(req) {
  return (req.headers["x-forwarded-for"] || "").split(",")[0].trim() || req.ip || "unknown";
}

app.use((req, res, next) => {
  if (req.method === "GET" && req.path.startsWith("/api/")) {
    if (!rateLimit(readBuckets, 60_000, READ_LIMIT, ipOf(req))) {
      return res.status(429).json({ error: "rate limit (read)" });
    }
  }
  if (req.method === "POST" && req.path === "/api/jobs") {
    if (!rateLimit(jobBuckets, 3_600_000, JOB_LIMIT, ipOf(req))) {
      return res.status(429).json({ error: "rate limit (jobs)" });
    }
  }
  next();
});

// Static UI (after auth, so the HTML is also protected)
app.use(express.static(path.join(__dirname, "public")));

// ---- helpers ----------------------------------------------------------------
function cleanHandle(h) {
  return String(h || "")
    .trim()
    .replace(/^https?:\/\/(www\.)?tiktok\.com\//i, "")
    .replace(/^@/, "")
    .replace(/\/.*$/, "")
    .replace(/\?.*$/, "")
    .toLowerCase();
}

function parseHandleList(body, fileBuf) {
  const raw = [];
  if (Array.isArray(body.handles)) raw.push(...body.handles);
  if (typeof body.handles === "string") raw.push(...body.handles.split(/[\s,]+/));
  if (fileBuf) raw.push(...fileBuf.toString("utf8").split(/[\s,]+/));
  const cleaned = raw.map(cleanHandle).filter((h) => h && /^[a-z0-9._]{2,30}$/.test(h));
  return Array.from(new Set(cleaned));
}

function csvEscape(v) {
  if (v === null || v === undefined) return "";
  const s = String(v).replace(/\r?\n/g, " ");
  return /[",]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

function parseRubric(input) {
  if (!input) return null;
  let obj = input;
  if (typeof input === "string") {
    const s = input.trim();
    if (!s) return null;
    try {
      obj = JSON.parse(s);
    } catch {
      return null;
    }
  }
  if (typeof obj !== "object" || Array.isArray(obj)) return null;
  // Normalize: { dim: "definition", ... } with reasonable bounds.
  const out = {};
  for (const k of Object.keys(obj).slice(0, 12)) {
    const def = obj[k];
    if (typeof def === "string" && def.length <= 300) {
      out[k.slice(0, 50)] = def;
    }
  }
  return Object.keys(out).length ? out : null;
}

// ---- Hashtag expansion (Claude pre-pass) -----------------------------------
// When the user gives a seed hashtag list, ask Claude to add 5-15 related ones.
// Cheap (one call per job) and improves transcript filtering recall.
async function expandHashtagsViaClaude(seed, description) {
  if (!process.env.ANTHROPIC_API_KEY) return seed;
  if (!seed || !seed.length) return [];
  try {
    const Anthropic = require("@anthropic-ai/sdk");
    const client = new Anthropic.default({ apiKey: process.env.ANTHROPIC_API_KEY });
    const resp = await client.messages.create({
      model: process.env.ANTHROPIC_MODEL || "claude-sonnet-4-6",
      max_tokens: 400,
      messages: [{
        role: "user",
        content: `Given these starting hashtags: ${JSON.stringify(seed)}\nAnd this target description:\n"""${description}"""\n\nReturn 5-15 additional related hashtags that TikTok creators in this niche likely use. Return ONLY a JSON array of strings, lowercase, no leading #. Example: ["nurselife","scrublife","nightshift"]`,
      }],
    });
    const text = resp.content.filter((b) => b.type === "text").map((b) => b.text).join("");
    const m = text.match(/\[[\s\S]*?\]/);
    if (!m) return seed;
    const expanded = JSON.parse(m[0]);
    if (!Array.isArray(expanded)) return seed;
    const merged = Array.from(new Set([
      ...seed.map((s) => String(s).trim().replace(/^#+/, "").toLowerCase()).filter(Boolean),
      ...expanded.map((s) => String(s).trim().replace(/^#+/, "").toLowerCase()).filter(Boolean),
    ]));
    return merged.slice(0, 30);
  } catch (e) {
    console.warn("[hashtag-expand] failed, falling back to seed:", e.message);
    return seed;
  }
}

function parseHashtagInput(input) {
  if (!input) return [];
  if (Array.isArray(input)) return input.map((s) => String(s).trim().replace(/^#+/, "").toLowerCase()).filter(Boolean);
  return String(input).split(/[,\s\n]+/).map((s) => s.trim().replace(/^#+/, "").toLowerCase()).filter(Boolean);
}

// ---- Jobs -------------------------------------------------------------------
app.post("/api/jobs", upload.single("handles_file"), async (req, res) => {
  try {
    const body = req.body || {};
    const handles = parseHandleList(body, req.file?.buffer);
    const description = String(body.description || "").trim();
    const name = String(body.name || "").trim() || null;
    const rubric = parseRubric(body.rubric);
    const truthy = (v) => v === true || v === "true" || v === "on" || v === "1";
    const enableVisual = truthy(body.enable_visual);
    const enableTranscripts = truthy(body.enable_transcripts);
    const seedHashtags = parseHashtagInput(body.hashtags);
    const expandHashtags = truthy(body.expand_hashtags ?? "true"); // default true
    const maxVideos = Math.max(1, Math.min(20, parseInt(body.max_videos_per_creator || "5", 10)));
    const costCap = Math.max(0.5, Math.min(1000, Number(body.cost_cap_usd) || 10.0));

    if (!description) return res.status(400).json({ error: "description is required" });
    if (handles.length === 0) return res.status(400).json({ error: "no valid handles provided" });
    if (handles.length > 50000) return res.status(400).json({ error: "max 50,000 handles per job" });

    // Optional Claude pre-pass to broaden hashtag coverage.
    let finalHashtags = seedHashtags;
    if (enableTranscripts && seedHashtags.length && expandHashtags) {
      finalHashtags = await expandHashtagsViaClaude(seedHashtags, description);
    }

    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const { rows: jobRows } = await client.query(
        `INSERT INTO jobs (name, description, rubric, enable_visual, enable_transcripts,
                           relevant_hashtags, max_videos_per_creator, cost_cap_usd,
                           status, total_creators)
         VALUES ($1,$2,$3::jsonb,$4,$5,$6::text[],$7,$8,'running',$9) RETURNING id`,
        [name, description, rubric ? JSON.stringify(rubric) : null,
         enableVisual, enableTranscripts, finalHashtags, maxVideos, costCap, handles.length]
      );
      const jobId = jobRows[0].id;

      await client.query(
        `INSERT INTO creators (handle, scrape_status)
         SELECT unnest($1::text[]), 'pending'
         ON CONFLICT (handle) DO NOTHING`,
        [handles]
      );

      // Initial per-row states depend on which features are enabled for this job.
      const visualInit     = enableVisual     ? "pending" : "skipped";
      const transcriptInit = enableTranscripts ? "pending" : "skipped";
      await client.query(
        `INSERT INTO job_creators (job_id, handle, visual_state, transcript_state)
         SELECT $1, unnest($2::text[]), $3, $4
         ON CONFLICT DO NOTHING`,
        [jobId, handles, visualInit, transcriptInit]
      );

      await client.query(
        `INSERT INTO creator_crm (handle)
         SELECT unnest($1::text[])
         ON CONFLICT (handle) DO NOTHING`,
        [handles]
      );

      const refreshDays = parseInt(process.env.BIO_REFRESH_DAYS || "30", 10);
      const refreshClause = refreshDays > 0
        ? `AND scraped_at > now() - interval '${refreshDays} days'`
        : "";
      await client.query(
        `UPDATE job_creators jc
         SET scrape_state='done'
         FROM creators c
         WHERE jc.job_id=$1 AND jc.handle=c.handle
           AND c.scrape_status='scraped'
           ${refreshClause}`,
        [jobId]
      );
      await client.query(
        `UPDATE jobs SET scraped_count = (
           SELECT count(*) FROM job_creators WHERE job_id=$1 AND scrape_state='done'
         ) WHERE id=$1`,
        [jobId]
      );

      await client.query("COMMIT");
      res.json({
        id: jobId,
        total_creators: handles.length,
        has_rubric: !!rubric,
        enable_visual: enableVisual,
        enable_transcripts: enableTranscripts,
        hashtags: finalHashtags,
        cost_cap_usd: costCap,
      });
    } catch (e) {
      await client.query("ROLLBACK");
      throw e;
    } finally {
      client.release();
    }
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

app.get("/api/jobs", async (_req, res) => {
  const { rows } = await pool.query(
    `SELECT id, name, description, rubric, status, total_creators, scraped_count,
            evaluated_count, matched_count, created_at, finished_at
     FROM jobs ORDER BY id DESC LIMIT 100`
  );
  res.json({ jobs: rows });
});

app.get("/api/jobs/:id", async (req, res) => {
  const { rows } = await pool.query(`SELECT * FROM jobs WHERE id=$1`, [req.params.id]);
  if (!rows.length) return res.status(404).json({ error: "not found" });

  const { rows: counts } = await pool.query(
    `
    SELECT
      count(*) FILTER (WHERE scrape_state='pending')      AS scrape_pending,
      count(*) FILTER (WHERE scrape_state='in_flight')    AS scrape_in_flight,
      count(*) FILTER (WHERE scrape_state='done')         AS scrape_done,
      count(*) FILTER (WHERE scrape_state='failed')       AS scrape_failed,
      count(*) FILTER (WHERE scrape_state='skipped')      AS scrape_skipped,
      count(*) FILTER (WHERE eval_state='pending')        AS eval_pending,
      count(*) FILTER (WHERE eval_state='in_flight')      AS eval_in_flight,
      count(*) FILTER (WHERE eval_state='done')           AS eval_done,
      count(*) FILTER (WHERE eval_state='failed')         AS eval_failed,
      count(*) FILTER (WHERE eval_state='skipped')        AS eval_skipped,
      count(*) FILTER (WHERE visual_state='pending')      AS visual_pending,
      count(*) FILTER (WHERE visual_state='in_flight')    AS visual_in_flight,
      count(*) FILTER (WHERE visual_state='done')         AS visual_done,
      count(*) FILTER (WHERE visual_state='failed')       AS visual_failed,
      count(*) FILTER (WHERE visual_state='skipped')      AS visual_skipped,
      count(*) FILTER (WHERE transcript_state='pending')  AS transcript_pending,
      count(*) FILTER (WHERE transcript_state='in_flight') AS transcript_in_flight,
      count(*) FILTER (WHERE transcript_state='done')     AS transcript_done,
      count(*) FILTER (WHERE transcript_state='failed')   AS transcript_failed,
      count(*) FILTER (WHERE transcript_state='skipped')  AS transcript_skipped,
      count(*) FILTER (WHERE matched IS TRUE)             AS matched,
      count(*) FILTER (WHERE matched IS FALSE)            AS rejected,
      count(*)                                            AS total
    FROM job_creators WHERE job_id=$1
    `,
    [req.params.id]
  );
  res.json({ ...rows[0], counts: counts[0] });
});

/**
 * Cost estimate for the UI form. Given a config + creator count, returns the
 * upper-bound dollar estimate so the user can size their cap before submitting.
 */
app.post("/api/cost-estimate", (req, res) => {
  const body = req.body || {};
  const n = parseInt(body.total_creators || "0", 10);
  const cfg = {
    enable_visual: !!body.enable_visual,
    enable_transcripts: !!body.enable_transcripts,
    relevant_hashtags: Array.isArray(body.hashtags) ? body.hashtags : [],
  };
  const usd = estimatedJobCost(cfg, n);
  res.json({ estimated_usd: Number(usd.toFixed(4)), total_creators: n });
});

app.post("/api/jobs/:id/pause", async (req, res) => {
  await pool.query(
    `UPDATE jobs SET status='paused' WHERE id=$1 AND status='running'`,
    [req.params.id]
  );
  res.json({ ok: true });
});

app.post("/api/jobs/:id/resume", async (req, res) => {
  await pool.query(
    `UPDATE jobs SET status='running', cost_paused_at=NULL WHERE id=$1 AND status='paused'`,
    [req.params.id]
  );
  res.json({ ok: true });
});

/**
 * Raise the cost cap on a paused job and resume it. Pass { new_cap_usd: number }
 * or { add_usd: number } to bump by a delta.
 */
app.post("/api/jobs/:id/raise-cap", async (req, res) => {
  const body = req.body || {};
  const newCap = Number(body.new_cap_usd);
  const addUsd = Number(body.add_usd);
  if (!Number.isFinite(newCap) && !Number.isFinite(addUsd)) {
    return res.status(400).json({ error: "provide new_cap_usd or add_usd" });
  }
  if (Number.isFinite(newCap)) {
    await pool.query(
      `UPDATE jobs SET cost_cap_usd=$2, status='running', cost_paused_at=NULL, error=NULL
       WHERE id=$1 AND status='paused' AND $2 > cost_spent_usd`,
      [req.params.id, newCap]
    );
  } else {
    await pool.query(
      `UPDATE jobs SET cost_cap_usd = cost_cap_usd + $2, status='running', cost_paused_at=NULL, error=NULL
       WHERE id=$1 AND status='paused'`,
      [req.params.id, addUsd]
    );
  }
  res.json({ ok: true });
});

app.post("/api/jobs/:id/cancel", async (req, res) => {
  await pool.query(
    `UPDATE jobs SET status='cancelled', finished_at=now() WHERE id=$1 AND status IN ('running','paused')`,
    [req.params.id]
  );
  res.json({ ok: true });
});

app.get("/api/jobs/:id/results", async (req, res) => {
  const jobId = req.params.id;
  const limit = Math.min(parseInt(req.query.limit || "100", 10), 1000);
  const offset = parseInt(req.query.offset || "0", 10);
  const matched = req.query.matched;
  const filters = ["jc.job_id = $1"];
  const params = [jobId];
  if (matched === "true") filters.push("jc.matched IS TRUE");
  if (matched === "false") filters.push("jc.matched IS FALSE");

  const { rows } = await pool.query(
    `
    SELECT jc.handle, jc.matched, jc.confidence, jc.reason, jc.rubric_scores,
           jc.match_signals, jc.match_sources, jc.match_cues,
           jc.scrape_state, jc.eval_state, jc.visual_state,
           jc.scrape_attempts, jc.eval_attempts, jc.visual_attempts,
           c.bio, c.bio_link, c.follower_count, c.nickname, c.verified, c.region,
           c.scrape_status, c.scrape_error, c.scraped_at,
           (SELECT json_agg(json_build_object('kind', kind, 'value', value, 'source', source))
              FROM contacts WHERE handle=c.handle) AS contacts,
           (SELECT json_agg(tag) FROM creator_tags WHERE handle=c.handle) AS tags,
           crm.status AS crm_status, crm.owner AS crm_owner,
           (SELECT json_build_object(
              'matched', va.matched, 'confidence', va.confidence,
              'likely_role', va.likely_role, 'cues', va.cues, 'reason', va.reason)
            FROM creator_visual_analyses va WHERE va.handle=c.handle AND va.job_id=jc.job_id) AS visual
    FROM job_creators jc
    JOIN creators c ON c.handle = jc.handle
    LEFT JOIN creator_crm crm ON crm.handle = c.handle
    WHERE ${filters.join(" AND ")}
    ORDER BY jc.matched DESC NULLS LAST, jc.confidence DESC NULLS LAST, jc.handle
    LIMIT ${limit} OFFSET ${offset}
    `,
    params
  );
  res.json({ results: rows, limit, offset });
});

// ---- Creators (CRM) ---------------------------------------------------------
app.get("/api/creators", async (req, res) => {
  const limit = Math.min(parseInt(req.query.limit || "100", 10), 1000);
  const offset = parseInt(req.query.offset || "0", 10);
  const filters = [];
  const params = [];
  let p = 0;

  if (req.query.status) {
    params.push(req.query.status); filters.push(`crm.status = $${++p}`);
  }
  if (req.query.tag) {
    params.push(req.query.tag); filters.push(`EXISTS (SELECT 1 FROM creator_tags t WHERE t.handle=c.handle AND t.tag = $${++p})`);
  }
  if (req.query.q) {
    params.push(`%${req.query.q}%`); filters.push(`(c.handle ILIKE $${++p} OR c.bio ILIKE $${p} OR c.nickname ILIKE $${p})`);
  }
  if (req.query.has_email === "true") {
    filters.push(`EXISTS (SELECT 1 FROM contacts ct WHERE ct.handle=c.handle AND ct.kind='email')`);
  }
  if (req.query.matched_in_job) {
    params.push(parseInt(req.query.matched_in_job, 10));
    filters.push(`EXISTS (SELECT 1 FROM job_creators jc WHERE jc.handle=c.handle AND jc.job_id=$${++p} AND jc.matched IS TRUE)`);
  }
  const where = filters.length ? `WHERE ${filters.join(" AND ")}` : "";

  const { rows } = await pool.query(
    `
    SELECT c.handle, c.nickname, c.bio, c.bio_link, c.follower_count, c.verified,
           c.scrape_status, c.scraped_at,
           crm.status AS crm_status, crm.owner AS crm_owner, crm.last_contacted,
           (SELECT count(*) FROM contacts WHERE handle=c.handle AND kind='email') AS email_count,
           (SELECT count(*) FROM contacts WHERE handle=c.handle AND kind='phone') AS phone_count,
           (SELECT json_agg(tag) FROM creator_tags WHERE handle=c.handle) AS tags
    FROM creators c
    LEFT JOIN creator_crm crm ON crm.handle = c.handle
    ${where}
    ORDER BY c.follower_count DESC NULLS LAST, c.handle
    LIMIT ${limit} OFFSET ${offset}
    `,
    params
  );
  res.json({ creators: rows, limit, offset });
});

app.get("/api/creators/:handle", async (req, res) => {
  const h = cleanHandle(req.params.handle);
  const { rows } = await pool.query(`SELECT * FROM creators WHERE handle=$1`, [h]);
  if (!rows.length) return res.status(404).json({ error: "not found" });
  const creator = rows[0];
  const [contacts, tags, notes, crm, jobs, linkFetches] = await Promise.all([
    pool.query(`SELECT id, kind, value, source, found_at FROM contacts WHERE handle=$1 ORDER BY found_at DESC`, [h]),
    pool.query(`SELECT tag FROM creator_tags WHERE handle=$1`, [h]),
    pool.query(`SELECT id, note, author, created_at FROM creator_notes WHERE handle=$1 ORDER BY created_at DESC`, [h]),
    pool.query(`SELECT * FROM creator_crm WHERE handle=$1`, [h]),
    pool.query(
      `SELECT jc.job_id, jc.matched, jc.confidence, jc.reason, jc.rubric_scores,
              jc.match_signals, jc.match_sources, jc.match_cues, jc.evaluated_at,
              j.description, j.name, j.rubric, j.enable_visual,
              (SELECT row_to_json(va) FROM creator_visual_analyses va
                WHERE va.handle=$1 AND va.job_id=jc.job_id) AS visual
       FROM job_creators jc JOIN jobs j ON j.id = jc.job_id
       WHERE jc.handle=$1 ORDER BY jc.job_id DESC`, [h]),
    pool.query(`SELECT url, status, http_status, contacts_found, fetched_at, error FROM link_fetches WHERE handle=$1 ORDER BY id DESC`, [h]),
  ]);
  res.json({
    creator,
    contacts: contacts.rows,
    tags: tags.rows.map((r) => r.tag),
    notes: notes.rows,
    crm: crm.rows[0] || null,
    jobs: jobs.rows,
    link_fetches: linkFetches.rows,
  });
});

/**
 * PATCH /api/creators/:handle
 *
 * Body fields:
 *   status         - new CRM status (or null to leave unchanged)
 *   owner          - new owner (or null to leave unchanged; pass {"__clear":true} to clear)
 *   last_contacted - ISO timestamp (or null to leave; {"__clear":true} to clear)
 *   custom         - object to MERGE into custom JSONB (key-level)
 *                    Pass __replace_custom: true alongside to replace instead.
 *                    Pass custom: {"__clear": true} to clear the entire blob.
 */
app.patch("/api/creators/:handle", async (req, res) => {
  const h = cleanHandle(req.params.handle);
  const body = req.body || {};

  // Decode clear sentinels
  function decodeClear(v) {
    if (v && typeof v === "object" && v.__clear === true) return { clear: true };
    if (v === undefined) return { skip: true };
    return { value: v };
  }
  const statusOp = decodeClear(body.status);
  const ownerOp = decodeClear(body.owner);
  const lastOp = decodeClear(body.last_contacted);

  // Build dynamic UPDATE
  const sets = [];
  const params = [h];
  let p = 1;

  if (!statusOp.skip) {
    if (statusOp.clear) {
      sets.push(`status = 'new'`); // status has NOT NULL default; clear means reset to 'new'
    } else {
      params.push(statusOp.value);
      sets.push(`status = $${++p}`);
    }
  }
  if (!ownerOp.skip) {
    if (ownerOp.clear) {
      sets.push(`owner = NULL`);
    } else {
      params.push(ownerOp.value);
      sets.push(`owner = $${++p}`);
    }
  }
  if (!lastOp.skip) {
    if (lastOp.clear) {
      sets.push(`last_contacted = NULL`);
    } else {
      params.push(lastOp.value);
      sets.push(`last_contacted = $${++p}`);
    }
  }

  if (body.custom !== undefined) {
    if (body.custom && typeof body.custom === "object" && body.custom.__clear === true) {
      sets.push(`custom = '{}'::jsonb`);
    } else if (body.__replace_custom) {
      params.push(JSON.stringify(body.custom || {}));
      sets.push(`custom = $${++p}::jsonb`);
    } else {
      // Merge: existing || incoming. The right-hand side wins on key conflicts.
      params.push(JSON.stringify(body.custom || {}));
      sets.push(`custom = custom || $${++p}::jsonb`);
    }
  }

  sets.push(`updated_at = now()`);

  // Ensure a row exists before updating
  await pool.query(`INSERT INTO creator_crm (handle) VALUES ($1) ON CONFLICT (handle) DO NOTHING`, [h]);

  if (sets.length === 1) {
    // Only updated_at — nothing to do meaningfully
    return res.json({ ok: true, noop: true });
  }

  await pool.query(`UPDATE creator_crm SET ${sets.join(", ")} WHERE handle=$1`, params);
  res.json({ ok: true });
});

app.post("/api/creators/:handle/tags", async (req, res) => {
  const h = cleanHandle(req.params.handle);
  const tag = String(req.body?.tag || "").trim();
  if (!tag) return res.status(400).json({ error: "tag required" });
  await pool.query(
    `INSERT INTO creator_tags (handle, tag) VALUES ($1,$2) ON CONFLICT DO NOTHING`,
    [h, tag]
  );
  res.json({ ok: true });
});

app.delete("/api/creators/:handle/tags/:tag", async (req, res) => {
  await pool.query(
    `DELETE FROM creator_tags WHERE handle=$1 AND tag=$2`,
    [cleanHandle(req.params.handle), req.params.tag]
  );
  res.json({ ok: true });
});

app.post("/api/creators/:handle/notes", async (req, res) => {
  const h = cleanHandle(req.params.handle);
  const note = String(req.body?.note || "").trim();
  if (!note) return res.status(400).json({ error: "note required" });
  await pool.query(
    `INSERT INTO creator_notes (handle, note, author) VALUES ($1,$2,$3)`,
    [h, note, req.body?.author || null]
  );
  res.json({ ok: true });
});

app.post("/api/creators/:handle/contacts", async (req, res) => {
  const h = cleanHandle(req.params.handle);
  const { kind, value, source } = req.body || {};
  if (!kind || !value) return res.status(400).json({ error: "kind and value required" });
  await pool.query(
    `INSERT INTO contacts (handle, kind, value, source) VALUES ($1,$2,$3,$4)
     ON CONFLICT (handle, kind, value) DO NOTHING`,
    [h, kind, value, source || "manual"]
  );
  res.json({ ok: true });
});

// ---- CSV export -------------------------------------------------------------
const CSV_MAX_ROWS = parseInt(process.env.CSV_MAX_ROWS || "1000000", 10);

app.get("/api/export.csv", async (req, res) => {
  const jobId = req.query.job ? parseInt(req.query.job, 10) : null;
  const matched = req.query.matched;
  const status = req.query.status;

  let sql, params;
  if (jobId) {
    const filters = ["jc.job_id = $1"];
    params = [jobId];
    if (matched === "true") filters.push("jc.matched IS TRUE");
    if (matched === "false") filters.push("jc.matched IS FALSE");
    sql = `
      SELECT c.handle, c.nickname, c.bio, c.bio_link, c.follower_count, c.verified, c.region,
             jc.matched, jc.confidence, jc.reason, jc.rubric_scores,
             (SELECT string_agg(value, ' | ') FROM contacts WHERE handle=c.handle AND kind='email') AS emails,
             (SELECT string_agg(value, ' | ') FROM contacts WHERE handle=c.handle AND kind='phone') AS phones,
             (SELECT string_agg(value, ' | ') FROM contacts WHERE handle=c.handle AND kind='link')  AS links,
             (SELECT string_agg(kind || ':' || value, ' | ') FROM contacts
                WHERE handle=c.handle AND kind IN ('instagram','youtube','facebook','twitter')) AS socials,
             crm.status AS crm_status, crm.owner AS crm_owner
      FROM job_creators jc
      JOIN creators c ON c.handle = jc.handle
      LEFT JOIN creator_crm crm ON crm.handle = c.handle
      WHERE ${filters.join(" AND ")}
      ORDER BY jc.matched DESC NULLS LAST, jc.confidence DESC NULLS LAST, jc.handle
      LIMIT ${CSV_MAX_ROWS}`;
  } else {
    const filters = [];
    params = [];
    if (status) { params.push(status); filters.push(`crm.status = $${params.length}`); }
    sql = `
      SELECT c.handle, c.nickname, c.bio, c.bio_link, c.follower_count, c.verified, c.region,
             NULL::boolean AS matched, NULL::numeric AS confidence, NULL::text AS reason, NULL::jsonb AS rubric_scores,
             (SELECT string_agg(value, ' | ') FROM contacts WHERE handle=c.handle AND kind='email') AS emails,
             (SELECT string_agg(value, ' | ') FROM contacts WHERE handle=c.handle AND kind='phone') AS phones,
             (SELECT string_agg(value, ' | ') FROM contacts WHERE handle=c.handle AND kind='link')  AS links,
             (SELECT string_agg(kind || ':' || value, ' | ') FROM contacts
                WHERE handle=c.handle AND kind IN ('instagram','youtube','facebook','twitter')) AS socials,
             crm.status AS crm_status, crm.owner AS crm_owner
      FROM creators c
      LEFT JOIN creator_crm crm ON crm.handle = c.handle
      ${filters.length ? `WHERE ${filters.join(" AND ")}` : ""}
      ORDER BY c.follower_count DESC NULLS LAST, c.handle
      LIMIT ${CSV_MAX_ROWS}`;
  }

  const { rows } = await pool.query(sql, params);
  res.setHeader("Content-Type", "text/csv");
  res.setHeader("Content-Disposition", `attachment; filename="creators-${Date.now()}.csv"`);
  const headers = ["handle","nickname","bio","bio_link","follower_count","verified","region",
                   "matched","confidence","reason","rubric_scores",
                   "emails","phones","links","socials","crm_status","crm_owner"];
  res.write(headers.join(",") + "\n");
  for (const r of rows) {
    res.write(headers.map((k) => csvEscape(typeof r[k] === "object" && r[k] !== null ? JSON.stringify(r[k]) : r[k])).join(",") + "\n");
  }
  res.end();
});

// ============================================================================
// Outreach (v1.5): senders, campaigns, templates, attachments, assignments,
// outbox. The whole email-sending pipeline lives behind these endpoints.
// ============================================================================

const uploadAttachment = multer({ limits: { fileSize: 15 * 1024 * 1024 } });

// ---- Senders ---------------------------------------------------------------
app.get("/api/senders", async (_req, res) => {
  const { rows } = await pool.query(
    `SELECT id, name, from_email, from_name, smtp_host, smtp_port, smtp_user,
            imap_host, imap_port, daily_cap, sent_today, sent_today_resets_at,
            signature, active, created_at
     FROM email_senders ORDER BY id ASC`
  );
  res.json({ senders: rows });
});

app.post("/api/senders", async (req, res) => {
  try {
    const b = req.body || {};
    const required = ["name", "from_email", "from_name", "smtp_user", "smtp_app_password"];
    for (const k of required) {
      if (!b[k]) return res.status(400).json({ error: `${k} required` });
    }
    const sender = {
      name: b.name,
      from_email: b.from_email,
      from_name: b.from_name,
      smtp_host: b.smtp_host || "smtp.gmail.com",
      smtp_port: parseInt(b.smtp_port || "587", 10),
      smtp_user: b.smtp_user,
      smtp_app_password: b.smtp_app_password,
      imap_host: b.imap_host || "imap.gmail.com",
      imap_port: parseInt(b.imap_port || "993", 10),
      daily_cap: parseInt(b.daily_cap || "50", 10),
      signature: b.signature || null,
    };

    if (b.verify !== false) {
      try {
        await verifyCredentials(sender);
      } catch (e) {
        return res.status(400).json({ error: `SMTP verification failed: ${e.message}` });
      }
    }

    const { rows } = await pool.query(
      `INSERT INTO email_senders
         (name, from_email, from_name, smtp_host, smtp_port, smtp_user,
          smtp_app_password, imap_host, imap_port, daily_cap, signature)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
       RETURNING id`,
      [sender.name, sender.from_email, sender.from_name, sender.smtp_host,
       sender.smtp_port, sender.smtp_user, sender.smtp_app_password,
       sender.imap_host, sender.imap_port, sender.daily_cap, sender.signature]
    );
    res.json({ id: rows[0].id });
  } catch (e) {
    console.error("[senders] create:", e);
    res.status(500).json({ error: e.message });
  }
});

app.patch("/api/senders/:id", async (req, res) => {
  const b = req.body || {};
  const fields = ["name","from_email","from_name","smtp_host","smtp_port","smtp_user",
                  "smtp_app_password","imap_host","imap_port","daily_cap","signature","active"];
  const sets = []; const vals = [];
  for (const f of fields) {
    if (b[f] !== undefined) { sets.push(`${f}=$${sets.length + 1}`); vals.push(b[f]); }
  }
  if (!sets.length) return res.status(400).json({ error: "no fields to update" });
  vals.push(req.params.id);
  await pool.query(`UPDATE email_senders SET ${sets.join(", ")} WHERE id=$${vals.length}`, vals);
  invalidateTransporter(req.params.id);
  res.json({ ok: true });
});

app.delete("/api/senders/:id", async (req, res) => {
  await pool.query(`UPDATE email_senders SET active=FALSE WHERE id=$1`, [req.params.id]);
  res.json({ ok: true });
});

// ---- Campaigns -------------------------------------------------------------
app.get("/api/campaigns", async (_req, res) => {
  const { rows } = await pool.query(
    `SELECT c.*, s.name AS sender_name, s.from_email AS sender_from_email,
            (SELECT count(*) FROM creator_campaigns WHERE campaign_id=c.id) AS creators,
            (SELECT count(*) FROM email_templates WHERE campaign_id=c.id AND approved_at IS NOT NULL) AS approved_templates,
            (SELECT count(*) FROM outreach_emails WHERE campaign_id=c.id AND status='sent') AS sent
     FROM email_campaigns c
     LEFT JOIN email_senders s ON s.id=c.sender_id
     ORDER BY c.id DESC`
  );
  res.json({ campaigns: rows });
});

app.post("/api/campaigns", async (req, res) => {
  try {
    const b = req.body || {};
    if (!b.name || !b.sender_id) return res.status(400).json({ error: "name and sender_id required" });
    const { rows } = await pool.query(
      `INSERT INTO email_campaigns
         (name, description, sender_id, delay_days_v1, delay_days_v2, delay_days_v3, delay_days_v4)
       VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id`,
      [b.name, b.description || null, b.sender_id,
       parseInt(b.delay_days_v1 || "4", 10),
       parseInt(b.delay_days_v2 || "7", 10),
       parseInt(b.delay_days_v3 || "10", 10),
       parseInt(b.delay_days_v4 || "14", 10)]
    );
    // Seed empty template rows for all 5 stages so the user can fill them in order
    const stages = ["initial","v1","v2","v3","v4"];
    for (const s of stages) {
      await pool.query(
        `INSERT INTO email_templates (campaign_id, stage, subject, body, personalization_instructions)
         VALUES ($1,$2,'','','')`,
        [rows[0].id, s]
      );
    }
    res.json({ id: rows[0].id });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get("/api/campaigns/:id", async (req, res) => {
  const { rows } = await pool.query(`SELECT * FROM email_campaigns WHERE id=$1`, [req.params.id]);
  if (!rows.length) return res.status(404).json({ error: "not found" });
  const { rows: tpl } = await pool.query(
    `SELECT * FROM email_templates WHERE campaign_id=$1 ORDER BY
       CASE stage WHEN 'initial' THEN 0 WHEN 'v1' THEN 1 WHEN 'v2' THEN 2 WHEN 'v3' THEN 3 WHEN 'v4' THEN 4 ELSE 5 END`,
    [req.params.id]
  );
  const { rows: atts } = await pool.query(
    `SELECT id, filename, mime_type, size_bytes, apply_to_stages, created_at
     FROM campaign_attachments WHERE campaign_id=$1 ORDER BY id ASC`,
    [req.params.id]
  );
  res.json({ ...rows[0], templates: tpl, attachments: atts });
});

app.patch("/api/campaigns/:id", async (req, res) => {
  const b = req.body || {};
  const fields = ["name","description","sender_id","delay_days_v1","delay_days_v2",
                  "delay_days_v3","delay_days_v4","status"];
  const sets = []; const vals = [];
  for (const f of fields) {
    if (b[f] !== undefined) { sets.push(`${f}=$${sets.length + 1}`); vals.push(b[f]); }
  }
  if (!sets.length) return res.status(400).json({ error: "no fields" });
  vals.push(req.params.id);
  await pool.query(`UPDATE email_campaigns SET ${sets.join(", ")} WHERE id=$${vals.length}`, vals);
  res.json({ ok: true });
});

app.delete("/api/campaigns/:id", async (req, res) => {
  await pool.query(`UPDATE email_campaigns SET status='archived' WHERE id=$1`, [req.params.id]);
  res.json({ ok: true });
});

// ---- Templates -------------------------------------------------------------
app.patch("/api/templates/:id", async (req, res) => {
  const b = req.body || {};
  const fields = ["subject","body","personalization_instructions"];
  const sets = []; const vals = [];
  for (const f of fields) {
    if (b[f] !== undefined) { sets.push(`${f}=$${sets.length + 1}`); vals.push(b[f]); }
  }
  // Editing a template clears its approval — user must regenerate sample + approve again.
  sets.push(`approved_at = NULL`);
  sets.push(`updated_at = now()`);
  if (!sets.length) return res.status(400).json({ error: "no fields" });
  vals.push(req.params.id);
  await pool.query(`UPDATE email_templates SET ${sets.join(", ")} WHERE id=$${vals.length}`, vals);
  res.json({ ok: true });
});

app.post("/api/templates/:id/approve", async (req, res) => {
  await pool.query(`UPDATE email_templates SET approved_at=now() WHERE id=$1`, [req.params.id]);
  res.json({ ok: true });
});

/**
 * Generate a sample personalized email against one creator from the DB,
 * without writing anything. Used by the campaign edit UI to preview the
 * personalization before approving the template.
 */
app.post("/api/templates/:id/sample", async (req, res) => {
  try {
    const { rows } = await pool.query(`SELECT * FROM email_templates WHERE id=$1`, [req.params.id]);
    if (!rows.length) return res.status(404).json({ error: "template not found" });
    const template = rows[0];

    let handle = req.body?.handle;
    if (!handle) {
      // Pick a recently-matched creator with transcripts available — this gives
      // a realistic sample. Fall back to any scraped creator if none.
      const { rows: hs } = await pool.query(
        `SELECT c.handle
         FROM creators c
         WHERE c.scrape_status='scraped'
         ORDER BY EXISTS(SELECT 1 FROM videos v WHERE v.handle=c.handle AND v.transcript IS NOT NULL) DESC,
                  c.updated_at DESC NULLS LAST
         LIMIT 1`
      );
      if (!hs.length) return res.status(400).json({ error: "no scraped creators available — run a scrape job first" });
      handle = hs[0].handle;
    }

    const generated = await generateEmail(template, handle);
    res.json({ handle, ...generated });
  } catch (e) {
    console.error("[sample]", e);
    res.status(500).json({ error: e.message });
  }
});

// ---- Attachments -----------------------------------------------------------
app.post("/api/campaigns/:id/attachments", uploadAttachment.single("file"), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: "no file" });
    const stagesRaw = req.body?.apply_to_stages || "initial,v1,v2,v3,v4";
    const stages = String(stagesRaw).split(",").map((s) => s.trim()).filter(Boolean);
    const { rows } = await pool.query(
      `INSERT INTO campaign_attachments
         (campaign_id, filename, mime_type, content_bytes, size_bytes, apply_to_stages)
       VALUES ($1,$2,$3,$4,$5,$6::text[]) RETURNING id, filename, mime_type, size_bytes, apply_to_stages`,
      [req.params.id, req.file.originalname, req.file.mimetype, req.file.buffer,
       req.file.size, stages]
    );
    res.json(rows[0]);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.delete("/api/attachments/:id", async (req, res) => {
  await pool.query(`DELETE FROM campaign_attachments WHERE id=$1`, [req.params.id]);
  res.json({ ok: true });
});

// ---- Assignments -----------------------------------------------------------
/**
 * Assign creators to a campaign. Accepts either an explicit list of handles
 * or a filter to pull from existing job results (e.g. matched=true on job X
 * with any cue containing "nurse"). Either way produces one creator_campaigns
 * row + one 'draft' initial outreach_email per creator.
 */
app.post("/api/campaigns/:id/assign", async (req, res) => {
  try {
    const campaignId = req.params.id;
    const b = req.body || {};

    const { rows: cr } = await pool.query(`SELECT * FROM email_campaigns WHERE id=$1`, [campaignId]);
    if (!cr.length) return res.status(404).json({ error: "campaign not found" });
    const campaign = cr[0];

    // Resolve which creators we are assigning. Either explicit handles list
    // or pulled from a job's matched creators.
    let candidates = [];
    if (Array.isArray(b.handles) && b.handles.length) {
      candidates = b.handles.map((h) => String(h).replace(/^@/, "").toLowerCase().trim()).filter(Boolean);
    } else if (b.from_job_id) {
      const cueFilter = Array.isArray(b.cue_filter) ? b.cue_filter : [];
      let q = `SELECT DISTINCT jc.handle FROM job_creators jc
               WHERE jc.job_id=$1 AND jc.matched=TRUE`;
      const qv = [b.from_job_id];
      if (cueFilter.length) {
        q += ` AND EXISTS (SELECT 1 FROM unnest(jc.match_cues) c
                          WHERE LOWER(c) = ANY($2::text[]))`;
        qv.push(cueFilter.map((c) => String(c).toLowerCase()));
      }
      const { rows } = await pool.query(q, qv);
      candidates = rows.map((r) => r.handle);
    }
    if (!candidates.length) return res.status(400).json({ error: "no candidates to assign" });

    // Resolve emails: prefer a per-handle override in b.emails_by_handle,
    // otherwise pull the first email contact from our DB.
    const overrideMap = b.emails_by_handle || {};
    const { rows: emailRows } = await pool.query(
      `SELECT handle, MIN(value) AS email
       FROM contacts WHERE kind='email' AND handle = ANY($1)
       GROUP BY handle`,
      [candidates]
    );
    const dbEmailMap = Object.fromEntries(emailRows.map((r) => [r.handle, r.email]));

    let assigned = 0, skipped = 0;
    const skippedDetails = [];
    for (const h of candidates) {
      const email = overrideMap[h] || dbEmailMap[h];
      if (!email) {
        skipped++;
        skippedDetails.push({ handle: h, reason: "no email on file" });
        continue;
      }
      try {
        // creator_campaigns: insert or no-op (UNIQUE on (handle, campaign_id))
        const { rows: cc } = await pool.query(
          `INSERT INTO creator_campaigns (handle, campaign_id, to_email, current_stage, status, next_send_at)
           VALUES ($1,$2,$3,'initial','active', now())
           ON CONFLICT (handle, campaign_id) DO NOTHING
           RETURNING id`,
          [h, campaignId, email]
        );
        if (!cc.length) {
          skipped++;
          skippedDetails.push({ handle: h, reason: "already assigned" });
          continue;
        }
        // Create the initial draft outreach_email. The generator will fill it in.
        await pool.query(
          `INSERT INTO outreach_emails
             (handle, campaign_id, stage, to_email, subject, body, sender_id, status, scheduled_at)
           VALUES ($1,$2,'initial',$3,'(pending generation)','(pending generation)',$4,'draft', now())`,
          [h, campaignId, email, campaign.sender_id]
        );
        assigned++;
      } catch (e) {
        skipped++;
        skippedDetails.push({ handle: h, reason: e.message });
      }
    }

    // Flip campaign to 'active' if it had been draft/ready
    await pool.query(
      `UPDATE email_campaigns SET status='active' WHERE id=$1 AND status IN ('draft','ready')`,
      [campaignId]
    );

    res.json({ assigned, skipped, skipped_details: skippedDetails.slice(0, 100) });
  } catch (e) {
    console.error("[assign]", e);
    res.status(500).json({ error: e.message });
  }
});

app.get("/api/campaigns/:id/creators", async (req, res) => {
  const { rows } = await pool.query(
    `SELECT cc.*, c.nickname, c.follower_count,
            (SELECT count(*) FROM outreach_emails WHERE handle=cc.handle AND campaign_id=cc.campaign_id AND status='sent') AS sent_count
     FROM creator_campaigns cc
     LEFT JOIN creators c ON c.handle = cc.handle
     WHERE cc.campaign_id=$1
     ORDER BY cc.created_at DESC
     LIMIT 1000`,
    [req.params.id]
  );
  res.json({ creators: rows });
});

app.post("/api/creator-campaigns/:id/pause", async (req, res) => {
  await pool.query(`UPDATE creator_campaigns SET status='paused' WHERE id=$1 AND status='active'`, [req.params.id]);
  res.json({ ok: true });
});

app.post("/api/creator-campaigns/:id/resume", async (req, res) => {
  await pool.query(`UPDATE creator_campaigns SET status='active' WHERE id=$1 AND status='paused'`, [req.params.id]);
  res.json({ ok: true });
});

// ---- Outbox / Email list --------------------------------------------------
app.get("/api/outreach-emails", async (req, res) => {
  const status = req.query.status || null;
  const campaign = req.query.campaign_id || null;
  const handle = req.query.handle || null;
  let q = `SELECT oe.*, c.name AS campaign_name
           FROM outreach_emails oe
           LEFT JOIN email_campaigns c ON c.id = oe.campaign_id
           WHERE 1=1`;
  const v = [];
  if (status)   { v.push(status);   q += ` AND oe.status=$${v.length}`; }
  if (campaign) { v.push(campaign); q += ` AND oe.campaign_id=$${v.length}`; }
  if (handle)   { v.push(handle);   q += ` AND oe.handle=$${v.length}`; }
  q += ` ORDER BY oe.scheduled_at DESC NULLS LAST, oe.id DESC LIMIT 500`;
  const { rows } = await pool.query(q, v);
  res.json({ emails: rows });
});

app.patch("/api/outreach-emails/:id", async (req, res) => {
  const b = req.body || {};
  const fields = ["subject","body","to_email","scheduled_at","status"];
  const sets = []; const vals = [];
  for (const f of fields) {
    if (b[f] !== undefined) { sets.push(`${f}=$${sets.length + 1}`); vals.push(b[f]); }
  }
  if (!sets.length) return res.status(400).json({ error: "no fields" });
  vals.push(req.params.id);
  await pool.query(`UPDATE outreach_emails SET ${sets.join(", ")} WHERE id=$${vals.length}`, vals);
  res.json({ ok: true });
});

app.post("/api/outreach-emails/:id/approve", async (req, res) => {
  await pool.query(
    `UPDATE outreach_emails SET status='approved', approved_by_user=TRUE
     WHERE id=$1 AND status='draft'`,
    [req.params.id]
  );
  res.json({ ok: true });
});

app.post("/api/outreach-emails/:id/cancel", async (req, res) => {
  await pool.query(
    `UPDATE outreach_emails SET status='cancelled'
     WHERE id=$1 AND status IN ('draft','approved','queued')`,
    [req.params.id]
  );
  res.json({ ok: true });
});

// Bulk-approve all drafts for a campaign (useful once template is approved)
app.post("/api/campaigns/:id/approve-all-drafts", async (req, res) => {
  const { rowCount } = await pool.query(
    `UPDATE outreach_emails
     SET status='approved', approved_by_user=TRUE
     WHERE campaign_id=$1 AND status='draft' AND personalized_content IS NOT NULL`,
    [req.params.id]
  );
  res.json({ approved: rowCount });
});

// Get full outreach history for one creator (for the creator detail page)
app.get("/api/creators/:handle/outreach", async (req, res) => {
  const { rows: assignments } = await pool.query(
    `SELECT cc.*, c.name AS campaign_name
     FROM creator_campaigns cc
     LEFT JOIN email_campaigns c ON c.id = cc.campaign_id
     WHERE cc.handle=$1
     ORDER BY cc.created_at DESC`,
    [req.params.handle]
  );
  const { rows: emails } = await pool.query(
    `SELECT id, campaign_id, stage, subject, status, scheduled_at, sent_at, message_id
     FROM outreach_emails
     WHERE handle=$1
     ORDER BY scheduled_at ASC NULLS LAST, id ASC`,
    [req.params.handle]
  );
  res.json({ assignments, emails });
});



app.get("/healthz", (_req, res) => res.json({ ok: true }));

// ---- boot -------------------------------------------------------------------
const PORT = process.env.PORT || 3000;

async function main() {
  await migrate();
  app.listen(PORT, () => {
    console.log(`[server] listening on :${PORT}`);
    if (!AUTH_ENABLED) {
      console.warn("[server] WARNING: BASIC_AUTH_PASS is not set — all endpoints are PUBLIC.");
      console.warn("[server] Set BASIC_AUTH_USER and BASIC_AUTH_PASS for any non-local deployment.");
    } else {
      console.log(`[server] Basic Auth enabled (user=${BASIC_USER})`);
    }
  });
  scraper.start();
  evaluator.start();
  linktree.start();
  vision.start();
  videos.start();
  emailGen.start();
  emailSend.start();
  imapListener.start();
}

main().catch((e) => {
  console.error("fatal:", e);
  process.exit(1);
});
