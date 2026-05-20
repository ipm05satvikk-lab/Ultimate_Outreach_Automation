// workers/videos.js — Fetches recent videos per creator, filters by hashtag
// relevance, transcribes the matching ones via OpenAI Whisper.
//
// Pipeline per creator:
//   1. Atomic claim from job_creators where transcript_state='pending' AND
//      job.enable_transcripts=true AND scrape_state='done'.
//   2. Check the videos table — if N recent videos are already cached within
//      VIDEO_REFRESH_DAYS, skip Apify and use cached.
//   3. Otherwise call the configured Apify video actor for up to N videos.
//   4. For each video, check hashtag overlap with job.relevant_hashtags.
//      Videos with no overlap stay in DB (cached) but get transcript_status='skipped'.
//      If the job has no hashtag filter, ALL fetched videos get transcribed.
//   5. For each video that needs a transcript: download mp4 → ffmpeg to mp3
//      (capped at 60s) → POST to /v1/audio/transcriptions → store transcript.
//   6. Charge cost at each paid step; auto-pause job if cap is breached.
//   7. Mark job_creators.transcript_state='done'.
//
// Disable entirely with TRANSCRIPTS_ENABLED=false. Per-job opt-in via
// jobs.enable_transcripts. Requires ffmpeg in the container (Dockerfile installs it).

const fs = require("fs");
const path = require("path");
const os = require("os");
const { execFile } = require("child_process");
const { pool } = require("../db");
const { chargeJob, UNIT } = require("../lib/cost");
const { ApifyClient } = require("apify-client");

const ENABLED = process.env.TRANSCRIPTS_ENABLED !== "false";
const POLL_MS = parseInt(process.env.TRANSCRIPT_POLL_MS || "6000", 10);
const MAX_ATTEMPTS = parseInt(process.env.TRANSCRIPT_MAX_ATTEMPTS || "3", 10);
const CLAIM_TIMEOUT_MS = parseInt(process.env.TRANSCRIPT_CLAIM_TIMEOUT_MS || "900000", 10); // 15 min

const APIFY_VIDEOS_ACTOR = process.env.APIFY_VIDEOS_ACTOR || "clockworks/free-tiktok-scraper";
const APIFY_TIMEOUT_S = parseInt(process.env.APIFY_TIMEOUT_S || "300", 10);
const VIDEO_REFRESH_DAYS = parseInt(process.env.VIDEO_REFRESH_DAYS || "30", 10);
const AUDIO_MAX_SECONDS = parseInt(process.env.AUDIO_MAX_SECONDS || "60", 10);
const WHISPER_MODEL = process.env.WHISPER_MODEL || "whisper-1";

const TMP_DIR = process.env.TRANSCRIPT_TMP_DIR || path.join(os.tmpdir(), "creator-audio");
fs.mkdirSync(TMP_DIR, { recursive: true });

let apify = null;
function apifyClient() {
  if (!apify) {
    if (!process.env.APIFY_TOKEN) throw new Error("APIFY_TOKEN env var is required");
    apify = new ApifyClient({ token: process.env.APIFY_TOKEN });
  }
  return apify;
}

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

// ---- Hashtag filtering -----------------------------------------------------
function normalizeHashtag(t) {
  return String(t || "").trim().replace(/^#+/, "").toLowerCase();
}

function hashtagOverlap(videoTags, jobTags) {
  if (!jobTags || !jobTags.length) return true; // no filter = pass everything
  if (!videoTags || !videoTags.length) return false;
  const wanted = new Set(jobTags.map(normalizeHashtag).filter(Boolean));
  for (const t of videoTags.map(normalizeHashtag)) {
    if (wanted.has(t)) return true;
  }
  return false;
}

// ---- Apify ----------------------------------------------------------------
/**
 * Normalize one Apify item into our canonical video shape. clockworks/free-tiktok-scraper
 * returns one item per video with fields like webVideoUrl, hashtags[], etc.
 */
function normalizeVideo(item) {
  if (!item) return null;
  const author = item.authorMeta || item.author || {};
  const handle = (author.uniqueId || author.name || item.authorUniqueId || "").toLowerCase();
  if (!handle) return null;
  const videoId = String(item.id || item.videoId || item.itemId || "");
  if (!videoId) return null;

  const hashtags = Array.isArray(item.hashtags)
    ? item.hashtags.map((h) => (typeof h === "string" ? h : h?.name)).filter(Boolean)
    : [];

  return {
    handle,
    video_id: videoId,
    video_url: item.webVideoUrl || item.videoUrl || item.url || null,
    caption: String(item.text || item.desc || "").slice(0, 2000),
    hashtags: hashtags.map(normalizeHashtag).filter(Boolean),
    cover_url: item.videoMeta?.coverUrl || item.covers?.[0] || null,
    duration_s: item.videoMeta?.duration ? Number(item.videoMeta.duration) : null,
    view_count: item.playCount ? Number(item.playCount) : null,
    posted_at: item.createTimeISO ? new Date(item.createTimeISO) : null,
  };
}

async function fetchVideosViaApify(handle, maxVideos) {
  const input = {
    profiles: [`https://www.tiktok.com/@${handle}`],
    resultsPerPage: maxVideos,
    shouldDownloadVideos: false,
    shouldDownloadCovers: false,
    shouldDownloadSubtitles: false,
  };
  if (process.env.APIFY_PROXY_COUNTRY) input.proxyCountryCode = process.env.APIFY_PROXY_COUNTRY;

  const run = await apifyClient().actor(APIFY_VIDEOS_ACTOR).call(input, { waitSecs: APIFY_TIMEOUT_S });
  const { items } = await apify().dataset(run.defaultDatasetId).listItems();
  return items.map(normalizeVideo).filter((v) => v && v.handle === handle).slice(0, maxVideos);
}

// ---- Audio + Whisper -------------------------------------------------------
async function downloadVideo(url, destPath) {
  const ctl = new AbortController();
  const t = setTimeout(() => ctl.abort(), 60_000);
  try {
    const res = await fetch(url, {
      headers: { "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36" },
      redirect: "follow",
      signal: ctl.signal,
    });
    if (!res.ok) throw new Error(`download HTTP ${res.status}`);
    const buf = Buffer.from(await res.arrayBuffer());
    if (buf.length > 50 * 1024 * 1024) throw new Error("video > 50MB, skipping");
    fs.writeFileSync(destPath, buf);
  } finally {
    clearTimeout(t);
  }
}

function extractAudio(videoPath, audioPath, maxSeconds) {
  return new Promise((resolve, reject) => {
    // -ac 1 mono, -ar 16000 Hz (Whisper's preferred), -t cap duration, -y overwrite,
    // -vn drop video, -loglevel error to keep stderr quiet on success.
    const args = ["-y", "-i", videoPath, "-vn", "-ac", "1", "-ar", "16000",
                  "-t", String(maxSeconds), "-loglevel", "error", audioPath];
    const child = execFile("ffmpeg", args, { timeout: 60_000 }, (err, _stdout, stderr) => {
      if (err) return reject(new Error("ffmpeg: " + (stderr || err.message).slice(0, 300)));
      resolve();
    });
    child.on("error", (err) => reject(err));
  });
}

async function whisperTranscribe(audioPath) {
  if (!process.env.OPENAI_API_KEY) throw new Error("OPENAI_API_KEY required for transcripts");

  const audioBuf = fs.readFileSync(audioPath);
  // Use the global FormData / Blob (Node 20+).
  const fd = new FormData();
  fd.append("file", new Blob([audioBuf], { type: "audio/mpeg" }), "audio.mp3");
  fd.append("model", WHISPER_MODEL);
  fd.append("response_format", "json");

  const res = await fetch("https://api.openai.com/v1/audio/transcriptions", {
    method: "POST",
    headers: { Authorization: `Bearer ${process.env.OPENAI_API_KEY}` },
    body: fd,
  });
  if (!res.ok) {
    const t = await res.text();
    throw new Error(`Whisper ${res.status}: ${t.slice(0, 300)}`);
  }
  const j = await res.json();
  return String(j.text || "").trim();
}

// ---- DB queue --------------------------------------------------------------
async function reapStaleClaims() {
  await pool.query(
    `UPDATE job_creators
     SET transcript_state='pending'
     WHERE transcript_state='in_flight'
       AND transcript_claimed_at < now() - ($1 || ' milliseconds')::interval`,
    [String(CLAIM_TIMEOUT_MS)]
  );
}

async function claimOne() {
  const { rows } = await pool.query(
    `
    WITH candidate AS (
      SELECT jc.job_id, jc.handle
      FROM job_creators jc
      JOIN jobs j ON j.id = jc.job_id
      WHERE j.status='running'
        AND j.enable_transcripts = TRUE
        AND jc.transcript_state='pending'
        AND jc.transcript_attempts < $1
        AND jc.scrape_state IN ('done','failed','skipped')
      ORDER BY jc.job_id ASC, jc.handle ASC
      LIMIT 1
      FOR UPDATE OF jc SKIP LOCKED
    )
    UPDATE job_creators jc
    SET transcript_state='in_flight',
        transcript_attempts = jc.transcript_attempts + 1,
        transcript_claimed_at = now()
    FROM candidate
    WHERE jc.job_id=candidate.job_id AND jc.handle=candidate.handle
    RETURNING jc.job_id, jc.handle, jc.transcript_attempts
    `,
    [MAX_ATTEMPTS]
  );
  if (!rows.length) return null;
  const row = rows[0];
  const { rows: jr } = await pool.query(
    `SELECT relevant_hashtags, max_videos_per_creator FROM jobs WHERE id=$1`,
    [row.job_id]
  );
  return { ...row, ...jr[0] };
}

async function releaseOrFail(jobId, handle, attempts, errMsg) {
  await pool.query(
    `UPDATE job_creators
     SET transcript_state = CASE WHEN $3 >= $4 THEN 'failed' ELSE 'pending' END,
         transcript_claimed_at = NULL
     WHERE job_id=$1 AND handle=$2`,
    [jobId, handle, attempts, MAX_ATTEMPTS]
  );
  if (errMsg) console.warn(`[videos] @${handle} job ${jobId}: ${errMsg}`);
}

async function markDone(jobId, handle) {
  await pool.query(
    `UPDATE job_creators SET transcript_state='done', transcript_claimed_at=NULL
     WHERE job_id=$1 AND handle=$2`,
    [jobId, handle]
  );
}

async function getCachedVideos(handle, maxVideos) {
  const { rows } = await pool.query(
    `SELECT * FROM videos
     WHERE handle=$1
       AND ($2::int = 0 OR fetched_at > now() - ($2 || ' days')::interval)
     ORDER BY posted_at DESC NULLS LAST, fetched_at DESC
     LIMIT $3`,
    [handle, VIDEO_REFRESH_DAYS, maxVideos]
  );
  return rows;
}

async function upsertVideo(v) {
  await pool.query(
    `INSERT INTO videos (handle, video_id, video_url, caption, hashtags, cover_url,
                         duration_s, view_count, posted_at, fetched_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,now())
     ON CONFLICT (handle, video_id) DO UPDATE SET
       video_url=EXCLUDED.video_url,
       caption=EXCLUDED.caption,
       hashtags=EXCLUDED.hashtags,
       cover_url=EXCLUDED.cover_url,
       duration_s=EXCLUDED.duration_s,
       view_count=EXCLUDED.view_count,
       posted_at=EXCLUDED.posted_at,
       fetched_at=now()`,
    [v.handle, v.video_id, v.video_url, v.caption, v.hashtags, v.cover_url,
     v.duration_s, v.view_count, v.posted_at]
  );
}

async function storeTranscript(handle, videoId, transcript, durationS) {
  await pool.query(
    `UPDATE videos
     SET transcript=$3,
         transcript_status='done',
         transcript_error=NULL,
         transcribed_at=now(),
         duration_s=COALESCE(duration_s, $4)
     WHERE handle=$1 AND video_id=$2`,
    [handle, videoId, transcript, durationS]
  );
}

async function markVideoSkippedOrFailed(handle, videoId, status, errMsg) {
  await pool.query(
    `UPDATE videos
     SET transcript_status=$3, transcript_error=$4, transcribed_at=now()
     WHERE handle=$1 AND video_id=$2`,
    [handle, videoId, status, errMsg ? errMsg.slice(0, 300) : null]
  );
}

// ---- Main process loop -----------------------------------------------------
async function processOne(claim) {
  const handle = claim.handle;
  const jobId = claim.job_id;
  const maxVideos = claim.max_videos_per_creator || 5;
  const jobTags = claim.relevant_hashtags || [];

  // Step 1: get videos (cached or fresh)
  let videos = await getCachedVideos(handle, maxVideos);
  if (videos.length < maxVideos) {
    // Cache miss or insufficient cache — pull fresh
    let fetched = [];
    try {
      fetched = await fetchVideosViaApify(handle, maxVideos);
    } catch (e) {
      await releaseOrFail(jobId, handle, claim.transcript_attempts, "Apify videos: " + e.message);
      return;
    }
    // Charge for the video listing call
    const cost = await chargeJob(jobId, UNIT.VIDEO_LIST, `video_list @${handle}`);
    if (cost?.paused) {
      await releaseOrFail(jobId, handle, claim.transcript_attempts - 1, "paused by cost cap");
      return;
    }
    for (const v of fetched) {
      try { await upsertVideo(v); } catch (e) { console.warn(`[videos] upsert ${v.video_id}: ${e.message}`); }
    }
    videos = await getCachedVideos(handle, maxVideos);
  }

  if (!videos.length) {
    // No videos for this creator — that's OK, transcript signal is just absent
    await markDone(jobId, handle);
    return;
  }

  // Step 2: filter by hashtag overlap and transcribe missing ones
  for (const v of videos) {
    if (v.transcript_status === "done") continue;
    if (!hashtagOverlap(v.hashtags || [], jobTags)) {
      await markVideoSkippedOrFailed(handle, v.video_id, "skipped", "no hashtag overlap");
      continue;
    }
    if (!v.video_url) {
      await markVideoSkippedOrFailed(handle, v.video_id, "skipped", "no video_url");
      continue;
    }

    const audioPath = path.join(TMP_DIR, `${handle}-${v.video_id}.mp3`);
    const videoPath = path.join(TMP_DIR, `${handle}-${v.video_id}.mp4`);

    try {
      await downloadVideo(v.video_url, videoPath);
      await extractAudio(videoPath, audioPath, AUDIO_MAX_SECONDS);
      const transcript = await whisperTranscribe(audioPath);
      const billedSeconds = Math.min(AUDIO_MAX_SECONDS, v.duration_s || AUDIO_MAX_SECONDS);
      await storeTranscript(handle, v.video_id, transcript, v.duration_s);
      // Charge Whisper by actual capped duration
      const charged = await chargeJob(jobId, UNIT.WHISPER_PER_SECOND * billedSeconds, `whisper ${v.video_id}`);
      if (charged?.paused) {
        // Stop processing the rest of this creator's videos; the worker will
        // exit the loop on next iteration anyway since the job is paused.
        break;
      }
    } catch (e) {
      await markVideoSkippedOrFailed(handle, v.video_id, "failed", e.message);
    } finally {
      // Clean up tmp files even on failure
      try { fs.unlinkSync(videoPath); } catch {}
      try { fs.unlinkSync(audioPath); } catch {}
    }
  }

  await markDone(jobId, handle);
}

async function loop() {
  if (!ENABLED) {
    console.log("[videos] disabled via TRANSCRIPTS_ENABLED=false");
    return;
  }
  if (!process.env.OPENAI_API_KEY) {
    console.warn("[videos] OPENAI_API_KEY not set — transcripts worker idling");
    setTimeout(loop, POLL_MS * 6);
    return;
  }
  if (!process.env.APIFY_TOKEN) {
    console.warn("[videos] APIFY_TOKEN not set — transcripts worker idling");
    setTimeout(loop, POLL_MS * 6);
    return;
  }

  try {
    await reapStaleClaims();
    const claim = await claimOne();
    if (claim) {
      try {
        await processOne(claim);
      } catch (e) {
        console.error("[videos] processOne error:", e);
        await releaseOrFail(claim.job_id, claim.handle, claim.transcript_attempts, e.message);
      }
    }
  } catch (e) {
    console.error("[videos] loop error:", e);
  } finally {
    setTimeout(loop, POLL_MS);
  }
}

function start() {
  console.log(`[videos] starting — actor=${APIFY_VIDEOS_ACTOR} max_audio=${AUDIO_MAX_SECONDS}s model=${WHISPER_MODEL}`);
  loop();
}

module.exports = { start };
