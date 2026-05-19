// workers/vision.js — Screenshot-based visual matching.
//
// Ported from Divij's verifier.py. Loads each TikTok profile in headless
// Chromium, dismisses the cookie/login modal, takes up to 2 viewport
// screenshots (initial view + one scroll down), sends them to a vision model
// with the job's description, and stores the per-(handle, job) verdict.
//
// Cost (default config: gpt-4o-mini at low detail, 2 screenshots per creator):
//   ~$0.0008 per creator, ~$8 per 10,000 creators.
//
// Anti-bot patterns ported verbatim:
//   - UA rotation across a small pool
//   - Random viewport sizes / locale / timezone
//   - navigator.webdriver / plugins / window.chrome stubs via init script
//   - Browser recycled every N profiles to avoid cookie/state buildup
//   - 3-8 second jittered delay between profiles
//   - Cookie banner / "not now" / modal-close dismissal before screenshot
//
// Disable entirely with VISION_ENABLED=false. Per-job opt-in via jobs.enable_visual.

const { pool } = require("../db");
const { chargeJob, UNIT } = require("../lib/cost");
const fs = require("fs");
const path = require("path");
const os = require("os");

const ENABLED = process.env.VISION_ENABLED !== "false";
const POLL_MS = parseInt(process.env.VISION_POLL_MS || "5000", 10);
const MAX_ATTEMPTS = parseInt(process.env.VISION_MAX_ATTEMPTS || "3", 10);
const CLAIM_TIMEOUT_MS = parseInt(process.env.VISION_CLAIM_TIMEOUT_MS || "600000", 10);
const RECYCLE_EVERY = parseInt(process.env.VISION_RECYCLE_EVERY || "15", 10);
const DELAY_MIN_MS = parseInt(process.env.VISION_DELAY_MIN_MS || "3000", 10);
const DELAY_MAX_MS = parseInt(process.env.VISION_DELAY_MAX_MS || "8000", 10);
const NAV_TIMEOUT_MS = parseInt(process.env.VISION_NAV_TIMEOUT_MS || "30000", 10);
const SCROLL_COUNT = Math.max(0, Math.min(4, parseInt(process.env.VISION_SCROLL_COUNT || "1", 10)));
const SCREENSHOT_DIR = process.env.VISION_SCREENSHOT_DIR || path.join(os.tmpdir(), "creator-screenshots");

// Vision provider: openai (default, cheap) | anthropic
const PROVIDER = (process.env.VISION_PROVIDER || "openai").toLowerCase();
const OPENAI_MODEL = process.env.VISION_OPENAI_MODEL || "gpt-4o-mini";
const OPENAI_DETAIL = process.env.VISION_OPENAI_DETAIL || "low"; // "low" | "high" | "auto"
const ANTHROPIC_MODEL = process.env.VISION_ANTHROPIC_MODEL || "claude-sonnet-4-6";

const USER_AGENTS = [
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
];

const COOKIE_SELECTORS = [
  'button:has-text("Accept all")',
  'button:has-text("Accept")',
  'button:has-text("Decline")',
  'button:has-text("Not now")',
  'button:has-text("Skip")',
  '[data-e2e="modal-close-inner-button"]',
];

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }
function randInt(min, max) { return min + Math.floor(Math.random() * (max - min + 1)); }
function pick(arr) { return arr[Math.floor(Math.random() * arr.length)]; }

fs.mkdirSync(SCREENSHOT_DIR, { recursive: true });

// ---- Prompt building -------------------------------------------------------
function buildVisionPrompt(description) {
  return `You are analyzing TikTok profile screenshots to decide whether the creator matches a target description.

TARGET DESCRIPTION:
"""
${description}
"""

Look at every screenshot carefully. Identify SPECIFIC visual signals you see (clothing, body, setting, props, on-screen text in the page) that support or refute the description.

Examples of visual signals:
  - Clothing: scrubs, lab coat, suit, hijab, gym wear, uniform
  - Body / appearance: bald, visibly pregnant, beard, tattoos, age range, gender presentation
  - Setting: hospital, clinic, pharmacy, gym, kitchen, outdoors
  - Props: stethoscope, lab equipment, dumbbells, baby items, prescription bottles
  - On-screen text in bio area: credentials (RN, MD, PharmD), specialty, location

Respond with ONLY this JSON object, no markdown, no preamble:
{
  "is_match": true | false,
  "confidence": "high" | "medium" | "low",
  "cues": ["short specific signal 1", "short specific signal 2"],
  "likely_role": "free-text label or 'unknown'",
  "reasoning": "one short sentence"
}

Mark is_match=true only when at least one cue is concrete and visible in a screenshot. Vague vibes do not count.`;
}

// ---- Vision providers ------------------------------------------------------
async function analyzeWithOpenAI(imagesB64, description) {
  if (!process.env.OPENAI_API_KEY) {
    throw new Error("OPENAI_API_KEY not set (required for VISION_PROVIDER=openai)");
  }
  const content = imagesB64.map((b64) => ({
    type: "image_url",
    image_url: { url: `data:image/png;base64,${b64}`, detail: OPENAI_DETAIL },
  }));
  content.push({ type: "text", text: buildVisionPrompt(description) });

  const res = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
    },
    body: JSON.stringify({
      model: OPENAI_MODEL,
      max_tokens: 500,
      messages: [{ role: "user", content }],
    }),
  });
  if (!res.ok) {
    const t = await res.text();
    const err = new Error(`OpenAI ${res.status}: ${t.slice(0, 300)}`);
    err.status = res.status;
    throw err;
  }
  const data = await res.json();
  const text = data?.choices?.[0]?.message?.content || "";
  return { text, model: OPENAI_MODEL };
}

async function analyzeWithAnthropic(imagesB64, description) {
  if (!process.env.ANTHROPIC_API_KEY) {
    throw new Error("ANTHROPIC_API_KEY not set (required for VISION_PROVIDER=anthropic)");
  }
  const content = imagesB64.map((b64) => ({
    type: "image",
    source: { type: "base64", media_type: "image/png", data: b64 },
  }));
  content.push({ type: "text", text: buildVisionPrompt(description) });

  const res = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-api-key": process.env.ANTHROPIC_API_KEY,
      "anthropic-version": "2023-06-01",
    },
    body: JSON.stringify({
      model: ANTHROPIC_MODEL,
      max_tokens: 600,
      messages: [{ role: "user", content }],
    }),
  });
  if (!res.ok) {
    const t = await res.text();
    const err = new Error(`Anthropic ${res.status}: ${t.slice(0, 300)}`);
    err.status = res.status;
    throw err;
  }
  const data = await res.json();
  const text = (data?.content || [])
    .filter((b) => b.type === "text")
    .map((b) => b.text)
    .join("\n");
  return { text, model: ANTHROPIC_MODEL };
}

async function callVision(imagesB64, description) {
  if (PROVIDER === "anthropic") return analyzeWithAnthropic(imagesB64, description);
  return analyzeWithOpenAI(imagesB64, description);
}

function parseVisionJson(text) {
  let s = String(text || "").trim();
  if (s.startsWith("```")) s = s.replace(/^```(?:json)?/i, "").replace(/```\s*$/i, "").trim();
  try { return JSON.parse(s); } catch {}
  const m = s.match(/\{[\s\S]*\}/);
  if (m) { try { return JSON.parse(m[0]); } catch {} }
  return null;
}

// ---- Playwright session ----------------------------------------------------
let playwright = null;
function loadPlaywright() {
  if (!playwright) {
    try {
      playwright = require("playwright");
    } catch (e) {
      throw new Error("playwright package not installed. Run: npm install playwright && npx playwright install chromium");
    }
  }
  return playwright;
}

async function makeBrowser() {
  const { chromium } = loadPlaywright();
  const browser = await chromium.launch({
    headless: true,
    args: [
      "--disable-blink-features=AutomationControlled",
      "--no-sandbox",
      "--disable-dev-shm-usage",
    ],
  });
  const context = await browser.newContext({
    viewport: { width: pick([1280, 1366, 1440]), height: pick([800, 900, 960]) },
    userAgent: pick(USER_AGENTS),
    locale: pick(["en-US", "en-GB"]),
    timezoneId: pick(["America/New_York", "America/Chicago", "America/Los_Angeles"]),
  });
  await context.addInitScript(() => {
    Object.defineProperty(navigator, "webdriver", { get: () => undefined });
    Object.defineProperty(navigator, "plugins", { get: () => [1, 2, 3, 4, 5] });
    // eslint-disable-next-line no-undef
    window.chrome = { runtime: {} };
  });
  const page = await context.newPage();
  return { browser, context, page };
}

async function dismissBanners(page) {
  for (const sel of COOKIE_SELECTORS) {
    try {
      const btn = page.locator(sel).first();
      if (await btn.isVisible({ timeout: 800 })) {
        await btn.click({ timeout: 1500 });
        await page.waitForTimeout(400);
      }
    } catch {}
  }
}

/**
 * Load profile and return up to (1 + SCROLL_COUNT) screenshots as Buffers,
 * plus the on-disk paths (for debug). Throws if the profile cannot be loaded.
 */
async function captureProfile(page, handle) {
  const clean = handle.replace(/^@+/, "");
  const url = `https://www.tiktok.com/@${clean}`;

  let lastErr;
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      await page.goto(url, { waitUntil: "networkidle", timeout: NAV_TIMEOUT_MS });
      await page.waitForTimeout(3000);
      await dismissBanners(page);
      await page.evaluate(() => window.scrollTo(0, 0));
      await page.waitForTimeout(400);

      const buffers = [];
      const paths = [];

      // Initial viewport
      const p0 = path.join(SCREENSHOT_DIR, `${clean}-0.png`);
      const b0 = await page.screenshot({ path: p0, fullPage: false });
      buffers.push(b0);
      paths.push(p0);

      // Scroll(s) and capture more
      for (let s = 1; s <= SCROLL_COUNT; s++) {
        await page.evaluate((step) => window.scrollBy(0, window.innerHeight * step), 1);
        await page.waitForTimeout(700);
        const pN = path.join(SCREENSHOT_DIR, `${clean}-${s}.png`);
        const bN = await page.screenshot({ path: pN, fullPage: false });
        buffers.push(bN);
        paths.push(pN);
      }

      return { buffers, paths };
    } catch (e) {
      lastErr = e;
      if (attempt < 3) await page.waitForTimeout(2000);
    }
  }
  throw lastErr || new Error("screenshot capture failed");
}

// ---- DB queue --------------------------------------------------------------
async function reapStaleClaims() {
  await pool.query(
    `UPDATE job_creators
     SET visual_state='pending'
     WHERE visual_state='in_flight'
       AND visual_claimed_at < now() - ($1 || ' milliseconds')::interval`,
    [String(CLAIM_TIMEOUT_MS)]
  );
}

/**
 * Claim one creator at a time so we don't hold a long DB transaction during
 * the (slow) browser navigation. Each call returns 0 or 1 rows.
 */
async function claimOne() {
  const { rows } = await pool.query(
    `
    WITH candidate AS (
      SELECT jc.job_id, jc.handle
      FROM job_creators jc
      JOIN jobs j ON j.id = jc.job_id
      WHERE j.status='running'
        AND j.enable_visual = TRUE
        AND jc.visual_state='pending'
        AND jc.visual_attempts < $1
        AND jc.scrape_state IN ('done','failed','skipped')
      ORDER BY jc.job_id ASC, jc.handle ASC
      LIMIT 1
      FOR UPDATE OF jc SKIP LOCKED
    )
    UPDATE job_creators jc
    SET visual_state='in_flight',
        visual_attempts = jc.visual_attempts + 1,
        visual_claimed_at = now()
    FROM candidate
    WHERE jc.job_id=candidate.job_id AND jc.handle=candidate.handle
    RETURNING jc.job_id, jc.handle, jc.visual_attempts
    `,
    [MAX_ATTEMPTS]
  );
  if (!rows.length) return null;
  const row = rows[0];
  const { rows: jr } = await pool.query(`SELECT description FROM jobs WHERE id=$1`, [row.job_id]);
  return { ...row, description: jr[0]?.description };
}

async function releaseOrFail(jobId, handle, attempts, errMsg) {
  await pool.query(
    `UPDATE job_creators
     SET visual_state = CASE WHEN $3 >= $4 THEN 'failed' ELSE 'pending' END,
         visual_claimed_at = NULL
     WHERE job_id=$1 AND handle=$2`,
    [jobId, handle, attempts, MAX_ATTEMPTS]
  );
  if (errMsg) {
    await pool.query(
      `INSERT INTO creator_visual_analyses (handle, job_id, matched, confidence, reason, model)
       VALUES ($1, $2, NULL, 'error', $3, $4)
       ON CONFLICT (handle, job_id) DO UPDATE SET reason=EXCLUDED.reason, analyzed_at=now()`,
      [handle, jobId, String(errMsg).slice(0, 500), PROVIDER]
    );
  }
}

async function storeVerdict(jobId, handle, verdict, screenshotPaths, model, raw) {
  await pool.query(
    `
    INSERT INTO creator_visual_analyses
      (handle, job_id, matched, confidence, likely_role, cues, reason, screenshot_paths, model, raw_response, analyzed_at)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,now())
    ON CONFLICT (handle, job_id) DO UPDATE SET
      matched=EXCLUDED.matched,
      confidence=EXCLUDED.confidence,
      likely_role=EXCLUDED.likely_role,
      cues=EXCLUDED.cues,
      reason=EXCLUDED.reason,
      screenshot_paths=EXCLUDED.screenshot_paths,
      model=EXCLUDED.model,
      raw_response=EXCLUDED.raw_response,
      analyzed_at=now()
    `,
    [
      handle, jobId,
      verdict.matched, verdict.confidence, verdict.likely_role,
      verdict.cues, verdict.reason,
      screenshotPaths, model,
      raw ? JSON.stringify(raw) : null,
    ]
  );
  await pool.query(
    `UPDATE job_creators SET visual_state='done', visual_claimed_at=NULL WHERE job_id=$1 AND handle=$2`,
    [jobId, handle]
  );
  // Charge after a successful vision verdict.
  await chargeJob(jobId, UNIT.VISION_PER_CREATOR, `vision @${handle}`);
}

// ---- Main loop -------------------------------------------------------------
async function loop() {
  if (!ENABLED) {
    console.log("[vision] disabled via VISION_ENABLED=false");
    return;
  }
  if (PROVIDER === "openai" && !process.env.OPENAI_API_KEY) {
    console.warn("[vision] OPENAI_API_KEY not set — vision worker idling (set OPENAI_API_KEY or VISION_PROVIDER=anthropic)");
    setTimeout(loop, POLL_MS * 6);
    return;
  }
  if (PROVIDER === "anthropic" && !process.env.ANTHROPIC_API_KEY) {
    console.warn("[vision] ANTHROPIC_API_KEY not set — vision worker idling");
    setTimeout(loop, POLL_MS * 6);
    return;
  }

  let browser, context, page;
  let counter = 0;

  async function ensureBrowser() {
    if (!browser || counter >= RECYCLE_EVERY) {
      if (browser) { try { await browser.close(); } catch {} }
      ({ browser, context, page } = await makeBrowser());
      counter = 0;
    }
  }

  try {
    await reapStaleClaims();

    while (true) {
      const claim = await claimOne();
      if (!claim) break;

      try {
        await ensureBrowser();
      } catch (e) {
        console.error("[vision] failed to start Chromium:", e.message);
        await releaseOrFail(claim.job_id, claim.handle, claim.visual_attempts, "chromium boot failed: " + e.message);
        break;
      }

      try {
        const { buffers, paths } = await captureProfile(page, claim.handle);
        counter++;

        const imagesB64 = buffers.map((b) => b.toString("base64"));
        let rawText = "", model = PROVIDER;
        try {
          const r = await callVision(imagesB64, claim.description);
          rawText = r.text;
          model = r.model;
        } catch (e) {
          await releaseOrFail(claim.job_id, claim.handle, claim.visual_attempts, "vision API: " + e.message);
          continue;
        }

        const parsed = parseVisionJson(rawText);
        if (!parsed) {
          await releaseOrFail(claim.job_id, claim.handle, claim.visual_attempts, "vision returned unparseable JSON");
          continue;
        }

        const verdict = {
          matched: parsed.is_match === true,
          confidence: String(parsed.confidence || "").slice(0, 20),
          likely_role: String(parsed.likely_role || "").slice(0, 100),
          cues: Array.isArray(parsed.cues) ? parsed.cues.map((c) => String(c).slice(0, 80)).slice(0, 20) : [],
          reason: String(parsed.reasoning || "").slice(0, 500),
        };
        await storeVerdict(claim.job_id, claim.handle, verdict, paths, model, parsed);

        // Jittered delay between profiles
        await sleep(randInt(DELAY_MIN_MS, DELAY_MAX_MS));
      } catch (e) {
        console.error(`[vision] capture failed for @${claim.handle}:`, e.message);
        await releaseOrFail(claim.job_id, claim.handle, claim.visual_attempts, e.message);
        // If the browser is wedged, force a recycle next iteration
        counter = RECYCLE_EVERY;
      }
    }
  } catch (e) {
    console.error("[vision] loop error:", e);
  } finally {
    if (browser) { try { await browser.close(); } catch {} }
    setTimeout(loop, POLL_MS);
  }
}

function start() {
  console.log(`[vision] starting — provider=${PROVIDER} screenshots=${1 + SCROLL_COUNT} recycle=${RECYCLE_EVERY} delay=${DELAY_MIN_MS}-${DELAY_MAX_MS}ms`);
  loop();
}

module.exports = { start };
