// lib/email_render.js — Build per-email personalization via Claude.
//
// A template body contains one or more [PERSONALIZED] placeholders. The
// personalization_instructions field on the template tells Claude what to
// fill into them (e.g. "Write 2-3 sentences referencing their content,
// connecting their niche to AshwaMag's stress/sleep angle.").
//
// This module fetches everything we know about a creator (bio, transcripts,
// match cues, visual signals), builds a prompt, gets Claude's reply, and
// substitutes the result into the template body. Returns { subject, body,
// personalized_content } ready to insert into outreach_emails.

const { pool } = require("../db");

const ANTHROPIC_MODEL = process.env.ANTHROPIC_MODEL || "claude-sonnet-4-6";
const MAX_TRANSCRIPTS_IN_PROMPT = parseInt(process.env.EMAIL_GEN_MAX_TRANSCRIPTS || "3", 10);
const PLACEHOLDER_RE = /\[PERSONALIZED\]/gi;

/**
 * Pull everything we know about this creator that's useful for personalization.
 * Returns null if the creator isn't in our DB (shouldn't happen since
 * assignment requires a successful scrape).
 */
async function fetchCreatorContext(handle) {
  const { rows: cr } = await pool.query(
    `SELECT c.handle, c.nickname, c.bio, c.region, c.follower_count, c.video_count,
            array_agg(DISTINCT ct.value) FILTER (WHERE ct.kind='link')  AS links,
            array_agg(DISTINCT ct.value) FILTER (WHERE ct.kind='email') AS emails
     FROM creators c
     LEFT JOIN contacts ct ON ct.handle = c.handle
     WHERE c.handle = $1
     GROUP BY c.handle`,
    [handle]
  );
  if (!cr.length) return null;
  const creator = cr[0];

  const { rows: tr } = await pool.query(
    `SELECT caption, hashtags, transcript, posted_at
     FROM videos
     WHERE handle=$1 AND transcript_status='done' AND transcript IS NOT NULL
     ORDER BY posted_at DESC NULLS LAST
     LIMIT $2`,
    [handle, MAX_TRANSCRIPTS_IN_PROMPT]
  );

  // Pull match signals from any prior eval, picking the most recent one with
  // positive matches (so we have cues / source attributions to lean on).
  const { rows: sig } = await pool.query(
    `SELECT match_signals, match_sources, match_cues, reason
     FROM job_creators
     WHERE handle=$1 AND matched IS TRUE
     ORDER BY evaluated_at DESC NULLS LAST
     LIMIT 1`,
    [handle]
  );
  const signals = sig[0] || null;

  return {
    handle,
    nickname: creator.nickname,
    bio: creator.bio,
    region: creator.region,
    follower_count: creator.follower_count,
    video_count: creator.video_count,
    links: creator.links || [],
    emails: creator.emails || [],
    transcripts: tr,
    signals,
  };
}

/**
 * Build the prompt for Claude. The instructions field steers what Claude
 * generates; the creator data is dumped in a stable structured format below.
 */
function buildPrompt(template, creator) {
  const transcripts = (creator.transcripts || []).map((t, i) => {
    const hashtags = (t.hashtags || []).join(", ");
    const cap = (t.caption || "").slice(0, 200);
    const txt = (t.transcript || "").slice(0, 800);
    return `[${i + 1}] caption: ${cap}\n    hashtags: ${hashtags}\n    transcript: ${txt}`;
  }).join("\n\n");

  const cues = creator.signals?.match_cues || [];
  const sources = creator.signals?.match_sources || [];
  const reason = creator.signals?.reason || "";

  return `You are personalizing an outreach email for a TikTok creator. Generate ONLY the personalized portion that will replace the [PERSONALIZED] placeholder in the template below. Do not include the rest of the template, do not add a greeting or signature, do not wrap the response in quotes.

PERSONALIZATION INSTRUCTIONS (this is the most important part — follow it exactly):
"""
${template.personalization_instructions || "Write 1-2 short sentences referencing their content and connecting it to our product."}
"""

CREATOR CONTEXT:
handle: @${creator.handle}
nickname: ${creator.nickname || "(none)"}
followers: ${creator.follower_count || "?"}
region: ${creator.region || "?"}
bio: ${creator.bio ? creator.bio.replace(/\s+/g, " ").slice(0, 500) : "(empty)"}

WHY THIS CREATOR MATCHED OUR TARGET (from earlier evaluation):
match cues: ${cues.length ? cues.join(", ") : "(none recorded)"}
matched on: ${sources.length ? sources.join(", ") : "(unknown)"}
reason: ${reason || "(none)"}

RECENT VIDEO TRANSCRIPTS (what they actually talk about):
${transcripts || "(no transcripts available — work from bio + cues only)"}

THE FULL EMAIL TEMPLATE FOR CONTEXT (so you know what the personalized portion needs to flow into — do NOT regenerate any of this, only fill the placeholder):
"""
${template.body}
"""

Return ONLY the text that replaces [PERSONALIZED]. Match the voice and tone of the surrounding template. Be specific — vague compliments ("love your content!") are forbidden. Reference one concrete thing they post about.`;
}

/**
 * Run the personalization. Returns the text Claude generated, ready to splice
 * into the template body.
 */
async function personalize(template, creator) {
  if (!process.env.ANTHROPIC_API_KEY) throw new Error("ANTHROPIC_API_KEY required");
  const Anthropic = require("@anthropic-ai/sdk");
  const client = new Anthropic.default({ apiKey: process.env.ANTHROPIC_API_KEY });

  const prompt = buildPrompt(template, creator);
  const resp = await client.messages.create({
    model: ANTHROPIC_MODEL,
    max_tokens: 600,
    messages: [{ role: "user", content: prompt }],
  });

  let text = resp.content.filter((b) => b.type === "text").map((b) => b.text).join("").trim();
  // Strip any wrapping quotes Claude might add despite instructions.
  text = text.replace(/^["']|["']$/g, "").trim();
  // Strip any "[PERSONALIZED]" the model may have echoed.
  text = text.replace(PLACEHOLDER_RE, "").trim();
  return text;
}

/**
 * Substitute the AI-generated personalized content into the template body,
 * also expanding a few non-AI placeholders like {{name}} for nickname or
 * {{handle}}.
 */
function renderBody(templateBody, personalizedContent, creator) {
  let out = templateBody;
  out = out.replace(PLACEHOLDER_RE, personalizedContent || "");
  out = out.replace(/\{\{\s*name\s*\}\}/gi, creator.nickname || creator.handle);
  out = out.replace(/\{\{\s*handle\s*\}\}/gi, creator.handle);
  out = out.replace(/\{\{\s*first_name\s*\}\}/gi, (creator.nickname || creator.handle).split(/\s+/)[0]);
  return out;
}

function renderSubject(templateSubject, creator) {
  let out = templateSubject || "";
  out = out.replace(/\{\{\s*name\s*\}\}/gi, creator.nickname || creator.handle);
  out = out.replace(/\{\{\s*handle\s*\}\}/gi, creator.handle);
  out = out.replace(/\{\{\s*first_name\s*\}\}/gi, (creator.nickname || creator.handle).split(/\s+/)[0]);
  return out;
}

/**
 * High-level: generate + render. Returns { subject, body, personalized_content }.
 */
async function generateEmail(template, handle) {
  const creator = await fetchCreatorContext(handle);
  if (!creator) throw new Error(`creator @${handle} not found in DB`);

  // Only call Claude if the template actually has a placeholder. Otherwise
  // skip the cost — some stages (e.g. v3, v4) may be fully boilerplate.
  let personalizedContent = "";
  if (PLACEHOLDER_RE.test(template.body)) {
    personalizedContent = await personalize(template, creator);
  }
  // Reset regex state since we used .test with /g
  PLACEHOLDER_RE.lastIndex = 0;

  const body = renderBody(template.body, personalizedContent, creator);
  const subject = renderSubject(template.subject, creator);
  return { subject, body, personalized_content: personalizedContent };
}

module.exports = { fetchCreatorContext, personalize, renderBody, renderSubject, generateEmail };
