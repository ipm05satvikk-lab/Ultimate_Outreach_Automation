// workers/email_generator.js — Fills personalization into draft outreach
// emails using Claude.
//
// Pipeline:
//   1. Claim outreach_emails where status='draft' AND personalized_content IS NULL.
//   2. Look up the matching template by (campaign_id, stage).
//   3. Look up creator context (bio + transcripts + match cues).
//   4. Call Claude with the template's personalization_instructions to fill
//      the [PERSONALIZED] placeholder.
//   5. Save the rendered subject/body and the standalone personalized_content
//      (so the user can audit what the AI wrote).
//   6. If the template is approved AND the user did not require per-email
//      review, advance status to 'approved' so the sender will pick it up.

const { pool } = require("../db");
const { chargeJob, UNIT } = require("../lib/cost");
const { generateEmail } = require("../lib/email_render");

const POLL_MS = parseInt(process.env.EMAIL_GEN_POLL_MS || "4000", 10);
const MAX_ATTEMPTS = parseInt(process.env.EMAIL_GEN_MAX_ATTEMPTS || "3", 10);
const ENABLED = process.env.EMAIL_GENERATOR_ENABLED !== "false";

async function claimOne() {
  const { rows } = await pool.query(
    `
    WITH candidate AS (
      SELECT oe.id
      FROM outreach_emails oe
      WHERE oe.status='draft'
        AND oe.personalized_content IS NULL
        AND oe.generation_attempts < $1
      ORDER BY oe.id ASC
      LIMIT 1
      FOR UPDATE OF oe SKIP LOCKED
    )
    UPDATE outreach_emails oe
    SET generation_attempts = oe.generation_attempts + 1
    FROM candidate
    WHERE oe.id = candidate.id
    RETURNING oe.*
    `,
    [MAX_ATTEMPTS]
  );
  return rows[0] || null;
}

async function fetchTemplate(campaignId, stage) {
  const { rows } = await pool.query(
    `SELECT t.*, c.status AS campaign_status
     FROM email_templates t
     JOIN email_campaigns c ON c.id = t.campaign_id
     WHERE t.campaign_id=$1 AND t.stage=$2`,
    [campaignId, stage]
  );
  return rows[0] || null;
}

async function processOne(email) {
  const template = await fetchTemplate(email.campaign_id, email.stage);
  if (!template) {
    await pool.query(
      `UPDATE outreach_emails SET status='failed', error='template missing for stage' WHERE id=$1`,
      [email.id]
    );
    return;
  }
  try {
    const generated = await generateEmail(template, email.handle);

    // If the template is approved, the email auto-advances to 'approved' so
    // the sender picks it up. If the template hasn't been approved yet (the
    // user is still in the sample-preview stage for this campaign), keep it
    // as 'draft' so the sample-approval flow can use it.
    const nextStatus = template.approved_at ? "approved" : "draft";
    await pool.query(
      `UPDATE outreach_emails
       SET subject=$1, body=$2, personalized_content=$3, status=$4, error=NULL
       WHERE id=$5`,
      [generated.subject, generated.body, generated.personalized_content, nextStatus, email.id]
    );
    // Email generation has a real Claude cost. We don't have a job_id here
    // (outreach is decoupled from scrape jobs), so for now we just log it.
    // A future enhancement would track per-campaign spend.
    console.log(`[email_gen] @${email.handle} ${email.stage} -> ${nextStatus} (est $${UNIT.EVAL_PER_CREATOR.toFixed(4)})`);
  } catch (e) {
    const message = String(e.message).slice(0, 500);
    const isTerminal = email.generation_attempts + 1 >= MAX_ATTEMPTS;
    await pool.query(
      `UPDATE outreach_emails
       SET status = CASE WHEN $3 THEN 'failed' ELSE 'draft' END,
           error = $1
       WHERE id=$2`,
      [message, email.id, isTerminal]
    );
    console.warn(`[email_gen] @${email.handle} ${email.stage} attempt ${email.generation_attempts + 1}: ${message}`);
  }
}

async function loop() {
  if (!ENABLED) {
    console.log("[email_gen] disabled");
    return;
  }
  if (!process.env.ANTHROPIC_API_KEY) {
    console.warn("[email_gen] ANTHROPIC_API_KEY not set — generator idling");
    setTimeout(loop, POLL_MS * 6);
    return;
  }
  try {
    const email = await claimOne();
    if (email) await processOne(email);
  } catch (e) {
    console.error("[email_gen] loop error:", e);
  } finally {
    setTimeout(loop, POLL_MS);
  }
}

function start() {
  console.log("[email_gen] starting");
  loop();
}

module.exports = { start };
