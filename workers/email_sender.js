// workers/email_sender.js — Sends approved outreach emails.
//
// On every tick:
//   1. Pick one approved email whose scheduled_at <= now() and whose sender
//      still has capacity in today's daily cap.
//   2. Pull the parent email if this is a follow-up (parent_email_id is set),
//      so we can thread via In-Reply-To + References.
//   3. Pull campaign attachments that apply to this stage.
//   4. Hand off to lib/smtp to actually send.
//   5. On success: store message_id, mark sent, increment sender's sent_today,
//      advance the creator_campaigns row (set next_send_at for next stage and
//      pre-create the next stage's draft outreach_email).
//   6. On failure: bump retry_count or mark failed terminally.

const { pool } = require("../db");
const { sendOne } = require("../lib/smtp");

const POLL_MS = parseInt(process.env.EMAIL_SEND_POLL_MS || "5000", 10);
const MAX_RETRIES = parseInt(process.env.EMAIL_SEND_MAX_RETRIES || "3", 10);
const ENABLED = process.env.EMAIL_SENDER_ENABLED !== "false";

const STAGE_ORDER = ["initial", "v1", "v2", "v3", "v4"];

function nextStage(current) {
  const i = STAGE_ORDER.indexOf(current);
  if (i < 0 || i >= STAGE_ORDER.length - 1) return null;
  return STAGE_ORDER[i + 1];
}

function delayForStage(campaign, stage) {
  return {
    v1: campaign.delay_days_v1,
    v2: campaign.delay_days_v2,
    v3: campaign.delay_days_v3,
    v4: campaign.delay_days_v4,
  }[stage] || 0;
}

async function rollDailyCapsIfNeeded() {
  await pool.query(
    `UPDATE email_senders
     SET sent_today = 0, sent_today_resets_at = now() + interval '24 hours'
     WHERE sent_today_resets_at <= now()`
  );
}

async function claimOne() {
  // Atomic claim: pick one email whose sender still has cap, lock the email
  // and the sender row together so concurrent workers don't oversend.
  const { rows } = await pool.query(
    `
    WITH candidate AS (
      SELECT oe.id, oe.sender_id
      FROM outreach_emails oe
      JOIN email_senders s ON s.id = oe.sender_id
      JOIN email_campaigns c ON c.id = oe.campaign_id
      LEFT JOIN creator_campaigns cc ON cc.handle = oe.handle AND cc.campaign_id = oe.campaign_id
      WHERE oe.status='approved'
        AND oe.scheduled_at <= now()
        AND oe.retry_count < $1
        AND s.active = TRUE
        AND s.sent_today < s.daily_cap
        AND c.status IN ('active','ready')
        AND (cc.status IS NULL OR cc.status='active')
      ORDER BY oe.scheduled_at ASC NULLS FIRST, oe.id ASC
      LIMIT 1
      FOR UPDATE OF oe SKIP LOCKED
    )
    UPDATE outreach_emails oe
    SET status='sending'
    FROM candidate
    WHERE oe.id = candidate.id
    RETURNING oe.*
    `,
    [MAX_RETRIES]
  );
  return rows[0] || null;
}

async function loadSender(id) {
  const { rows } = await pool.query(`SELECT * FROM email_senders WHERE id=$1`, [id]);
  return rows[0] || null;
}

async function loadCampaign(id) {
  const { rows } = await pool.query(`SELECT * FROM email_campaigns WHERE id=$1`, [id]);
  return rows[0] || null;
}

async function loadParent(parentId) {
  if (!parentId) return null;
  const { rows } = await pool.query(
    `SELECT message_id, subject FROM outreach_emails WHERE id=$1`,
    [parentId]
  );
  return rows[0] || null;
}

async function loadAttachments(campaignId, stage) {
  const { rows } = await pool.query(
    `SELECT filename, mime_type, content_bytes
     FROM campaign_attachments
     WHERE campaign_id=$1 AND $2 = ANY(apply_to_stages)`,
    [campaignId, stage]
  );
  return rows.map((r) => ({
    filename: r.filename,
    content: r.content_bytes,
    contentType: r.mime_type,
  }));
}

async function scheduleNextStage(campaign, email) {
  const next = nextStage(email.stage);
  if (!next) {
    // We just sent v4. Mark the creator_campaigns row as completed.
    await pool.query(
      `UPDATE creator_campaigns
       SET current_stage='completed', status='completed', next_send_at=NULL, completed_at=now()
       WHERE handle=$1 AND campaign_id=$2`,
      [email.handle, email.campaign_id]
    );
    return;
  }

  const delayDays = delayForStage(campaign, next);
  const sendAt = new Date(Date.now() + delayDays * 24 * 60 * 60 * 1000);

  await pool.query(
    `UPDATE creator_campaigns
     SET current_stage=$1, next_send_at=$2
     WHERE handle=$3 AND campaign_id=$4`,
    [next, sendAt, email.handle, email.campaign_id]
  );

  // Create the draft outreach_email row for the next stage now, scheduled for
  // sendAt. Generator worker will pick it up and personalize it shortly. Root
  // of the thread chain points back to the very first email (the initial).
  const threadRoot = email.thread_root_id || email.id;
  await pool.query(
    `INSERT INTO outreach_emails
       (handle, campaign_id, stage, to_email, subject, body, sender_id,
        status, scheduled_at, parent_email_id, thread_root_id)
     VALUES ($1,$2,$3,$4,'(pending generation)','(pending generation)',$5,
             'draft',$6,$7,$8)`,
    [email.handle, email.campaign_id, next, email.to_email, email.sender_id,
     sendAt, email.id, threadRoot]
  );
}

async function processOne(email) {
  const sender = await loadSender(email.sender_id);
  if (!sender) {
    await pool.query(`UPDATE outreach_emails SET status='failed', error='sender not found' WHERE id=$1`, [email.id]);
    return;
  }
  const campaign = await loadCampaign(email.campaign_id);
  if (!campaign) {
    await pool.query(`UPDATE outreach_emails SET status='failed', error='campaign not found' WHERE id=$1`, [email.id]);
    return;
  }
  const parent = await loadParent(email.parent_email_id);
  const attachments = await loadAttachments(email.campaign_id, email.stage);

  try {
    const result = await sendOne(
      sender,
      { to_email: email.to_email, subject: email.subject, body: email.body },
      parent,
      attachments,
    );

    // Mark sent + record message_id + increment sender's daily counter atomically.
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(
        `UPDATE outreach_emails
         SET status='sent', sent_at=now(), message_id=$1, error=NULL
         WHERE id=$2`,
        [result.messageId, email.id]
      );
      await client.query(
        `UPDATE email_senders SET sent_today = sent_today + 1 WHERE id=$1`,
        [sender.id]
      );
      await client.query("COMMIT");
    } catch (txErr) {
      await client.query("ROLLBACK");
      throw txErr;
    } finally {
      client.release();
    }

    await scheduleNextStage(campaign, email);
    console.log(`[email_send] sent @${email.handle} ${email.stage} → ${email.to_email} (msg-id ${result.messageId})`);
  } catch (e) {
    const message = String(e.message || e).slice(0, 500);
    const retryCount = (email.retry_count || 0) + 1;
    const terminal = retryCount >= MAX_RETRIES;
    await pool.query(
      `UPDATE outreach_emails
       SET status = CASE WHEN $3 THEN 'failed' ELSE 'approved' END,
           retry_count = $2,
           error = $1
       WHERE id=$4`,
      [message, retryCount, terminal, email.id]
    );
    if (terminal) {
      await pool.query(
        `UPDATE creator_campaigns SET status='failed', last_error=$1
         WHERE handle=$2 AND campaign_id=$3`,
        [message, email.handle, email.campaign_id]
      );
    }
    console.warn(`[email_send] @${email.handle} ${email.stage} attempt ${retryCount}: ${message}`);
  }
}

async function loop() {
  if (!ENABLED) {
    console.log("[email_send] disabled");
    return;
  }
  try {
    await rollDailyCapsIfNeeded();
    const email = await claimOne();
    if (email) await processOne(email);
  } catch (e) {
    console.error("[email_send] loop error:", e);
  } finally {
    setTimeout(loop, POLL_MS);
  }
}

function start() {
  console.log("[email_send] starting");
  loop();
}

module.exports = { start };
